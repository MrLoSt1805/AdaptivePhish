"""Reusable, safety-checked HTTP range reader for remote Parquet files.

Future authorized use: call run_poc() explicitly. The 50,000,000-byte limit
counts response bodies returned to Python, including known partial reads.
An exception without a partial payload makes accounting unverifiable: the
reader stops and reports this. It is NOT a hard network cap: headers, TLS,
socket buffering and bytes sent but not consumed are outside this counter.
No automatic retries or prefetch.
Only a single pinned shard and the first row group are supported. Footer
statistics can contain sensitive values; never print metadata or exceptions.
"""

import collections
import contextlib
import datetime
import http.client
import io
import json
import struct
import time
from urllib.parse import urljoin, urlsplit

import pyarrow as pa
import pyarrow.parquet as pq

URL = ('https://huggingface.co/datasets/phreshphish/phreshphish/resolve/'
       'eca603e0ecd4f698f8d4d226c088659e72681239/data/train-001.parquet')
SIZE = 322_018_201
BUDGET = 50_000_000
COLUMNS = ('sha256', 'url', 'label', 'date')
MAX_FOOTER = 4_000_000
OVERHEAD = 65_536


class SafetyError(Exception):
    """Fixed, sanitized failure; never includes data or remote error text."""

    def __init__(self, category):
        self.category = category
        super().__init__(category)


def safe_url(url):
    p = urlsplit(url)
    host = p.hostname or ''
    if (p.scheme != 'https' or p.username or p.password or p.port not in (None, 443)
            or not (host == 'huggingface.co' or host.endswith('.huggingface.co')
                    or host.endswith('.hf.co'))):
        raise SafetyError('Unapproved transport destination')
    return p


@contextlib.contextmanager
def https_response(url, headers):
    """One GET, no proxy discovery, redirect following, or automatic retries."""
    p = safe_url(url)
    conn = http.client.HTTPSConnection(p.hostname, timeout=30)
    try:
        path = p.path + (('?' + p.query) if p.query else '')
        conn.request('GET', path, headers=headers)
        response = conn.getresponse()
        try:
            yield response
        finally:
            response.close()
    finally:
        conn.close()


class RangeFile(io.RawIOBase):
    """Read-only seekable file, with exact approved ranges and in-memory cache.

    Single-threaded only. Rejected/error/redirect bodies are never consumed.
    Every consumed body byte is counted immediately, even if a range fails.
    After any transport/validation error the reader is permanently poisoned.
    """

    def __init__(self, url=URL, size=SIZE, budget=BUDGET, transport=https_response,
                 request_limit=32):
        super().__init__()
        safe_url(url)
        if (size < 12 or not 0 < budget <= BUDGET
                or type(request_limit) is not int or not 1 <= request_limit <= 256):
            raise SafetyError('Invalid limits')
        self.url, self.size, self.budget = url, size, budget
        self.transport = transport
        self.request_limit = request_limit
        self.body_bytes = self.request_count = self.pos = 0
        self.failed = False
        self.accounting_complete = True
        self.violation_category = None
        self.allowed = []
        self.cache = []
        self.ranges = []  # Numeric intervals only; never data or signed URLs.
        self.plan = None
        self.deadline = None
        self.clock = time.monotonic

    @property
    def remaining(self):
        return self.budget - self.body_bytes

    def readable(self):
        return True

    def seekable(self):
        return True

    def writable(self):
        return False

    def write(self, data):
        raise io.UnsupportedOperation('Read-only file')

    def tell(self):
        self._checkClosed()
        return self.pos

    def seek(self, offset, whence=0):
        self._checkClosed()
        if whence not in (0, 1, 2):
            raise SafetyError('Invalid seek')
        target = offset + (0 if whence == 0 else self.pos if whence == 1 else self.size)
        if target < 0 or target > self.size:
            raise SafetyError('Seek outside file')
        self.pos = target
        return target

    @staticmethod
    def header(response, name, required=False):
        values = [v for k, v in response.getheaders() if k.lower() == name.lower()]
        if len(values) > 1 or (required and not values):
            raise SafetyError('Missing or ambiguous HTTP header')
        return values[0].strip() if values else None

    def fetch(self, start, end):
        """Fetch [start, end), refusing all bytes outside approved intervals."""
        self._checkClosed()
        if self.failed:
            raise SafetyError('Reader stopped after failure')
        if not (0 <= start < end <= self.size) or not any(
                a <= start and end <= b for a, b in self.allowed):
            self.violation_category = 'unapproved_range'
            raise SafetyError('unapproved_range')
        for a, b, data in self.cache:
            if a <= start and end <= b:
                return data[start-a:end-a]
        length = end - start
        if length > self.remaining:
            raise SafetyError('budget_precheck')
        url = self.url
        try:
            for redirect in range(6):
                safe_url(url)
                if self.deadline is not None and self.clock() >= self.deadline:
                    raise SafetyError('Runtime limit exceeded')
                if self.request_count >= self.request_limit:
                    raise SafetyError('Request count limit')
                self.request_count += 1
                with self.transport(url, {'Range': f'bytes={start}-{end-1}',
                                         'Accept-Encoding': 'identity'}) as response:
                    if response.status in (301, 302, 303, 307, 308):
                        if redirect == 5:
                            raise SafetyError('Redirect limit')
                        url = urljoin(url, self.header(response, 'Location', True))
                        continue  # Close without reading the redirect body.
                    if response.status != 206:
                        raise SafetyError('Expected HTTP 206')
                    if self.header(response, 'Content-Range', True) != f'bytes {start}-{end-1}/{self.size}':
                        raise SafetyError('Content-Range mismatch')
                    if self.header(response, 'Content-Length', True) != str(length):
                        raise SafetyError('Content-Length mismatch')
                    if self.header(response, 'Content-Encoding') not in (None, 'identity'):
                        raise SafetyError('Unexpected encoding')
                    if self.header(response, 'Transfer-Encoding') is not None:
                        raise SafetyError('Unexpected transfer encoding')
                    chunks, consumed = [], 0
                    while consumed < length:
                        count = min(65_536, length-consumed, self.remaining)
                        try:
                            part = response.read(count)
                        except http.client.IncompleteRead as exc:
                            self.body_bytes += len(exc.partial)
                            raise SafetyError('incomplete_response') from None
                        except Exception:
                            # A socket/transport may have consumed bytes before
                            # raising without exposing them. Do not pretend that
                            # the counter is complete, and never retry.
                            self.accounting_complete = False
                            raise SafetyError('transport_unmetered') from None
                        self.body_bytes += len(part)
                        if len(part) > count or self.body_bytes > self.budget:
                            raise SafetyError('overlong_response')
                        if not part:
                            raise SafetyError('incomplete_response')
                        consumed += len(part)
                        chunks.append(part)
                    data = b''.join(chunks)
                    self.cache.append((start, end, data))
                    self.ranges.append((start, end))
                    return data
            raise SafetyError('Redirect limit')
        except SafetyError as exc:
            self.failed = True
            raise SafetyError(exc.category) from None
        except Exception:
            self.failed = True
            raise SafetyError('transport_error') from None

    def read(self, size=-1):
        self._checkClosed()
        if size is None or size < 0:
            raise SafetyError('Unbounded reads disabled')
        end = min(self.size, self.pos + size)
        if end == self.pos:
            return b''
        data = self.fetch(self.pos, end)
        self.pos = end
        return data

    def readinto(self, buffer):
        data = self.read(len(buffer))
        buffer[:len(data)] = data
        return len(data)

    def close(self):
        self.cache.clear()
        super().close()


def inspect_footer(reader):
    """Read exact footer bytes, never Arrow's speculative tail window."""
    n = reader.size
    reader.allowed = [(0, 4), (n-8, n)]
    if reader.fetch(0, 4) != b'PAR1':
        raise SafetyError('Invalid Parquet header')
    trailer = reader.fetch(n-8, n)
    length = struct.unpack('<I', trailer[:4])[0]
    if trailer[4:] != b'PAR1' or not 0 < length <= min(MAX_FOOTER, n-12):
        raise SafetyError('Invalid or oversized footer')
    start = n-8-length
    reader.allowed.append((start, n-8))
    footer = reader.fetch(start, n-8)
    # Parse locally. Column offsets in metadata still refer to the original file.
    metadata = pq.read_metadata(pa.BufferReader(b'PAR1' + footer + trailer))
    return metadata, start


def plan_first_group(reader, metadata, footer_start):
    """Validate all chunk boundaries and budget before fetching any column."""
    if metadata.num_row_groups < 1 or metadata.row_group(0).num_rows < 1:
        raise SafetyError('Empty Parquet file')
    selected, all_chunks = {}, []
    for group_index in range(metadata.num_row_groups):
        group = metadata.row_group(group_index)
        for i in range(group.num_columns):
            column = group.column(i)
            if column.file_path:
                raise SafetyError('External column file forbidden')
            offsets = [column.data_page_offset]
            if column.has_dictionary_page:
                offsets.append(column.dictionary_page_offset)
            if any(x is None or x < 4 for x in offsets):
                raise SafetyError('Invalid column offset')
            start = min(offsets)
            end = start + column.total_compressed_size
            if not 4 <= start < end <= footer_start:
                raise SafetyError('Invalid column bounds')
            all_chunks.append((start, end))
            if group_index == 0 and column.path_in_schema in COLUMNS:
                if column.path_in_schema in selected:
                    raise SafetyError('Duplicate column')
                selected[column.path_in_schema] = (start, end)
    ordered = sorted(all_chunks)
    if any(a[1] > b[0] for a, b in zip(ordered, ordered[1:])):
        raise SafetyError('Overlapping column chunks')
    if set(selected) != set(COLUMNS):
        raise SafetyError('Required columns absent')
    # The generic overlap check above includes every column in every row
    # group. Keep this explicit invariant close to the authorization step.
    html_chunks = []
    for group_index in range(metadata.num_row_groups):
        group = metadata.row_group(group_index)
        for i in range(group.num_columns):
            column = group.column(i)
            if column.path_in_schema == 'html':
                offsets = [column.data_page_offset]
                if column.has_dictionary_page:
                    offsets.append(column.dictionary_page_offset)
                start = min(offsets)
                html_chunks.append((start, start + column.total_compressed_size))
    if any(a < d and c < b for a, b in selected.values() for c, d in html_chunks):
        raise SafetyError('html_overlap')
    sizes = {name: b-a for name, (a, b) in selected.items()}
    estimate = sum(sizes.values()) + OVERHEAD
    reader.plan = {'compressed_bytes': sizes, 'overhead_bytes': OVERHEAD,
                   'estimated_bytes': estimate, 'remaining_bytes': reader.remaining}
    if estimate > reader.remaining:
        raise SafetyError('Selected chunks exceed remaining budget')
    # Only these chunks can subsequently be fetched. HTML is never authorized.
    reader.allowed.extend(selected.values())
    return selected


def sample(reader):
    metadata, footer_start = inspect_footer(reader)
    selected = plan_first_group(reader, metadata, footer_start)
    # Exact bounded chunk reads, not uncontrolled library read-ahead. Loading
    # whole selected chunks may cover many rows, but only <=10 are materialized
    # in the returned batch. Compressed pages may decode internally beyond 10.
    for name in COLUMNS:
        reader.fetch(*selected[name])
    parquet = pq.ParquetFile(reader, metadata=metadata, pre_buffer=False,
                            buffer_size=0, arrow_extensions_enabled=False)
    batches = parquet.iter_batches(batch_size=10, row_groups=[0],
                                   columns=list(COLUMNS), use_threads=False)
    try:
        batch = next(batches)
        if batch.num_rows > 10 or batch.schema.names != list(COLUMNS):
            raise SafetyError('Unexpected output shape')
        labels = batch.column('label').to_pylist()
        dates = batch.column('date').to_pylist()
        if any(x not in ('benign', 'phish') for x in labels):
            raise SafetyError('Unexpected label')
        if any(type(x) is not datetime.date for x in dates):
            raise SafetyError('Unexpected date')
        return {'row_count': batch.num_rows, 'selected_columns': list(COLUMNS),
                'labels': dict(collections.Counter(labels)),
                'date_range': [min(dates).isoformat(), max(dates).isoformat()]}
    finally:
        batches.close()
        parquet.close()


def run_poc(reader=None):
    """Future authorized entry point; failure text/data are never printed."""
    reader = reader if reader is not None else RangeFile()
    result = {'row_count': 0, 'selected_columns': list(COLUMNS),
              'labels': {}, 'date_range': None, 'success': False,
              'failure_category': None}
    try:
        result.update(sample(reader))
        result['success'] = True
    except SafetyError as exc:
        result['failure_category'] = _failure_category(exc.category)
    except Exception:
        # Arrow or another dependency may embed sample data in exception text.
        result['failure_category'] = reader.violation_category or 'parquet_read_error'
    finally:
        result.update(http_request_count=reader.request_count,
                      application_response_body_bytes=reader.body_bytes,
                      application_body_accounting_complete=reader.accounting_complete,
                      application_budget_exceeded=reader.body_bytes > reader.budget)
        reader.close()
    print(json.dumps(result, sort_keys=True))
    return result


def _failure_category(reason):
    """Finite public vocabulary; never echo untrusted exception strings."""
    if reason in {'budget_precheck', 'Selected chunks exceed remaining budget'}:
        return 'budget_precheck'
    if reason == 'overlong_response':
        return 'overlong_response'
    if reason == 'transport_unmetered':
        return 'transport_unmetered'
    if reason in {'transport_error', 'Request count limit', 'Redirect limit',
                  'Unapproved transport destination'}:
        return 'transport_error'
    if reason in {'unapproved_range', 'Unapproved byte range',
                  'Unbounded reads disabled', 'Seek outside file', 'Invalid seek'}:
        return 'unapproved_range'
    if reason in {'incomplete_response', 'Truncated response'}:
        return 'incomplete_response'
    if reason in {'Expected HTTP 206', 'Content-Range mismatch',
                  'Content-Length mismatch', 'Unexpected encoding',
                  'Unexpected transfer encoding', 'Missing or ambiguous HTTP header'}:
        return 'invalid_http_response'
    if reason in {'Invalid Parquet header', 'Invalid or oversized footer',
                  'Invalid column offset', 'Invalid column bounds',
                  'Overlapping column chunks', 'html_overlap',
                  'Required columns absent', 'Duplicate column',
                  'External column file forbidden', 'Empty Parquet file'}:
        return 'invalid_parquet_metadata'
    return 'parquet_read_error'


