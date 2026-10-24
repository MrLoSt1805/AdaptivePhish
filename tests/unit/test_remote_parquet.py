"""Offline only: synthetic in-memory Parquet plus mocked HTTP responses.

Run: .venv/Scripts/python.exe -B -m unittest discover -s tests/unit
"""

import contextlib
import datetime
import http.client
import importlib.util
import io
import json
from pathlib import Path
import struct
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

SCRIPT = Path(__file__).resolve().parents[2] / 'scripts' / 'test_remote_parquet.py'
spec = importlib.util.spec_from_file_location('remote_poc', SCRIPT)
poc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(poc)


class Response:
    def __init__(self, data, headers, status=206):
        self.data = io.BytesIO(data)
        self.headers = headers
        self.status = status
        self.read_count = 0

    def getheaders(self):
        return self.headers

    def read(self, count):
        self.read_count += 1
        return self.data.read(count)


class FakeHTTP:
    def __init__(self, data, mutate=None):
        self.data, self.mutate = data, mutate
        self.requests = []
        self.responses = []

    @contextlib.contextmanager
    def __call__(self, url, headers):
        start, end = map(int, headers['Range'][6:].split('-'))
        assert headers['Accept-Encoding'] == 'identity'
        self.requests.append((start, end+1))
        response = Response(self.data[start:end+1], [
            ('Content-Range', f'bytes {start}-{end}/{len(self.data)}'),
            ('Content-Length', str(end-start+1))])
        if self.mutate:
            self.mutate(response)
        self.responses.append(response)
        yield response


def synthetic():
    # Reserved .invalid URLs and invented IDs only; never real dataset records.
    table = pa.table({
        'sha256': ['a'*64]*24,
        'url': ['https://synthetic.invalid/example']*24,
        'label': ['benign', 'phish']*12,
        'date': [datetime.date(2025, 1, 1) + datetime.timedelta(days=i) for i in range(24)],
        'html': ['SYNTHETIC_HTML_DO_NOT_PRINT_' + str(i)*500 for i in range(24)],
    })
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, row_group_size=12, compression='NONE')
    return sink.getvalue().to_pybytes()


class RangeReaderTests(unittest.TestCase):
    def setUp(self):
        # Even a mistakenly invoked live transport cannot contact any host.
        self.network = patch('socket.socket.connect', side_effect=AssertionError('Network forbidden'))
        self.network.start()
        self.addCleanup(self.network.stop)
        self.data = synthetic()

    def reader(self, data=None, mutate=None, budget=poc.BUDGET):
        data = self.data if data is None else data
        fake = FakeHTTP(data, mutate)
        reader = poc.RangeFile(size=len(data), budget=budget, transport=fake)
        self.addCleanup(reader.close)
        return reader, fake

    def test_success_and_no_html_or_other_row_group_ranges(self):
        reader, fake = self.reader()
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            result = poc.run_poc(reader)
        self.assertTrue(result['success'])
        self.assertIsNone(result['failure_category'])
        self.assertTrue(result['application_body_accounting_complete'])
        self.assertFalse(result['application_budget_exceeded'])
        self.assertEqual(result['row_count'], 10)
        self.assertEqual(result['labels'], {'benign': 5, 'phish': 5})
        self.assertEqual(result['date_range'], ['2025-01-01', '2025-01-10'])
        self.assertEqual(result['selected_columns'], list(poc.COLUMNS))
        self.assertEqual(reader.request_count, 7)  # header, trailer, footer, four chunks
        self.assertEqual(reader.body_bytes, sum(b-a for a, b in fake.requests))
        metadata = pq.read_metadata(pa.BufferReader(self.data))
        for gi in range(metadata.num_row_groups):
            group = metadata.row_group(gi)
            for ci in range(group.num_columns):
                col = group.column(ci)
                if gi == 0 and col.path_in_schema in poc.COLUMNS:
                    continue
                start = min(col.data_page_offset, col.dictionary_page_offset)
                end = start + col.total_compressed_size
                self.assertFalse(any(a < end and b > start for a, b in fake.requests))
        for secret in ('synthetic.invalid', 'a'*64, 'SYNTHETIC_HTML'):
            self.assertNotIn(secret, output.getvalue())
        self.assertEqual(json.loads(output.getvalue()), result)

    def test_reject_headers_before_reading_body(self):
        cases = [
            lambda r: setattr(r, 'status', 200),
            lambda r: setattr(r, 'status', 500),
            lambda r: r.headers.append(('Content-Encoding', 'gzip')),
            lambda r: r.headers.append(('Transfer-Encoding', 'chunked')),
            lambda r: r.headers.append(('Content-Length', '4')),
            lambda r: r.headers.__setitem__(0, ('Content-Range', 'bytes 0-3/999')),
            lambda r: r.headers.__setitem__(1, ('Content-Length', '999')),
            lambda r: r.headers.pop(0),
            lambda r: r.headers.pop(1),
        ]
        for mutate in cases:
            with self.subTest(case=cases.index(mutate)):
                reader, fake = self.reader(mutate=mutate)
                reader.allowed = [(0, 4)]
                with self.assertRaises(poc.SafetyError):
                    reader.fetch(0, 4)
                self.assertEqual(reader.body_bytes, 0)
                self.assertEqual(fake.responses[0].read_count, 0)
                with self.assertRaises(poc.SafetyError):
                    reader.fetch(0, 4)
                self.assertEqual(reader.request_count, 1)

    def test_short_body_counts_bytes_and_never_retries(self):
        reader, fake = self.reader(mutate=lambda r: setattr(r, 'data', io.BytesIO(b'PA')))
        reader.allowed = [(0, 4)]
        with self.assertRaises(poc.SafetyError):
            reader.fetch(0, 4)
        self.assertEqual(reader.body_bytes, 2)
        self.assertEqual(len(fake.requests), 1)

    def test_incomplete_read_exception_counts_partial(self):
        def mutate(response):
            def broken(count):
                raise http.client.IncompleteRead(b'PA', 2)
            response.read = broken
        reader, _ = self.reader(mutate=mutate)
        reader.allowed = [(0, 4)]
        with self.assertRaises(poc.SafetyError):
            reader.fetch(0, 4)
        self.assertEqual(reader.body_bytes, 2)

    def test_budget_counts_repeated_requests_and_cache_is_free(self):
        reader, fake = self.reader(budget=8)
        reader.allowed = [(0, 4)]
        reader.fetch(0, 4)
        reader.fetch(0, 4)
        self.assertEqual(reader.body_bytes, 4)
        reader.cache.clear()
        reader.fetch(0, 4)
        self.assertEqual(reader.body_bytes, 8)
        reader.cache.clear()
        with self.assertRaises(poc.SafetyError):
            reader.fetch(0, 4)
        self.assertEqual(len(fake.requests), 2)

    def test_preflight_aborts_before_column_gets(self):
        footer_length = struct.unpack('<I', self.data[-8:-4])[0]
        reader, fake = self.reader(budget=footer_length+12+poc.OVERHEAD)
        with self.assertRaises(poc.SafetyError):
            poc.sample(reader)
        self.assertEqual(len(fake.requests), 3)
        self.assertGreater(reader.plan['estimated_bytes'], reader.remaining)

    def test_invalid_footer_is_not_fetched(self):
        data = self.data[:-8] + struct.pack('<I', poc.MAX_FOOTER+1) + b'PAR1'
        reader, fake = self.reader(data=data)
        with self.assertRaises(poc.SafetyError):
            poc.inspect_footer(reader)
        self.assertEqual(len(fake.requests), 2)

    def test_seek_readinto_and_denied_operations(self):
        reader, fake = self.reader()
        reader.allowed = [(0, 4)]
        buffer = bytearray(4)
        self.assertEqual(reader.readinto(buffer), 4)
        self.assertEqual(buffer, b'PAR1')
        self.assertEqual(reader.seek(-4, 1), 0)
        self.assertEqual(reader.read(4), b'PAR1')
        self.assertEqual(reader.seek(0, 2), len(self.data))
        self.assertEqual(reader.read(1), b'')
        for operation in (lambda: reader.seek(-1), lambda: reader.seek(0, 3),
                          lambda: reader.read(), lambda: reader.fetch(4, 8)):
            with self.assertRaises(poc.SafetyError):
                operation()
        with self.assertRaises(io.UnsupportedOperation):
            reader.write(b'no')
        self.assertEqual(len(fake.requests), 1)

    def test_redirects_are_counted_but_bodies_not_read(self):
        fake = FakeHTTP(self.data)
        redirect = Response(b'PRIVATE', [('Location', 'https://us.aws.cdn.hf.co/object')], 302)
        calls = []
        @contextlib.contextmanager
        def transport(url, headers):
            calls.append(url)
            if len(calls) == 1:
                yield redirect
            else:
                with fake(url, headers) as r:
                    yield r
        reader = poc.RangeFile(size=len(self.data), transport=transport)
        self.addCleanup(reader.close)
        reader.allowed = [(0, 4)]
        self.assertEqual(reader.fetch(0, 4), b'PAR1')
        self.assertEqual(reader.request_count, 2)
        self.assertEqual(reader.body_bytes, 4)
        self.assertEqual(redirect.read_count, 0)

    def test_unsafe_redirect_and_redirect_loop(self):
        for location, expected in [('http://huggingface.co/x', 1),
                                   ('https://example.invalid/x', 1),
                                   ('https://huggingface.co/loop', 6)]:
            with self.subTest(location=location):
                def mutate(r):
                    r.status = 302
                    r.headers.append(('Location', location))
                reader, fake = self.reader(mutate=mutate)
                reader.allowed = [(0, 4)]
                with self.assertRaises(poc.SafetyError):
                    reader.fetch(0, 4)
                self.assertEqual(reader.request_count, expected)
                self.assertTrue(all(r.read_count == 0 for r in fake.responses))

    def test_failure_output_does_not_leak_exception(self):
        @contextlib.contextmanager
        def transport(url, headers):
            raise RuntimeError('SENSITIVE_URL_AND_IDENTIFIER')
            yield
        reader = poc.RangeFile(size=len(self.data), transport=transport)
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            result = poc.run_poc(reader)
        self.assertFalse(result['success'])
        self.assertNotIn('SENSITIVE', output.getvalue())
        self.assertEqual(result['failure_category'], 'transport_error')

    def test_html_request_denied_even_after_planning(self):
        reader, fake = self.reader()
        metadata, start = poc.inspect_footer(reader)
        poc.plan_first_group(reader, metadata, start)
        html = metadata.row_group(0).column(4)
        offset = min(html.data_page_offset, html.dictionary_page_offset)
        with self.assertRaises(poc.SafetyError):
            reader.fetch(offset, offset + html.total_compressed_size)
        self.assertEqual(len(fake.requests), 3)

    def test_missing_column_aborts_before_data(self):
        table = pa.table({'url': ['https://synthetic.invalid/']})
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink)
        reader, fake = self.reader(data=sink.getvalue().to_pybytes())
        with self.assertRaises(poc.SafetyError):
            poc.sample(reader)
        self.assertEqual(len(fake.requests), 3)

    def test_bad_magic_aborts_immediately(self):
        reader, fake = self.reader(data=b'NOPE' + self.data[4:])
        with self.assertRaises(poc.SafetyError):
            poc.inspect_footer(reader)
        self.assertEqual(len(fake.requests), 1)

    def test_unexpected_label_is_not_printed(self):
        table = pq.read_table(pa.BufferReader(self.data))
        table = table.set_column(2, 'label', pa.array(['PRIVATE_LABEL']*24))
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink, row_group_size=12)
        reader, _ = self.reader(data=sink.getvalue().to_pybytes())
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            result = poc.run_poc(reader)
        self.assertFalse(result['success'])
        self.assertNotIn('PRIVATE_LABEL', output.getvalue())
        self.assertEqual(result['failure_category'], 'parquet_read_error')

    def test_more_bytes_than_requested_are_counted_and_rejected(self):
        def mutate(response):
            response.data = io.BytesIO(b'PAR1EXTRA')
            response.read = lambda count: response.data.read(count + 5)
        reader, fake = self.reader(mutate=mutate, budget=4)
        reader.allowed = [(0, 4)]
        with self.assertRaises(poc.SafetyError) as error:
            reader.fetch(0, 4)
        self.assertEqual(error.exception.category, 'overlong_response')
        self.assertEqual(reader.body_bytes, 9)
        self.assertTrue(reader.failed)
        self.assertEqual(len(fake.requests), 1)
        self.assertGreater(reader.body_bytes, reader.budget)

    def test_ambiguous_and_invalid_http_headers(self):
        cases = [
            lambda r: r.headers.append(('content-range', 'bytes 0-3/999')),
            lambda r: r.headers.append(('content-length', '4')),
            lambda r: r.headers.extend([('Content-Encoding', 'identity'),
                                        ('content-encoding', 'identity')]),
            lambda r: r.headers.__setitem__(0, ('Content-Range', 'BYTES 0-3/4')),
            lambda r: r.headers.__setitem__(1, ('Content-Length', '-4')),
            lambda r: r.headers.append(('Transfer-Encoding', 'chunked')),
        ]
        for index, mutate in enumerate(cases):
            with self.subTest(case=index):
                reader, fake = self.reader(mutate=mutate)
                reader.allowed = [(0, 4)]
                with self.assertRaises(poc.SafetyError) as error:
                    reader.fetch(0, 4)
                self.assertEqual(poc._failure_category(error.exception.category),
                                 'invalid_http_response')
                self.assertEqual(reader.body_bytes, 0)
                self.assertEqual(fake.responses[0].read_count, 0)

    def test_budget_after_partial_read_never_retries(self):
        reader, fake = self.reader(budget=6)
        reader.allowed = [(0, 4)]
        reader.fetch(0, 4)
        reader.allowed.append((4, 8))
        with self.assertRaises(poc.SafetyError) as error:
            reader.fetch(4, 8)
        self.assertEqual(error.exception.category, 'budget_precheck')
        self.assertEqual(reader.body_bytes, 4)
        self.assertEqual(len(fake.requests), 1)

    def test_transport_exception_after_partial_read_fails_closed(self):
        def mutate(response):
            calls = 0
            def broken(count):
                nonlocal calls
                calls += 1
                if calls == 1:
                    return b'PA'
                raise OSError('SENSITIVE_TRANSPORT_CONTEXT')
            response.read = broken
        reader, fake = self.reader(mutate=mutate)
        reader.allowed = [(0, 4)]
        with self.assertRaises(poc.SafetyError) as error:
            reader.fetch(0, 4)
        self.assertEqual(error.exception.category, 'transport_unmetered')
        self.assertEqual(reader.body_bytes, 2)
        self.assertFalse(reader.accounting_complete)
        self.assertTrue(reader.failed)
        with self.assertRaises(poc.SafetyError):
            reader.fetch(0, 4)
        self.assertEqual(len(fake.requests), 1)

    def test_unexpected_pyarrow_range_request_is_denied(self):
        reader, fake = self.reader()
        html = pq.read_metadata(pa.BufferReader(self.data)).row_group(0).column(4)
        html_offset = min(html.data_page_offset, html.dictionary_page_offset)
        class UnexpectedRead:
            def __init__(self, source, **kwargs):
                self.source = source
            def iter_batches(self, **kwargs):
                class Batches:
                    def __iter__(self):
                        return self
                    def __next__(self):
                        self_source = reader
                        self_source.seek(html_offset)
                        self_source.read(1)  # HTML data chunk is never authorized.
                    def close(self):
                        pass
                return Batches()
            def close(self):
                pass
        output = io.StringIO()
        with patch.object(poc.pq, 'ParquetFile', UnexpectedRead), contextlib.redirect_stdout(output):
            result = poc.run_poc(reader)
        self.assertFalse(result['success'])
        self.assertEqual(result['failure_category'], 'unapproved_range')
        self.assertEqual(len(fake.requests), 7)
        self.assertNotIn('synthetic.invalid', output.getvalue())

    def test_metadata_overlap_with_html_fails_before_selected_column_gets(self):
        reader, fake = self.reader()
        metadata, footer_start = poc.inspect_footer(reader)
        original_group = metadata.row_group
        first = original_group(0)
        html = first.column(4)
        selected = first.column(0)
        class OverlappingColumn:
            def __getattr__(self, name):
                if name == 'data_page_offset':
                    return selected.data_page_offset
                if name == 'dictionary_page_offset':
                    return selected.dictionary_page_offset
                return getattr(html, name)
        class Group:
            num_columns = first.num_columns
            num_rows = first.num_rows
            def column(self, index):
                return OverlappingColumn() if index == 4 else first.column(index)
        class Metadata:
            num_row_groups = metadata.num_row_groups
            def row_group(self, index):
                return Group() if index == 0 else original_group(index)
        with self.assertRaises(poc.SafetyError) as error:
            poc.plan_first_group(reader, Metadata(), footer_start)
        self.assertEqual(poc._failure_category(error.exception.category),
                         'invalid_parquet_metadata')
        self.assertEqual(len(fake.requests), 3)


if __name__ == '__main__':
    unittest.main()
