"""Explainable lexical features that never resolve or visit a URL."""

import ipaddress
import math
import re
from urllib.parse import urlsplit


FEATURE_SCHEMA_VERSION = 'url-lexical-v1'
FEATURE_NAMES = (
    'url_length', 'hostname_length', 'path_length', 'query_length',
    'fragment_length', 'dot_count', 'digit_count', 'hyphen_count',
    'slash_count', 'subdomain_count', 'https_flag', 'ip_literal_flag',
    'at_sign_flag', 'percent_encoding_count', 'underscore_count',
    'equals_count', 'ampersand_count', 'question_mark_count',
    'special_character_count', 'digit_ratio', 'hostname_ratio',
    'path_ratio', 'query_parameter_count', 'suspicious_keyword_count',
    'punycode_flag', 'explicit_port_flag',
)

SUSPICIOUS_KEYWORDS = (
    'account', 'bank', 'confirm', 'login', 'password', 'secure', 'signin',
    'update', 'verify', 'wallet',
)
_PERCENT_ENCODING = re.compile(r'%[0-9a-fA-F]{2}')
_SPECIAL = frozenset('@%_=&#?~!$*,;:+[](){}|\\^`<>"\'')


class FeatureExtractionError(Exception):
    """Sanitized feature-extraction failure."""


def _parse(value):
    candidate = value if '://' in value else f'http://{value}'
    try:
        return urlsplit(candidate, allow_fragments=True)
    except (TypeError, ValueError):
        return urlsplit('http://')


def extract_url_features(url):
    """Return one ordered tuple of finite numeric URL lexical features."""
    if not isinstance(url, str):
        raise FeatureExtractionError('invalid_url_type')
    parsed = _parse(url)
    try:
        hostname = parsed.hostname or ''
        port_flag = int(parsed.port is not None)
    except ValueError:
        hostname, port_flag = '', 0
    path, query, fragment = parsed.path or '', parsed.query or '', parsed.fragment or ''
    length = len(url)
    host_labels = [part for part in hostname.strip('.').split('.') if part]
    try:
        ip_flag = int(bool(hostname) and ipaddress.ip_address(hostname) is not None)
    except ValueError:
        ip_flag = 0
    lowered = url.casefold()
    digit_count = sum(character.isdigit() for character in url)
    special_count = sum(character in _SPECIAL for character in url)
    denominator = max(length, 1)
    values = (
        length, len(hostname), len(path), len(query), len(fragment),
        url.count('.'), digit_count, url.count('-'), url.count('/'),
        0 if ip_flag else max(0, len(host_labels) - 2),
        int(parsed.scheme.casefold() == 'https'), ip_flag, int('@' in url),
        len(_PERCENT_ENCODING.findall(url)), url.count('_'), url.count('='),
        url.count('&'), url.count('?'), special_count,
        digit_count / denominator, len(hostname) / denominator,
        len(path) / denominator,
        0 if not query else query.count('&') + 1,
        sum(lowered.count(keyword) for keyword in SUSPICIOUS_KEYWORDS),
        int(any(label.casefold().startswith('xn--') for label in host_labels)),
        port_flag,
    )
    if len(values) != len(FEATURE_NAMES) or not all(
            isinstance(value, (int, float)) and math.isfinite(value)
            for value in values):
        raise FeatureExtractionError('feature_extraction_failed')
    return values


def feature_matrix(urls):
    """Use the identical ordered extractor for training and inference."""
    return [list(extract_url_features(url)) for url in urls]
