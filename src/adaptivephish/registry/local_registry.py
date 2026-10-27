"""Single-writer local registry: governed promotion, immutable history, rollback."""

from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
from datetime import datetime, timezone

from adaptivephish.retraining.governance import GovernanceError, promotion_decision
from adaptivephish.monitoring.metrics import REGISTRY_EVENTS


class RegistryError(Exception):
    """Sanitized registry failure."""


def checksum(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_json(path, document):
    path = Path(path)
    descriptor, temporary = tempfile.mkstemp(dir=path.parent, suffix='.tmp')
    try:
        with os.fdopen(descriptor, 'w', encoding='utf-8') as stream:
            json.dump(document, stream, sort_keys=True, indent=2, allow_nan=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


@contextmanager
def _locked(root):
    root.mkdir(parents=True, exist_ok=True)
    lock = root / '.writer.lock'
    try:
        descriptor = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        raise RegistryError('registry_busy') from None
    try:
        os.close(descriptor)
        yield
    finally:
        lock.unlink()


def _validate_version(version):
    if not isinstance(version, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,79}', version):
        raise RegistryError('invalid_registry_input')


def _publish(candidate, root, version, metrics, evidence, previous, event):
    _validate_version(version)
    source = Path(candidate).resolve()
    destination = root / f'{version}.joblib'
    if not source.is_file() or destination.exists():
        raise RegistryError('model_version_exists_or_source_missing')
    descriptor, temporary = tempfile.mkstemp(dir=root, suffix='.tmp')
    os.close(descriptor)
    try:
        shutil.copyfile(source, temporary)
        document = {'version': version, 'artifact': destination.name,
                    'sha256': checksum(temporary), 'metrics': metrics,
                    'evidence': evidence, 'previous': previous, 'event': event}
        history = (resolve_champion(root)[1].get('audit_history', []) if previous else [])
        document['audit_history'] = history + [{'action': event, 'version': version,
                                               'at': datetime.now(timezone.utc).isoformat()}]
        json.dumps(document, allow_nan=False)
        os.replace(temporary, destination)
        atomic_json(root / f'{version}.json', document)
        atomic_json(root / 'champion.json', document)
        REGISTRY_EVENTS.labels(action=event).inc()
        return document
    finally:
        Path(temporary).unlink(missing_ok=True)


def initialize(candidate, registry_directory, version, metrics, evidence):
    """Explicit first-model bootstrap; never replaces an existing champion."""
    root = Path(registry_directory).resolve()
    with _locked(root):
        if (root / 'champion.json').exists():
            raise RegistryError('registry_already_initialized')
        try:
            promotion_decision(metrics, metrics)
        except GovernanceError:
            raise RegistryError('invalid_governance_metrics') from None
        return _publish(candidate, root, version, metrics, evidence, None, 'initialize')


def promote(candidate, registry_directory, version, metrics, *,
            champion_metrics=None, evidence=None, expected_champion=None):
    """Enforce gates on fresh metrics from the same held-out evaluation.

    Trusted local orchestration API, not an untrusted client endpoint.
    Evidence binds both artifact checksums and the evaluation digest.
    """
    root = Path(registry_directory).resolve()
    with _locked(root):
        _, current = resolve_champion(root)
        if (not evidence or not evidence.get('evaluation_fingerprint')
                or not evidence.get('dataset_fingerprint')
                or not evidence.get('mlflow_run_id')
                or evidence.get('fit_evaluation_overlap') != 0
                or evidence.get('candidate_sha256') != checksum(candidate)
                or evidence.get('champion_sha256') != current['sha256']
                or expected_champion != current['version']):
            raise RegistryError('invalid_promotion_evidence')
        try:
            decision = promotion_decision(champion_metrics or {}, metrics)
        except GovernanceError:
            raise RegistryError('invalid_governance_metrics') from None
        if not decision['approved']:
            raise RegistryError('promotion_rejected')
        return _publish(candidate, root, version, metrics,
                        {**evidence, 'decision': decision}, current['version'], 'promote')


def resolve_champion(registry_directory):
    try:
        root = Path(registry_directory).resolve()
        document = json.loads((root / 'champion.json').read_text(encoding='utf-8'))
        artifact = (root / document['artifact']).resolve()
        if artifact.parent != root or checksum(artifact) != document['sha256']:
            raise ValueError
        return artifact, document
    except Exception:
        raise RegistryError('invalid_champion') from None


def rollback(registry_directory):
    root = Path(registry_directory).resolve()
    with _locked(root):
        _, current = resolve_champion(root)
        previous = current.get('previous')
        _validate_version(previous)
        document = json.loads((root / f'{previous}.json').read_text(encoding='utf-8'))
        artifact = (root / document['artifact']).resolve()
        if artifact.parent != root or checksum(artifact) != document['sha256']:
            raise RegistryError('invalid_rollback_artifact')
        document = {**document, 'event': 'rollback', 'rolled_back_from': current['version']}
        document['audit_history'] = current.get('audit_history', []) + [
            {'action': 'rollback', 'version': previous, 'from': current['version'],
             'at': datetime.now(timezone.utc).isoformat()}]
        atomic_json(root / 'champion.json', document)
        REGISTRY_EVENTS.labels(action='rollback').inc()
        return document
