"""Trusted local model artifact publication and inference."""

import os
from pathlib import Path
import tempfile

from adaptivephish.data.output_writer import WORKSPACE_ROOT
from adaptivephish.features.url_features import (
    FEATURE_NAMES, FEATURE_SCHEMA_VERSION, feature_matrix,
)


MODEL_VERSION = 'phase3-v1'
ARTIFACT_PATH = Path(os.environ.get('ADAPTIVEPHISH_MODEL_PATH',
                    WORKSPACE_ROOT / 'artifacts' / 'adaptivephish_model.joblib'))


class ModelArtifactError(Exception):
    """Sanitized artifact or inference failure."""


def save_model(model, metadata, path=None, overwrite=False):
    target = Path(ARTIFACT_PATH if path is None else path).resolve()
    if target != ARTIFACT_PATH.resolve():
        raise ModelArtifactError('unapproved_artifact_path')
    if target.exists() and not overwrite:
        raise ModelArtifactError('artifact_already_exists')
    try:
        import joblib
        target.parent.mkdir(parents=True, exist_ok=True)
        bundle = {
            'model': model,
            'model_version': MODEL_VERSION,
            'feature_schema_version': FEATURE_SCHEMA_VERSION,
            'feature_names': list(FEATURE_NAMES),
            'metadata': dict(metadata),
        }
        descriptor, temporary = tempfile.mkstemp(
            prefix='.adaptivephish-model-', suffix='.tmp', dir=target.parent
        )
        os.close(descriptor)
        try:
            joblib.dump(bundle, temporary)
            os.replace(temporary, target)
        finally:
            Path(temporary).unlink(missing_ok=True)
        return bundle
    except ModelArtifactError:
        raise
    except ImportError:
        raise ModelArtifactError('ml_dependencies_unavailable') from None
    except Exception:
        raise ModelArtifactError('artifact_write_failed') from None


def load_trusted_model(path=None):
    """Load only the fixed local artifact; joblib files must never be untrusted."""
    registry = os.environ.get('ADAPTIVEPHISH_REGISTRY')
    registry_document = None
    if registry and path is None:
        from adaptivephish.registry.local_registry import RegistryError, resolve_champion
        try:
            path, registry_document = resolve_champion(registry)
        except RegistryError:
            raise ModelArtifactError('invalid_model_registry') from None
    target = Path(ARTIFACT_PATH if path is None else path).resolve()
    if registry_document is None and target != ARTIFACT_PATH.resolve():
        raise ModelArtifactError('unapproved_artifact_path')
    if not target.is_file():
        raise ModelArtifactError('model_artifact_missing')
    try:
        import joblib
        bundle = joblib.load(target)
        if (not isinstance(bundle, dict)
                or bundle.get('model_version') != (registry_document['version'] if registry_document else MODEL_VERSION)
                or bundle.get('feature_schema_version') != FEATURE_SCHEMA_VERSION
                or bundle.get('feature_names') != list(FEATURE_NAMES)
                or not hasattr(bundle.get('model'), 'predict_proba')):
            raise ModelArtifactError('incompatible_model_artifact')
        return bundle
    except ModelArtifactError:
        raise
    except ImportError:
        raise ModelArtifactError('ml_dependencies_unavailable') from None
    except Exception:
        raise ModelArtifactError('corrupt_model_artifact') from None


def predict_url(url):
    bundle = load_trusted_model()
    try:
        probability = float(bundle['model'].predict_proba(feature_matrix([url]))[0][1])
        return {
            'predicted_label': 'phish' if probability >= 0.5 else 'benign',
            'phishing_probability': probability,
            'model_version': bundle['model_version'],
            'feature_schema_version': bundle['feature_schema_version'],
        }
    except Exception:
        raise ModelArtifactError('prediction_failed') from None
