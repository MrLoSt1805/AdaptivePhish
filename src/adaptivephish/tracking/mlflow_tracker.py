"""Local MLflow experiment tracking without a network dependency."""

from contextlib import contextmanager
from pathlib import Path


class TrackingError(Exception):
    """Sanitized tracking failure."""


@contextmanager
def tracked_run(tracking_directory, experiment, run_name, parameters=None,
                tags=None):
    """Create a local file-backed MLflow run and yield its run identifier."""
    previous_uri = None
    client = None
    try:
        import mlflow
        previous_uri = mlflow.get_tracking_uri()
        directory = Path(tracking_directory).resolve()
        directory.mkdir(parents=True, exist_ok=True)
        # MLflow 3 keeps the legacy filesystem backend in maintenance mode.
        # SQLite is the supported local, offline tracking store.
        database = (directory / 'mlflow.db').as_posix()
        mlflow.set_tracking_uri(f'sqlite:///{database}')
        from mlflow.tracking import MlflowClient
        client = MlflowClient()
        if client.get_experiment_by_name(experiment) is None:
            client.create_experiment(experiment, artifact_location=(directory / 'artifacts').as_uri())
        mlflow.set_experiment(experiment)
        with mlflow.start_run(run_name=run_name, tags=tags or {}) as run:
            if parameters:
                mlflow.log_params(parameters)
            yield run.info.run_id
    except Exception:
        raise TrackingError('experiment_tracking_failed') from None
    finally:
        if client is not None:
            # MLflow's SQLite store retains pooled handles on Windows.
            client._tracking_client.store.engine.dispose()
        if previous_uri is not None:
            mlflow.set_tracking_uri(previous_uri)


def log_metrics(metrics, step=None):
    """Log only finite numeric scalar metrics to the active run."""
    try:
        import math
        import mlflow
        cleaned = {name: float(value) for name, value in metrics.items()
                   if isinstance(value, (int, float))
                   and math.isfinite(float(value))}
        mlflow.log_metrics(cleaned, step=step)
    except Exception:
        raise TrackingError('metric_logging_failed') from None
