"""Four deterministic, modest candidate classifiers."""


RANDOM_SEED = 42
CANDIDATE_ORDER = ('logistic_regression', 'random_forest', 'xgboost', 'lightgbm')


class TrainingDependencyError(Exception):
    """A required Phase 3 candidate dependency is unavailable."""


def build_candidates(seed=RANDOM_SEED):
    try:
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.linear_model import LogisticRegression
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import StandardScaler
        from xgboost import XGBClassifier
        from lightgbm import LGBMClassifier
    except ImportError:
        raise TrainingDependencyError('ml_dependencies_unavailable') from None
    return {
        'logistic_regression': Pipeline([
            ('scale', StandardScaler()),
            ('model', LogisticRegression(max_iter=1_000, random_state=seed)),
        ]),
        'random_forest': RandomForestClassifier(
            n_estimators=160, max_depth=14, min_samples_leaf=2,
            random_state=seed, n_jobs=1,
        ),
        'xgboost': XGBClassifier(
            n_estimators=140, max_depth=4, learning_rate=0.08,
            subsample=0.9, colsample_bytree=0.9, random_state=seed,
            n_jobs=1, eval_metric='logloss',
        ),
        'lightgbm': LGBMClassifier(
            n_estimators=140, max_depth=8, learning_rate=0.08,
            random_state=seed, n_jobs=1, verbosity=-1,
        ),
    }
