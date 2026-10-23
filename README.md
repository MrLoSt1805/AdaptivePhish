# AdaptivePhish

**An MLOps-Based Phishing URL Detection System with Temporal Drift Detection and Automated Retraining**

AdaptivePhish is an academic research project planned to study phishing URL detection as URL patterns change over time. It will explore a reproducible workflow for data preparation, model evaluation, drift monitoring, and conditional retraining.

## Research objectives

- Investigate URL features for distinguishing phishing and legitimate URLs.
- Evaluate detection performance using time-aware data splits.
- Study temporal drift and its relationship to model performance.
- Compare conditional retraining with a fixed-model baseline.
- Establish reproducible experiments and model governance criteria before deployment.

## Planned technology stack

- Python 3.12–3.14, with an isolated virtual environment.
- PhreshPhish dataset and DVC for data versioning.
- pandas, NumPy, and scikit-learn for feature engineering and machine learning.
- MLflow for experiment tracking and model management.
- Drift monitoring and conditional retraining components.
- FastAPI for prediction serving and Docker for packaging.
- GitHub Actions for CI/CD and AWS EC2 for deployment.

These technologies are planned; their integrations and dependencies have not been added in this step.

## Planned MLOps workflow

Acquire and version data → prepare temporal splits → engineer URL features → train and track experiments → evaluate and review models → deploy approved models → monitor drift and performance → conditionally retrain → evaluate again before promotion.

## Current status

**Project setup only.** The directory skeleton and starter metadata are present. Dataset handling, features, training, experiment tracking, drift detection, retraining, governance, serving, containers, CI/CD, and deployment are not implemented.

`requirements.txt` intentionally contains comments only. Configuration and implementation will be added one small step at a time.
