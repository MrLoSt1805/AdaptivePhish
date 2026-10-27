"""Low-cardinality aggregate monitoring. Never use URLs as metric labels."""
from prometheus_client import Counter, Gauge

PREDICTED_CLASSES = Counter('adaptivephish_predicted_class_total', 'Predicted classes', ['label', 'version'])
DRIFT = Gauge('adaptivephish_maximum_psi', 'Last monitored maximum feature PSI')
RETRAINING = Counter('adaptivephish_retraining_events_total', 'Retraining outcomes', ['outcome'])
REGISTRY_EVENTS = Counter('adaptivephish_registry_events_total', 'Registry events', ['action'])
