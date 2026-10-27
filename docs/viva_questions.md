# AdaptivePhish viva questions and concise answers

1. **What problem does AdaptivePhish solve?** It classifies URLs as benign or phishing while studying drift and reproducible retraining.
2. **Why URL-only modeling?** It supports fast, non-invasive inference without fetching potentially dangerous pages.
3. **Why pin the dataset revision?** It prevents a mutable upstream release from silently changing experiments.
4. **Why use HTTP range reads?** Parquet column chunks can be selected without transferring the large HTML payload.
5. **Is the byte budget a network cap?** No. It counts response-body bytes exposed to the application, not protocol overhead or hidden buffering.
6. **Why reject HTTP 200?** A server ignoring `Range` could otherwise send an entire shard.
7. **Why preserve publisher train/test identity?** It maintains provenance and avoids accidentally mixing benchmark roles.
8. **What does DVC store in Git?** Small pointer metadata; actual data stays in the cache or configured remote.
9. **Why is a DVC remote needed?** Without it, another machine cannot retrieve local cached data from the repository alone.
10. **Why was a Git ignore fix needed?** Directory-wide wildcard rules caused DVC discovery to prune pointer ancestors.
11. **Which fields are modeled?** Only lexical information derived locally from `url`; identifiers, labels, and dates are not predictive inputs.
12. **Why keep `sha256`?** It supports provenance and duplicate/conflict checks, not prediction.
13. **Why retain `date`?** It enables chronological evaluation and monitoring.
14. **What leakage is prevented?** URL overlap across splits, label-derived fields, future preprocessing, and forbidden source columns.
15. **Why four candidate algorithms?** They provide interpretable linear and varied tree-based baselines.
16. **How was the Phase 3 champion chosen?** Validation F1, then ROC AUC, accuracy, and deterministic candidate order.
17. **Why is the final test set separate?** It estimates performance after model selection rather than influencing selection.
18. **Why is random splitting insufficient?** It does not reproduce deployment on later URL patterns.
19. **How does the temporal split work?** Whole calendar dates before a cutoff train the model; the cutoff day and later dates form the test.
20. **What is PSI?** Population Stability Index measures distribution change between reference and current feature samples.
21. **Does PSI prove performance degradation?** No. It is an unlabeled covariate-shift signal.
22. **What PSI threshold is critical here?** The configured default is 0.25 or greater for any monitored feature.
23. **What triggered critical drift?** Hostname ratio reached approximately 0.307 in the verified local temporal experiment.
24. **Was the temporal candidate promoted?** No; its F1 and ROC AUC were worse than the champion.
25. **Why require labels before retraining?** Drift alone cannot show whether a new model improves phishing detection.
26. **What is fail-closed governance?** Missing, invalid, or regressing evidence results in rejection rather than promotion.
27. **How is rollback supported?** Immutable versioned artifacts and a checksum-verified champion pointer retain prior versions.
28. **What does MLflow record?** Local parameters, metrics, tags, and run identity in SQLite.
29. **Why SQLite instead of MLflow file tracking?** MLflow 3 places its legacy filesystem backend in maintenance mode.
30. **What API endpoints exist?** `/health`, `/predict`, and `/metrics`.
31. **Does the API echo submitted URLs?** No; responses and errors are aggregate and sanitized.
32. **Why expose Prometheus metrics?** They enable request outcome and latency monitoring without storing raw URLs.
33. **Why disable access logging in the local runner?** Request targets and payload-adjacent data can be sensitive.
34. **Why use a non-root container user?** It reduces the impact of a container compromise.
35. **What does CI verify?** Dependency installation, the offline unit suite, and an API import smoke test.
36. **Why was AWS not launched automatically?** Cloud actions require credentials, security choices, and explicit billing authorization.
37. **What is the largest current research limitation?** Only four train shards are locally verified, not the full 666,315-row dataset.
38. **Why are collection dates imperfect?** They do not necessarily equal first-seen or label-availability timestamps.
39. **What makes the demo safe?** It uses reserved `.invalid` strings, performs local lexical inference, and prints no URL values.
40. **What is the next defensible research step?** Complete the separately approved bounded shard campaign, then repeat temporal evaluation across preserved publisher splits.
