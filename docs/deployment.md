# AdaptivePhish deployment guide

The API serves the trusted local model artifact and never visits submitted URLs.

Local development:

```powershell
.\.venv\Scripts\python.exe -B scripts\run_api.py
Invoke-RestMethod http://127.0.0.1:8000/health
```

Container build and local run:

```powershell
docker build -t adaptivephish:local .
docker run --rm -p 8000:8000 adaptivephish:local
```

The service exposes `GET /health`, `POST /predict`, and aggregate Prometheus
metrics at `GET /metrics`. Prediction responses do not echo the submitted URL.
Do not enable verbose request/access logging in production because URLs are
sensitive input data.

AWS EC2 deployment is intentionally not executed by project automation. It
requires explicit approval because an instance, registry, networking, DNS, and
monitoring can incur charges. A future deployment should use HTTPS, a restricted
security group, least-privilege IAM, a private artifact source, log retention,
and a documented rollback to the previous immutable image tag.
