#!/usr/bin/env bash
# Run on an already authorized EC2 host with Docker and registry credentials.
set -euo pipefail
image=${1:?immutable image digest required}
model=${2:?absolute trusted model path required}
expected=${3:?model SHA256 required}
[[ "$image" =~ ^[a-zA-Z0-9./:_-]+@sha256:[a-f0-9]{64}$ ]] || exit 2
[[ "$model" = /* && -f "$model" && "$expected" =~ ^[a-f0-9]{64}$ ]] || exit 2
actual=$(sha256sum "$model")
[[ "${actual%% *}" = "$expected" ]] || exit 3
docker pull "$image"
docker rm -f adaptivephish-candidate >/dev/null 2>&1 || true
docker run -d --name adaptivephish-candidate --read-only --tmpfs /tmp \
  --cap-drop ALL --security-opt no-new-privileges \
  -p 127.0.0.1:18000:8000 -v "$model:/models/adaptivephish_model.joblib:ro" "$image"
healthy=false
for attempt in {1..30}; do
  if curl --fail --silent http://127.0.0.1:18000/ready >/dev/null; then healthy=true; break; fi
  sleep 2
done
if [[ "$healthy" != true ]] || ! curl --fail --silent \
  -H 'Content-Type: application/json' -d '{"url":"https://example.invalid/home"}' \
  http://127.0.0.1:18000/predict >/dev/null; then
  docker rm -f adaptivephish-candidate
  exit 4
fi
docker rm -f adaptivephish-candidate
docker rm -f adaptivephish-previous >/dev/null 2>&1 || true
if docker container inspect adaptivephish >/dev/null 2>&1; then
  docker stop adaptivephish
  docker rename adaptivephish adaptivephish-previous
fi
rollback() {
  docker rm -f adaptivephish >/dev/null 2>&1 || true
  if docker container inspect adaptivephish-previous >/dev/null 2>&1; then
    docker rename adaptivephish-previous adaptivephish
    docker start adaptivephish
  fi
}
trap rollback ERR
docker run -d --name adaptivephish --restart unless-stopped --read-only --tmpfs /tmp \
  --cap-drop ALL --security-opt no-new-privileges \
  -p 127.0.0.1:8000:8000 -v "$model:/models/adaptivephish_model.joblib:ro" "$image"
healthy=false
for attempt in {1..30}; do
  if curl --fail --silent http://127.0.0.1:8000/ready >/dev/null; then healthy=true; break; fi
  sleep 2
done
[[ "$healthy" == true ]]
trap - ERR
