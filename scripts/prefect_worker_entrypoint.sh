#!/usr/bin/env bash
set -euo pipefail

FLOW_NAME="${RECOVERY_FLOW_NAME:-alor-realtime-etl}"
DEPLOYMENT_NAME="${RECOVERY_DEPLOYMENT_NAME:-alor-realtime-etl/alor-realtime}"
POOL_NAME="${PREFECT_WORKER_POOL:-vkr-docker-pool}"
QUEUE_NAME="${PREFECT_WORKER_QUEUE:-default}"
WORKER_LIMIT="${PREFECT_WORKER_LIMIT:-3}"
AUTO_START="${RECOVERY_AUTO_START:-true}"
API_READY_MAX_ATTEMPTS="${PREFECT_API_READY_MAX_ATTEMPTS:-60}"
API_READY_SLEEP_SECONDS="${PREFECT_API_READY_SLEEP_SECONDS:-2}"

echo "[startup-recovery] flow=${FLOW_NAME} deployment=${DEPLOYMENT_NAME}"

for attempt in $(seq 1 "${API_READY_MAX_ATTEMPTS}"); do
  if prefect deployment ls >/dev/null 2>&1; then
    echo "[startup-recovery] prefect api is reachable"
    break
  fi
  if [[ "${attempt}" -eq "${API_READY_MAX_ATTEMPTS}" ]]; then
    echo "[startup-recovery] prefect api not ready after ${API_READY_MAX_ATTEMPTS} attempts"
  else
    echo "[startup-recovery] waiting prefect api (${attempt}/${API_READY_MAX_ATTEMPTS})..."
    sleep "${API_READY_SLEEP_SECONDS}"
  fi
done

deployment_json="$(prefect deployment inspect "${DEPLOYMENT_NAME}" -o json 2>/dev/null || true)"
deployment_id="$(python - <<'PY' "${deployment_json}"
import json
import re
import sys

raw = sys.argv[1] if len(sys.argv) > 1 else ""
if not raw.strip():
    print("")
    raise SystemExit(0)

try:
    data = json.loads(raw)
except Exception:
    # Defensive fallback: remove ANSI/control chars and parse with relaxed strictness.
    cleaned = re.sub(r"\x1B\[[0-?]*[ -/]*[@-~]", "", raw)
    cleaned = "".join(ch for ch in cleaned if ch in "\n\r\t" or ord(ch) >= 32)
    try:
        data = json.loads(cleaned, strict=False)
    except Exception:
        data = {}

print(data.get("id", ""))
PY
)"
if [[ -n "${deployment_id}" ]]; then
  gcl_name="deployment:${deployment_id}"
  echo "[startup-recovery] reset active slots for ${gcl_name}"
  prefect global-concurrency-limit update "${gcl_name}" --active-slots 0 >/dev/null 2>&1 || true
fi

runs_json="$(
  prefect flow-run ls \
    --flow-name "${FLOW_NAME}" \
    --state RUNNING \
    --state LATE \
    --state SCHEDULED \
    --state PENDING \
    --state AWAITINGCONCURRENCYSLOT \
    --output json 2>/dev/null || echo '[]'
)"
python - <<'PY' "${runs_json}"
import json
import re
import subprocess
import sys

raw = sys.argv[1] if len(sys.argv) > 1 else ""
if not raw.strip():
    runs = []
else:
    try:
        runs = json.loads(raw)
    except Exception:
        cleaned = re.sub(r"\x1B\[[0-?]*[ -/]*[@-~]", "", raw)
        cleaned = "".join(ch for ch in cleaned if ch in "\n\r\t" or ord(ch) >= 32)
        try:
            runs = json.loads(cleaned, strict=False)
        except Exception:
            runs = []

for run in runs:
    run_id = run.get("id")
    state_type = str(run.get("state_type") or "").upper()
    if not run_id:
        continue
    print(f"[startup-recovery] cancel stale run {run_id} state={state_type}")
    subprocess.run(["prefect", "flow-run", "cancel", run_id], check=False)
PY

if [[ "${AUTO_START}" == "true" || "${AUTO_START}" == "1" || "${AUTO_START}" == "yes" ]]; then
  all_runs_json="$(prefect flow-run ls --flow-name "${FLOW_NAME}" --output json 2>/dev/null || echo '[]')"
  active_count="$(
    python - <<'PY' "${all_runs_json}"
import json
import re
import sys

raw = sys.argv[1] if len(sys.argv) > 1 else ""
if not raw.strip():
    print(0)
    raise SystemExit(0)

try:
    runs = json.loads(raw)
except Exception:
    cleaned = re.sub(r"\x1B\[[0-?]*[ -/]*[@-~]", "", raw)
    cleaned = "".join(ch for ch in cleaned if ch in "\n\r\t" or ord(ch) >= 32)
    try:
        runs = json.loads(cleaned, strict=False)
    except Exception:
        runs = []

# Only truly active states should block startup.
# SCHEDULED/LATE runs are often stale after machine restart.
active_states = {
    "RUNNING",
    "PENDING",
    "AWAITINGCONCURRENCYSLOT",
}
count = 0
for run in runs:
    st = str(run.get("state_type") or "").upper()
    if st in active_states:
        count += 1
print(count)
PY
  )"

  if [[ "${active_count}" == "0" ]]; then
    echo "[startup-recovery] no active ${FLOW_NAME} run found, starting ${DEPLOYMENT_NAME}"
    prefect deployment run "${DEPLOYMENT_NAME}" >/dev/null 2>&1 || true
  else
    echo "[startup-recovery] active ${FLOW_NAME} runs found (${active_count}), skip auto-start"
  fi
fi

echo "[startup-recovery] starting worker pool=${POOL_NAME} queue=${QUEUE_NAME} limit=${WORKER_LIMIT}"
exec prefect worker start --pool "${POOL_NAME}" --type docker --work-queue "${QUEUE_NAME}" --limit "${WORKER_LIMIT}"
