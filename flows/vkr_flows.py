# flows/vkr_flows.py
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional

from prefect import flow, task


REPO_ROOT = Path(__file__).resolve().parents[1]


@task(retries=3, retry_delay_seconds=60, log_prints=True)
def run_script(
    script_name: str,
    args: Optional[List[str]] = None,
    extra_env: Optional[Dict[str, str]] = None,
) -> None:
    script_path = REPO_ROOT / script_name
    if not script_path.exists():
        raise FileNotFoundError(f"Скрипт не найден: {script_path}")

    cmd = [sys.executable, str(script_path)] + (args or [])
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)

    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout)
    if result.returncode != 0:
        if result.stderr:
            print(result.stderr)
        raise RuntimeError(f"{script_name} завершился с ошибкой. returncode={result.returncode}")


@flow(name="alor-realtime-etl", log_prints=True)
def fetch_alor_to_pgraw_1min():
    run_script.submit("fetch_alor_to_pgraw_1min.py").result()


@flow(name="moex-fallback-guard-1m", log_prints=True)
def fetch_moex_iss_fallback_to_pgraw_1min():
    run_script.submit("fetch_moex_iss_fallback_to_pgraw_1min.py").result()


@flow(name="alor-raw-to-dwh-1m", log_prints=True)
def load_alor_pgraw_to_pgdwh_1min():
    run_script.submit("load_alor_pgraw_to_pgdwh_1min.py").result()
    run_script.submit("run_data_quality_checks.py").result()


@flow(name="alor-daily-to-dwh", log_prints=True)
def aggregate_alor_pgdwh_1min_to_1d():
    run_script.submit("aggregate_alor_pgdwh_1min_to_1d.py").result()


@flow(name="moex-bars-fixed", log_prints=True)
def fetch_moex_iss_to_pgraw_1d():
    run_script.submit("fetch_moex_iss_to_pgraw_1d.py").result()


@flow(name="moex-raw-to-dwh-daily", log_prints=True)
def load_moex_pgraw_to_pgdwh_1d():
    run_script.submit("load_moex_pgraw_to_pgdwh_1d.py").result()
    run_script.submit("run_data_quality_checks.py").result()


@flow(name="moex-raw-to-dwh-flow", log_prints=True)
def transform_moex_pgraw_to_pgdwh_agg():
    run_script.submit("transform_moex_pgraw_to_pgdwh_agg.py").result()
    run_script.submit("run_data_quality_checks.py").result()


@flow(name="pgraw-retention-cleanup", log_prints=True)
def cleanup_pgraw_retention():
    run_script.submit("cleanup_pgraw_retention.py").result()
