import datetime as dt
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

EVENTS_DIR = "/tmp/events"
STATS_DIR = "/tmp/stats"
EVENTS_URL = "http://127.0.0.1:5003/events"

with DAG(
    dag_id="11_atomic_send",
    schedule="@daily",
    start_date=dt.datetime(2019, 1, 1),
    end_date=dt.datetime(2019, 1, 5),
    catchup=True,
) as dag:

    fetch_events = BashOperator(
        task_id="fetch_events",
        bash_command=(
            "set -euo pipefail; "
            f"mkdir -p {EVENTS_DIR} && "
            f"curl -sf -o {EVENTS_DIR}/{{{{ ds }}}}.json "
            f"'{EVENTS_URL}?start_date={{{{ ds }}}}&end_date={{{{ data_interval_end | ds }}}}'"
        ),
    )

    def _calculate_stats(ds: str, **_):
        """Calculates event statistics."""
        input_path = Path(EVENTS_DIR) / f"{ds}.json"
        output_path = Path(STATS_DIR) / f"{ds}.csv"

        events = pd.read_json(input_path)
        stats = events.groupby(["date", "user"]).size().reset_index(name="count")

        output_path.parent.mkdir(parents=True, exist_ok=True)
        stats.to_csv(output_path, index=False)

    calculate_stats = PythonOperator(
        task_id="calculate_stats",
        python_callable=_calculate_stats,
    )

    def _email_stats(stats: pd.DataFrame, email: str) -> None:
        """Send an email... (simulated)"""
        print(f"Sending stats to {email}...")

    def _send_stats(ds: str, email: str, **_):
        stats_path = Path(STATS_DIR) / f"{ds}.csv"
        stats = pd.read_csv(stats_path)
        _email_stats(stats, email=email)

    send_stats = PythonOperator(
        task_id="send_stats",
        python_callable=_send_stats,
        op_kwargs={"email": "user@example.com"},
    )

    fetch_events >> calculate_stats >> send_stats