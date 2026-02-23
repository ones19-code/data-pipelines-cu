import datetime as dt
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

EVENTS_PATH = "/tmp/events/events.json"
STATS_PATH = "/tmp/stats/stats.csv"
EVENTS_URL = "http://localhost:5003/events"

dag = DAG(
    dag_id="03_with_end_date",
    schedule="@daily",  # ✅ بدل schedule_interval
    start_date=dt.datetime(year=2019, month=1, day=1),
    end_date=dt.datetime(year=2019, month=1, day=5),
    catchup=True,
)

fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command=(
        f"mkdir -p {Path(EVENTS_PATH).parent} && "
        f"curl -sf -o {EVENTS_PATH} '{EVENTS_URL}'"
    ),
    dag=dag,
)

def _calculate_stats(input_path, output_path):
    """Calculates event statistics."""
    events = pd.read_json(input_path)
    stats = events.groupby(["date", "user"]).size().reset_index(name="count")

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    stats.to_csv(output_path, index=False)

calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
    op_kwargs={"input_path": EVENTS_PATH, "output_path": STATS_PATH},
    dag=dag,
)

fetch_events >> calculate_stats