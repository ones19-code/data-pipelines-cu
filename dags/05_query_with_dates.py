import datetime as dt
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

EVENTS_PATH = "/tmp/events/events.json"
STATS_PATH = "/tmp/stats/stats.csv"

EVENTS_URL = (
    "http://localhost:5003/events?"
    "start_date=2019-01-01&"
    "end_date=2019-01-02"
)

dag = DAG(
    dag_id="05_query_with_dates",
    schedule="@daily",
    start_date=dt.datetime(2019, 1, 1),
    end_date=dt.datetime(2019, 1, 5),
    catchup=True,
)

fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command=(
        f"mkdir -p {Path(EVENTS_PATH).parent} && "
        f"curl -sSf -o {EVENTS_PATH} '{EVENTS_URL}'"
    ),
    dag=dag,
)

def _calculate_stats(input_path, output_path):
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