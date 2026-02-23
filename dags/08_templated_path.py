import datetime as dt
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

BASE_URL = "http://localhost:5003/events"

dag = DAG(
    dag_id="08_templated_path",
    schedule="@daily",
    start_date=dt.datetime(2019, 1, 1),
    end_date=dt.datetime(2019, 1, 5),
    catchup=True,
)

fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command=(
        "mkdir -p /tmp/events && "
        "curl -sSf -o /tmp/events/{{ds}}.json "
        f"\"{BASE_URL}?"
        "start_date={{ds}}&"
        "end_date={{ data_interval_end | ds }}\""
    ),
    dag=dag,
)

def _calculate_stats(**context):
    input_path = context["templates_dict"]["input_path"]
    output_path = context["templates_dict"]["output_path"]

    events = pd.read_json(input_path)
    stats = events.groupby(["date", "user"]).size().reset_index(name="count")

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    stats.to_csv(output_path, index=False)

calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
    templates_dict={
        "input_path": "/tmp/events/{{ds}}.json",
        "output_path": "/tmp/stats/{{ds}}.csv",
    },
    dag=dag,
)

fetch_events >> calculate_stats