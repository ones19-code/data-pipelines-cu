"""
Lecture 4 - Exercise: StockSense Wikipedia Pageviews ETL

Complete ETL pipeline that fetches Wikipedia pageviews for tracked companies
and saves to CSV. Uses Jinja templating for dynamic date handling.

Pipeline: get_data → extract_gz → fetch_pageviews → add_to_db

Data source: https://dumps.wikimedia.org/other/pageviews/
Format: domain_code page_title view_count response_size (space-separated)
"""

from pathlib import Path
import pendulum

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

PAGENAMES = {"Google", "Amazon", "Apple", "Microsoft", "Facebook"}
OUTPUT_DIR = "/data/stocksense/pageview_counts"


def _get_data(year, month, day, hour, output_path, **_):
    """Download Wikipedia pageviews for the given hour (templated op_kwargs)."""
    from urllib import request

    url = (
        f"https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{int(month):02d}/"
        f"pageviews-{year}{int(month):02d}{int(day):02d}-{int(hour):02d}0000.gz"
    )
    print(f"Downloading {url}")
    request.urlretrieve(url, output_path)


def _fetch_pageviews(pagenames, logical_date, **context):
    """
    Parse pageviews file, extract counts for tracked companies, save to CSV.
    logical_date is injected by Airflow from task context (replacement of execution_date).
    output_path comes from templates_dict (date-partitioned path).
    """
    result = dict.fromkeys(pagenames, 0)

    with open("/tmp/wikipageviews", "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) >= 4:
                domain_code, page_title, view_count = parts[0], parts[1], parts[2]
                if domain_code == "en" and page_title in pagenames:
                    result[page_title] = int(view_count)

    output_path = context["templates_dict"]["output_path"]
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    # Stable datetime string like: 2026-02-28 10:00:00+00:00
    dt_str = logical_date.to_datetime_string() + "+00:00"

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("pagename,pageviewcount,datetime\n")
        for pagename, count in result.items():
            f.write(f'"{pagename}",{count},{dt_str}\n')

    print(f"Saved pageview counts to {output_path}")
    print(f"Counts: {result}")
    return result


def _add_to_db(**context):
    """
    Read the CSV at templates_dict['output_path'] and insert into SQLite.
    Table: pageviews(pagename, pageviewcount, datetime) with PK(pagename, datetime)
    """
    import csv
    import os
    import sqlite3

    output_path = context["templates_dict"]["output_path"]
    if not os.path.exists(output_path):
        raise FileNotFoundError(f"CSV not found: {output_path}")

    db_path = "/data/stocksense/stocksense.db"
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS pageviews (
                pagename TEXT NOT NULL,
                pageviewcount INTEGER NOT NULL,
                datetime TEXT NOT NULL,
                PRIMARY KEY (pagename, datetime)
            )
            """
        )

        with open(output_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = []
            for row in reader:
                rows.append(
                    (
                        row["pagename"].strip('"'),
                        int(row["pageviewcount"]),
                        row["datetime"],
                    )
                )

        cur.executemany(
            "INSERT OR REPLACE INTO pageviews (pagename, pageviewcount, datetime) VALUES (?, ?, ?)",
            rows,
        )
        conn.commit()
        print(f"Inserted/updated {len(rows)} rows into {db_path}")
    finally:
        conn.close()


dag = DAG(
    dag_id="lecture4_stocksense_exercise",
    # choose a past start_date
    start_date=pendulum.datetime(2026, 2, 27, tz="UTC"),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["lecture4", "exercise", "stocksense", "etl"],
)

get_data = PythonOperator(
    task_id="get_data",
    python_callable=_get_data,
    op_kwargs={
        "year": "{{ logical_date.year }}",
        "month": "{{ logical_date.month }}",
        "day": "{{ logical_date.day }}",
        "hour": "{{ logical_date.hour }}",
        "output_path": "/tmp/wikipageviews.gz",
    },
    dag=dag,
)

extract_gz = BashOperator(
    task_id="extract_gz",
    bash_command="gunzip -f /tmp/wikipageviews.gz",
    dag=dag,
)

fetch_pageviews = PythonOperator(
    task_id="fetch_pageviews",
    python_callable=_fetch_pageviews,
    op_kwargs={"pagenames": PAGENAMES},
    templates_dict={"output_path": f"{OUTPUT_DIR}/{{{{ ds }}}}.csv"},
    dag=dag,
)

add_to_db = PythonOperator(
    task_id="add_to_db",
    python_callable=_add_to_db,
    templates_dict={"output_path": f"{OUTPUT_DIR}/{{{{ ds }}}}.csv"},
    dag=dag,
)

get_data >> extract_gz >> fetch_pageviews >> add_to_db