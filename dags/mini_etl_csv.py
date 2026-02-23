from datetime import datetime
from pathlib import Path
import csv

from airflow import DAG
from airflow.decorators import task

BASE_DIR = Path("/home/enovo/airflow/data")
INPUT_FILE = BASE_DIR / "sales.csv"
OUTPUT_FILE = BASE_DIR / "sales_summary.txt"

with DAG(
    dag_id="mini_etl_csv",
    start_date=datetime(2026, 2, 8),
    schedule=None,      # lancement manuel
    catchup=False,
    tags=["mini-project", "etl"],
) as dag:

    @task
    def create_input():
        BASE_DIR.mkdir(parents=True, exist_ok=True)
        rows = [
            ("date", "product", "amount"),
            ("2026-02-01", "A", "10"),
            ("2026-02-02", "B", "25"),
            ("2026-02-03", "A", "15"),
        ]
        with INPUT_FILE.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerows(rows)

    @task
    def compute_total():
        total = 0
        with INPUT_FILE.open() as f:
            reader = csv.DictReader(f)
            for row in reader:
                total += int(row["amount"])
        return total

    @task
    def write_report(total: int):
        OUTPUT_FILE.write_text(f"TOTAL SALES = {total}\n", encoding="utf-8")

    create_input() >> write_report(compute_total())
