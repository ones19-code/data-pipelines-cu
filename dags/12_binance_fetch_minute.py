"""
Binance Price Fetcher - Minute Level
Fetch BTCUSDT avg price every minute and save raw data to CSV.

Writes:
- /tmp/binance/raw/{ds}/price_{HH}_{MM}.csv
- /tmp/binance/raw/{ds}/daily_raw.csv (append)
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

API_URL = "https://api.binance.com/api/v3/avgPrice?symbol=BTCUSDT"
BASE_DIR = Path("/tmp/binance/raw")


def _fetch_binance_price(ds: str, data_interval_start=None, **_):
    # Use Airflow interval start for consistent naming
    t = data_interval_start or datetime.now(timezone.utc)
    hour_str = t.strftime("%H")
    minute_str = t.strftime("%M")

    output_dir = BASE_DIR / ds
    output_dir.mkdir(parents=True, exist_ok=True)

    # Fetch from Binance
    response = requests.get(API_URL, timeout=10)
    response.raise_for_status()
    data = response.json()

    # Enrich
    now = datetime.now(timezone.utc)
    data["timestamp"] = now.isoformat()
    data["fetch_time"] = now.strftime("%Y-%m-%d %H:%M:%S")
    data["price_float"] = float(data["price"])

    df = pd.DataFrame([data])

    # 1) Individual minute file
    output_file = output_dir / f"price_{hour_str}_{minute_str}.csv"
    df.to_csv(output_file, index=False)

    # 2) Append to daily_raw.csv (without reading whole file)
    daily_file = output_dir / "daily_raw.csv"
    write_header = not daily_file.exists()
    df.to_csv(daily_file, mode="a", header=write_header, index=False)

    print(f"Fetched price: {data['price']} at {data['fetch_time']}")
    print(f"Saved minute file: {output_file}")
    print(f"Appended to: {daily_file}")

    return data


with DAG(
    dag_id="binance_fetch_minute",
    description="Fetches Bitcoin price from Binance every minute",
    schedule=timedelta(minutes=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["binance", "crypto", "price", "minute"],
    default_args={"retries": 3, "retry_delay": timedelta(minutes=1)},
) as dag:
    fetch_price = PythonOperator(
        task_id="fetch_binance_price",
        python_callable=_fetch_binance_price,
    )