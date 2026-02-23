"""
Binance Price Aggregator - Hourly Average
Reads minute-level daily_raw.csv and aggregates ONE hour window.

Reads:
- /tmp/binance/raw/{ds}/daily_raw.csv
Writes:
- /tmp/binance/hourly/{ds}/hourly_avg.csv
"""

from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

RAW_BASE = Path("/tmp/binance/raw")
HOURLY_BASE = Path("/tmp/binance/hourly")


def _calculate_hourly_average(ds: str, data_interval_start=None, data_interval_end=None, **_):
    now = datetime.now()
    start = pd.to_datetime(data_interval_start) if data_interval_start else None
    end = pd.to_datetime(data_interval_end) if data_interval_end else None

    raw_file = RAW_BASE / ds / "daily_raw.csv"
    if not raw_file.exists():
        print(f"No raw data file found at {raw_file}")
        return

    df = pd.read_csv(raw_file)
    if df.empty:
        print("Raw file is empty.")
        return

    df["fetch_time"] = pd.to_datetime(df["fetch_time"])
    df["price_float"] = df["price_float"].astype(float)

    # Filter to ONLY this hour window (best practice)
    if start is not None and end is not None:
        hour_df = df[(df["fetch_time"] >= start) & (df["fetch_time"] < end)].copy()
        hour_str = start.strftime("%H")
    else:
        # fallback
        df["hour"] = df["fetch_time"].dt.strftime("%H")
        hour_str = datetime.now().strftime("%H")
        hour_df = df[df["hour"] == hour_str].copy()

    if hour_df.empty:
        print(f"No data for hour window: {start} -> {end}")
        return

    hourly_stats = {
        "date": ds,
        "hour": hour_str,
        "avg_price": hour_df["price_float"].mean(),
        "min_price": hour_df["price_float"].min(),
        "max_price": hour_df["price_float"].max(),
        "first_price": hour_df["price_float"].iloc[0],
        "last_price": hour_df["price_float"].iloc[-1],
        "data_points": len(hour_df),
        "calculated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
    }

    out_dir = HOURLY_BASE / ds
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "hourly_avg.csv"

    hourly_df = pd.DataFrame([hourly_stats])

    # Upsert: remove same hour if exists, then append
    if out_file.exists():
        existing = pd.read_csv(out_file)
        existing = existing[existing["hour"] != hour_str]
        hourly_df = pd.concat([existing, hourly_df], ignore_index=True)

    hourly_df.to_csv(out_file, index=False)

    print(f"Saved hourly stats to: {out_file}")
    return hourly_stats


with DAG(
    dag_id="binance_calculate_hourly",
    description="Calculates hourly average Bitcoin price from minute data",
    schedule=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["binance", "crypto", "price", "hourly", "aggregation"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
) as dag:
    calculate_hourly = PythonOperator(
        task_id="calculate_hourly_average",
        python_callable=_calculate_hourly_average,
    )