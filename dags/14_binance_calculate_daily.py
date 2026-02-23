"""
Binance Price Aggregator - Daily Average
Reads hourly_avg.csv and computes daily metrics.

Reads:
- /tmp/binance/hourly/{ds}/hourly_avg.csv
Writes:
- /tmp/binance/daily/daily_avg.csv
"""

from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

HOURLY_BASE = Path("/tmp/binance/hourly")
DAILY_DIR = Path("/tmp/binance/daily")


def _calculate_daily_average(ds: str, **_):
    now = datetime.now()

    hourly_file = HOURLY_BASE / ds / "hourly_avg.csv"
    if not hourly_file.exists():
        print(f"No hourly data file found at {hourly_file}")
        return

    df = pd.read_csv(hourly_file)
    if df.empty:
        print(f"No hourly data found for {ds}")
        return

    # Ensure correct ordering by hour
    df["hour"] = df["hour"].astype(str).str.zfill(2)
    df = df.sort_values("hour")

    opening_price = float(df["first_price"].iloc[0])
    closing_price = float(df["last_price"].iloc[-1])

    daily_stats = {
        "date": ds,
        "avg_price": df["avg_price"].mean(),
        "min_price": df["min_price"].min(),
        "max_price": df["max_price"].max(),
        "opening_price": opening_price,
        "closing_price": closing_price,
        "price_change": closing_price - opening_price,
        "price_change_pct": ((closing_price - opening_price) / opening_price) * 100 if opening_price else 0.0,
        "total_data_points": int(df["data_points"].sum()),
        "hours_with_data": int(len(df)),
        "calculated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
    }

    DAILY_DIR.mkdir(parents=True, exist_ok=True)
    out_file = DAILY_DIR / "daily_avg.csv"

    daily_df = pd.DataFrame([daily_stats])

    # Upsert by date
    if out_file.exists():
        existing = pd.read_csv(out_file)
        existing = existing[existing["date"] != ds]
        daily_df = pd.concat([existing, daily_df], ignore_index=True)

    daily_df = daily_df.sort_values("date")
    daily_df.to_csv(out_file, index=False)

    print(f"Saved daily stats to: {out_file}")
    return daily_stats


with DAG(
    dag_id="binance_calculate_daily",
    description="Calculates daily average Bitcoin price from hourly data",
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["binance", "crypto", "price", "daily", "aggregation"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
) as dag:
    calculate_daily = PythonOperator(
        task_id="calculate_daily_average",
        python_callable=_calculate_daily_average,
    )