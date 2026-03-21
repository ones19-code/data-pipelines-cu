from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import sys

# Permet d'importer ton projet
sys.path.append("/home/enovo/airflow")

from lecture6.src.fetch_gold import fetch_gold_prices
from lecture6.src.fetch_news import fetch_war_news
from lecture6.src.process_data import compute_sentiment_and_merge
from lecture6.src.train_model import train_model


with DAG(
    dag_id="gold_war_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@weekly"
    catchup=False,
    description="Gold price prediction using war news sentiment",
) as dag:

    t1 = PythonOperator(
        task_id="fetch_gold_prices",
        python_callable=fetch_gold_prices,
    )

    t2 = PythonOperator(
        task_id="fetch_war_news",
        python_callable=fetch_war_news,
    )

    t3 = PythonOperator(
        task_id="compute_sentiment_and_merge",
        python_callable=compute_sentiment_and_merge,
    )

    t4 = PythonOperator(
        task_id="train_model",
        python_callable=train_model,
    )

    # ordre des tâches
    t1 >> t2 >> t3 >> t4