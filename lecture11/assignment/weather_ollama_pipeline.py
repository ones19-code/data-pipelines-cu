from datetime import datetime
import json
import os
import requests

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from jsonschema import validate


OUTPUT_DIR = os.path.expanduser("~/lecture11-airflow-ollama/output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

WEATHER_URL = (
    "https://api.open-meteo.com/v1/forecast"
    "?latitude=53.0793&longitude=8.8017"
    "&current=temperature_2m,relative_humidity_2m,wind_speed_10m,weather_code"
)

OLLAMA_URL = "http://127.0.0.1:11434/api/generate"
OLLAMA_MODEL = "tinyllama"

SCHEMA = {
    "type": "object",
    "properties": {
        "city": {"type": "string"},
        "temperature_c": {"type": "number"},
        "humidity_percent": {"type": "number"},
        "wind_speed_kmh": {"type": "number"},
        "weather_code": {"type": "number"},
        "summary": {"type": "string"},
    },
    "required": [
        "city",
        "temperature_c",
        "humidity_percent",
        "wind_speed_kmh",
        "weather_code",
        "summary",
    ],
}


def fetch_weather(**context):
    response = requests.get(WEATHER_URL, timeout=30)
    response.raise_for_status()
    weather_data = response.json()
    context["ti"].xcom_push(key="raw_weather", value=weather_data)


def transform_with_ollama(**context):
    raw_weather = context["ti"].xcom_pull(
        key="raw_weather",
        task_ids="fetch_weather",
    )

    current = raw_weather["current"]

    temperature_c = float(current["temperature_2m"])
    humidity_percent = float(current["relative_humidity_2m"])
    wind_speed_kmh = float(current["wind_speed_10m"])
    weather_code = float(current["weather_code"])

    prompt = f"""
Write one short plain-English weather summary in one sentence.
No markdown.
No JSON.
No bullet points.

Weather data:
- City: Bremen
- Temperature: {temperature_c} C
- Humidity: {humidity_percent} %
- Wind speed: {wind_speed_kmh} km/h
- Weather code: {weather_code}
"""

    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
    }

    response = requests.post(OLLAMA_URL, json=payload, timeout=120)
    response.raise_for_status()
    result = response.json()

    summary = result.get("response", "").strip()
    if not summary:
        summary = "Weather conditions are available but no summary was generated."

    structured = {
        "city": "Bremen",
        "temperature_c": temperature_c,
        "humidity_percent": humidity_percent,
        "wind_speed_kmh": wind_speed_kmh,
        "weather_code": weather_code,
        "summary": summary,
    }

    context["ti"].xcom_push(key="structured_weather", value=structured)


def validate_json_task(**context):
    structured = context["ti"].xcom_pull(
        key="structured_weather",
        task_ids="transform_with_ollama",
    )
    validate(instance=structured, schema=SCHEMA)


def save_output(**context):
    structured = context["ti"].xcom_pull(
        key="structured_weather",
        task_ids="transform_with_ollama",
    )

    output_file = os.path.join(
        OUTPUT_DIR,
        f"weather_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
    )

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(structured, f, indent=2)

    print(f"Saved: {output_file}")


with DAG(
    dag_id="weather_ollama_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["lecture11", "airflow", "ollama", "weather"],
) as dag:

    fetch_weather_task = PythonOperator(
        task_id="fetch_weather",
        python_callable=fetch_weather,
    )

    transform_task = PythonOperator(
        task_id="transform_with_ollama",
        python_callable=transform_with_ollama,
    )

    validate_task = PythonOperator(
        task_id="validate_json",
        python_callable=validate_json_task,
    )

    save_task = PythonOperator(
        task_id="save_output",
        python_callable=save_output,
    )

    fetch_weather_task >> transform_task >> validate_task >> save_task