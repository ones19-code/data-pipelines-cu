# Lecture 11: Airflow + Ollama — Unstructured to Structured Data

This project implements an Apache Airflow pipeline that fetches raw weather data from the Open-Meteo API, sends it to Ollama for transformation into a stable JSON schema, validates the result, and stores the structured output.

## Pipeline Steps

1. Fetch weather data from Open-Meteo
2. Send the raw response to Ollama
3. Convert the response into structured JSON
4. Validate the JSON schema
5. Save the final output to a file

## Requirements

- Python 3.10+
- Apache Airflow 2.x
- Ollama running locally
- tinyllama model pulled locally

## Run Ollama

```bash
ollama serve