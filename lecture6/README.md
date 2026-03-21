# Mid-Semester Assignment – Gold & War ML Pipeline

## Overview
This project builds an ETL + ML pipeline using Apache Airflow to predict gold price movement based on war-related news sentiment.

## Features
- Gold price data via yfinance (GC=F)
- War-related news via NYT RSS
- Sentiment analysis using TextBlob
- ML classification (up/down)
- Automated weekly pipeline (Airflow)

## Schedule
schedule="@weekly"

## Model
- Features: sentiment_mean, news_count
- Target: price movement (1 = up, 0 = down)

## Accuracy
~0.58

## Notes
The DAG was tested using a 10-minute schedule before switching to weekly.