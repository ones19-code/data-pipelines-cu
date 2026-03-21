import pandas as pd
from textblob import TextBlob
import os

DATA_DIR = "lecture6/data"


def compute_sentiment_and_merge():
    
    gold = pd.read_csv(f"{DATA_DIR}/gold_prices.csv")
    news = pd.read_csv(f"{DATA_DIR}/war_news.csv")

 
    gold["date"] = pd.to_datetime(gold["date"])
    news["date"] = pd.to_datetime(news["date"])

   
    def get_sentiment(text):
        return TextBlob(str(text)).sentiment.polarity


    news["text"] = news["title"].fillna("") + " " + news["summary"].fillna("")
    news["sentiment"] = news["text"].apply(get_sentiment)

 
    news_daily = news.groupby("date").agg(
        sentiment_mean=("sentiment", "mean"),
        news_count=("sentiment", "count")
    ).reset_index()

  
    df = pd.merge(gold, news_daily, on="date", how="left")

   
    df["sentiment_mean"] = df["sentiment_mean"].fillna(0)
    df["news_count"] = df["news_count"].fillna(0)

   
    df["target"] = (df["close"].shift(-1) > df["close"]).astype(int)

  
    df = df.dropna()

    output_path = f"{DATA_DIR}/training_data.csv"
    df.to_csv(output_path, index=False)

    print(f"Training data saved to {output_path}")