import feedparser
import pandas as pd
from datetime import datetime
import os

DATA_DIR = "lecture6/data"


def fetch_war_news():
    os.makedirs(DATA_DIR, exist_ok=True)

    url = "https://rss.nytimes.com/services/xml/rss/nyt/World.xml"
    feed = feedparser.parse(url)

    keywords = ["war", "conflict", "attack", "military", "invasion"]

    data = []

    for entry in feed.entries:
        text = (entry.title + " " + entry.summary).lower()

        if any(keyword in text for keyword in keywords):
            date = datetime(*entry.published_parsed[:6]).date()

            data.append({
                "date": date,
                "title": entry.title,
                "summary": entry.summary
            })

    df = pd.DataFrame(data)

    output_path = f"{DATA_DIR}/war_news.csv"
    df.to_csv(output_path, index=False)

    print(f"War news saved to {output_path}")