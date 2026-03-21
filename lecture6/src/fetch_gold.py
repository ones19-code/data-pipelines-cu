import yfinance as yf
import pandas as pd
from datetime import datetime
import os

DATA_DIR = "lecture6/data"


def fetch_gold_prices():
    
    os.makedirs(DATA_DIR, exist_ok=True)

    # Téléchargement 
    df = yf.download(
        "GC=F",
        start="2024-01-01",
        end=datetime.today().strftime('%Y-%m-%d')
    )

    
    df.reset_index(inplace=True)

  
    df = df.rename(columns={
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close"
    })

    df = df[["date", "open", "high", "low", "close"]]

  
    df["date"] = pd.to_datetime(df["date"]).dt.date

   
    output_path = f"{DATA_DIR}/gold_prices.csv"
    df.to_csv(output_path, index=False)

    print(f"Gold prices saved to {output_path}")