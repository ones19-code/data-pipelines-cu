import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
import joblib
import os

DATA_DIR = "lecture6/data"
MODEL_DIR = f"{DATA_DIR}/models"


def train_model():
    
    os.makedirs(MODEL_DIR, exist_ok=True)

   
    df = pd.read_csv(f"{DATA_DIR}/training_data.csv")

  
    X = df[["sentiment_mean", "news_count"]]

   
    y = df["target"]

   
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, shuffle=False
    )

  
    model = RandomForestClassifier(n_estimators=100, random_state=42)

 
    model.fit(X_train, y_train)

    
    accuracy = model.score(X_test, y_test)
    print(f"Model accuracy: {accuracy:.2f}")

  
    model_path = f"{MODEL_DIR}/gold_model.pkl"
    joblib.dump(model, model_path)

    print(f"Model saved to {model_path}")