import argparse
import joblib
from pathlib import Path
import pandas as pd


def test_model(model_path: Path, data_dir: Path):
    # Charger modèle
    model = joblib.load(model_path)

    # Charger données
    df = pd.read_csv(data_dir / "training_data.csv")

    X = df[["sentiment_mean", "news_count"]]
    y = df["target"]

    accuracy = model.score(X, y)
    preds = model.predict(X)

    sample = df[["date", "close", "target"]].head(10).copy()
    sample["predicted"] = preds[:10]

    return accuracy, sample


def main():
    parser = argparse.ArgumentParser(description="Test model")

    parser.add_argument(
        "--model",
        type=Path,
        default=Path("lecture6/gold_model.pkl"),
    )

    parser.add_argument(
        "--data",
        type=Path,
        default=Path("lecture6/data"),
    )

    args = parser.parse_args()

    accuracy, sample = test_model(args.model, args.data)

    print("\n=== Model Test Results ===")
    print(f"Accuracy: {accuracy:.3f}")
    print("\nSample predictions:")
    print(sample.to_string(index=False))


if __name__ == "__main__":
    main()