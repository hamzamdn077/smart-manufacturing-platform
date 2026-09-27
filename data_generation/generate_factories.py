import pandas as pd
from config import OUTPUT_DIR


def generate_factories():

    factories = [
        {
            "factory_id": "F01",
            "factory_name": "Casablanca Manufacturing Plant",
            "city": "Casablanca",
            "country": "Morocco",
            "latitude": 33.5731,
            "longitude": -7.5898
        },
        {
            "factory_id": "F02",
            "factory_name": "Tangier Manufacturing Plant",
            "city": "Tangier",
            "country": "Morocco",
            "latitude": 35.7595,
            "longitude": -5.8340
        },
        {
            "factory_id": "F03",
            "factory_name": "Fez Manufacturing Plant",
            "city": "Fez",
            "country": "Morocco",
            "latitude": 34.0331,
            "longitude": -5.0003
        }
    ]

    return pd.DataFrame(factories)


def main():

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    df = generate_factories()

    path = OUTPUT_DIR / "factories.csv"

    df.to_csv(
        path,
        index=False
    )

    print(f"Created {path}")
    print(f"Records: {len(df)}")


if __name__ == "__main__":
    main()