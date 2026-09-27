import pandas as pd
import numpy as np

from config import (
    OUTPUT_DIR,
    NUM_PRODUCTS,
    START_DATE,
    DAYS,
    RANDOM_SEED
)

np.random.seed(RANDOM_SEED)


def generate_production():

    machines = pd.read_csv(
        OUTPUT_DIR / "machines.csv"
    )

    products = [
        f"P{i:03d}"
        for i in range(1, NUM_PRODUCTS + 1)
    ]

    records = []

    production_id = 1

    start_date = pd.Timestamp(START_DATE)

    for day in range(DAYS):

        current_date = (
            start_date +
            pd.Timedelta(days=day)
        )

        for _, machine in machines.iterrows():

            # Some machines are not used every day
            if np.random.random() < 0.15:
                continue

            number_of_orders = np.random.randint(2, 5)

            for _ in range(number_of_orders):

                start_hour = np.random.randint(6, 20)
                duration_hours = np.random.randint(1, 5)

                production_start = (
                    current_date +
                    pd.Timedelta(hours=start_hour)
                )

                production_end = (
                    production_start +
                    pd.Timedelta(hours=duration_hours)
                )

                quantity = np.random.randint(
                    100,
                    1000
                )

                records.append({
                    "production_id": f"PR{production_id:06d}",
                    "machine_id": machine["machine_id"],
                    "product_id": np.random.choice(products),
                    "production_start": production_start,
                    "production_end": production_end,
                    "quantity_produced": quantity
                })

                production_id += 1

    return pd.DataFrame(records)


def main():

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df = generate_production()

    path = OUTPUT_DIR / "production_orders.csv"

    df.to_csv(path, index=False)

    print(f"Created {path}")
    print(f"Records: {len(df):,}")


if __name__ == "__main__":
    main()