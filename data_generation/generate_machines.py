import pandas as pd
import numpy as np

from config import (
    OUTPUT_DIR,
    NUM_MACHINES,
    START_DATE,
    RANDOM_SEED
)

np.random.seed(RANDOM_SEED)


def generate_machines():

    factories = pd.read_csv(
        OUTPUT_DIR / "factories.csv"
    )

    machine_types = [
        "CNC",
        "Assembly",
        "Packaging",
        "Lathe",
        "Milling"
    ]

    machines = []

    start_date = pd.Timestamp(START_DATE)

    for i in range(1, NUM_MACHINES + 1):

        factory = factories.iloc[
            (i - 1) % len(factories)
        ]

        machines.append({
            "machine_id": f"M{i:03d}",
            "factory_id": factory["factory_id"],
            "machine_type": np.random.choice(
                machine_types
            ),
            "installation_date": (
                start_date -
                pd.Timedelta(
                    days=np.random.randint(
                        365,
                        2500
                    )
                )
            ).date(),
            "rated_power_kw": round(
                np.random.uniform(
                    10,
                    50
                ),
                2
            ),
            "status": "active"
        })

    return pd.DataFrame(machines)


def main():

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    df = generate_machines()

    path = OUTPUT_DIR / "machines.csv"

    df.to_csv(
        path,
        index=False
    )

    print(f"Created {path}")
    print(f"Records: {len(df)}")


if __name__ == "__main__":
    main()