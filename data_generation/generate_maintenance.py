import pandas as pd
import numpy as np

from config import (
    OUTPUT_DIR,
    NUM_TECHNICIANS,
    RANDOM_SEED
)

np.random.seed(RANDOM_SEED)


def generate_maintenance():

    machines = pd.read_csv(
        OUTPUT_DIR / "machines.csv"
    )

    telemetry = pd.read_csv(
        OUTPUT_DIR / "telemetry.csv"
    )

    telemetry["timestamp"] = pd.to_datetime(
        telemetry["timestamp"]
    )

    records = []

    maintenance_id = 1

    for _, machine in machines.iterrows():

        machine_id = machine["machine_id"]

        machine_data = telemetry[
            telemetry["machine_id"] == machine_id
        ]

        # High vibration indicates possible failure
        problematic = machine_data[
            machine_data["vibration_mm_s"] > 0.9
        ]

        if problematic.empty:
            continue

        # Maximum 3 maintenance events per machine
        number_of_events = min(
            3,
            len(problematic)
        )

        selected = problematic.sample(
            n=number_of_events,
            random_state=RANDOM_SEED
        )

        for _, event in selected.iterrows():

            maintenance_type = np.random.choice([
                "Preventive",
                "Corrective",
                "Inspection"
            ])

            failure_type = np.random.choice([
                "Bearing Wear",
                "Motor Overheating",
                "Excessive Vibration",
                "Pressure Anomaly"
            ])

            records.append({
                "maintenance_id": f"MT{maintenance_id:06d}",
                "machine_id": machine_id,
                "maintenance_date": event["timestamp"],
                "maintenance_type": maintenance_type,
                "failure_type": failure_type,
                "downtime_minutes": np.random.randint(
                    30, 360
                ),
                "technician": (
                    f"TECH{np.random.randint(1, NUM_TECHNICIANS + 1):02d}"
                ),
                "cost": round(
                    np.random.uniform(100, 2500),
                    2
                )
            })

            maintenance_id += 1

    return pd.DataFrame(records)


def main():

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df = generate_maintenance()

    path = OUTPUT_DIR / "maintenance.csv"

    df.to_csv(path, index=False)

    print(f"Created {path}")
    print(f"Records: {len(df):,}")


if __name__ == "__main__":
    main()