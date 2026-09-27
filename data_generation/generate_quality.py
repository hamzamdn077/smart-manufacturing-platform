import pandas as pd
import numpy as np

from config import (
    OUTPUT_DIR,
    RANDOM_SEED
)

np.random.seed(RANDOM_SEED)


def generate_quality():

    production = pd.read_csv(
        OUTPUT_DIR / "production_orders.csv"
    )

    telemetry = pd.read_csv(
        OUTPUT_DIR / "telemetry.csv"
    )

    telemetry["timestamp"] = pd.to_datetime(
        telemetry["timestamp"]
    )

    production["production_start"] = pd.to_datetime(
        production["production_start"]
    )

    production["production_end"] = pd.to_datetime(
        production["production_end"]
    )

    records = []

    inspection_id = 1

    for _, order in production.iterrows():

        machine_data = telemetry[
            telemetry["machine_id"] ==
            order["machine_id"]
        ]

        relevant = machine_data[
            (machine_data["timestamp"] >= order["production_start"]) &
            (machine_data["timestamp"] <= order["production_end"])
        ]

        if relevant.empty:
            avg_vibration = 0.4
            avg_temperature = 70

        else:
            avg_vibration = (
                relevant["vibration_mm_s"].mean()
            )

            avg_temperature = (
                relevant["temperature_c"].mean()
            )

        # Base defect probability
        defect_probability = 0.02

        # Poor machine conditions increase defects
        if avg_vibration > 0.8:
            defect_probability += 0.15

        if avg_temperature > 85:
            defect_probability += 0.15

        passed = (
            np.random.random() >
            defect_probability
        )

        if passed:

            quality_score = np.random.uniform(
                85,
                100
            )

            defect_type = "None"

        else:

            quality_score = np.random.uniform(
                40,
                84
            )

            defect_type = np.random.choice([
                "Dimensional Error",
                "Surface Defect",
                "Assembly Error",
                "Material Defect",
                "Packaging Defect"
            ])

        records.append({
            "inspection_id": f"QI{inspection_id:06d}",
            "production_id": order["production_id"],
            "machine_id": order["machine_id"],
            "inspection_timestamp": order["production_end"],
            "quality_score": round(
                quality_score,
                2
            ),
            "defect_type": defect_type,
            "passed": passed
        })

        inspection_id += 1

    return pd.DataFrame(records)


def main():

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df = generate_quality()

    path = OUTPUT_DIR / "quality_inspections.csv"

    df.to_csv(path, index=False)

    print(f"Created {path}")
    print(f"Records: {len(df):,}")


if __name__ == "__main__":
    main()