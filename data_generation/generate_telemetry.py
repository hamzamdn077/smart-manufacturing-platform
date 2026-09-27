import pandas as pd
import numpy as np

from config import (
    OUTPUT_DIR,
    START_DATE,
    DAYS,
    TELEMETRY_INTERVAL_MINUTES,
    RANDOM_SEED
)

np.random.seed(RANDOM_SEED)


def generate_telemetry():

    machines = pd.read_csv(
        OUTPUT_DIR / "machines.csv"
    )

    timestamps = pd.date_range(
        start=START_DATE,
        periods=(DAYS * 24 * 60) // TELEMETRY_INTERVAL_MINUTES,
        freq=f"{TELEMETRY_INTERVAL_MINUTES}min"
    )

    records = []

    for _, machine in machines.iterrows():

        machine_id = machine["machine_id"]

        # Every machine starts degrading at a different point
        degradation_start = np.random.randint(
            int(len(timestamps) * 0.45),
            int(len(timestamps) * 0.85)
        )

        base_temperature = np.random.uniform(60, 75)
        base_vibration = np.random.uniform(0.2, 0.5)
        base_pressure = np.random.uniform(4, 7)

        base_power = (
            machine["rated_power_kw"] *
            np.random.uniform(0.55, 0.8)
        )

        for i, timestamp in enumerate(timestamps):

            temperature = (
                base_temperature +
                np.random.normal(0, 2)
            )

            vibration = (
                base_vibration +
                np.random.normal(0, 0.05)
            )

            pressure = (
                base_pressure +
                np.random.normal(0, 0.15)
            )

            power = (
                base_power +
                np.random.normal(0, 1.5)
            )

            status = "running"

            # Simulate machine degradation
            if i >= degradation_start:

                degradation = (
                    (i - degradation_start) /
                    (len(timestamps) - degradation_start)
                )

                temperature += degradation * 20
                vibration += degradation * 0.8
                pressure += degradation * 1.2
                power += degradation * 8

                if degradation > 0.85:
                    status = "critical"

                elif degradation > 0.65:
                    status = "warning"

            # Random idle periods
            if np.random.random() < 0.01:
                status = "idle"

            records.append({
                "timestamp": timestamp,
                "machine_id": machine_id,
                "temperature_c": round(
                    max(20, temperature), 2
                ),
                "vibration_mm_s": round(
                    max(0, vibration), 3
                ),
                "pressure_bar": round(
                    max(0, pressure), 2
                ),
                "power_consumption_kw": round(
                    max(0, power), 2
                ),
                "machine_status": status
            })

    return pd.DataFrame(records)


def main():

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df = generate_telemetry()

    path = OUTPUT_DIR / "telemetry.csv"

    df.to_csv(path, index=False)

    print(f"Created {path}")
    print(f"Records: {len(df):,}")


if __name__ == "__main__":
    main()