from pathlib import Path

# Output directory
OUTPUT_DIR = Path(__file__).parent / "output"

# Factory configuration
NUM_MACHINES = 20
NUM_PRODUCTS = 8
NUM_TECHNICIANS = 10

# Generation period
START_DATE = "2026-01-01"
DAYS = 30

# Telemetry
TELEMETRY_INTERVAL_MINUTES = 5

# Reproducibility
RANDOM_SEED = 42