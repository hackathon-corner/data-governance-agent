"""
run_pipeline.py

Entry point for the sample data pipeline.
For now, this script will:
1. Load config
2. Load schema
3. Load raw events data
4. Print basic stats

Later, this will be orchestrated by the multi-agent system.
"""

import json
import pathlib
from typing import Dict, Any

import pandas as pd
import yaml

from src.pipeline.schema_validator import validate_schema, print_schema_validation_results


BASE_DIR = pathlib.Path(__file__).resolve().parents[2]  # repo root
DATA_DIR = BASE_DIR / "data"
CONFIG_DIR = BASE_DIR / "config"
SCHEMA_DIR = DATA_DIR / "schema"
RAW_DIR = DATA_DIR / "raw"
CURATED_DIR = DATA_DIR / "curated"
REPORTS_DIR = BASE_DIR / "reports"
LOGS_DIR = BASE_DIR / "logs"


def load_config() -> Dict[str, Any]:
    config_path = CONFIG_DIR / "pipeline_config.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_schema(schema_name: str) -> Dict[str, Any]:
    schema_path = SCHEMA_DIR / schema_name
    with open(schema_path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_events_raw(filename: str) -> pd.DataFrame:
    events_path = RAW_DIR / filename
    return pd.read_csv(events_path)


def main() -> None:
    print("=== Data Pipeline Governance: Sample Run ===")

    config = load_config()
    print("Loaded config:")
    print(config)

    events_schema = load_schema("events_schema.json")
    print("\nLoaded events schema:")
    print(events_schema)

    events_filename = config["sources"]["events"]["filename"]
    df_events = load_events_raw(events_filename)

    print(f"\nLoaded raw events data from {events_filename}")
    print(f"Rows: {len(df_events)}, Columns: {list(df_events.columns)}")

    # Stage 2 - Schema Validation
    print("\nRunning schema validation...")
    schema_results = validate_schema(df_events, events_schema)
    print_schema_validation_results(schema_results)

    # Placeholder for future stages:
    # - Data quality checks
    # - Policy/PII enforcement
    # - Transformations
    # - Load to curated
    # - Lineage & governance report

    print("\nPipeline skeleton executed successfully (no governance checks yet).")


if __name__ == "__main__":
    main()
