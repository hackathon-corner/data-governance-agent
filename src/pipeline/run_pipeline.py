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

from src.agents.dq_agent import DataQualityAgent
from src.agents.pii_policy_agent import PiiPolicyAgent
from src.agents.schema_agent import SchemaValidationAgent
from src.pipeline.run_summary import build_run_summary, save_run_summary, print_run_summary
from src.pipeline.foreign_keys import validate_foreign_keys




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

def load_all_sources(config: Dict) -> Dict[str, pd.DataFrame]:
    """
    Load all configured source tables into a dict:
    {
      "events": <DataFrame>,
      "users": <DataFrame>,
      "courses": <DataFrame>,
      ...
    }
    """
    dfs: Dict[str, pd.DataFrame] = {}
    src_cfg = config.get("sources", {})

    if "events" in src_cfg:
        dfs["events"] = load_events_raw(src_cfg["events"]["filename"])
    if "users" in src_cfg:
        dfs["users"] = load_events_raw(src_cfg["users"]["filename"])
    if "courses" in src_cfg:
        dfs["courses"] = load_events_raw(src_cfg["courses"]["filename"])

    return dfs

def save_events_curated(df: pd.DataFrame, filename: str) -> None:
    CURATED_DIR.mkdir(parents=True, exist_ok=True)
    out_path = CURATED_DIR / filename
    df.to_csv(out_path, index=False)
    print(f"\nSaved curated events data to {out_path}")


def main():
    config = load_config()

    # Load all source tables
    dfs = load_all_sources(config)
    df_raw = dfs["events"]  # main fact table

    events_schema = load_schema("events_schema.json")
    source_filename = config["sources"]["events"]["filename"]

    print(f"Loaded raw events data from {source_filename}")
    print(f"Rows: {len(df_raw)}, Columns: {list(df_raw.columns)}")

    # === Schema validation (events only, for CLI path) ===
    schema_agent = SchemaValidationAgent()
    schema_results = schema_agent.run(df=df_raw, schema=events_schema)

    # === Data quality ===
    dq_config = config.get("data_quality", {})
    dq_agent = DataQualityAgent()
    dq_results = dq_agent.run(df=df_raw, dq_config=dq_config)

    # === PII / policy ===
    policy_config = config.get("policy", {})
    pii_agent = PiiPolicyAgent()
    pii_results = pii_agent.run(
        df=df_raw, schema=events_schema, policy_config=policy_config
    )
    df_curated = pii_results["df_curated"]

    # === Foreign-key checks (real validation) ===
    fk_config = config.get("schema", {}).get("foreign_keys", [])
    fk_results = validate_foreign_keys(dfs, fk_config)

    # Save curated data
    curated_filename = config["targets"]["events_curated"]["filename"]
    save_events_curated(df_curated, curated_filename)

    rows_in = len(df_raw)
    rows_out = len(df_curated)

    # Build + save summary
    summary = build_run_summary(
        config,
        schema_results,
        dq_results,
        pii_results,
        fk_results,          # real FK results now
        source_filename,
        curated_filename,
        rows_in,
        rows_out,
    )

    print_run_summary(summary)
    save_run_summary(summary)


if __name__ == "__main__":
    main()

