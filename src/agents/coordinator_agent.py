"""
coordinator_agent.py

High-level coordinator that runs the full governance pipeline
using the other agents.
"""

from typing import Any, Dict, Optional

import pandas as pd

from src.agents.base_agent import BaseAgent
from src.agents.schema_agent import SchemaValidationAgent
from src.agents.dq_agent import DataQualityAgent
from src.agents.pii_policy_agent import PiiPolicyAgent
from src.agents.run_summary_agent import RunSummaryAgent

# Reuse the I/O helpers from run_pipeline
from src.pipeline.run_pipeline import (
    load_config,
    load_schema,
    save_events_curated,
    load_all_sources
)
from src.pipeline.foreign_keys import validate_foreign_keys

class CoordinatorAgent(BaseAgent):
    def __init__(self) -> None:
        super().__init__(
            name="CoordinatorAgent",
            description="Coordinates schema, DQ, and PII agents to run the full pipeline.",
        )
        self.schema_agent = SchemaValidationAgent()
        self.dq_agent = DataQualityAgent()
        self.pii_agent = PiiPolicyAgent()
        self.summary_agent = RunSummaryAgent()

    def run(self, config_override: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        print("=== CoordinatorAgent: Starting governance pipeline run ===")

        # Load config
        if config_override is not None:
            config = config_override
        else:
            config = load_config()

        # Load all sources
        dfs = load_all_sources(config)
        df_events = dfs["events"]

        # events_schema = load_schema("events_schema.json")
        # events_filename = config["sources"]["events"]["filename"]

        # print(f"\nLoaded raw events data from {events_filename}")
        # print(f"Rows: {len(df_events)}, Columns: {list(df_events.columns)}")

        # # Schema
        # schema_results = self.schema_agent.run(df=df_events, schema=events_schema)

        events_schema = load_schema("events_schema.json")
        users_schema = load_schema("users_schema.json")
        courses_schema = load_schema("courses_schema.json")

        dfs = load_all_sources(config)
        df_events = dfs["events"]
        df_users = dfs.get("users")
        df_courses = dfs.get("courses")

        events_filename = config["sources"]["events"]["filename"]

        print(f"\nLoaded raw events data from {events_filename}")
        print(f"Rows: {len(df_events)}, Columns: {list(df_events.columns)}")

        schema_tables: Dict[str, Dict[str, Any]] = {}

        events_schema_result = self.schema_agent.run(df=df_events, schema=events_schema)
        schema_tables["events"] = events_schema_result

        if df_users is not None:
            users_schema_result = self.schema_agent.run(df=df_users, schema=users_schema)
            schema_tables["users"] = users_schema_result

        if df_courses is not None:
            courses_schema_result = self.schema_agent.run(df=df_courses, schema=courses_schema)
            schema_tables["courses"] = courses_schema_result

        schema_passed = all(tbl.get("passed") for tbl in schema_tables.values())

        schema_results = {
            "passed": schema_passed,
            "tables": schema_tables,
        }


        # Data quality
        dq_config = config.get("data_quality", {})
        dq_results = self.dq_agent.run(df=df_events, dq_config=dq_config)

        # PII / policy
        policy_config = config.get("policy", {})
        pii_results = self.pii_agent.run(
            df=df_events, schema=events_schema, policy_config=policy_config
        )
        df_curated = pii_results["df_curated"]

        # Foreign keys
        fk_config = config.get("schema", {}).get("foreign_keys", [])
        fk_results = validate_foreign_keys(dfs, fk_config)

        # Save curated
        curated_filename = config["targets"]["events_curated"]["filename"]
        save_events_curated(df_curated, curated_filename)

        rows_in = len(df_events)
        rows_out = len(df_curated)

        summary_result = self.summary_agent.run(
            config=config,
            schema_results=schema_results,
            dq_results=dq_results,
            pii_results=pii_results,
            fk_results=fk_results,
            source_filename=events_filename,
            curated_filename=curated_filename,
            rows_in=rows_in,
            rows_out=rows_out,
        )

        overall_passed = summary_result["summary"]["overall_passed"]

        print(
            "\n=== CoordinatorAgent: Finished run "
            f"(overall_passed={overall_passed}) ==="
        )

        return {
            "config": config,
            "schema_results": schema_results,
            "dq_results": dq_results,
            "pii_results": pii_results,
            "fk_results": fk_results,
            "summary": summary_result["summary"],
        }


