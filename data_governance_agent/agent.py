from google.adk.agents.llm_agent import Agent

# Import your existing coordinator that runs the whole pipeline
from src.agents.coordinator_agent import CoordinatorAgent


def run_governance_pipeline() -> dict:
    """
    ADK tool:
    Runs the full governance pipeline (schema, DQ, PII, summary)
    and returns a compact result for the LLM to reason about.
    """
    coordinator = CoordinatorAgent()
    result = coordinator.run()

    summary = result["summary"]
    checks = summary["checks"]

    return {
        "status": "success",
        "overall_passed": summary["overall_passed"],
        "schema_passed": checks["schema"]["passed"],
        "dq_passed": checks["data_quality"]["passed"],
        "pii_passed": checks["pii_policy"]["passed"],
        "rows_in": summary["lineage"]["source"]["rows_in"],
        "rows_out": summary["lineage"]["target"]["rows_out"],
        "source_filename": summary["lineage"]["source"]["filename"],
        "target_filename": summary["lineage"]["target"]["filename"],
        "schema_issues": {
            "missing_columns": checks["schema"]["missing_columns"],
            "extra_columns": checks["schema"]["extra_columns"],
            "invalid_values": checks["schema"]["invalid_values"],
        },
        "dq_issues": {
            "columns_exceeding_null_threshold": checks["data_quality"]["columns_exceeding_null_threshold"],
            "non_null_violations": checks["data_quality"]["non_null_violations"],
            "unique_key_violations": checks["data_quality"]["unique_key_violations"],
            "invalid_event_types": checks["data_quality"]["invalid_event_types"],
        },
        "pii_issues": {
            "detected_pii_columns": checks["pii_policy"]["detected_pii_columns"],
            "removed_pii_columns": checks["pii_policy"]["removed_pii_columns"],
            "remaining_pii_in_curated": checks["pii_policy"]["remaining_pii_in_curated"],
        },
        "note": "See JSON summary in the reports/ directory for full details.",
    }


root_agent = Agent(
    model="gemini-2.5-flash",
    name="data_governance_root",
    description="Root agent for the Data Pipeline Governance capstone.",
    instruction=(
        "You are a data governance assistant for an event analytics pipeline. "
        "Use the 'run_governance_pipeline' tool to execute schema, data quality, "
        "and PII checks. Then:\n"
        "1) Clearly state whether the run PASSED or FAILED overall.\n"
        "2) If it failed, summarize the key schema/data-quality/PII issues.\n"
        "3) Suggest concrete next steps to fix the data or configuration.\n"
        "Keep explanations concise and non-technical when possible."
    ),
    tools=[run_governance_pipeline],
)
