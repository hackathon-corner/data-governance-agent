from google.adk.agents.llm_agent import Agent

# Import your existing coordinator that runs the whole pipeline
from src.agents.coordinator_agent import CoordinatorAgent
from google.adk.agents import LlmAgent  # we'll use SequentialAgent later

from src.agents.coordinator_agent import CoordinatorAgent
from src.agents.schema_agent import SchemaValidationAgent
from src.agents.dq_agent import DataQualityAgent
from src.agents.pii_policy_agent import PiiPolicyAgent
from src.pipeline.run_pipeline import load_config, load_schema, load_events_raw


GEMINI_MODEL = "gemini-2.5-flash"


# def run_governance_pipeline() -> dict:
#     """
#     ADK tool:
#     Runs the full governance pipeline (schema, DQ, PII, summary)
#     and returns a compact result for the LLM to reason about.
#     """
#     coordinator = CoordinatorAgent()
#     result = coordinator.run()

#     summary = result["summary"]
#     checks = summary["checks"]

#     return {
#         "status": "success",
#         "overall_passed": summary["overall_passed"],
#         "schema_passed": checks["schema"]["passed"],
#         "dq_passed": checks["data_quality"]["passed"],
#         "pii_passed": checks["pii_policy"]["passed"],
#         "rows_in": summary["lineage"]["source"]["rows_in"],
#         "rows_out": summary["lineage"]["target"]["rows_out"],
#         "source_filename": summary["lineage"]["source"]["filename"],
#         "target_filename": summary["lineage"]["target"]["filename"],
#         "schema_issues": {
#             "missing_columns": checks["schema"]["missing_columns"],
#             "extra_columns": checks["schema"]["extra_columns"],
#             "invalid_values": checks["schema"]["invalid_values"],
#         },
#         "dq_issues": {
#             "columns_exceeding_null_threshold": checks["data_quality"]["columns_exceeding_null_threshold"],
#             "non_null_violations": checks["data_quality"]["non_null_violations"],
#             "unique_key_violations": checks["data_quality"]["unique_key_violations"],
#             "invalid_event_types": checks["data_quality"]["invalid_event_types"],
#         },
#         "pii_issues": {
#             "detected_pii_columns": checks["pii_policy"]["detected_pii_columns"],
#             "removed_pii_columns": checks["pii_policy"]["removed_pii_columns"],
#             "remaining_pii_in_curated": checks["pii_policy"]["remaining_pii_in_curated"],
#         },
#         "note": "See JSON summary in the reports/ directory for full details.",
#     }


# root_agent = Agent(
#     model="gemini-2.5-flash",
#     name="data_governance_root",
#     description="Root agent for the Data Pipeline Governance capstone.",
#     instruction=(
#         "You are a data governance assistant for an event analytics pipeline. "
#         "Use the 'run_governance_pipeline' tool to execute schema, data quality, "
#         "and PII checks. Then:\n"
#         "1) Clearly state whether the run PASSED or FAILED overall.\n"
#         "2) If it failed, summarize the key schema/data-quality/PII issues.\n"
#         "3) Suggest concrete next steps to fix the data or configuration.\n"
#         "Keep explanations concise and non-technical when possible."
#     ),
#     tools=[run_governance_pipeline],
# )

def run_full_governance_pipeline() -> dict:
    coordinator = CoordinatorAgent()
    result = coordinator.run()
    summary = result["summary"]
    checks = summary["checks"]
    return {
        "mode": "full",
        "status": "success",
        "overall_passed": summary["overall_passed"],
        "schema_passed": checks["schema"]["passed"],
        "dq_passed": checks["data_quality"]["passed"],
        "pii_passed": checks["pii_policy"]["passed"],
        "rows_in": summary["lineage"]["source"]["rows_in"],
        "rows_out": summary["lineage"]["target"]["rows_out"],
        "source_filename": summary["lineage"]["source"]["filename"],
        "target_filename": summary["lineage"]["target"]["filename"],
    }


def run_schema_checks_only() -> dict:
    config = load_config()
    schema = load_schema("events_schema.json")
    events_filename = config["sources"]["events"]["filename"]
    df = load_events_raw(events_filename)

    agent = SchemaValidationAgent()
    results = agent.run(df=df, schema=schema)

    return {
        "mode": "schema_only",
        "status": "success",
        "schema_passed": results["passed"],
        "missing_columns": results["missing_columns"],
        "extra_columns": results["extra_columns"],
        "invalid_values": results["invalid_values"],
        "source_filename": events_filename,
    }


def run_data_quality_checks_only() -> dict:
    config = load_config()
    events_filename = config["sources"]["events"]["filename"]
    df = load_events_raw(events_filename)

    dq_config = config.get("data_quality", {})
    agent = DataQualityAgent()
    results = agent.run(df=df, dq_config=dq_config)

    return {
        "mode": "dq_only",
        "status": "success",
        "dq_passed": results["passed"],
        "null_fractions": results["null_fractions"],
        "columns_exceeding_null_threshold": results["columns_exceeding_null_threshold"],
        "non_null_violations": results["non_null_violations"],
        "unique_key_violations": results["unique_key_violations"],
        "invalid_event_types": results["invalid_event_types"],
        "source_filename": events_filename,
    }


def run_pii_policy_checks_only() -> dict:
    config = load_config()
    schema = load_schema("events_schema.json")
    events_filename = config["sources"]["events"]["filename"]
    df = load_events_raw(events_filename)

    policy_config = config.get("policy", {})
    agent = PiiPolicyAgent()
    results = agent.run(df=df, schema=schema, policy_config=policy_config)

    return {
        "mode": "pii_only",
        "status": "success",
        "pii_passed": results["passed"],
        "detected_pii_columns": results["detected_pii_columns"],
        "removed_pii_columns": results["removed_pii_columns"],
        "remaining_pii_in_curated": results["remaining_pii_in_curated"],
        "source_filename": events_filename,
    }

schema_llm_agent = LlmAgent(
    name="SchemaAgent",
    model=GEMINI_MODEL,
    description=(
        "Specialist agent that validates event data schema: required columns, "
        "extra columns, and allowed values."
    ),
    instruction=(
        "You are a schema governance specialist for an event analytics pipeline.\n"
        "Call the 'run_schema_checks_only' tool exactly once to inspect the data. "
        "Then:\n"
        "1) Summarize whether the schema passed or failed.\n"
        "2) List any missing or extra columns and invalid values.\n"
        "3) Suggest specific schema changes or data fixes.\n"
        "Write a concise JSON-style summary and explanation; "
        "this will be used by downstream agents."
    ),
    tools=[run_schema_checks_only],
    # ADK will store the final LLM response in session.state['schema_summary']
    output_key="schema_summary",
)

dq_llm_agent = LlmAgent(
    name="DataQualityAgent",
    model=GEMINI_MODEL,
    description=(
        "Specialist agent that analyzes data quality issues such as nulls, "
        "non-null constraints, uniqueness, and invalid categories."
    ),
    instruction=(
        "You are a data quality specialist for event analytics data.\n"
        "Use the 'run_data_quality_checks_only' tool exactly once. Then:\n"
        "1) Explain whether data quality checks passed or failed.\n"
        "2) Highlight which columns violated thresholds (nulls, uniqueness, etc.).\n"
        "3) Recommend concrete remediation steps (e.g., drop rows, backfill values, "
        "tighten constraints).\n"
        "Assume that another agent may have already run schema checks; "
        "if 'schema_summary' is present in state, you may reference it in your reasoning.\n"
        "Produce a compact summary that downstream agents can read."
    ),
    tools=[run_data_quality_checks_only],
    output_key="dq_summary",
)

pii_llm_agent = LlmAgent(
    name="PiiAgent",
    model=GEMINI_MODEL,
    description=(
        "Specialist agent that focuses on PII detection and enforcement rules "
        "for the analytics pipeline."
    ),
    instruction=(
        "You are a privacy/PII governance expert.\n"
        "Call the 'run_pii_policy_checks_only' tool once. Then:\n"
        "1) Explain which columns are considered PII.\n"
        "2) Describe what the pipeline did with each PII field "
        "(kept in raw, removed in curated, etc.).\n"
        "3) Flag any remaining PII in curated outputs and suggest how to fix it.\n"
        "Use any prior 'schema_summary' or 'dq_summary' in state if available "
        "for additional context.\n"
        "Return a structured summary suitable for a final report."
    ),
    tools=[run_pii_policy_checks_only],
    output_key="pii_summary",
)

report_llm_agent = LlmAgent(
    name="ReportAgent",
    model=GEMINI_MODEL,
    description=(
        "Reporting agent that reads schema, data quality, and PII summaries "
        "from shared state and produces an overall governance report."
    ),
    instruction=(
        "You are a reporting and advisory agent.\n"
        "You will be run after other governance agents. In session.state, you may "
        "find keys like 'schema_summary', 'dq_summary', and 'pii_summary'.\n"
        "Using whatever information is available:\n"
        "1) State the overall governance status (pass/fail/needs-review).\n"
        "2) Summarize key issues by category (schema, data quality, PII).\n"
        "3) Provide 3–5 concrete next steps for the data/platform team.\n"
        "Write your answer in clear markdown; this will be shown directly to users "
        "and may be stored in a run report.\n"
    ),
    tools=[],  # no direct tools; relies on shared state
    output_key="governance_report",
)

root_agent = LlmAgent(
    model=GEMINI_MODEL,
    name="data_governance_root",
    description="Root agent for the Data Pipeline Governance capstone.",
    instruction=(
        "You are a data governance assistant for an event analytics pipeline.\n"
        "You currently have access to tools that can run the full pipeline or "
        "individual checks. For now, choose the best single tool to answer "
        "the user's question (full pipeline vs schema-only vs DQ-only vs PII-only), "
        "call it, and then explain the result in clear language."
    ),
    tools=[
        run_full_governance_pipeline,
        run_schema_checks_only,
        run_data_quality_checks_only,
        run_pii_policy_checks_only,
    ],
)

