from google.adk.agents.llm_agent import Agent

# Import your existing coordinator that runs the whole pipeline
from src.agents.coordinator_agent import CoordinatorAgent
from google.adk.agents import LlmAgent, SequentialAgent

from src.agents.coordinator_agent import CoordinatorAgent
from src.agents.schema_agent import SchemaValidationAgent
from src.agents.dq_agent import DataQualityAgent
from src.agents.pii_policy_agent import PiiPolicyAgent
from src.pipeline.run_pipeline import load_config, load_schema, load_events_raw
from src.pipeline.report_markdown import (build_markdown_from_summary, save_markdown_report)


GEMINI_MODEL = "gemini-2.5-flash"


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

def generate_markdown_report() -> dict:
    """
    ADK tool: runs the full governance pipeline, builds a markdown report,
    saves it to disk, and returns a small summary.
    """
    coordinator = CoordinatorAgent()
    result = coordinator.run()
    summary = result["summary"]

    markdown = build_markdown_from_summary(summary)
    report_path = save_markdown_report(markdown)

    return {
        "status": "success",
        "overall_passed": summary.get("overall_passed", False),
        "report_path": report_path,
        # Include a short preview so the LLM can quote it if helpful
        "markdown_preview": "\n".join(markdown.splitlines()[:40]),
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

governance_workflow = SequentialAgent(
    name="GovernanceWorkflow",
    description=(
        "Runs the full multi-step governance workflow in order: "
        "schema validation → data quality validation → PII policy checks → final reporting. "
        "Each sub-agent writes its results to session.state."
    ),
    sub_agents=[
        schema_llm_agent,
        dq_llm_agent,
        pii_llm_agent,
        report_llm_agent,
    ],
)

root_agent = LlmAgent(
    name="DataGovernanceRoot",
    model=GEMINI_MODEL,
    description=(
        "Root coordinator agent for the data governance pipeline. "
        "You decide whether to run the full multi-agent workflow or call "
        "individual tools based on the user's request."
    ),
    instruction=(
        "You coordinate a data governance workflow for an event analytics pipeline.\n"
        "- If the user asks to 'run the full pipeline', 'run full governance', "
        "'validate everything', or similar, delegate to the 'GovernanceWorkflow' "
        "sequential agent.\n"
        "- If the user only asks about schema, call the 'run_schema_checks_only' tool.\n"
        "- If the user only asks about data quality, call the "
        "'run_data_quality_checks_only' tool.\n"
        "- If the user only asks about PII or privacy, call the "
        "'run_pii_policy_checks_only' tool.\n"
        "After calling a workflow or tool, explain the results clearly and suggest "
        "concrete next steps. Do not try to add sub-agents yourself; use the "
        "provided tools and the GovernanceWorkflow sub-agent."
        "- If the user asks to 'generate a report', 'save a markdown report', "
        "or similar, call the 'generate_markdown_report' tool.\n"
    ),
    # Tools the root can call directly for targeted checks
    tools=[
        run_full_governance_pipeline,      # optional: a single-call alternative
        run_schema_checks_only,
        run_data_quality_checks_only,
        run_pii_policy_checks_only,
        generate_markdown_report,
    ],
    # IMPORTANT: only one sub-agent here, to avoid multiple parents
    sub_agents=[
        governance_workflow,
    ],
)


