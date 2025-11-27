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
from src.agents.run_summary_agent import generate_markdown_report
from src.pipeline.auto_runner import auto_run_once




GEMINI_MODEL = "gemini-2.0-flash-lite"
print("[DEBUG] Using GEMINI_MODEL:", GEMINI_MODEL)
print("[DEBUG] Loading ReportAgent prompt...")

RUN_SUMMARY_INSTRUCTION = """You are a senior data governance specialist.

You will be given a JSON object describing the results of a data governance run.
Your job is to read that JSON carefully and write a concise, clear Markdown report
for data engineers and platform owners.

Your output MUST be Markdown and MUST contain **exactly four numbered sections in this order**:

1. **Overall Governance Status**
2. **Summary of Issues by Category**
3. **Foreign Key / Referential Integrity Governance**
4. **Concrete Next Steps for the Data/Platform Team**

Follow these rules when generating the report:

---------------------------------------------
### 1. Overall Governance Status
---------------------------------------------
- Provide a one-sentence status summary based on:
  - schema validation,
  - data quality validation,
  - PII policy enforcement,
  - foreign key consistency.
- Use strong action language such as:
  - “Passed”
  - “Failed”
  - “Needs Review”
- If any major category failed, the overall status should signal required action.

---------------------------------------------
### 2. Summary of Issues by Category
---------------------------------------------
For each category below, summarize clearly:

- **Schema**
  - Identify which tables failed schema validation.
  - List invalid values, missing required columns, or type mismatches.

- **Data Quality**
  - Report null-fraction violations.
  - Report invalid enumerations or category values.
  - Highlight any fields that broke quality thresholds.

- **PII Policy**
  - Summarize which PII columns were detected and confirm whether:
      - they were properly removed (if required), or
      - improperly present in curated output.

For each category, include:
- a short “Status: PASSED/FAILED” line
- bullet points explaining the issues.

---------------------------------------------
### 3. Foreign Key / Referential Integrity Governance
---------------------------------------------
Use the “foreign_keys” section of the JSON to report:
- Which relationships were evaluated (e.g., events.user_id → users.user_id)
- Whether each relationship PASSED or FAILED
- For each failure:
  - List missing or orphan keys
  - Include counts of violations
  - Provide a short explanation (e.g., “events contains 1 user_id not found in users.csv”)

If all foreign keys pass, write:  
> “All referential integrity checks passed successfully.”

---------------------------------------------
### 4. Concrete Next Steps for the Data/Platform Team
---------------------------------------------
This section MUST contain actionable steps.

Use bullet points and write in the imperative tone (“Fix…”, “Investigate…”, “Update…”).

Include actions for:
- Schema fixes (e.g., correcting invalid event_types)
- Data quality remediation (e.g., investigating null rates, upstream ingestion fixes)
- PII policy improvements (if any issues occurred)
- Foreign key fixes (e.g., backfilling missing reference records, dropping invalid rows)
- Any upstream ingestion or ETL recommendations

DO NOT repeat large JSON blocks.
DO NOT invent data not present in the input.

Your tone should be professional, precise, and focused on operational remediation.
"""

print(
    "[DEBUG] Loaded ReportAgent prompt first line:",
    RUN_SUMMARY_INSTRUCTION.splitlines()[0],
    "FROM:",
    __file__,
)


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
    instruction=RUN_SUMMARY_INSTRUCTION,
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
        "- If the user asks to 'generate a report', 'save a markdown report', "
        "or similar, call the 'generate_markdown_report' tool.\n"
        "- If the user asks to 'auto-run', 'check for new data and run', or "
        "similar, call the 'auto_run_once' tool. This should be treated as an "
        "idempotent, non-interactive run: if there is no new data, just report "
        "that fact; if there is new data, run the pipeline and return where the "
        "report was saved.\n"
        "After calling a workflow or tool, explain the results clearly and suggest "
        "concrete next steps. Do not try to add sub-agents yourself; use the "
        "provided tools and the GovernanceWorkflow sub-agent."
    ),
    tools=[
        run_full_governance_pipeline,
        run_schema_checks_only,
        run_data_quality_checks_only,
        run_pii_policy_checks_only,
        generate_markdown_report,   # ← now imported from run_summary_agent.py
        auto_run_once,              # ← now imported from auto_runner.py
    ],
    sub_agents=[governance_workflow],
)


