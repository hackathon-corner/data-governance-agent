"""
run_summary_agent.py

LLM agent that turns a pipeline run summary (JSON) into a human-readable
data governance report.

This agent does **not** run the pipeline itself. It expects the calling
agent (the coordinator/root) to:
  - Run the data pipeline
  - Collect a JSON summary of the run (schema, data quality, PII, FK checks)
  - Send that summary as context / messages to this agent

The agent’s job is purely narrative:
  - Read the JSON summary (including multiple tables and FK checks)
  - Explain what passed vs failed
  - Call out specific issues by table / category
  - Recommend concrete next actions for a data/platform team

The JSON it receives will typically contain keys like:
  - "run_id", "description"
  - "schema"        -> overall schema governance results
  - "data_quality"  -> nulls, invalid values, etc.
  - "pii_policy"    -> PII detection/enforcement
  - "foreign_keys"  -> referential integrity checks between tables
  - possibly per-table breakdowns (e.g. "events", "users", etc.)

The exact shape may evolve; the agent should robustly handle missing keys
and still produce a useful report.
"""

from google.adk import agents as adk_agents


# -----------------------------------------------------------------------------
# Instruction / system prompt for the governance report agent
# -----------------------------------------------------------------------------

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
    "[DEBUG] RunSummaryAgent prompt starts with:",
    RUN_SUMMARY_INSTRUCTION.splitlines()[0]
)



# -----------------------------------------------------------------------------
# LLM Agent definition
# -----------------------------------------------------------------------------

report_agent = adk_agents.LlmAgent(
    name="ReportAgent",
    model="gemini-2.0-flash-lite",  # Same model family as your root agent
    description=(
        "Reads a JSON pipeline run summary (schema, data quality, PII, "
        "foreign-key checks) and produces a Markdown data governance report."
    ),
    instruction=RUN_SUMMARY_INSTRUCTION,
)

# If your root agent expects a different symbol name from this module,
# you can also export an alias, for example:
#
#   run_summary_agent = report_agent
#
# and adjust the import in your root agent accordingly.

RunSummaryAgent = report_agent
