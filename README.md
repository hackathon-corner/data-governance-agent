# Data Pipeline Governance Agent

Capstone project for the 5-Day AI Agents Intensive Course with Google.

## Goal

Build a multi-agent system that performs governance checks on a data pipeline, including:

- Schema validation
- Data quality checks
- PII and policy enforcement
- Lineage tracking
- Governance reporting

This repository is structured to separate:

- `src/pipeline/` – The sample data pipeline (ETL, schemas, configs).
- `src/agents/` – Agent definitions (coordinator, schema agent, DQ agent, etc.).
- `src/tools/` – Tools used by agents (file access, schema validation, etc.).
- `data/` – Raw, schema, and curated data.
- `config/` – Pipeline configuration and rules.
- `reports/` – Governance run reports.
- `logs/` – Execution logs.
- `tests/` – Unit tests and evaluation harness.
