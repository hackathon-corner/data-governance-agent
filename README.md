# Data Pipeline Governance Agent

This repository is a compact, practical sample project that demonstrates
how to build a data governance pipeline and multi-agent workflow. It was
developed as a capstone-style project for an AI Agents intensive course,
but the architecture and components are intentionally generalizable.

High-level capabilities
-----------------------
- Validate table schemas (required columns, allowed values, type checks)
- Perform data quality (DQ) checks: null fraction, unique-key violations,
	required non-null fields, invalid enumerations
- Detect and enforce PII/Privacy policies (drop or report sensitive fields)
- Verify referential integrity / foreign keys across source tables
- Produce consistent, JSON-serializable run summaries
- Generate human-friendly markdown run reports for review and sharing
- Lightweight Streamlit dashboard to run pipeline variants and inspect
	reports interactively

Repository layout
-----------------
- `src/pipeline/` — core pipeline utilities: reading data, schema checks,
	data quality, foreign key checks, saving curated outputs, report builders.
- `src/agents/` — small agent wrappers (CoordinatorAgent, Schema/DQ/PII agents),
	and the `run_summary_agent` used by the UI.
- `src/ui/` — minimal Streamlit dashboard to run or inspect reports.
- `config/` — pipeline configuration(s). The repo includes a default
	config plus `pipeline_config_success.yaml` and `pipeline_config_issues.yaml`
	for demo runs that are expected to pass or fail checks respectively.
- `data/raw/test_data/` — small CSV fixtures used by tests and demos.
- `data/curated/` — curated outputs written by pipeline runs.
- `data/run_summaries/` — normalized JSON run summaries persisted by runs.
- `reports/` — generated Markdown reports (one per run).
- `tests/` — unit tests that exercise pipeline logic and edge cases.

Why use this project
--------------------
- Educational: demonstrates a practical monitoring and governance loop.
- Minimal dependencies and straightforward structure — good starting point
	for experimentation.
- Extensible: add connectors, additional checks, or integrate with real
	orchestration tools (Kubernetes, Airflow, Dataflow).

Contents of this README
-----------------------
1. Setup and development environment
2. How the pipeline and agents work (architecture)
3. Running locally (CLI + Streamlit UI)
4. Configuration options (multi-config demo)
5. Testing and CI guidance
6. Troubleshooting & common issues
7. Next steps and contributor notes


1) Setup & development environment
-------------------------------

System prerequisites
- Python 3.10+ (this project was developed and tested with 3.10).
- A Python virtual environment tool such as venv or conda (recommended).

Create a development environment (Conda example):

```bash
# from the repo root
conda create -n data-governance-agent python=3.10 -y
conda activate data-governance-agent
pip install -r requirements.txt
```

Or with python -m venv:

```bash
python -m venv .venv
source .venv/bin/activate  # macOS / Linux
pip install -r requirements.txt
```

After installing dependencies you should be able to run the local tests and
the Streamlit UI from the repository root.


2) Architecture & what this repo contains (detailed)
----------------------------------------------------

Core pieces
- src/pipeline — pipeline utilities and functions. Includes CSV readers,
	schema validators, data-quality logic (null fractions, unique checks),
	PII policy enforcement, foreign-key checks, helpers to build and persist
	run summaries and markdown reports.

- src/agents — small thin agent wrappers used by the coordinator. These
	are intentionally small so they are easily testable and reusable. Central
	components:
	- CoordinatorAgent — orchestrates schema, dq, pii checks and composes
		results
	- SchemaValidationAgent, DataQualityAgent, PiiPolicyAgent — small APIs
		around the core pipeline functions
	- RunSummaryAgent — builds normalized summary JSON and writes the
		report+summary pair used by the dashboard.

- src/ui — Streamlit dashboard that allows you to run the pipeline with
	different configurations and inspect reports instantly.

Data & config layout
- data/raw — sample input CSVs, including small test fixtures in
	`data/raw/test_data/` used by unit tests and UI demos.
- data/curated — written curated outputs (analytics events) after a run.
- data/run_summaries — JSON summaries saved with timestamped filenames.
- reports — Markdown reports built from summaries (also timestamped).
- config — pipeline configuration YAML files. The repo includes three
	options: the default `pipeline_config.yaml`, a `pipeline_config_success.yaml`
	which points at the passing fixtures, and `pipeline_config_issues.yaml`
	which intentionally uses fixtures that surface errors. This makes it easy
	to demonstrate pass/fail behavior in the dashboard.


3) How to run it locally — quick reference
-----------------------------------------

Run the pipeline via the CLI (non-interactive):

```bash
# default (uses config/pipeline_config.yaml)
python -m src.pipeline.run_pipeline

# or run the same module from the project root
python -m src.pipeline.run_pipeline
```

Run the multi-agent Coordinator (programmatic):

```python
from src.agents.coordinator_agent import CoordinatorAgent
coordinator = CoordinatorAgent()
result = coordinator.run()  # returns component dict
```

Create a normalized summary + markdown report programmatically:

```python
from src.agents.run_summary_agent import generate_markdown_report
# optionally pass a config dict as an override (useful for the dashboard)
generate_markdown_report(config_override=my_config_dict)
```

Streamlit dashboard (interactive)

The Streamlit UI is in `src/ui/dashboard.py` — it provides a developer-friendly
way to run the pipeline using multiple example configs and immediately view
the generated Markdown report and the matching JSON summary.

Start the dashboard (from repo root):

```bash
streamlit run src/ui/dashboard.py
```

Left column → pick a configuration (Default / Success / Issues)
- You can preview the YAML before running and toggle "Auto-select newly
	generated report" so the dashboard shows results automatically.


4) Configuration options — multi-config demo
--------------------------------------------

This repo includes multiple configs in the `config/` directory:

- pipeline_config.yaml — the default configuration
- pipeline_config_success.yaml — points to passing dataset fixtures (good run)
- pipeline_config_issues.yaml — points to richer fixtures that should show
	schema/DQ/foreign-key issues for demonstration

How the dashboard uses these
- When you choose a config and click "Run pipeline and generate report",
	the dashboard loads the configured YAML then calls `generate_markdown_report`
	with a `config_override` dict so the coordinator runs using that config
	without modifying the on-disk default.

Running programmatically
- Call `load_config(filename="pipeline_config_success.yaml")` to load a config
	by name, or pass the dict to `CoordinatorAgent.run(config_override=cfg)`.


5) Testing & CI guidance
------------------------

Run the unit test suite (pytest):

```bash
pytest -q
```

Tests are designed to be deterministic and to avoid external network calls.
They rely on small fixtures in `data/raw/test_data/`. Test coverage includes:
- pipeline logic (schema, DQ, FK checks)
- PII policy enforcement
- run-summary sanitization (ensures JSON-serializable output)
- a few integration-like checks for report generation and pipeline coordination

If you add features that call external services, prefer to provide a mock
interface or a local fast-mode for tests so CI stays deterministic.


6) Troubleshooting & common issues
----------------------------------

Streamlit / session state selectbox TypeError
- When switching UI option sets during iterative development you may see
	`TypeError: bad argument type for built-in operation` coming from a
	Streamlit selectbox. This happens when session_state contains a value
	that no longer matches the current set of allowed options. The dashboard
	now guards against stale values by resetting session state to a valid
	default when needed.

JSON serialization / numpy/pandas scalars
- You may encounter `TypeError: Object of type int64 is not JSON serializable`
	when saving pipeline summaries. This project includes a sanitizer that
	converts numpy / pandas scalars (int64, bool_, float64, timestamps) to native
	python types before saving summaries to JSON. If you add new non-standard
	objects to the summary, add logic to serialize them or coerce them to
	strings in `src/pipeline/run_summary.py`.

503 / external API failures
- External 503 Service Unavailable errors generally happen at runtime when
	calling external APIs (e.g., ADK / LLM or other services). Strategies:
	1. Verify credentials, quotas and environment (network access, region).
	2. Use retry with exponential backoff + jitter for transient 5xx errors.
	3. Add better logging & error messages so you can identify and handle
		 transient vs permanent failures.


7) Development notes & next steps
---------------------------------

Where to extend
- Add connectors to consume data from cloud storage or databases instead of
	local CSVs.
- Add further DQ checks (drift detection, distribution changes, schema
	versioning, thresholds per-table), and rule-based/ML-based anomaly detection.
- Hook reports into a pipeline orchestration/CI system (e.g., Airflow,
	GitHub Actions) for automated monitoring runs and alerting.

Developer convenience
- `src/ui/dashboard.py` offers a preview and convenient multi-config demos so
	you can switch between deterministic passing and failing fixtures during
	exploration.

Contribution & code style
- Tests: add pytest tests under `tests/` when you add features.
- Keep agents thin and test core pipeline functions in `src/pipeline/`.
- Follow standard Python packaging practices and keep dependencies minimal.

License
-------
This project is a small, instructional repository — treat it as MIT-style
friendly sample code (please check and adapt license fields if converting
the code to an internal project).

If you'd like, I can also:
- Add a 'developer quickstart' script that bootstraps a dev environment and
	creates the expected folders/fixtures
- Add a Streamlit UI button to explicitly clear or reset session_state values
	(handy during interactive development)
- Add CI steps to run tests and check that JSON summaries are serializable

If you want, I can create a short `CONTRIBUTING.md` or improve the project
README to include any other project-specific deployment details you expect.

