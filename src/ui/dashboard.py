import json
from datetime import datetime
from pathlib import Path

import streamlit as st

# -------------------------------------------------------------------
# Paths – adjust these if your project layout is different
# -------------------------------------------------------------------
# This file lives in src/ui/dashboard.py → project root is two levels up.
PROJECT_ROOT = Path(__file__).resolve().parents[2]

REPORTS_DIR = PROJECT_ROOT / "reports"
SUMMARIES_DIR = PROJECT_ROOT / "data" / "run_summaries"

# Import the tool that runs the pipeline + generates a markdown report
from src.agents.run_summary_agent import generate_markdown_report  # type: ignore
from src.pipeline.run_pipeline import load_config


# -------------------------------------------------------------------
# Helpers for reports + summaries
# -------------------------------------------------------------------
def list_markdown_reports():
    """Return a list of (label, path) for governance_report_*.md files."""
    if not REPORTS_DIR.exists():
        return []

    files = sorted(
        REPORTS_DIR.glob("governance_report_*.md"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )

    items = []
    for p in files:
        mtime = datetime.fromtimestamp(p.stat().st_mtime)
        label = f"{p.name} (modified: {mtime:%Y-%m-%d %H:%M:%S})"
        items.append((label, p))
    return items


def extract_timestamp_from_report_name(report_path: Path) -> str | None:
    """
    governance_report_20251126_205040.md → 20251126_205040

    Assumes filenames created like this:
      governance_report_{timestamp}.md
    where {timestamp} matches the summary file:
      run_summary_{timestamp}.json
    """
    stem = report_path.stem  # governance_report_20251126_205040
    prefix = "governance_report_"
    if stem.startswith(prefix):
        return stem[len(prefix) :]
    return None


def load_summary_for_report(report_path: Path) -> dict | None:
    """
    Try to find the corresponding run_summary_*.json for this report.

    If not found or not JSON, returns None.
    """
    ts = extract_timestamp_from_report_name(report_path)
    if ts is None:
        return None

    summary_path = SUMMARIES_DIR / f"run_summary_{ts}.json"
    if not summary_path.exists():
        return None

    try:
        with summary_path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def get_safe(d: dict, path: list[str], default="unknown"):
    """Safe nested dict getter: get_safe(summary, ['lineage','source','filename'])."""
    cur = d
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def render_status_chip(label: str, passed: bool) -> str:
    """Return HTML for a small colored status chip."""
    bg = "#e8f5e9" if passed else "#ffebee"
    text = "#2e7d32" if passed else "#c62828"
    icon = "✅" if passed else "❌"
    border = f"{text}33"  # add a little alpha

    return (
        f"<span style='background-color:{bg}; color:{text}; "
        f"border-radius:999px; padding:0.15rem 0.6rem; "
        f"margin-right:0.3rem; font-size:0.8rem; "
        f"border:1px solid {border};'>"
        f"{icon} {label}"
        f"</span>"
    )


# -------------------------------------------------------------------
# Streamlit layout
# -------------------------------------------------------------------
st.set_page_config(
    page_title="Data Governance Pipeline Dashboard",
    layout="wide",
)

st.title("🧭 Data Governance Pipeline Dashboard")

# High-level description of what the pipeline does
st.markdown(
    """
This dashboard lets you **run and inspect a data governance pipeline** for your
event analytics data.

The pipeline:

1. **Validates schema** for events, users, and courses (required columns, allowed values).  
2. **Checks data quality** (null thresholds, non-null constraints, unique keys, category values).  
3. **Enforces PII policy**, ensuring sensitive fields are removed from curated outputs.  
4. **Verifies referential integrity** across events ↔ users ↔ courses.  
5. Writes curated analytics events and generates a **markdown governance report** for each run.
"""
)

left_col, right_col = st.columns([1, 1])

# -------------------------------------------------------------------
# Left column: Run pipeline + report picker
# -------------------------------------------------------------------
with left_col:
    st.subheader("Run Governance Pipeline")

    st.caption("Choose a configuration to run")

    # Option format: (label, filename, description)
    config_options = [
        (
            "Success (pipeline_config_success.yaml)",
            "pipeline_config_success.yaml",
            "A small sample configuration that uses 'good' sample files and should pass all checks.",
        ),
        (
            "Issues (pipeline_config_issues.yaml)",
            "pipeline_config.yaml",
            "A config pointing to richer test fixtures designed to surface schema, DQ, and FK issues.",
        ),
    ]

    config_labels = [c[0] for c in config_options]
    # Selectbox returns the chosen label. This avoids storing an index value in
    # session_state that could be mutated to a non-int and cause errors.
    # Ensure any persisted session_state value is valid for this set of labels
    if "config_choice_label" in st.session_state and st.session_state.config_choice_label not in config_labels:
        st.session_state.config_choice_label = config_labels[0]

    selected_label = st.selectbox(
        "Configuration",
        options=config_labels,
        index=0,
        key="config_choice_label",
    )

    config_choice_index = config_labels.index(selected_label)
    selected_config_filename = config_options[config_choice_index][1]
    selected_config_description = config_options[config_choice_index][2]

    # show a small description and a preview expander so the user can inspect the YAML
    st.markdown(f"**Description:** {selected_config_description}")
    with st.expander("Preview config YAML"):
        try:
            cfg = load_config(filename=selected_config_filename)
            import yaml as _yaml

            st.code(_yaml.safe_dump(cfg, sort_keys=False), language="yaml")
        except Exception as e:
            st.warning(f"Could not load preview for {selected_config_filename}: {e}")

    # let the user optionally auto-select the newly created report after a run
    if "auto_select_new_report" not in st.session_state:
        st.session_state.auto_select_new_report = True
    st.checkbox("Auto-select newly generated report after run", value=st.session_state.auto_select_new_report, key="auto_select_new_report")

    if st.button("▶️ Run pipeline and generate report"):
        with st.spinner("Running governance pipeline..."):
            cfg = load_config(filename=selected_config_filename)
            result = generate_markdown_report(config_override=cfg)
        st.success("Pipeline run completed.")

        overall = result.get("overall_passed", False)
        report_path = result.get("report_path")

        if report_path:
            st.info(f"New report generated: `{report_path}`")

            # Re-populate the reports listing and optionally auto-select the new report
            reports = list_markdown_reports()
            # find the matching label for the generated report file and set session state
            if st.session_state.get("auto_select_new_report", False):
                matching = [label for label, p in reports if p.name == Path(report_path).name]
                if matching:
                    # programmatically set the selectbox value so the right report is displayed
                    st.session_state["report_select"] = matching[0]

        if overall:
            st.success("Overall status: PASSED ✅")
        else:
            st.error("Overall status: FAILED ❌")

    st.markdown("---")
    st.subheader("Available Reports")

    reports = list_markdown_reports()
    if not reports:
        st.write("No reports found yet. Run the pipeline to generate one.")
        selected_report_path = None
    else:
        labels = [label for label, _ in reports]
        default_index = 0  # most recent first
        # remember selection in session_state so the auto-select feature can
        # programmatically change which report is displayed
        if "report_select" not in st.session_state:
            st.session_state.report_select = labels[default_index]
        # If the session contains a stale report_select value not present in
        # the current labels list (e.g. reports rotated/removed), reset to
        # the default most-recent label.
        if st.session_state.report_select not in labels:
            st.session_state.report_select = labels[default_index]

        selected_label = st.selectbox(
            "Select a report to view:",
            options=labels,
            key="report_select",
        )
        selected_report_path = dict(reports)[selected_label]

        # "Clickable" / downloadable actions for the report file
        with st.expander("Report file actions"):
            st.markdown(
                f"[Open raw markdown file]({selected_report_path.as_uri()})  \n"
                f"`{selected_report_path}`",
                unsafe_allow_html=True,
            )
            try:
                text = selected_report_path.read_text(encoding="utf-8")
                st.download_button(
                    "⬇️ Download markdown",
                    data=text,
                    file_name=selected_report_path.name,
                    mime="text/markdown",
                )
            except Exception as e:
                st.warning(f"Could not read report: {e}")

    st.caption(f"Reports directory: `{REPORTS_DIR}`")

# -------------------------------------------------------------------
# Right column: Governance report + dynamic metadata + chips
# -------------------------------------------------------------------
with right_col:
    st.subheader("Governance Report")

    if not selected_report_path:
        st.write("Select a report on the left to view details.")
    else:
        st.write(
            "Viewing:",
            f"`{selected_report_path.name}`",
        )

        # Load matching summary JSON (if available)
        summary = load_summary_for_report(selected_report_path) or {}

        # ---- Overall status ----
        overall_passed = bool(summary.get("overall_passed", False))
        if overall_passed:
            st.markdown("**Overall Status:** ✅ PASSED")
        else:
            st.markdown("**Overall Status:** ❌ FAILED")

        # ---- Status chips by category ----
        checks = summary.get("checks", {})

        schema_passed = bool(get_safe(checks, ["schema", "passed"], default=False))
        dq_passed = bool(get_safe(checks, ["data_quality", "passed"], default=False))
        pii_passed = bool(get_safe(checks, ["pii_policy", "passed"], default=False))
        fk_passed = bool(get_safe(checks, ["foreign_keys", "passed"], default=False))

        chips_html = (
            render_status_chip("Schema", schema_passed)
            + render_status_chip("Data Quality", dq_passed)
            + render_status_chip("PII Policy", pii_passed)
            + render_status_chip("Foreign Keys", fk_passed)
        )

        st.markdown("#### Check Status")
        st.markdown(chips_html, unsafe_allow_html=True)
        st.caption(
            "Each chip shows whether that governance dimension passed on this run."
        )

        # ---- High-level metadata ----
        run_id = get_safe(summary, ["metadata", "run_id"], default="N/A")
        generated_at = get_safe(summary, ["metadata", "generated_at_utc"], default="N/A")
        description = get_safe(summary, ["metadata", "description"], default="N/A")

        st.markdown(
            f"""
**Run ID:** `{run_id}`  
**Generated at (UTC):** `{generated_at}`  
**Description:** {description}
"""
        )

        st.markdown("---")
        st.markdown("### Dataset Lineage")

        source_file = get_safe(summary, ["lineage", "source", "filename"])
        rows_in = get_safe(summary, ["lineage", "source", "rows_in"])
        target_file = get_safe(summary, ["lineage", "target", "filename"])
        rows_out = get_safe(summary, ["lineage", "target", "rows_out"])

        col1, col2 = st.columns(2)
        with col1:
            st.markdown(
                f"""
**Source file:** `{source_file}`  
**Rows in (raw):** `{rows_in}`
"""
            )
        with col2:
            st.markdown(
                f"""
**Target file:** `{target_file}`  
**Rows out (curated):** `{rows_out}`
"""
            )

        st.markdown("---")
        st.markdown("### Full Markdown Report")

        # Render the markdown content inline
        try:
            markdown_text = selected_report_path.read_text(encoding="utf-8")
            st.markdown(markdown_text)
        except Exception as e:
            st.error(f"Error reading report markdown: {e}")
