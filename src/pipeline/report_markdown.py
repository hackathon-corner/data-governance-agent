# src/pipeline/report_markdown.py

from __future__ import annotations

from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional


def build_markdown_from_summary(summary: Dict[str, Any]) -> str:
    """
    Convert a governance summary dict into a human-readable markdown report.
    Assumes the structure produced by CoordinatorAgent / RunSummaryAgent.
    """

    overall_passed: bool = summary.get("overall_passed", False)
    checks = summary.get("checks", {})
    schema = checks.get("schema", {})
    dq = checks.get("data_quality", {})
    pii = checks.get("pii_policy", {})

    lineage = summary.get("lineage", {})
    source = lineage.get("source", {})
    target = lineage.get("target", {})

    # Top-level status
    status = "✅ PASSED" if overall_passed else "❌ FAILED"

    lines = []

    # Title + status
    lines.append("# Data Pipeline Governance Report")
    lines.append("")
    lines.append(f"**Overall Status:** {status}")
    lines.append("")
    lines.append(f"- **Run ID:** `{summary.get('run_id', 'unknown')}`")
    lines.append(f"- **Description:** {summary.get('description', 'N/A')}")
    lines.append("")

    # Lineage
    lines.append("## Dataset Lineage")
    lines.append("")
    lines.append(f"- **Source file:** `{source.get('filename', 'unknown')}`")
    lines.append(f"- **Rows in (raw):** {source.get('rows_in', 'unknown')}")
    lines.append(f"- **Target file:** `{target.get('filename', 'unknown')}`")
    lines.append(f"- **Rows out (curated):** {target.get('rows_out', 'unknown')}")
    lines.append("")

    # Schema section
    lines.append("## Schema Validation")
    lines.append(f"- **Overall status:** {'✅ Passed' if schema.get('passed') else '❌ Failed'}")
    lines.append("")

    tables = schema.get("tables") or {}
    if tables:
        lines.append("### Per-table schema status")
        for table_name, table_result in tables.items():
            t_status = "✅ Passed" if table_result.get("passed") else "❌ Failed"
            lines.append(f"- **{table_name}**: {t_status}")
        lines.append("")
        
    missing_cols = schema.get("missing_columns") or []
    extra_cols = schema.get("extra_columns") or []
    invalid_vals = schema.get("invalid_values") or {}

    if missing_cols or extra_cols or invalid_vals:
        lines.append("")
        if missing_cols:
            lines.append("- **Missing columns:** " + ", ".join(f"`{c}`" for c in missing_cols))
        if extra_cols:
            lines.append("- **Extra columns:** " + ", ".join(f"`{c}`" for c in extra_cols))
        if invalid_vals:
            lines.append("- **Columns with invalid values:**")
            for col, vals in invalid_vals.items():
                # vals might be a list or count; handle generically
                lines.append(f"  - `{col}`: {vals}")
    else:
        lines.append("")
        lines.append("No schema issues detected.")
    lines.append("")

    # Data quality section
    lines.append("## Data Quality Checks")
    lines.append(f"- **Status:** {'✅ Passed' if dq.get('passed') else '❌ Failed'}")
    lines.append("")
    null_fracs = dq.get("null_fractions") or {}
    exceeding = dq.get("columns_exceeding_null_threshold") or {}
    non_null_viol = dq.get("non_null_violations") or []
    unique_viol = dq.get("unique_key_violations") or []
    invalid_event_types = dq.get("invalid_event_types") or []

    if null_fracs:
        lines.append("- **Null fraction per column:**")
        for col, frac in null_fracs.items():
            lines.append(f"  - `{col}`: {frac:.3f}")
    if exceeding:
        lines.append("- **Columns exceeding null threshold:**")
        for col, frac in exceeding.items():
            lines.append(f"  - `{col}`: {frac:.3f}")
    if non_null_viol:
        lines.append("- **Non-null violations (examples):**")
        for item in non_null_viol[:5]:
            lines.append(f"  - {item}")
    if unique_viol:
        lines.append("- **Unique key violations (examples):**")
        for item in unique_viol[:5]:
            lines.append(f"  - {item}")
    if invalid_event_types:
        lines.append("- **Invalid event_type values (examples):**")
        for item in invalid_event_types[:5]:
            lines.append(f"  - {item}")
    if not (null_fracs or exceeding or non_null_viol or unique_viol or invalid_event_types):
        lines.append("No data quality issues detected.")
    lines.append("")

    fk = checks.get("foreign_keys", {})

    lines.append("## Cross-Table / Foreign Key Checks")
    lines.append(f"- **Status:** {'✅ Passed' if fk.get('passed') else '❌ Failed'}")
    lines.append("")
    violations = fk.get("violations") or []
    if violations:
        lines.append("- **Violations:**")
        for v in violations:
            lines.append(
                f"  - `{v['table']}.{v['column']}` has values not found in "
                f"`{v['ref_table']}.{v['ref_column']}` "
                f"(examples: {v['missing_keys']})"
            )
    else:
        lines.append("No foreign key violations detected.")
    lines.append("")


    # PII / policy section
    lines.append("## PII / Policy Enforcement")
    lines.append(f"- **Status:** {'✅ Passed' if pii.get('passed') else '❌ Failed'}")
    lines.append("")
    detected = pii.get("detected_pii_columns") or []
    removed = pii.get("removed_pii_columns") or []
    remaining = pii.get("remaining_pii_in_curated") or []

    lines.append("- **Detected PII columns in raw:** " +
                 (", ".join(f"`{c}`" for c in detected) if detected else "None"))
    lines.append("- **Removed from curated:** " +
                 (", ".join(f"`{c}`" for c in removed) if removed else "None"))
    lines.append("- **Remaining PII in curated:** " +
                 (", ".join(f"`{c}`" for c in remaining) if remaining else "None"))
    lines.append("")

    # Recommendations (very simple, driven by flags)
    lines.append("## Recommendations")
    lines.append("")
    if not overall_passed:
        lines.append("- Investigate and resolve the failing checks above, then rerun the pipeline.")
    if missing_cols:
        lines.append("- Align upstream event producers to include all required schema columns.")
    if exceeding:
        lines.append("- Reduce nulls in critical columns (e.g., enforce required fields at write time).")
    if remaining:
        lines.append("- Remove or hash remaining PII fields from curated outputs to satisfy policy.")
    if not lines[-1].startswith("-"):
        lines.append("- No major governance issues detected. Continue monitoring for regressions.")
    lines.append("")

    return "\n".join(lines)


def save_markdown_report(markdown: str, filename: Optional[str] = None) -> str:
    """
    Save markdown report under the reports/ directory.
    Returns the path as a string.
    """
    reports_dir = Path("reports")
    reports_dir.mkdir(parents=True, exist_ok=True)

    if filename is None:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"governance_report_{ts}.md"

    path = reports_dir / filename
    path.write_text(markdown, encoding="utf-8")
    return str(path)
