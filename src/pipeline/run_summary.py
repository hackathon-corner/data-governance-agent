"""
run_summary.py

Builds a run summary and simple lineage object for the governance pipeline.
This will later be used by a Report Generator Agent.
"""

from typing import Dict, Any
import pathlib
import json
from datetime import datetime

BASE_DIR = pathlib.Path(__file__).resolve().parents[2]
REPORTS_DIR = BASE_DIR / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def build_run_summary(
    config: Dict[str, Any],
    schema_results: Dict[str, Any],
    dq_results: Dict[str, Any],
    pii_results: Dict[str, Any],
    source_filename: str,
    curated_filename: str,
    rows_in: int,
    rows_out: int,
) -> Dict[str, Any]:
    """Create a structured summary + simple lineage description."""
    now = datetime.utcnow().isoformat() + "Z"
    run_id = config.get("run_id", "unknown-run")

    summary: Dict[str, Any] = {
        "run_metadata": {
            "run_id": run_id,
            "timestamp_utc": now,
            "description": config.get("description", ""),
        },
        "lineage": {
            "source": {
                "filename": source_filename,
                "table_name": config["sources"]["events"]["table_name"],
                "rows_in": rows_in,
            },
            "target": {
                "filename": curated_filename,
                "table_name": config["targets"]["events_curated"]["table_name"],
                "rows_out": rows_out,
            },
            "transformations": [
                "schema_validation",
                "data_quality_checks",
                "pii_policy_enforcement",
            ],
        },
        "checks": {
            "schema": {
                "passed": schema_results.get("passed", False),
                "missing_columns": schema_results.get("missing_columns", []),
                "extra_columns": schema_results.get("extra_columns", []),
                "invalid_values": schema_results.get("invalid_values", {}),
            },
            "data_quality": {
                "passed": dq_results.get("passed", False),
                "null_fractions": dq_results.get("null_fractions", {}),
                "columns_exceeding_null_threshold": dq_results.get(
                    "columns_exceeding_null_threshold", {}
                ),
                "non_null_violations": dq_results.get("non_null_violations", {}),
                "unique_key_violations": dq_results.get("unique_key_violations", {}),
                "invalid_event_types": dq_results.get("invalid_event_types", []),
            },
            "pii_policy": {
                "passed": pii_results.get("passed", False),
                "detected_pii_columns": pii_results.get("detected_pii_columns", []),
                "removed_pii_columns": pii_results.get("removed_pii_columns", []),
                "remaining_pii_in_curated": pii_results.get(
                    "remaining_pii_in_curated", []
                ),
                "pii_allowed_in_raw": pii_results.get("pii_allowed_in_raw", True),
                "pii_allowed_in_curated": pii_results.get(
                    "pii_allowed_in_curated", False
                ),
            },
        },
        "overall_passed": (
            schema_results.get("passed", False)
            and dq_results.get("passed", False)
            and pii_results.get("passed", False)
        ),
    }

    return summary


def save_run_summary(summary: Dict[str, Any]) -> str:
    """Save summary to JSON in the reports directory. Returns path as string."""
    run_id = summary["run_metadata"].get("run_id", "unknown-run")
    timestamp = summary["run_metadata"].get("timestamp_utc", "no-ts").replace(":", "-")
    filename = f"governance_run_{run_id}_{timestamp}.json"
    out_path = REPORTS_DIR / filename

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    return str(out_path)


def print_run_summary(summary: Dict[str, Any]) -> None:
    """Print a concise human-readable summary to the console."""
    print("\n=== Governance Run Summary ===")
    meta = summary["run_metadata"]
    lineage = summary["lineage"]
    checks = summary["checks"]

    print(f"Run ID: {meta['run_id']}")
    print(f"Timestamp (UTC): {meta['timestamp_utc']}")
    print(f"Description: {meta['description']}")

    print("\nLineage:")
    print(
        f"  Source: {lineage['source']['table_name']} "
        f"({lineage['source']['filename']}), rows_in={lineage['source']['rows_in']}"
    )
    print(
        f"  Target: {lineage['target']['table_name']} "
        f"({lineage['target']['filename']}), rows_out={lineage['target']['rows_out']}"
    )

    print("\nChecks status:")
    print(f"  Schema:      {'PASSED' if checks['schema']['passed'] else 'FAILED'}")
    print(f"  DataQuality: {'PASSED' if checks['data_quality']['passed'] else 'FAILED'}")
    print(f"  PII Policy:  {'PASSED' if checks['pii_policy']['passed'] else 'FAILED'}")

    overall = "PASSED" if summary["overall_passed"] else "FAILED"
    print(f"\nOverall governance status: {overall}")
