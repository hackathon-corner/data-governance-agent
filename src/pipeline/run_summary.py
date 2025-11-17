# src/pipeline/run_summary.py

from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime
from typing import Any, Dict
import pandas as pd

def _remove_dataframes(d: Dict[str, Any]) -> Dict[str, Any]:
    """
    Return a shallow copy of d with any pandas DataFrame values removed.

    The pipeline agent results sometimes include full DataFrames (e.g. df_curated),
    which are not JSON-serializable. Those should not be part of the persisted
    summary; we keep only scalar / list / dict metadata.
    """
    clean: Dict[str, Any] = {}
    for k, v in d.items():
        if isinstance(v, pd.DataFrame):
            # skip DataFrames in the summary
            continue
        clean[k] = v
    return clean


def build_run_summary(
    config: Dict[str, Any],
    schema_results: Dict[str, Any],
    dq_results: Dict[str, Any],
    pii_results: Dict[str, Any],
    fk_results: Dict[str, Any],
    source_filename: str,
    curated_filename: str,
    rows_in: int,
    rows_out: int,
) -> Dict[str, Any]:
    """
    Construct a normalized summary dictionary for a single governance pipeline run.

    This is the single source of truth for the summary structure used by:
      - CoordinatorAgent
      - Markdown report generation
      - Any external reporting / dashboards
    """

    # Strip out any DataFrames from the result dicts before persisting
    schema_results_clean = _remove_dataframes(schema_results)
    dq_results_clean = _remove_dataframes(dq_results)
    pii_results_clean = _remove_dataframes(pii_results)
    fk_results_clean = _remove_dataframes(fk_results)

    summary: Dict[str, Any] = {
        "run_id": config.get("run_id"),
        "description": config.get("description", ""),
        "overall_passed": None,  # set below
        "checks": {
            "schema": schema_results_clean,
            "data_quality": dq_results_clean,
            "pii_policy": pii_results_clean,
            "foreign_keys": fk_results_clean,
        },
        "lineage": {
            "source": {
                "filename": source_filename,
                "rows_in": rows_in,
            },
            "target": {
                "filename": curated_filename,
                "rows_out": rows_out,
            },
        },
        "metadata": {
            "generated_at_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        },
    }

    # Normalize boolean flags for each check; default to False if missing
    schema_passed = bool(schema_results_clean.get("passed"))
    dq_passed = bool(dq_results_clean.get("passed"))
    pii_passed = bool(pii_results_clean.get("passed"))
    fk_passed = bool(fk_results_clean.get("passed"))

    summary["checks"]["schema"]["passed"] = schema_passed
    summary["checks"]["data_quality"]["passed"] = dq_passed
    summary["checks"]["pii_policy"]["passed"] = pii_passed
    summary["checks"]["foreign_keys"]["passed"] = fk_passed

    # Overall run passes only if all individual checks pass
    summary["overall_passed"] = schema_passed and dq_passed and pii_passed and fk_passed

    return summary


def save_run_summary(summary: Dict[str, Any], output_dir: str = "reports") -> str:
    """
    Persist the run summary to a JSON file under the given output directory.

    Returns:
        The path to the written JSON file as a string.
    """
    reports_dir = Path(output_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)

    run_id = summary.get("run_id") or "unknown_run"
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"run_summary_{run_id}_{ts}.json"

    path = reports_dir / filename
    path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    return str(path)


class RunSummaryAgent:
    """
    Thin wrapper around build_run_summary + save_run_summary so the coordinator
    can remain simple and testable.
    """

    def run(
        self,
        config: Dict[str, Any],
        schema_results: Dict[str, Any],
        dq_results: Dict[str, Any],
        pii_results: Dict[str, Any],
        fk_results: Dict[str, Any],
        source_filename: str,
        curated_filename: str,
        rows_in: int,
        rows_out: int,
    ) -> Dict[str, Any]:
        summary = build_run_summary(
            config=config,
            schema_results=schema_results,
            dq_results=dq_results,
            pii_results=pii_results,
            fk_results=fk_results,
            source_filename=source_filename,
            curated_filename=curated_filename,
            rows_in=rows_in,
            rows_out=rows_out,
        )

        summary_path = save_run_summary(summary)
        return {
            "summary": summary,
            "summary_path": summary_path,
        }

def print_run_summary(summary: Dict[str, Any]) -> None:
    """
    Convenience helper for CLI runs: print a short textual summary
    of the governance checks and overall status.
    """
    print("\n=== Governance Run Summary ===")

    checks = summary.get("checks", {})
    for name, result in checks.items():
        # name will be 'schema', 'data_quality', 'pii_policy', 'foreign_keys', etc.
        status = "PASSED" if result.get("passed") else "FAILED"
        print(f"- {name}: {status}")

    overall = summary.get("overall_passed")
    print(f"\nOverall passed: {overall}")
