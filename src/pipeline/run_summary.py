# src/pipeline/run_summary.py

from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime
from typing import Any, Dict
import numpy as np

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RUN_SUMMARIES_DIR = PROJECT_ROOT / "data" / "run_summaries"
RUN_SUMMARIES_DIR.mkdir(parents=True, exist_ok=True)

def _remove_dataframes(d: Dict[str, Any]) -> Dict[str, Any]:
    """
    Return a shallow copy of d with any pandas DataFrame values removed.

    The pipeline agent results sometimes include full DataFrames (e.g. df_curated),
    which are not JSON-serializable. Those should not be part of the persisted
    summary; we keep only scalar / list / dict metadata.
    """
    clean: Dict[str, Any] = {}
    for k, v in d.items():
        # drop DataFrame values at the top-level.
        # nested DataFrames will be handled by the sanitizer below.
        if isinstance(v, pd.DataFrame):
            continue
        clean[k] = v
    return clean


def _sanitize_for_json(obj: Any) -> Any:
    """
    Recursively convert pandas / numpy scalars and containers into
    plain Python builtins that json.dump will accept.

    - numpy integer/float/bool -> int/float/bool
    - numpy arrays / pandas Index -> lists
    - pandas Timestamp -> ISO string
    - pd.NA / NaN / NaT -> None
    - dict/list/tuple/set -> converted recursively
    """

    # simple fast-paths
    if obj is None:
        return None

    # pandas/numpy NA values -> None
    try:
        if pd.isna(obj):
            return None
    except Exception:
        # pd.isna can raise for some types (e.g. dict) - ignore
        pass

    # numpy scalar types
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    if isinstance(obj, (np.bool_,)):
        return bool(obj)

    # pandas types
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()

    # containers
    if isinstance(obj, dict):
        return {str(k): _sanitize_for_json(v) for k, v in obj.items()}

    if isinstance(obj, (list, tuple, set)):
        return [_sanitize_for_json(x) for x in obj]

    # numpy arrays / pandas Index
    if isinstance(obj, (np.ndarray, pd.Index)):
        return [_sanitize_for_json(x) for x in list(obj)]

    # default: leave as-is (json.dumps will raise if unhandled)
    return obj


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

    # Make sure the summary contains only JSON-friendly values before returning
    sanitized = _sanitize_for_json(summary)
    return sanitized


def save_run_summary(summary: dict, timestamp: str | None = None) -> tuple[Path, str]:
    """
    Save a JSON summary to data/run_summaries/run_summary_<timestamp>.json.

    Returns (summary_path, timestamp).
    """
    if timestamp is None:
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    summary_path = RUN_SUMMARIES_DIR / f"run_summary_{timestamp}.json"

    def default(o):
        # Make everything JSON-serializable
        return str(o)

    with summary_path.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, default=default)

    return summary_path, timestamp


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
    print("\n=== Governance Run Summary ===")

    checks = summary.get("checks", {})

    # Schema (with per-table breakdown)
    schema = checks.get("schema", {})
    schema_status = "PASSED" if schema.get("passed") else "FAILED"
    print(f"- schema: {schema_status}")
    tables = schema.get("tables") or {}
    for table_name, table_result in tables.items():
        t_status = "PASSED" if table_result.get("passed") else "FAILED"
        print(f"    - {table_name}: {t_status}")

    # Other checks
    for name in ("data_quality", "pii_policy", "foreign_keys"):
        result = checks.get(name, {})
        status = "PASSED" if result.get("passed") else "FAILED"
        print(f"- {name}: {status}")

    overall = summary.get("overall_passed")
    print(f"\nOverall passed: {overall}")

