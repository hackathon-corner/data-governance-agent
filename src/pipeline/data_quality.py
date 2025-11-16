"""
data_quality.py

Performs data quality checks on a dataframe using rules
defined in the pipeline config.

Checks implemented:
- Null fraction per column vs a max threshold
- Required non-null columns
- Unique key violations
- Allowed event types (from config)
"""

from typing import Dict, Any, List
import pandas as pd


def _null_fractions(df: pd.DataFrame) -> Dict[str, float]:
    """Return null fraction per column."""
    return {col: float(df[col].isna().mean()) for col in df.columns}


def validate_data_quality(
    df: pd.DataFrame,
    dq_config: Dict[str, Any],
) -> Dict[str, Any]:
    results: Dict[str, Any] = {
        "null_fractions": {},                  # col -> fraction
        "columns_exceeding_null_threshold": {},# col -> fraction
        "non_null_violations": {},             # col -> count of nulls
        "unique_key_violations": {},           # key or tuple -> count of duplicates
        "invalid_event_types": [],             # list of invalid event_type values
        "passed": True,
    }

    max_null_fraction = dq_config.get("max_null_fraction_per_column", 1.0)
    non_null_columns: List[str] = dq_config.get("non_null_columns", [])
    unique_keys: List[str] = dq_config.get("unique_keys", [])
    allowed_event_types: List[str] = dq_config.get("allowed_event_types", [])

    # ---- Null fraction per column ----
    null_fracs = _null_fractions(df)
    results["null_fractions"] = null_fracs

    # Only enforce threshold on configured important columns (if provided),
    # otherwise apply to all columns.
    threshold_cols = dq_config.get("null_threshold_columns")
    if threshold_cols is None:
        cols_to_check = list(null_fracs.keys())
    else:
        cols_to_check = [c for c in threshold_cols if c in df.columns]

    for col in cols_to_check:
        frac = null_fracs.get(col, 0.0)
        if frac > max_null_fraction:
            results["columns_exceeding_null_threshold"][col] = frac
            results["passed"] = False

    # ---- Non-null required columns ----
    for col in non_null_columns:
        if col in df.columns:
            null_count = int(df[col].isna().sum())
            if null_count > 0:
                results["non_null_violations"][col] = null_count
                results["passed"] = False

    # ---- Unique key violations ----
    # For now, we support single-column keys from the config.
    for key_col in unique_keys:
        if key_col in df.columns:
            dup_mask = df.duplicated(subset=[key_col])
            dup_count = int(dup_mask.sum())
            if dup_count > 0:
                results["unique_key_violations"][key_col] = dup_count
                results["passed"] = False

    # ---- Allowed event types (if configured) ----
    if allowed_event_types and "event_type" in df.columns:
        invalid_mask = ~df["event_type"].isin(allowed_event_types)
        invalid_values = sorted(set(df.loc[invalid_mask, "event_type"]))
        if invalid_values:
            results["invalid_event_types"] = invalid_values
            results["passed"] = False

    return results


def print_data_quality_results(results: Dict[str, Any]) -> None:
    print("\n=== Data Quality Check Results ===")

    if results["passed"]:
        print("✓ Data quality checks passed.")
    else:
        print("✗ Data quality checks failed.")

    # Null fractions
    print("\nNull fraction per column:")
    for col, frac in results["null_fractions"].items():
        print(f"  - {col}: {frac:.3f}")

    if results["columns_exceeding_null_threshold"]:
        print("\nColumns exceeding max null fraction threshold:")
        for col, frac in results["columns_exceeding_null_threshold"].items():
            print(f"  - {col}: {frac:.3f}")

    if results["non_null_violations"]:
        print("\nNon-null violations (required columns with nulls):")
        for col, count in results["non_null_violations"].items():
            print(f"  - {col}: {count} null values")

    if results["unique_key_violations"]:
        print("\nUnique key violations:")
        for key, dup_count in results["unique_key_violations"].items():
            print(f"  - {key}: {dup_count} duplicate rows")

    if results["invalid_event_types"]:
        print("\nInvalid event_type values:")
        print(f"  - {results['invalid_event_types']}")
