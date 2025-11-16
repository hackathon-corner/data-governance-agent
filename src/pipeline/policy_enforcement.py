"""
policy_enforcement.py

Applies data governance policies to a dataframe, focused on PII handling.

Uses:
- policy configuration from pipeline_config.yaml
- optional PII tags from the schema
"""

from typing import Dict, Any, List
import pandas as pd


def _pii_columns_from_schema(schema: Dict[str, Any]) -> List[str]:
    pii_cols = []
    for col in schema.get("columns", []):
        if col.get("pii", False):
            pii_cols.append(col["name"])
    return pii_cols


def enforce_pii_policy(
    df: pd.DataFrame,
    schema: Dict[str, Any],
    policy_config: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Returns a dict with:
      - df_curated: PII-safe dataframe
      - detected_pii_columns: list of columns identified as PII
      - removed_pii_columns: list of columns dropped from curated
      - remaining_pii_in_curated: list of PII cols still present (if any)
      - passed: bool
    """
    df_curated = df.copy()

    # PII columns from config + schema tags
    config_pii_cols: List[str] = policy_config.get("pii_columns", [])
    schema_pii_cols: List[str] = _pii_columns_from_schema(schema)
    detected_pii = sorted(set(config_pii_cols) | set(schema_pii_cols))

    pii_allowed_in_raw = policy_config.get("pii_allowed_in_raw", True)
    pii_allowed_in_curated = policy_config.get("pii_allowed_in_curated", False)

    removed_pii_columns: List[str] = []

    # If PII is not allowed in curated, drop those columns
    if not pii_allowed_in_curated:
        for col in detected_pii:
            if col in df_curated.columns:
                df_curated.drop(columns=[col], inplace=True)
                removed_pii_columns.append(col)

    # Check what PII columns (if any) are still in curated
    remaining_pii = [c for c in detected_pii if c in df_curated.columns]

    passed = len(remaining_pii) == 0 if not pii_allowed_in_curated else True

    return {
        "df_curated": df_curated,
        "detected_pii_columns": detected_pii,
        "removed_pii_columns": removed_pii_columns,
        "remaining_pii_in_curated": remaining_pii,
        "pii_allowed_in_raw": pii_allowed_in_raw,
        "pii_allowed_in_curated": pii_allowed_in_curated,
        "passed": passed,
    }


def print_pii_policy_results(results: Dict[str, Any]) -> None:
    print("\n=== PII / Policy Enforcement Results ===")

    if results["passed"]:
        print("✓ PII policy enforcement passed.")
    else:
        print("✗ PII policy enforcement failed.")

    print("\nDetected PII columns (from config/schema):")
    if results["detected_pii_columns"]:
        for col in results["detected_pii_columns"]:
            print(f"  - {col}")
    else:
        print("  (none)")

    print("\nRemoved PII columns from curated data:")
    if results["removed_pii_columns"]:
        for col in results["removed_pii_columns"]:
            print(f"  - {col}")
    else:
        print("  (none)")

    if results["remaining_pii_in_curated"]:
        print("\nWARNING: PII columns still present in curated data:")
        for col in results["remaining_pii_in_curated"]:
            print(f"  - {col}")
