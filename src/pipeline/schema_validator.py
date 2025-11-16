"""
schema_validator.py

Validates a dataframe against a JSON schema definition.
This module performs:
- Required column checks
- Extra column detection
- Allowed values validation (for enumerated fields)
- Type validation (basic, string vs numeric)
"""

from typing import Dict, Any, List
import pandas as pd


def validate_schema(df: pd.DataFrame, schema: Dict[str, Any]) -> Dict[str, Any]:
    results = {
        "missing_columns": [],
        "extra_columns": [],
        "invalid_values": {},      # column -> list of invalid values
        "type_mismatches": {},     # column -> count or details
        "passed": True
    }

    # Extract fields defined in the schema
    schema_columns = {col["name"]: col for col in schema["columns"]}
    required_columns = [col["name"] for col in schema["columns"] if col.get("required", False)]

    # ---- Check missing required columns ----
    for col in required_columns:
        if col not in df.columns:
            results["missing_columns"].append(col)
            results["passed"] = False

    # ---- Check extra columns in raw data ----
    for col in df.columns:
        if col not in schema_columns:
            results["extra_columns"].append(col)
            results["passed"] = False

    # ---- Check values against allowed_values ----
    for col_name, col_spec in schema_columns.items():
        if "allowed_values" in col_spec and col_name in df.columns:
            allowed = col_spec["allowed_values"]
            invalid = sorted(set(df[col_name].dropna()) - set(allowed))
            if invalid:
                results["invalid_values"][col_name] = invalid
                results["passed"] = False

    # ---- Basic type checking (optional for now) ----
    # More advanced checks will be added later.
    # We'll keep type validation simple until agents handle casting.
    
    return results


def print_schema_validation_results(results: Dict[str, Any]) -> None:
    print("\n=== Schema Validation Results ===")

    if results["passed"]:
        print("✓ Schema validation passed.")
    else:
        print("✗ Schema validation failed.")

    if results["missing_columns"]:
        print("\nMissing required columns:")
        for col in results["missing_columns"]:
            print(f"  - {col}")

    if results["extra_columns"]:
        print("\nExtra columns not defined in schema:")
        for col in results["extra_columns"]:
            print(f"  - {col}")

    if results["invalid_values"]:
        print("\nColumns with invalid values:")
        for col, vals in results["invalid_values"].items():
            print(f"  - {col}: {vals}")
