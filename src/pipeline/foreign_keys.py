# src/pipeline/foreign_keys.py

from __future__ import annotations

from typing import Dict, List, Any
import pandas as pd


def validate_foreign_keys(
    dfs: Dict[str, pd.DataFrame],
    fk_config: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Validate foreign-key style constraints across tables.

    dfs:
      mapping of table name -> DataFrame, e.g. {"events": df_events, "users": df_users, ...}

    fk_config:
      list of configs like:
      {
        "table": "events",
        "column": "user_id",
        "ref_table": "users",
        "ref_column": "user_id"
      }

    Returns:
      {
        "passed": bool,
        "violations": [
          {
            "table": str,
            "column": str,
            "ref_table": str,
            "ref_column": str,
            "missing_keys": [...],
            "missing_count": int
          },
          ...
        ]
      }
    """
    violations: List[Dict[str, Any]] = []

    for fk in fk_config:
        table = fk["table"]
        column = fk["column"]
        ref_table = fk["ref_table"]
        ref_column = fk["ref_column"]

        df = dfs.get(table)
        df_ref = dfs.get(ref_table)

        # If either side missing, skip (could also treat as failure)
        if df is None or df_ref is None:
            continue

        child_vals = set(df[column].dropna().unique())
        parent_vals = set(df_ref[ref_column].dropna().unique())

        missing = sorted(child_vals - parent_vals)
        if missing:
            violations.append(
                {
                    "table": table,
                    "column": column,
                    "ref_table": ref_table,
                    "ref_column": ref_column,
                    "missing_keys": missing[:20],  # cap examples
                    "missing_count": len(missing),
                }
            )

    return {
        "passed": len(violations) == 0,
        "violations": violations,
    }
