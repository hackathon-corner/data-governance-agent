import pandas as pd
from src.pipeline.foreign_keys import validate_foreign_keys
from src.pipeline.run_summary import build_run_summary
from src.pipeline.policy_enforcement import enforce_pii_policy


def test_validate_foreign_keys_detects_missing_parent_keys():
    # child table events references user_id 1,2,3 but users table only contains 1
    df_events = pd.DataFrame({"event_id": [1, 2, 3], "user_id": [1, 2, 3]})
    df_users = pd.DataFrame({"user_id": [1]})

    dfs = {"events": df_events, "users": df_users}
    fk_config = [
        {"table": "events", "column": "user_id", "ref_table": "users", "ref_column": "user_id"}
    ]

    result = validate_foreign_keys(dfs, fk_config)

    assert result["passed"] is False
    assert len(result["violations"]) == 1
    violation = result["violations"][0]
    assert violation["table"] == "events"
    assert violation["column"] == "user_id"
    # missing keys should include 2 and 3
    assert set(violation["missing_keys"]) == {2, 3}


def test_validate_foreign_keys_passes_when_all_parents_exist():
    df_events = pd.DataFrame({"event_id": [1, 2], "user_id": [1, 2]})
    df_users = pd.DataFrame({"user_id": [1, 2, 3]})

    dfs = {"events": df_events, "users": df_users}
    fk_config = [
        {"table": "events", "column": "user_id", "ref_table": "users", "ref_column": "user_id"}
    ]

    result = validate_foreign_keys(dfs, fk_config)
    assert result["passed"] is True
    assert result["violations"] == []


def test_validate_foreign_keys_skips_when_tables_missing():
    # When ref_table missing, validate_foreign_keys should skip rather than raise
    df_events = pd.DataFrame({"event_id": [1], "user_id": [1]})
    dfs = {"events": df_events}
    fk_config = [
        {"table": "events", "column": "user_id", "ref_table": "users", "ref_column": "user_id"}
    ]

    result = validate_foreign_keys(dfs, fk_config)
    assert result["passed"] is True
    assert result["violations"] == []


def test_build_run_summary_removes_dataframes_and_normalizes_booleans():
    # Build inputs where some results contain DataFrame objects
    df_sample = pd.DataFrame({"a": [1, 2]})

    config = {"run_id": "t1", "description": "desc", "sources": {"events": {"filename": "f.csv"}}, "targets": {"events_curated": {"filename": "out.csv"}}}

    schema_results = {"passed": True, "tables": {"events": {"passed": True}, "users": {"passed": True}}, "debug_df": df_sample}
    dq_results = {"passed": False, "null_fractions": {}, "debug": df_sample}
    pii_results = {"passed": True, "df_curated": df_sample}
    fk_results = {"passed": True}

    summary = build_run_summary(config, schema_results, dq_results, pii_results, fk_results, "f.csv", "out.csv", 2, 2)

    # No pandas DataFrame objects should be present anywhere in the top-level summary
    def contains_df(obj):
        if isinstance(obj, pd.DataFrame):
            return True
        if isinstance(obj, dict):
            return any(contains_df(v) for v in obj.values())
        if isinstance(obj, list):
            return any(contains_df(v) for v in obj)
        return False

    assert contains_df(summary) is False
    # overall_passed should be False because dq_results.passed is False
    assert summary["overall_passed"] is False


def test_enforce_pii_policy_allows_or_removes_cols_based_on_flags():
    df = pd.DataFrame({"id": [1, 2], "email": ["a@example.com", "b@example.com"], "ip": ["1.1.1.1", "2.2.2.2"]})
    schema = {"columns": [{"name": "id"}, {"name": "email", "pii": True}, {"name": "ip", "pii": True}]}

    # Case A: curated allows pii -> nothing removed, passed True
    policy_cfg = {"pii_allowed_in_curated": True, "pii_allowed_in_raw": True, "pii_columns": []}
    results = enforce_pii_policy(df, schema, policy_cfg)
    assert results["passed"] is True
    assert results["removed_pii_columns"] == []
    # curated should still contain pii columns
    assert set(results["df_curated"].columns) >= {"email", "ip"}

    # Case B: curated disallows pii, but schema lists pii columns only partly present
    policy_cfg2 = {"pii_allowed_in_curated": False, "pii_allowed_in_raw": True, "pii_columns": ["phone"]}
    results2 = enforce_pii_policy(df, schema, policy_cfg2)
    # phone not present -> removed_pii_columns should include only schema PII
    assert "email" in results2["removed_pii_columns"]
    assert "ip" in results2["removed_pii_columns"]
    assert results2["passed"] is True
