import pandas as pd
import numpy as np
import json
from src.pipeline.foreign_keys import validate_foreign_keys
from src.pipeline.run_summary import build_run_summary
from src.pipeline.policy_enforcement import enforce_pii_policy
from pathlib import Path as _P


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

    # Also verify numeric / boolean types were normalized (JSON-safe). The
    # underlying inputs used numpy scalars; ensure those became native
    # Python types (json.dumps-friendly)
    # find a nested numeric/boolean value in the summary
    def contains_numpy_scalar(obj):
        import numpy as _np

        if isinstance(obj, (_np.integer, _np.floating, _np.bool_)):
            return True
        if isinstance(obj, dict):
            return any(contains_numpy_scalar(v) for v in obj.values())
        if isinstance(obj, list):
            return any(contains_numpy_scalar(v) for v in obj)
        return False

    assert contains_numpy_scalar(summary) is False


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


def test_save_run_summary_handles_numpy_scalars(tmp_path, monkeypatch):
    # Construct a summary with numpy scalar types and nested structures
    import numpy as _np

    summary = {
        "run_id": "t-np",
        "checks": {
            "data_quality": {"passed": _np.bool_(False), "null_count": _np.int64(3)}
        },
        "overall_passed": _np.bool_(False),
    }

    from src.pipeline.run_summary import save_run_summary

    # Redirect RUN_SUMMARIES_DIR in the module to a tmp dir and call function
    import src.pipeline.run_summary as rs
    target = tmp_path / "run_summaries"
    target.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(rs, "RUN_SUMMARIES_DIR", target)

    path, ts = save_run_summary(summary)

    # The function should have returned a path under the tmp dir
    assert str(target) in str(path)

    # the file should exist
    assert _P(path).exists()


def test_generate_markdown_report_from_component_shape(monkeypatch, tmp_path):
    # Make a mock result that mirrors CoordinatorAgent.run returning component pieces
    import pandas as _pd

    cfg = {
        "run_id": "t-mock",
        "description": "mock run",
        "sources": {"events": {"filename": "test_data/events_sample.csv"}},
        "targets": {"events_curated": {"filename": "analytics_events.csv"}},
    }

    df_events = _pd.DataFrame({"event_id": ["e1"], "user_id": ["u1"], "course_id": ["c1"], "event_type": ["page_view"], "timestamp": ["2025-01-01T00:00:00Z"]})
    df_curated = df_events.copy()

    component_result = {
        "config": cfg,
        "schema_results": {"passed": True, "tables": {"events": {"passed": True}}},
        "dq_results": {"passed": True, "null_fractions": {}},
        "pii_results": {"passed": True, "df_curated": df_curated},
        "fk_results": {"passed": True, "violations": []},
    }

    # Monkeypatch CoordinatorAgent.run to return our mock component dict
    from src.agents.coordinator_agent import CoordinatorAgent

    monkeypatch.setattr(CoordinatorAgent, "run", lambda self, config_override=None: component_result)

    # Run the helper which should build a normalized summary, save JSON + markdown
    from src.agents.run_summary_agent import generate_markdown_report

    info = generate_markdown_report()

    # Expect keys and that files exist
    assert info["status"] == "success"
    assert "timestamp" in info

    from pathlib import Path as _P2

    report_path = _P2(info["report_path"])
    summary_path = _P2(info["summary_path"])

    assert report_path.exists()
    assert summary_path.exists()

    # The summary JSON should have the normalized 'checks' field
    import json as _json

    data = _json.loads(summary_path.read_text(encoding="utf-8"))
    assert "checks" in data
    assert data["overall_passed"] is True


def test_build_run_summary_json_serializable_with_numpy_scalars():
    # Build a result that contains numpy scalars and ensure json.dumps succeeds
    df_sample = pd.DataFrame({"a": [1, 2]})

    config = {"run_id": "t-json", "description": "desc", "sources": {"events": {"filename": "f.csv"}}, "targets": {"events_curated": {"filename": "out.csv"}}}

    # include numpy scalar values in nested results
    schema_results = {"passed": np.bool_(True), "tables": {"events": {"passed": np.bool_(True), "count": np.int64(2)}}}
    dq_results = {"passed": np.bool_(True), "null_fractions": {"a": np.float64(0.0)}}
    pii_results = {"passed": np.bool_(True), "df_curated": df_sample}
    fk_results = {"passed": np.bool_(True)}

    summary = build_run_summary(config, schema_results, dq_results, pii_results, fk_results, "f.csv", "out.csv", np.int64(2), np.int64(2))

    # should be JSON serializable without TypeError
    json.dumps(summary)
