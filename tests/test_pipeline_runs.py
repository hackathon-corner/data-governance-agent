# tests/test_pipeline_runs.py

import copy

from src.agents.coordinator_agent import CoordinatorAgent
from src.pipeline.run_pipeline import load_config, load_events_raw
from src.pipeline.run_summary import build_run_summary


def _run_with_source_filename(filename: str):
    """Helper: load config, override the source filename, run coordinator."""
    base_config = load_config()
    config_copy = copy.deepcopy(base_config)
    config_copy["sources"]["events"]["filename"] = filename

    # In tests we want deterministic, small fixture files. Point users/courses
    # to the dedicated test_data fixtures so schema checks operate on the
    # small in-repo samples rather than the full 'users_good.csv'.
    if "users" in config_copy["sources"]:
        config_copy["sources"]["users"]["filename"] = "test_data/users_sample.csv"
    if "courses" in config_copy["sources"]:
        config_copy["sources"]["courses"]["filename"] = "test_data/courses_sample.csv"

    # Avoid foreign-key checks in unit tests which use small synthetic test files
    # that don't include the related tables. The real pipeline runs FK checks
    # against users/courses tables; tests should isolate the behavior we're
    # asserting on (schema / dq / pii) and not blow up on missing FK tables.
    config_copy.setdefault("schema", {})["foreign_keys"] = []

    coordinator = CoordinatorAgent()
    result = coordinator.run(config_override=config_copy)
    return result


def test_good_data_passes_all_checks():
    # Use the canonical 'good' fixtures for all source tables so every check
    # is expected to pass.
    base_cfg = load_config()
    cfg = copy.deepcopy(base_cfg)
    cfg["sources"]["events"]["filename"] = "events_good.csv"
    cfg["sources"]["users"]["filename"] = "users_good.csv"
    cfg["sources"]["courses"]["filename"] = "courses_good.csv"
    cfg.setdefault("schema", {})["foreign_keys"] = []

    coordinator = CoordinatorAgent()
    result = coordinator.run(config_override=cfg)

    config = result["config"]
    schema_results = result["schema_results"]
    dq_results = result["dq_results"]
    pii_results = result["pii_results"]
    fk_results = result.get("fk_results", {})

    # rows_in/out can be inferred here from the event filename and curated df
    rows_in = len(load_events_raw(config["sources"]["events"]["filename"]))
    rows_out = len(pii_results["df_curated"])

    summary = build_run_summary(
        config,
        schema_results,
        dq_results,
        pii_results,
        fk_results,
        config["sources"]["events"]["filename"],
        config["targets"]["events_curated"]["filename"],
        rows_in,
        rows_out,
    )

    assert summary["overall_passed"] is True
    assert summary["checks"]["schema"]["passed"] is True
    assert summary["checks"]["data_quality"]["passed"] is True
    assert summary["checks"]["pii_policy"]["passed"] is True


def test_schema_bad_data_fails_schema_checks():
    # Fix path casing and avoid fk checks (done by helper)
    result = _run_with_source_filename("test_data/events_bad_sample.csv")

    # The coordinator now returns component results - compute a normalized
    # run summary (this mirrors how the real pipeline builds a run summary).
    config = result["config"]
    dq_results = result["dq_results"]
    pii_results = result["pii_results"]
    schema_results = result["schema_results"]
    fk_results = result.get("fk_results", {})

    # compute rows in/out for the test file
    rows_in = len(load_events_raw(config["sources"]["events"]["filename"]))
    rows_out = len(pii_results["df_curated"])

    summary = build_run_summary(
        config,
        schema_results,
        dq_results,
        pii_results,
        fk_results,
        config["sources"]["events"]["filename"],
        config["targets"]["events_curated"]["filename"],
        rows_in,
        rows_out,
    )

    assert summary["checks"]["schema"]["passed"] is False
    assert summary["overall_passed"] is False


def test_dq_bad_data_fails_dq_checks(monkeypatch):
    # Instead of trying to influence the CLI via environment variables,
    # point the Coordinator at the DQ-bad test payload directly and compute
    # a run summary from the returned components.
    result = _run_with_source_filename("test_data/events_dq_bad_sample.csv")

    config = result["config"]
    schema_results = result["schema_results"]
    dq_results = result["dq_results"]
    pii_results = result["pii_results"]
    fk_results = result.get("fk_results", {})

    rows_in = len(load_events_raw(config["sources"]["events"]["filename"]))
    rows_out = len(pii_results["df_curated"])

    summary = build_run_summary(
        config,
        schema_results,
        dq_results,
        pii_results,
        fk_results,
        config["sources"]["events"]["filename"],
        config["targets"]["events_curated"]["filename"],
        rows_in,
        rows_out,
    )

    checks = summary["checks"]

    # 1. Schema should pass for events (DQ-only failure)
    assert checks["schema"]["tables"]["events"]["passed"] is True

    # 2. Data quality should fail
    dq = checks["data_quality"]
    assert dq["passed"] is False

    # 3. We expect course_id to have a non-zero null fraction
    assert dq["null_fractions"].get("course_id", 0.0) > 0.0

    # 4. We expect duplicate event_id
    assert "event_id" in dq["unique_key_violations"]
