# tests/test_pipeline_runs.py

import copy

from src.agents.coordinator_agent import CoordinatorAgent
from src.pipeline.run_pipeline import load_config


def _run_with_source_filename(filename: str):
    """Helper: load config, override the source filename, run coordinator."""
    base_config = load_config()
    config_copy = copy.deepcopy(base_config)
    config_copy["sources"]["events"]["filename"] = filename

    coordinator = CoordinatorAgent()
    result = coordinator.run(config_override=config_copy)
    return result


def test_good_data_passes_all_checks():
    result = _run_with_source_filename("events_sample.csv")
    summary = result["summary"]

    assert summary["overall_passed"] is True
    assert summary["checks"]["schema"]["passed"] is True
    assert summary["checks"]["data_quality"]["passed"] is True
    assert summary["checks"]["pii_policy"]["passed"] is True


def test_schema_bad_data_fails_schema_checks():
    result = _run_with_source_filename("events_bad_sample.csv")
    summary = result["summary"]

    assert summary["checks"]["schema"]["passed"] is False
    assert summary["overall_passed"] is False


def test_dq_bad_data_fails_dq_checks():
    result = _run_with_source_filename("events_dq_bad_sample.csv")
    summary = result["summary"]

    assert summary["checks"]["data_quality"]["passed"] is False
    assert summary["overall_passed"] is False
