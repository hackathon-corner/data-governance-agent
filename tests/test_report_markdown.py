from pathlib import Path

from src.pipeline.report_markdown import (
    build_markdown_from_summary,
    save_markdown_report,
)


def _minimal_fake_summary():
    return {
        "run_id": "test-run-123",
        "description": "Test summary for report generation.",
        "overall_passed": True,
        "lineage": {
            "source": {"filename": "events_sample.csv", "rows_in": 4},
            "target": {"filename": "analytics_events.csv", "rows_out": 4},
        },
        "checks": {
            "schema": {
                "passed": True,
                "missing_columns": [],
                "extra_columns": [],
                "invalid_values": {},
            },
            "data_quality": {
                "passed": True,
                "null_fractions": {},
                "columns_exceeding_null_threshold": {},
                "non_null_violations": [],
                "unique_key_violations": [],
                "invalid_event_types": [],
            },
            "pii_policy": {
                "passed": True,
                "detected_pii_columns": ["user_email", "ip_address"],
                "removed_pii_columns": ["user_email", "ip_address"],
                "remaining_pii_in_curated": [],
            },
        },
    }


def test_build_markdown_from_summary_returns_non_empty_markdown():
    summary = _minimal_fake_summary()
    md = build_markdown_from_summary(summary)

    assert isinstance(md, str)
    assert "# Data Pipeline Governance Report" in md
    assert "Overall Status" in md
    assert "Schema Validation" in md
    assert len(md) > 50  # some minimal length


def test_save_markdown_report_creates_file(tmp_path, monkeypatch):
    """Ensure save_markdown_report writes a file under a reports/ directory."""
    summary = _minimal_fake_summary()
    md = build_markdown_from_summary(summary)

    # Run the function with cwd set to a temporary directory,
    # so Path("reports") will resolve under tmp_path.
    monkeypatch.chdir(tmp_path)

    path_str = save_markdown_report(md, filename="test_report.md")

    report_file = Path(path_str)
    assert report_file.exists()
    content = report_file.read_text(encoding="utf-8")
    assert "Data Pipeline Governance Report" in content
