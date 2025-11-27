from typing import Dict, Any

from src.agents.coordinator_agent import CoordinatorAgent
from src.pipeline.report_markdown import (
    build_markdown_from_summary,
    save_markdown_report,
)
from src.pipeline.run_summary import save_run_summary, build_run_summary
from src.pipeline.run_pipeline import load_events_raw

def generate_markdown_report(config_override: Dict[str, Any] | None = None) -> dict:
    """
    Run the full governance pipeline, save both JSON summary and markdown report,
    and return basic info for the UI.
    """
    coordinator = CoordinatorAgent()
    # If a full config dict is provided, pass it as a config_override.
    if isinstance(config_override, dict):
        result = coordinator.run(config_override=config_override)
    else:
        # Backwards-compatible: call without overrides
        result = coordinator.run()

    # Handle both shapes:
    # 1) {"summary": {...}}
    # 2) {...}  (summary dict directly)
    # CoordinatorAgent.run() historically returned a normalized summary dict
    # but newer implementations may return the component pieces instead:
    # {"config", "schema_results", "dq_results", "pii_results", "fk_results"}
    # Detect both shapes and convert the component shape into the normalized
    # summary expected by the report builders.
    if isinstance(result, dict) and "summary" in result:
        summary = result["summary"]
    elif isinstance(result, dict) and all(
        k in result for k in ("config", "schema_results", "dq_results", "pii_results")
    ):
        cfg = result["config"]
        schema_results = result["schema_results"]
        dq_results = result["dq_results"]
        pii_results = result["pii_results"]
        fk_results = result.get("fk_results", {})

        # Derive rows_in / rows_out for the run summary
        events_filename = cfg["sources"]["events"]["filename"]
        try:
            rows_in = len(load_events_raw(events_filename))
        except Exception:
            rows_in = 0

        rows_out = 0
        try:
            df_curated = pii_results.get("df_curated")
            if df_curated is not None:
                rows_out = len(df_curated)
        except Exception:
            rows_out = 0

        curated_filename = cfg.get("targets", {}).get("events_curated", {}).get("filename", "analytics_events.csv")

        summary = build_run_summary(
            cfg,
            schema_results,
            dq_results,
            pii_results,
            fk_results,
            events_filename,
            curated_filename,
            rows_in,
            rows_out,
        )
    else:
        summary = result

    # 1) Save JSON summary and get a timestamp we can reuse
    summary_path, timestamp = save_run_summary(summary)

    # 2) Build markdown and save report with the same timestamp
    markdown = build_markdown_from_summary(summary)
    report_path = save_markdown_report(markdown, timestamp=timestamp)

    # 3) Return metadata for the Streamlit dashboard
    return {
        "status": "success",
        "overall_passed": summary.get("overall_passed", False),
        "report_path": str(report_path),
        "summary_path": str(summary_path),
        "timestamp": timestamp,
        "markdown_preview": "\n".join(markdown.splitlines()[:40]),
    }
