from typing import Dict, Any

from src.agents.coordinator_agent import CoordinatorAgent
from src.pipeline.report_markdown import (
    build_markdown_from_summary,
    save_markdown_report,
)


def generate_markdown_report() -> Dict[str, Any]:
    """
    Run the full governance pipeline, build a markdown report,
    save it under the reports/ directory, and return metadata.

    This is used as an ADK tool, so it must only return JSON-serializable
    values (no pandas DataFrames, no custom objects).
    """
    coordinator = CoordinatorAgent()
    pipeline_output = coordinator.run()

    # If CoordinatorAgent.run() returns {"summary": ...}, use that.
    # If it already returns the summary, use it directly.
    summary = pipeline_output.get("summary", pipeline_output)

    markdown = build_markdown_from_summary(summary)
    report_path = save_markdown_report(markdown)

    return {
        "status": "success",
        "overall_passed": bool(summary.get("overall_passed", False)),
        "report_path": str(report_path),
        # small preview so the LLM can reference it if needed
        "markdown_preview": "\n".join(markdown.splitlines()[:40]),
        # NOTE: we deliberately do NOT return the full `summary` here,
        # to avoid leaking any pandas objects into the tool result.
    }
