"""
run_summary_agent.py

Agent wrapper for building, saving, and printing the governance run summary.
"""

from typing import Any, Dict

from src.agents.base_agent import BaseAgent
from src.pipeline.run_summary import (
    build_run_summary,
    save_run_summary,
    print_run_summary,
)


class RunSummaryAgent(BaseAgent):
    def __init__(self) -> None:
        super().__init__(
            name="RunSummaryAgent",
            description="Builds lineage and governance summary for the run.",
        )

    def run(
        self,
        config: Dict[str, Any],
        schema_results: Dict[str, Any],
        dq_results: Dict[str, Any],
        pii_results: Dict[str, Any],
        source_filename: str,
        curated_filename: str,
        rows_in: int,
        rows_out: int,
    ) -> Dict[str, Any]:
        summary = build_run_summary(
            config=config,
            schema_results=schema_results,
            dq_results=dq_results,
            pii_results=pii_results,
            source_filename=source_filename,
            curated_filename=curated_filename,
            rows_in=rows_in,
            rows_out=rows_out,
        )
        path = save_run_summary(summary)
        print_run_summary(summary)
        print(f"\nRun summary saved to: {path}")
        return {"summary": summary, "summary_path": path}
