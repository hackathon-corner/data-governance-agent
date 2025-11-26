# src/ui/dashboard.py

from __future__ import annotations

from pathlib import Path
from typing import List, Tuple

import streamlit as st

from src.agents.run_summary_agent import generate_markdown_report
import sys
import os

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_DIR)

# Adjust if your reports directory lives elsewhere
PROJECT_ROOT = Path(__file__).resolve().parents[2]
REPORTS_DIR = PROJECT_ROOT / "reports"


def list_reports() -> List[Tuple[Path, float]]:
    """
    Return a list of (path, mtime) for all markdown reports, newest first.
    """
    if not REPORTS_DIR.exists():
        return []

    reports = []
    for path in REPORTS_DIR.glob("*.md"):
        reports.append((path, path.stat().st_mtime))

    # Sort by last modified time, descending (newest first)
    reports.sort(key=lambda x: x[1], reverse=True)
    return reports


def load_report_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except Exception as e:
        return f"Error reading report {path}: {e}"


def main() -> None:
    st.set_page_config(
        page_title="Data Governance Dashboard",
        layout="wide",
    )

    st.title("🧭 Data Governance Pipeline Dashboard")

    st.markdown(
        """
This dashboard lets you:
- Run the data governance pipeline on the latest raw events,
- View the most recent governance report,
- Browse previously generated reports.
"""
    )

    # Layout: left = controls / run status, right = report viewer
    col_left, col_right = st.columns([1, 2], gap="large")

    # --- Left column: controls & list of reports ---
    with col_left:
        st.subheader("Run Governance Pipeline")

        if st.button("▶ Run pipeline and generate report"):
            with st.spinner("Running pipeline and generating governance report..."):
                result = generate_markdown_report()
            status = result.get("status", "unknown")
            report_path_str = result.get("report_path", "N/A")
            overall_passed = result.get("overall_passed", False)

            if status == "success":
                if overall_passed:
                    st.success(f"Pipeline succeeded. Governance report saved to:\n{report_path_str}")
                else:
                    st.warning(
                        f"Pipeline completed with issues. Governance report saved to:\n{report_path_str}"
                    )
            else:
                st.error(f"Report generation failed with status '{status}'.")
                st.json(result)

        st.markdown("---")
        st.subheader("Available Reports")

        reports = list_reports()
        if not reports:
            st.info("No reports found yet. Run the pipeline to generate one.")
            selected_report = None
        else:
            # Show a selectbox of available reports
            labels = [
                f"{path.name} (modified: {Path(path).stat().st_mtime:.0f})"
                for path, _ in reports
            ]
            default_index = 0  # newest by default
            selected_label = st.selectbox(
                "Select a report to view:",
                options=labels,
                index=default_index,
            )
            selected_report = reports[labels.index(selected_label)][0]

        st.markdown("---")
        st.caption(f"Reports directory: `{REPORTS_DIR}`")

    # --- Right column: report viewer ---
    with col_right:
        st.subheader("Governance Report")

        if selected_report is None:
            st.info("No report selected.")
        else:
            st.write(f"**Viewing:** `{selected_report.name}`")
            st.markdown("---")

            content = load_report_text(selected_report)
            # Treat the saved report as markdown
            st.markdown(content, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
