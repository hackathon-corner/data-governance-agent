# src/pipeline/auto_runner.py

from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from src.pipeline.run_pipeline import load_config
from src.pipeline.report_markdown import (
    build_markdown_from_summary,
    save_markdown_report,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
STATE_DIR = PROJECT_ROOT / "data" / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)
STATE_PATH = STATE_DIR / "auto_runner_state.json"


@dataclass
class AutoRunState:
    last_processed_mtime: float = 0.0
    last_run_utc: str | None = None
    events_path: str | None = None
    last_report_path: str | None = None

    @classmethod
    def load(cls) -> "AutoRunState":
        if not STATE_PATH.exists():
            return cls()
        try:
            data = json.loads(STATE_PATH.read_text())
        except Exception:
            # If the state file is corrupted, start fresh but don't crash.
            return cls()
        return cls(
            last_processed_mtime=data.get("last_processed_mtime", 0.0),
            last_run_utc=data.get("last_run_utc"),
            events_path=data.get("events_path"),
            last_report_path=data.get("last_report_path"),
        )

    def save(self) -> None:
        data = {
            "last_processed_mtime": self.last_processed_mtime,
            "last_run_utc": self.last_run_utc,
            "events_path": self.events_path,
            "last_report_path": self.last_report_path,
        }
        STATE_PATH.write_text(json.dumps(data, indent=2))


def _get_events_path() -> Path:
    """Resolve the raw events CSV path from config."""
    config = load_config()
    events_cfg = config["sources"]["events"]
    filename = events_cfg["filename"]
    # This mirrors how run_pipeline loads from data/raw/<filename>
    return RAW_DIR / filename


def _extract_json(stdout: str) -> Dict[str, Any]:
    """Pull the JSON blob out of run_pipeline --print-json output."""
    start = stdout.find("{")
    end = stdout.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise ValueError("Could not find JSON in run_pipeline output.")
    json_str = stdout[start : end + 1]
    return json.loads(json_str)


def _run_pipeline_and_get_summary() -> Dict[str, Any]:
    """
    Call `python -m src.pipeline.run_pipeline --print-json`
    and return the parsed summary dict.
    """
    cmd = [sys.executable, "-m", "src.pipeline.run_pipeline", "--print-json"]
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as e:
        return {
            "status": "error",
            "error": "run_pipeline_failed",
            "detail": e.stderr or str(e),
        }

    try:
        summary = _extract_json(proc.stdout)
    except Exception as e:
        return {
            "status": "error",
            "error": "parse_error",
            "detail": f"Failed to parse summary JSON: {e}",
            "raw_output": proc.stdout,
        }

    summary.setdefault("status", "success")
    return summary


def auto_run_once() -> Dict[str, Any]:
    """
    Tool-friendly entry point.

    1. Check if the raw events CSV has a newer modification time than the
       last processed one (stored in data/state/auto_runner_state.json).
    2. If there is no new data, return a `no_new_data` status.
    3. If there is new data, run the full governance pipeline
       (`run_pipeline --print-json`), build a markdown report, save it,
       update state, and return a detailed result.

    This is safe to call repeatedly: when nothing changed it’s a cheap no-op.
    """
    state = AutoRunState.load()

    events_path = _get_events_path()
    try:
        current_mtime = events_path.stat().st_mtime
    except FileNotFoundError:
        return {
            "status": "error",
            "error": "file_not_found",
            "message": f"Events file not found: {events_path}",
        }

    if current_mtime <= state.last_processed_mtime:
        # Nothing new since the last successful run
        return {
            "status": "no_new_data",
            "message": "No new raw events data since last auto-run.",
            "events_path": str(events_path),
            "last_processed_mtime": state.last_processed_mtime,
            "current_mtime": current_mtime,
            "last_run_utc": state.last_run_utc,
            "last_report_path": state.last_report_path,
        }

    # New data detected → run the governance pipeline
    summary = _run_pipeline_and_get_summary()
    if summary.get("status") == "error":
        # Pipeline failed; don't update state so we can retry later
        return {
            "status": "error",
            "error": summary.get("error", "unknown"),
            "message": "Governance pipeline failed while processing new data.",
            "detail": summary.get("detail"),
            "events_path": str(events_path),
        }

    # Build and save a markdown report from the summary
    markdown = build_markdown_from_summary(summary)
    report_path = save_markdown_report(markdown)

    # Update and persist state
    state.last_processed_mtime = current_mtime
    state.last_run_utc = datetime.now(timezone.utc).isoformat()
    state.events_path = str(events_path)
    state.last_report_path = str(report_path)
    state.save()

    return {
        "status": "run_completed",
        "message": "New data detected; pipeline executed and report generated.",
        "events_path": str(events_path),
        "report_path": str(report_path),
        "last_run_utc": state.last_run_utc,
        "summary": summary,
    }


if __name__ == "__main__":
    # Simple manual test: python -m src.pipeline.auto_runner
    result = auto_run_once()
    print(json.dumps(result, indent=2))
