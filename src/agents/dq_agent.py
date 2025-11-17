"""
dq_agent.py

Agent wrapper around data quality checks.
"""

from typing import Any, Dict
import pandas as pd

from src.agents.base_agent import BaseAgent
from src.pipeline.data_quality import validate_data_quality, print_data_quality_results


class DataQualityAgent(BaseAgent):
    def __init__(self) -> None:
        super().__init__(
            name="DataQualityAgent",
            description="Performs data quality checks (nulls, uniques, enums).",
        )

    def run(self, df: pd.DataFrame, dq_config: Dict[str, Any]) -> Dict[str, Any]:
        results = validate_data_quality(df, dq_config)
        print_data_quality_results(results)
        return results
