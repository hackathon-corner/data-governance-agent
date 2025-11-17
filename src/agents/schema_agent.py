"""
schema_agent.py

Agent wrapper around schema validation logic.
"""

from typing import Any, Dict
import pandas as pd

from src.agents.base_agent import BaseAgent
from src.pipeline.schema_validator import validate_schema, print_schema_validation_results


class SchemaValidationAgent(BaseAgent):
    def __init__(self) -> None:
        super().__init__(
            name="SchemaValidationAgent",
            description="Validates dataframe against declared schema.",
        )

    def run(self, df: pd.DataFrame, schema: Dict[str, Any]) -> Dict[str, Any]:
        results = validate_schema(df, schema)
        print_schema_validation_results(results)
        return results
