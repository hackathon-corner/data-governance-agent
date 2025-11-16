"""
pii_policy_agent.py

Agent wrapper for PII / policy enforcement.
"""

from typing import Any, Dict
import pandas as pd

from src.agents.base_agent import BaseAgent
from src.pipeline.policy_enforcement import enforce_pii_policy, print_pii_policy_results


class PiiPolicyAgent(BaseAgent):
    def __init__(self) -> None:
        super().__init__(
            name="PiiPolicyAgent",
            description="Enforces PII policy rules and produces a curated dataframe.",
        )

    def run(
        self,
        df: pd.DataFrame,
        schema: Dict[str, Any],
        policy_config: Dict[str, Any],
    ) -> Dict[str, Any]:
        results = enforce_pii_policy(df, schema, policy_config)
        print_pii_policy_results(results)
        return results
