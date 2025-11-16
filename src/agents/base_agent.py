"""
base_agent.py

Simple base class for pipeline agents.
This is a lightweight abstraction we can later map to ADK agents.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict


class BaseAgent(ABC):
    def __init__(self, name: str, description: str = "") -> None:
        self.name = name
        self.description = description

    @abstractmethod
    def run(self, **kwargs) -> Dict[str, Any]:
        """
        Run the agent with keyword arguments.

        Returns a dict containing structured results.
        """
        raise NotImplementedError
