"""
run_with_agents.py

Alternative entrypoint that uses CoordinatorAgent
instead of calling the pipeline functions directly.
"""

from src.agents.coordinator_agent import CoordinatorAgent


def main() -> None:
    coordinator = CoordinatorAgent()
    coordinator.run()


if __name__ == "__main__":
    main()
