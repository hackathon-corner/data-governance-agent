# tests/conftest.py
import sys
import pathlib

# Project root (the folder that contains src/, tests/, etc.)
ROOT = pathlib.Path(__file__).resolve().parents[1]

# Add to sys.path if not already there
root_str = str(ROOT)
if root_str not in sys.path:
    sys.path.insert(0, root_str)
