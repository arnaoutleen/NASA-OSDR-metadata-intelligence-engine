#!/usr/bin/env python3
"""Backward-compatible wrapper for the organ-first ranking CLI.

Prefer:
    python -m cli.rank_by_organ --organ liver

This wrapper remains so older instructions using ``organs_script.py`` keep
working while delegating to the real CLI implementation.
"""

from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

from cli.rank_by_organ import main


if __name__ == "__main__":
    raise SystemExit(main())
