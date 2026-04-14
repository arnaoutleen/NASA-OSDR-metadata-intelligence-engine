#!/usr/bin/env python3
"""Backward-compatible wrapper for the organ-ranking CLI."""

from cli.rank_by_organ import main


if __name__ == "__main__":
    raise SystemExit(main())
