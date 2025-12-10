"""
NASA OSDR Metadata Intelligence Engine - Utilities Module

This module provides shared utilities for configuration, logging, and file I/O.
"""

from src.utils.config import Config, get_project_root, get_default_paths
from src.utils.logging_utils import get_logger, setup_logging
from src.utils.file_utils import (
    load_csv,
    save_csv,
    load_json,
    save_json,
    ensure_dir,
)
from src.utils.flexible_loader import (
    load_flexible,
    FlexibleLoaderResult,
    load_schema_config,
    get_required_fields,
    get_all_aliases,
)

__all__ = [
    # Config
    "Config",
    "get_project_root",
    "get_default_paths",
    # Logging
    "get_logger",
    "setup_logging",
    # File utilities
    "load_csv",
    "save_csv",
    "load_json",
    "save_json",
    "ensure_dir",
    # Flexible loader
    "load_flexible",
    "FlexibleLoaderResult",
    "load_schema_config",
    "get_required_fields",
    "get_all_aliases",
]

