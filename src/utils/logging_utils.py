"""
NASA OSDR Metadata Intelligence Engine - Logging Utilities

This module provides structured logging for pipeline operations,
including enrichment actions, API calls, and validation results.
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional


# Default log format
DEFAULT_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_logging(
    level: int = logging.INFO,
    log_file: Optional[Path] = None,
    format_string: Optional[str] = None,
    date_format: Optional[str] = None,
) -> None:
    """
    Configure logging for the OSDR pipeline.
    
    Args:
        level: Logging level (default: INFO)
        log_file: Optional file path for log output
        format_string: Optional custom format string
        date_format: Optional custom date format
    """
    fmt = format_string or DEFAULT_FORMAT
    datefmt = date_format or DEFAULT_DATE_FORMAT
    
    # Create formatter
    formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)
    
    # Get root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    # Clear existing handlers
    root_logger.handlers.clear()
    
    # Add console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # Add file handler if specified
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger with the specified name.
    
    Args:
        name: Logger name (typically __name__ or module path)
        
    Returns:
        Configured Logger instance
    """
    return logging.getLogger(name)


class PipelineLogger:
    """
    Specialized logger for pipeline operations.
    
    Provides structured logging for:
    - Study processing
    - Enrichment actions
    - API calls
    - Validation results
    """
    
    def __init__(self, name: str = "osdr.pipeline"):
        """Initialize pipeline logger."""
        self.logger = logging.getLogger(name)
        self._current_study: Optional[str] = None
        self._current_sample: Optional[str] = None
    
    def set_context(
        self,
        study: Optional[str] = None,
        sample: Optional[str] = None,
    ) -> None:
        """Set logging context for study/sample."""
        self._current_study = study
        self._current_sample = sample
    
    def clear_context(self) -> None:
        """Clear logging context."""
        self._current_study = None
        self._current_sample = None
    
    def _format_context(self) -> str:
        """Format current context for log messages."""
        parts = []
        if self._current_study:
            parts.append(self._current_study)
        if self._current_sample:
            parts.append(self._current_sample)
        return "/".join(parts) if parts else "global"
    
    def study_start(self, osd_id: str) -> None:
        """Log start of study processing."""
        self.set_context(study=osd_id)
        self.logger.info(f"[{osd_id}] Starting study processing")
    
    def study_complete(self, osd_id: str, samples_count: int) -> None:
        """Log completion of study processing."""
        self.logger.info(f"[{osd_id}] Complete: {samples_count} samples processed")
        self.clear_context()
    
    def api_call(self, endpoint: str, success: bool, details: str = "") -> None:
        """Log API call result."""
        ctx = self._format_context()
        status = "✓" if success else "✗"
        msg = f"[{ctx}] API {status}: {endpoint}"
        if details:
            msg += f" ({details})"
        
        if success:
            self.logger.debug(msg)
        else:
            self.logger.warning(msg)
    
    def enrichment(
        self,
        field: str,
        value: str,
        source: str,
        confidence: str = "high",
    ) -> None:
        """Log enrichment action."""
        ctx = self._format_context()
        self.logger.debug(
            f"[{ctx}] Enriched {field}={value[:50]} (source={source}, conf={confidence})"
        )
    
    def conflict(
        self,
        field: str,
        values: dict,
        resolution: str = "unresolved",
    ) -> None:
        """Log conflict detection."""
        ctx = self._format_context()
        self.logger.warning(
            f"[{ctx}] Conflict in {field}: {values} -> {resolution}"
        )
    
    def validation_warning(self, message: str) -> None:
        """Log validation warning."""
        ctx = self._format_context()
        self.logger.warning(f"[{ctx}] Validation: {message}")
    
    def validation_error(self, message: str) -> None:
        """Log validation error."""
        ctx = self._format_context()
        self.logger.error(f"[{ctx}] Validation: {message}")
    
    def info(self, message: str) -> None:
        """Log info message with context."""
        ctx = self._format_context()
        self.logger.info(f"[{ctx}] {message}")
    
    def debug(self, message: str) -> None:
        """Log debug message with context."""
        ctx = self._format_context()
        self.logger.debug(f"[{ctx}] {message}")
    
    def error(self, message: str, exc: Optional[Exception] = None) -> None:
        """Log error message with context."""
        ctx = self._format_context()
        self.logger.error(f"[{ctx}] {message}", exc_info=exc)


# Global pipeline logger instance
pipeline_logger = PipelineLogger()

