"""
NASA OSDR Metadata Intelligence Engine - File Utilities

This module provides file I/O utilities for CSV and JSON handling,
with consistent encoding and error handling.
"""

import csv
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Union


def ensure_dir(path: Path) -> Path:
    """
    Ensure a directory exists, creating it if necessary.
    
    Args:
        path: Directory path (or file path, in which case parent is created)
        
    Returns:
        The directory path
    """
    if path.suffix:
        # It's a file path, ensure parent directory
        path.parent.mkdir(parents=True, exist_ok=True)
        return path.parent
    else:
        # It's a directory path
        path.mkdir(parents=True, exist_ok=True)
        return path


def load_csv(
    path: Path,
    encoding: str = "utf-8",
    skip_header_rows: int = 0,
) -> tuple[List[Dict[str, Any]], List[str]]:
    """
    Load a CSV file as a list of dictionaries.
    
    Args:
        path: Path to CSV file
        encoding: File encoding (default: utf-8)
        skip_header_rows: Number of header rows to skip before the column names
        
    Returns:
        Tuple of (rows as list of dicts, column headers)
        
    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If file is empty or invalid
    """
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {path}")
    
    with open(path, "r", encoding=encoding) as f:
        # Skip extra header rows if specified
        for _ in range(skip_header_rows):
            next(f)
        
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        
        if not headers:
            raise ValueError(f"CSV file has no headers: {path}")
        
        rows = list(reader)
    
    return rows, list(headers)


def save_csv(
    path: Path,
    rows: List[Dict[str, Any]],
    headers: Optional[List[str]] = None,
    encoding: str = "utf-8",
) -> None:
    """
    Save a list of dictionaries to a CSV file.
    
    Args:
        path: Output path
        rows: List of row dictionaries
        headers: Optional column headers (extracted from rows if not provided)
        encoding: File encoding (default: utf-8)
    """
    ensure_dir(path)
    
    # Determine headers
    if headers is None:
        if rows:
            headers = list(rows[0].keys())
        else:
            headers = []
    
    with open(path, "w", encoding=encoding, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def load_csv_raw(
    path: Path,
    encoding: str = "utf-8",
) -> tuple[List[List[str]], int]:
    """
    Load a CSV file as raw rows (list of lists).
    
    Useful for files with complex multi-row headers.
    
    Args:
        path: Path to CSV file
        encoding: File encoding
        
    Returns:
        Tuple of (all rows as lists, total row count)
    """
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {path}")
    
    with open(path, "r", encoding=encoding) as f:
        reader = csv.reader(f)
        rows = list(reader)
    
    return rows, len(rows)


def save_csv_raw(
    path: Path,
    rows: List[List[str]],
    encoding: str = "utf-8",
) -> None:
    """
    Save raw rows to a CSV file.
    
    Args:
        path: Output path
        rows: List of row lists
        encoding: File encoding
    """
    ensure_dir(path)
    
    with open(path, "w", encoding=encoding, newline="") as f:
        writer = csv.writer(f)
        writer.writerows(rows)


def load_json(
    path: Path,
    encoding: str = "utf-8",
) -> Union[Dict[str, Any], List[Any]]:
    """
    Load a JSON file.
    
    Args:
        path: Path to JSON file
        encoding: File encoding
        
    Returns:
        Parsed JSON data (dict or list)
        
    Raises:
        FileNotFoundError: If file doesn't exist
        json.JSONDecodeError: If file is invalid JSON
    """
    if not path.exists():
        raise FileNotFoundError(f"JSON file not found: {path}")
    
    with open(path, "r", encoding=encoding) as f:
        return json.load(f)


def save_json(
    path: Path,
    data: Union[Dict[str, Any], List[Any]],
    indent: int = 2,
    encoding: str = "utf-8",
) -> None:
    """
    Save data to a JSON file.
    
    Args:
        path: Output path
        data: Data to serialize (dict or list)
        indent: Indentation level for pretty-printing
        encoding: File encoding
    """
    ensure_dir(path)
    
    with open(path, "w", encoding=encoding) as f:
        json.dump(data, f, indent=indent, ensure_ascii=False)


def read_text(
    path: Path,
    encoding: str = "utf-8",
) -> str:
    """
    Read a text file as a string.
    
    Args:
        path: Path to text file
        encoding: File encoding
        
    Returns:
        File contents as string
    """
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    
    with open(path, "r", encoding=encoding) as f:
        return f.read()


def write_text(
    path: Path,
    content: str,
    encoding: str = "utf-8",
) -> None:
    """
    Write a string to a text file.
    
    Args:
        path: Output path
        content: Text content
        encoding: File encoding
    """
    ensure_dir(path)
    
    with open(path, "w", encoding=encoding) as f:
        f.write(content)


def list_files(
    directory: Path,
    pattern: str = "*",
    recursive: bool = False,
) -> List[Path]:
    """
    List files in a directory matching a pattern.
    
    Args:
        directory: Directory to search
        pattern: Glob pattern (default: all files)
        recursive: Whether to search recursively
        
    Returns:
        List of matching file paths
    """
    if not directory.exists():
        return []
    
    if recursive:
        return list(directory.rglob(pattern))
    else:
        return list(directory.glob(pattern))


def file_exists(path: Path) -> bool:
    """Check if a file exists."""
    return path.exists() and path.is_file()


def dir_exists(path: Path) -> bool:
    """Check if a directory exists."""
    return path.exists() and path.is_dir()

