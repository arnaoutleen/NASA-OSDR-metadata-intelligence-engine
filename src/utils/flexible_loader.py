"""
NASA OSDR Metadata Enrichment Engine - Flexible Data Loader

This module provides multi-format file loading with automatic column alias mapping.
Supports CSV, TSV, Excel, and JSON inputs with researcher-defined column names.

Usage:
    from src.utils.flexible_loader import load_flexible, FlexibleLoaderResult
    
    result = load_flexible("path/to/data.csv")
    rows = result.rows  # List of dicts with normalized column names
    warnings = result.warnings  # Any issues encountered
"""

import csv
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Path to the schema configuration
SCHEMA_CONFIG_PATH = Path(__file__).parent.parent.parent / "resources" / "schema" / "data_schema_flex.json"


@dataclass
class FlexibleLoaderResult:
    """Result from loading a flexible-format file."""
    rows: List[Dict[str, Any]] = field(default_factory=list)
    original_headers: List[str] = field(default_factory=list)
    normalized_headers: List[str] = field(default_factory=list)
    header_mapping: Dict[str, str] = field(default_factory=dict)  # original -> canonical
    warnings: List[str] = field(default_factory=list)
    skipped_rows: int = 0
    file_format: str = ""
    total_rows: int = 0


@dataclass
class SchemaConfig:
    """Parsed schema configuration."""
    field_aliases: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    file_types: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    validation_rules: Dict[str, Any] = field(default_factory=dict)
    graceful_handling: Dict[str, str] = field(default_factory=dict)


def load_schema_config(config_path: Optional[Path] = None) -> SchemaConfig:
    """
    Load the flexible schema configuration from JSON.
    
    Args:
        config_path: Optional path to config file. Defaults to resources/data_schema_flex.json
        
    Returns:
        SchemaConfig object with parsed settings
    """
    if config_path is None:
        config_path = SCHEMA_CONFIG_PATH
    
    config = SchemaConfig()
    
    if not config_path.exists():
        logger.warning(f"Schema config not found at {config_path}, using defaults")
        return _get_default_schema_config()
    
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        config.field_aliases = data.get("field_aliases", {})
        config.file_types = data.get("supported_file_types", {})
        config.validation_rules = data.get("validation_rules", {})
        config.graceful_handling = data.get("graceful_handling", {})
        
        return config
    except Exception as e:
        logger.warning(f"Failed to load schema config: {e}, using defaults")
        return _get_default_schema_config()


def _get_default_schema_config() -> SchemaConfig:
    """Return default schema configuration."""
    return SchemaConfig(
        field_aliases={
            "osd_id": {"canonical": "osd_id", "aliases": ["OSD ID", "study_id", "OSD_ID", "accession"], "required": True},
            "sample_id": {"canonical": "sample_id", "aliases": ["Sample Name", "sample_name", "source_name"], "required": True},
            "mouse_id": {"canonical": "mouse_id", "aliases": ["subject_id", "animal_id", "Mouse ID"], "required": False},
            "organism": {"canonical": "organism", "aliases": ["species", "Organism", "Species"], "required": False},
            "age": {"canonical": "age", "aliases": ["Age", "days_old", "weeks_old"], "required": False},
            "sex": {"canonical": "sex", "aliases": ["Sex", "gender", "Gender"], "required": False},
            "tissue": {"canonical": "tissue", "aliases": ["organ", "tissue_type", "Tissue", "Organ"], "required": False},
            "strain": {"canonical": "strain", "aliases": ["mouse_strain", "Strain", "genotype"], "required": False},
        },
        file_types={
            "csv": {"delimiter": ",", "encoding": "utf-8"},
            "tsv": {"delimiter": "\t", "encoding": "utf-8"},
        },
        validation_rules={
            "osd_id_pattern": r"^OSD-\d+$",
            "skip_empty_rows": True,
            "trim_whitespace": True,
            "case_insensitive_headers": True,
        },
        graceful_handling={
            "missing_required_field": "skip_row_with_warning",
            "unknown_columns": "preserve_as_is",
        },
    )


def detect_format(file_path: Path) -> str:
    """
    Detect file format from extension.
    
    Args:
        file_path: Path to the input file
        
    Returns:
        Format string: "csv", "tsv", "xlsx", or "json"
    """
    suffix = file_path.suffix.lower()
    
    format_map = {
        ".csv": "csv",
        ".tsv": "tsv",
        ".txt": "tsv",  # Assume tab-separated for .txt
        ".xlsx": "xlsx",
        ".xls": "xlsx",
        ".json": "json",
    }
    
    return format_map.get(suffix, "csv")  # Default to CSV


def build_alias_map(headers: List[str], config: SchemaConfig) -> Dict[str, str]:
    """
    Build a mapping from original column names to canonical names.
    
    Args:
        headers: Original column headers from the file
        config: Schema configuration with alias definitions
        
    Returns:
        Dict mapping original header -> canonical name
    """
    mapping = {}
    case_insensitive = config.validation_rules.get("case_insensitive_headers", True)
    
    for original_header in headers:
        header_to_match = original_header.strip()
        if case_insensitive:
            header_lower = header_to_match.lower()
        else:
            header_lower = header_to_match
        
        matched = False
        for canonical, field_info in config.field_aliases.items():
            # Check if it matches the canonical name
            canonical_check = canonical.lower() if case_insensitive else canonical
            if header_lower == canonical_check:
                mapping[original_header] = canonical
                matched = True
                break
            
            # Check aliases
            aliases = field_info.get("aliases", [])
            for alias in aliases:
                alias_check = alias.lower() if case_insensitive else alias
                if header_lower == alias_check:
                    mapping[original_header] = canonical
                    matched = True
                    break
            
            if matched:
                break
        
        # If no match, preserve original (unknown columns are kept as-is)
        if not matched:
            mapping[original_header] = original_header
    
    return mapping


def normalize_row(row: Dict[str, Any], mapping: Dict[str, str], config: SchemaConfig) -> Dict[str, Any]:
    """
    Normalize a single row using the column mapping.
    
    Args:
        row: Original row dict
        mapping: Header mapping from original to canonical
        config: Schema configuration
        
    Returns:
        Normalized row with canonical column names
    """
    normalized = {}
    trim_whitespace = config.validation_rules.get("trim_whitespace", True)
    
    for original_key, value in row.items():
        canonical_key = mapping.get(original_key, original_key)
        
        # Trim whitespace if configured
        if trim_whitespace and isinstance(value, str):
            value = value.strip()
        
        normalized[canonical_key] = value
    
    return normalized


def validate_row(row: Dict[str, Any], config: SchemaConfig) -> Tuple[bool, Optional[str]]:
    """
    Validate a row against schema requirements.
    
    Args:
        row: Normalized row dict
        config: Schema configuration
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    import re
    
    # Check required fields
    for field_name, field_info in config.field_aliases.items():
        if field_info.get("required", False):
            value = row.get(field_name, "")
            if not value or (isinstance(value, str) and not value.strip()):
                return False, f"Missing required field: {field_name}"
    
    # Validate OSD ID pattern if present
    osd_id_pattern = config.validation_rules.get("osd_id_pattern")
    if osd_id_pattern and "osd_id" in row:
        osd_id = str(row["osd_id"]).strip()
        if osd_id and not re.match(osd_id_pattern, osd_id):
            return False, f"Invalid OSD ID format: {osd_id}"
    
    return True, None


def load_csv_tsv(file_path: Path, config: SchemaConfig, delimiter: str = ",") -> FlexibleLoaderResult:
    """
    Load CSV or TSV file.
    
    Args:
        file_path: Path to the file
        config: Schema configuration
        delimiter: Field delimiter
        
    Returns:
        FlexibleLoaderResult with loaded data
    """
    result = FlexibleLoaderResult(file_format="csv" if delimiter == "," else "tsv")
    encoding = config.file_types.get("csv", {}).get("encoding", "utf-8")
    
    try:
        with open(file_path, "r", encoding=encoding, newline="") as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            
            if reader.fieldnames:
                result.original_headers = list(reader.fieldnames)
                result.header_mapping = build_alias_map(result.original_headers, config)
                result.normalized_headers = [
                    result.header_mapping.get(h, h) for h in result.original_headers
                ]
            
            for row_num, row in enumerate(reader, start=2):  # Start at 2 (header is 1)
                result.total_rows += 1
                
                # Skip empty rows if configured
                if config.validation_rules.get("skip_empty_rows", True):
                    if all(not v or (isinstance(v, str) and not v.strip()) for v in row.values()):
                        result.skipped_rows += 1
                        continue
                
                # Normalize the row
                normalized = normalize_row(row, result.header_mapping, config)
                
                # Validate the row
                is_valid, error_msg = validate_row(normalized, config)
                
                if not is_valid:
                    handling = config.graceful_handling.get("missing_required_field", "skip_row_with_warning")
                    if handling == "skip_row_with_warning":
                        result.warnings.append(f"Row {row_num}: {error_msg}")
                        result.skipped_rows += 1
                        continue
                
                result.rows.append(normalized)
    
    except Exception as e:
        result.warnings.append(f"Error loading file: {e}")
    
    return result


def load_excel(file_path: Path, config: SchemaConfig) -> FlexibleLoaderResult:
    """
    Load Excel file.
    
    Args:
        file_path: Path to the file
        config: Schema configuration
        
    Returns:
        FlexibleLoaderResult with loaded data
    """
    result = FlexibleLoaderResult(file_format="xlsx")
    
    try:
        import openpyxl
    except ImportError:
        result.warnings.append("openpyxl not installed. Install with: pip install openpyxl")
        return result
    
    try:
        sheet_idx = config.file_types.get("xlsx", {}).get("sheet", 0)
        wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
        sheet = wb.worksheets[sheet_idx]
        
        rows_iter = sheet.iter_rows(values_only=True)
        
        # First row is headers
        header_row = next(rows_iter, None)
        if header_row:
            result.original_headers = [str(h) if h else f"Column_{i}" for i, h in enumerate(header_row)]
            result.header_mapping = build_alias_map(result.original_headers, config)
            result.normalized_headers = [
                result.header_mapping.get(h, h) for h in result.original_headers
            ]
        
        for row_num, row_values in enumerate(rows_iter, start=2):
            result.total_rows += 1
            
            # Convert to dict
            row = dict(zip(result.original_headers, row_values))
            
            # Skip empty rows
            if config.validation_rules.get("skip_empty_rows", True):
                if all(not v for v in row.values()):
                    result.skipped_rows += 1
                    continue
            
            # Normalize and validate
            normalized = normalize_row(row, result.header_mapping, config)
            is_valid, error_msg = validate_row(normalized, config)
            
            if not is_valid:
                result.warnings.append(f"Row {row_num}: {error_msg}")
                result.skipped_rows += 1
                continue
            
            result.rows.append(normalized)
        
        wb.close()
    
    except Exception as e:
        result.warnings.append(f"Error loading Excel file: {e}")
    
    return result


def load_json_file(file_path: Path, config: SchemaConfig) -> FlexibleLoaderResult:
    """
    Load JSON file (expects array of record objects).
    
    Args:
        file_path: Path to the file
        config: Schema configuration
        
    Returns:
        FlexibleLoaderResult with loaded data
    """
    result = FlexibleLoaderResult(file_format="json")
    
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        if not isinstance(data, list):
            result.warnings.append("JSON file must contain an array of records")
            return result
        
        if not data:
            return result
        
        # Get headers from first record
        first_record = data[0]
        if isinstance(first_record, dict):
            result.original_headers = list(first_record.keys())
            result.header_mapping = build_alias_map(result.original_headers, config)
            result.normalized_headers = [
                result.header_mapping.get(h, h) for h in result.original_headers
            ]
        
        for row_num, record in enumerate(data, start=1):
            result.total_rows += 1
            
            if not isinstance(record, dict):
                result.warnings.append(f"Row {row_num}: Not a valid record object")
                result.skipped_rows += 1
                continue
            
            # Normalize and validate
            normalized = normalize_row(record, result.header_mapping, config)
            is_valid, error_msg = validate_row(normalized, config)
            
            if not is_valid:
                result.warnings.append(f"Row {row_num}: {error_msg}")
                result.skipped_rows += 1
                continue
            
            result.rows.append(normalized)
    
    except json.JSONDecodeError as e:
        result.warnings.append(f"Invalid JSON: {e}")
    except Exception as e:
        result.warnings.append(f"Error loading JSON file: {e}")
    
    return result


def load_flexible(
    file_path: str | Path,
    config_path: Optional[Path] = None,
) -> FlexibleLoaderResult:
    """
    Load a data file with flexible format detection and column alias mapping.
    
    This is the main entry point for the flexible loader. It:
    1. Detects the file format from extension
    2. Loads the schema configuration
    3. Parses the file and normalizes column names
    4. Validates rows and handles missing required fields gracefully
    
    Args:
        file_path: Path to the input file (CSV, TSV, Excel, or JSON)
        config_path: Optional path to schema config file
        
    Returns:
        FlexibleLoaderResult with:
        - rows: List of normalized row dicts
        - header_mapping: Original to canonical column mapping
        - warnings: Any issues encountered
        - skipped_rows: Count of rows that failed validation
    
    Example:
        >>> result = load_flexible("data/samples.csv")
        >>> for row in result.rows:
        ...     print(row["osd_id"], row["sample_id"])
    """
    file_path = Path(file_path)
    
    if not file_path.exists():
        result = FlexibleLoaderResult()
        result.warnings.append(f"File not found: {file_path}")
        return result
    
    # Load configuration
    config = load_schema_config(config_path)
    
    # Detect format
    file_format = detect_format(file_path)
    
    # Load based on format
    if file_format == "csv":
        return load_csv_tsv(file_path, config, delimiter=",")
    elif file_format == "tsv":
        return load_csv_tsv(file_path, config, delimiter="\t")
    elif file_format == "xlsx":
        return load_excel(file_path, config)
    elif file_format == "json":
        return load_json_file(file_path, config)
    else:
        # Default to CSV
        return load_csv_tsv(file_path, config, delimiter=",")


def get_required_fields(config_path: Optional[Path] = None) -> List[str]:
    """
    Get list of required fields from configuration.
    
    Args:
        config_path: Optional path to schema config
        
    Returns:
        List of required field names
    """
    config = load_schema_config(config_path)
    return [
        field_name
        for field_name, field_info in config.field_aliases.items()
        if field_info.get("required", False)
    ]


def get_all_aliases(config_path: Optional[Path] = None) -> Dict[str, List[str]]:
    """
    Get all column aliases from configuration.
    
    Args:
        config_path: Optional path to schema config
        
    Returns:
        Dict mapping canonical name to list of aliases
    """
    config = load_schema_config(config_path)
    return {
        field_name: field_info.get("aliases", [])
        for field_name, field_info in config.field_aliases.items()
    }

