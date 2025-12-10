"""
NASA OSDR Metadata Intelligence Engine - Context Loader

This module loads and parses CURSOR_CONTEXT.md to extract project constraints,
goals, valid data sources, and enrichment components into a structured dictionary.

The PROJECT_CONTEXT dictionary provides grounding for the AI assistant to:
- Understand what the project does
- Know what constraints must be followed (no hallucination, provenance required)
- Identify valid data sources and their priority
- Understand the component architecture

Usage:
    from src.intelligence.context_loader import load_project_context, PROJECT_CONTEXT
    
    context = load_project_context()
    # or use the cached version:
    context = PROJECT_CONTEXT
"""

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.intelligence.exceptions import ContextLoadError


@dataclass
class ProjectContext:
    """
    Structured representation of the project context.
    
    Contains all information extracted from CURSOR_CONTEXT.md that
    the AI assistant needs for grounded reasoning.
    """
    goals: List[str] = field(default_factory=list)
    constraints: List[str] = field(default_factory=list)
    valid_sources: List[str] = field(default_factory=list)
    components: Dict[str, List[str]] = field(default_factory=dict)
    trust_policy: Dict[str, bool] = field(default_factory=dict)
    supported_organisms: List[str] = field(default_factory=list)
    key_studies: List[str] = field(default_factory=list)
    sample_patterns: Dict[str, str] = field(default_factory=dict)
    api_endpoints: Dict[str, str] = field(default_factory=dict)
    cli_commands: Dict[str, str] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "goals": self.goals,
            "constraints": self.constraints,
            "valid_sources": self.valid_sources,
            "components": self.components,
            "trust_policy": self.trust_policy,
            "supported_organisms": self.supported_organisms,
            "key_studies": self.key_studies,
            "sample_patterns": self.sample_patterns,
            "api_endpoints": self.api_endpoints,
            "cli_commands": self.cli_commands,
        }


def get_context_path() -> Path:
    """Get the path to CURSOR_CONTEXT.md."""
    # Try multiple locations - prefer the intelligence module version
    possible_paths = [
        # New location: src/intelligence/CURSOR_CONTEXT.md (primary)
        Path(__file__).parent / "CURSOR_CONTEXT.md",
        # Legacy location: intelligence/CURSOR_CONTEXT.md
        Path(__file__).parent.parent.parent / "intelligence" / "CURSOR_CONTEXT.md",
        Path.cwd() / "intelligence" / "CURSOR_CONTEXT.md",
        Path.cwd() / "src" / "intelligence" / "CURSOR_CONTEXT.md",
    ]
    
    for path in possible_paths:
        if path.exists():
            return path
    
    raise ContextLoadError(
        "Could not find CURSOR_CONTEXT.md",
        missing_sections=["CURSOR_CONTEXT.md file"]
    )


def extract_json_blocks(content: str) -> List[Dict[str, Any]]:
    """Extract all JSON blocks from markdown content."""
    import json
    blocks = []
    
    # Find all ```json ... ``` blocks
    pattern = r'```json\s*\n(.*?)\n```'
    matches = re.findall(pattern, content, re.DOTALL)
    
    for match in matches:
        try:
            parsed = json.loads(match)
            blocks.append(parsed)
        except json.JSONDecodeError:
            continue
    
    return blocks


def merge_json_blocks(blocks: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Merge multiple JSON blocks into a single dict."""
    merged = {}
    for block in blocks:
        for key, value in block.items():
            if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
                merged[key].update(value)
            else:
                merged[key] = value
    return merged


def parse_markdown_section(content: str, heading: str) -> Optional[str]:
    """Extract content under a specific markdown heading."""
    # Match both ## and ### level headings
    pattern = rf"##\s*{re.escape(heading)}.*?\n(.*?)(?=\n##|\Z)"
    match = re.search(pattern, content, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return None


def extract_list_items(content: str) -> List[str]:
    """Extract list items from markdown content."""
    items = []
    # Match both numbered and bulleted lists
    for line in content.split("\n"):
        line = line.strip()
        if line.startswith(("- ", "* ", "1.", "2.", "3.", "4.", "5.")):
            # Remove list marker
            item = re.sub(r"^[-*\d.]+\s*", "", line).strip()
            if item:
                items.append(item)
    return items


def extract_table_rows(content: str) -> List[Dict[str, str]]:
    """Extract table rows from markdown table."""
    rows = []
    lines = content.split("\n")
    headers = []
    
    for i, line in enumerate(lines):
        line = line.strip()
        if "|" in line and "---" not in line:
            cells = [c.strip() for c in line.split("|") if c.strip()]
            if not headers:
                headers = cells
            else:
                if len(cells) == len(headers):
                    rows.append(dict(zip(headers, cells)))
    
    return rows


def load_project_context(context_path: Optional[Path] = None) -> ProjectContext:
    """
    Load and parse CURSOR_CONTEXT.md into a ProjectContext object.
    
    Args:
        context_path: Optional path to CURSOR_CONTEXT.md
        
    Returns:
        ProjectContext with all extracted information
        
    Raises:
        ContextLoadError: If the file cannot be loaded or parsed
    """
    if context_path is None:
        context_path = get_context_path()
    
    try:
        with open(context_path, "r", encoding="utf-8") as f:
            content = f.read()
    except Exception as e:
        raise ContextLoadError(f"Failed to read CURSOR_CONTEXT.md: {e}")
    
    context = ProjectContext()
    missing_sections = []
    
    # Extract and merge all JSON blocks from the file
    json_blocks = extract_json_blocks(content)
    merged_json = merge_json_blocks(json_blocks)
    
    # Extract trust policy from JSON blocks
    if "trust_policy" in merged_json:
        tp = merged_json["trust_policy"]
        context.trust_policy = {
            "hallucination": tp.get("hallucination_allowed", False),
            "provenance_required": tp.get("provenance_required", True),
            "overwrite_existing": tp.get("overwrite_existing", False),
            "deterministic": tp.get("deterministic_output", True),
        }
    else:
        # Fallback to defaults
        context.trust_policy = {
            "hallucination": False,
            "provenance_required": True,
            "overwrite_existing": False,
            "deterministic": True,
        }
    
    # Extract goals from System Purpose section
    if "system_type" in merged_json:
        context.goals = [
            merged_json.get("primary_function", "metadata_enrichment"),
            merged_json.get("secondary_function", "provenance_tracking"),
        ]
    else:
        context.goals = ["science-grade metadata enrichment", "zero hallucination"]
    
    # Extract Core Design Principles (Goals and Constraints)
    principles_section = parse_markdown_section(content, "Core Design Principles")
    if principles_section:
        items = extract_list_items(principles_section)
        context.constraints = items
    
    # Also try Trust Policies section for constraints
    trust_section = parse_markdown_section(content, "Trust Policies")
    if trust_section:
        context.constraints = extract_list_items(trust_section)
    
    # Try Policy Definitions subsection
    policy_section = parse_markdown_section(content, "Policy Definitions")
    if policy_section:
        context.constraints = extract_list_items(policy_section)
    
    if not context.constraints:
        # Hardcoded fallback from design principles
        context.constraints = [
            "Never overwrite existing CSV data",
            "Never hallucinate or guess values",
            "Only fill blanks when NASA data contains supporting evidence",
            "Provenance required for every enriched value",
            "Same input must always produce same output",
        ]
    
    # Extract valid provenance sources from JSON blocks
    if "provenance_sources" in merged_json:
        ps = merged_json["provenance_sources"]
        context.valid_sources = (
            ps.get("high_confidence", []) +
            ps.get("medium_confidence", []) +
            ps.get("low_confidence", [])
        )
    else:
        provenance_section = parse_markdown_section(content, "Valid Provenance Sources")
        if provenance_section:
            context.valid_sources = extract_list_items(provenance_section)
        else:
            context.valid_sources = [
                "from_osdr_metadata",
                "from_isa_characteristics",
                "from_isa_factor_values",
                "from_study_description",
                "inferred_from_sample_name_structure",
                "not_applicable",
            ]
    
    # Extract components from enrichment_rules JSON block
    if "enrichment_rules" in merged_json:
        rules = merged_json["enrichment_rules"]
        implemented = [r["field"] for r in rules.get("implemented", [])]
        context.components = {
            "core": ["constants.py", "provenance.py", "osdr_client.py", 
                     "isa_parser.py", "enrichment_rules.py", "pipeline.py"],
            "intelligence": ["pattern_extractor.py", "unit_inference.py",
                           "biological_rules.py", "ai_reasoner.py",
                           "context_loader.py", "research_planner.py",
                           "feedback_loop.py", "pipeline_connector.py"],
            "validation": ["schema_validator.py", "conflict_checker.py",
                          "replicate_checker.py", "timeline_reconstructor.py"],
            "cli": ["process_csv.py", "process_osd_study.py"],
            "enriched_fields": implemented,
        }
    else:
        context.components = {
            "core": ["constants.py", "provenance.py", "osdr_client.py", 
                     "isa_parser.py", "enrichment_rules.py", "pipeline.py"],
            "intelligence": ["pattern_extractor.py", "unit_inference.py",
                           "biological_rules.py", "ai_reasoner.py"],
            "validation": ["schema_validator.py", "conflict_checker.py",
                          "replicate_checker.py", "timeline_reconstructor.py"],
            "cli": ["process_csv.py", "process_osd_study.py"],
        }
    
    # Extract supported organisms from JSON blocks
    if "organisms" in merged_json:
        orgs = merged_json["organisms"].get("supported", [])
        context.supported_organisms = [o.get("name", "") for o in orgs if o.get("name")]
    else:
        context.supported_organisms = [
            "Mus musculus",
            "Rattus norvegicus",
            "Homo sapiens",
        ]
    
    # Extract sample patterns from JSON blocks
    if "sample_patterns" in merged_json:
        sp = merged_json["sample_patterns"]
        context.sample_patterns = {}
        context.sample_patterns.update(sp.get("organism_prefix", {}))
        context.sample_patterns.update(sp.get("tissue_codes", {}))
        context.sample_patterns.update(sp.get("condition_codes", {}))
    else:
        context.sample_patterns = {
            "Mmus_": "Mus musculus",
            "Rnor_": "Rattus norvegicus",
            "_FLT_": "Flight",
            "_GC_": "Ground Control",
        }
    
    # Extract key studies from JSON blocks
    if "test_studies" in merged_json:
        context.key_studies = [s.get("id", "") for s in merged_json["test_studies"] if s.get("id")]
    else:
        studies_section = parse_markdown_section(content, "Key Studies for Testing")
        if studies_section:
            rows = extract_table_rows(studies_section)
            context.key_studies = [row.get("OSD ID", "") for row in rows if row.get("OSD ID")]
        else:
            context.key_studies = ["OSD-202", "OSD-242", "OSD-546", "OSD-661", "OSD-102"]
    
    # Extract CLI commands from JSON blocks
    if "cli_hooks" in merged_json:
        hooks = merged_json["cli_hooks"]
        context.cli_commands = {
            name: data.get("command", "") for name, data in hooks.items()
        }
    else:
        context.cli_commands = {
            "enrich_csv": "python -m cli.process_csv <input.csv> -o <output.csv>",
            "process_study": "python -m cli.process_osd_study <OSD-ID>",
            "clear_cache": "python -m cli.process_csv <input.csv> --clear-cache",
        }
    
    # Extract API endpoints
    context.api_endpoints = {
        "biodata": "https://visualization.osdr.nasa.gov/biodata/api/v2/",
        "developer": "https://osdr.nasa.gov/osdr/data/",
    }
    
    # Check for critical missing sections
    if missing_sections:
        # Log warning but don't fail - use defaults
        pass
    
    return context


def get_project_context() -> Dict[str, Any]:
    """
    Get the project context as a dictionary.
    
    This is the main entry point for accessing project context.
    Results are cached after first load.
    
    Returns:
        Dictionary containing all project context information
    """
    global _cached_context
    if _cached_context is None:
        _cached_context = load_project_context()
    return _cached_context.to_dict()


# Module-level cache
_cached_context: Optional[ProjectContext] = None


# Convenience function to get context object
def get_context_object() -> ProjectContext:
    """Get the ProjectContext object (not dictionary)."""
    global _cached_context
    if _cached_context is None:
        _cached_context = load_project_context()
    return _cached_context


# Lazy-loaded PROJECT_CONTEXT for backwards compatibility
class _LazyProjectContext:
    """Lazy loader for PROJECT_CONTEXT dictionary."""
    
    def __init__(self):
        self._context = None
    
    def _load(self):
        if self._context is None:
            self._context = get_project_context()
        return self._context
    
    def __getitem__(self, key):
        return self._load()[key]
    
    def __contains__(self, key):
        return key in self._load()
    
    def get(self, key, default=None):
        return self._load().get(key, default)
    
    def keys(self):
        return self._load().keys()
    
    def values(self):
        return self._load().values()
    
    def items(self):
        return self._load().items()
    
    def __repr__(self):
        return repr(self._load())


# Export as lazy-loaded dict-like object
PROJECT_CONTEXT = _LazyProjectContext()

