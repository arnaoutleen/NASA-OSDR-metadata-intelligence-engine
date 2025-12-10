"""
NASA OSDR Metadata Intelligence Engine - ISA-Tab Parser

This module provides parsing functionality for ISA-Tab archives,
extracting structured metadata from Investigation, Study, and Assay files.

ISA-Tab Structure:
- i_*.txt: Investigation file (project-level metadata)
- s_*.txt: Study file (sample characteristics, source-sample mappings)
- a_*.txt: Assay file(s) (technology-specific sample info)

The Study file is the primary source for sample-level characteristics and factor values.
"""

import csv
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class ISACharacteristic:
    """A single characteristic from ISA-Tab (e.g., Characteristics[Strain])."""
    category: str
    value: str
    unit: Optional[str] = None
    term_source: Optional[str] = None
    term_accession: Optional[str] = None


@dataclass
class ISAFactorValue:
    """A single factor value from ISA-Tab (e.g., Factor Value[Spaceflight])."""
    factor_name: str
    value: str
    unit: Optional[str] = None
    term_source: Optional[str] = None
    term_accession: Optional[str] = None


@dataclass
class ISASample:
    """Parsed sample from ISA-Tab Study file."""
    sample_name: str
    source_name: str
    characteristics: List[ISACharacteristic] = field(default_factory=list)
    factor_values: List[ISAFactorValue] = field(default_factory=list)
    protocols: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "id": self.sample_name,
            "source_name": self.source_name,
            "characteristics": [
                {
                    "category": c.category,
                    "value": c.value,
                    "unit": c.unit,
                }
                for c in self.characteristics
            ],
            "factor_values": {
                fv.factor_name: fv.value
                for fv in self.factor_values
            },
            # Convenience extractions
            "strain": self._get_characteristic("strain"),
            "sex": self._get_characteristic("sex"),
            "age": self._get_characteristic("age"),
            "material_type": self._get_characteristic("organism part") or self._get_characteristic("material type"),
        }
    
    def _get_characteristic(self, category: str) -> str:
        """Get value for a characteristic by category (case-insensitive)."""
        category_lower = category.lower()
        for c in self.characteristics:
            if category_lower in c.category.lower():
                return c.value
        return ""


@dataclass
class ISAAssay:
    """Parsed assay from ISA-Tab Assay file."""
    filename: str
    measurement_type: str
    technology_type: str
    technology_platform: str
    samples: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "filename": self.filename,
            "type": self.technology_type,
            "platform": self.technology_platform,
            "measurement_type": self.measurement_type,
            "sample_count": len(self.samples),
        }


@dataclass
class ISAStudyMetadata:
    """Complete parsed ISA-Tab metadata for a study."""
    osd_id: str
    investigation_title: str = ""
    investigation_description: str = ""
    study_title: str = ""
    study_description: str = ""
    samples: List[ISASample] = field(default_factory=list)
    assays: List[ISAAssay] = field(default_factory=list)
    factors: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "accession": self.osd_id,
            "title": self.study_title or self.investigation_title,
            "description": self.study_description or self.investigation_description,
            "samples": [s.to_dict() for s in self.samples],
            "assays": [a.to_dict() for a in self.assays],
            "factor_names": "     ".join(self.factors),
        }


class ISAParser:
    """
    Parser for ISA-Tab archives.
    
    Parses Investigation, Study, and Assay files to extract
    structured metadata with characteristics and factor values.
    """
    
    def __init__(self, isa_tab_dir: Path):
        """
        Initialize ISA-Tab parser.
        
        Args:
            isa_tab_dir: Path to directory containing ISA-Tab files
        """
        self.isa_tab_dir = isa_tab_dir
    
    def parse(self, osd_id: str) -> Optional[ISAStudyMetadata]:
        """
        Parse all ISA-Tab files for a study.
        
        Args:
            osd_id: The OSD identifier
            
        Returns:
            ISAStudyMetadata with all parsed data, or None if files not found
        """
        study_dir = self.isa_tab_dir / osd_id
        
        if not study_dir.exists():
            return None
        
        metadata = ISAStudyMetadata(osd_id=osd_id)
        
        # Parse Investigation file
        investigation_files = list(study_dir.glob("i_*.txt"))
        if investigation_files:
            self._parse_investigation(investigation_files[0], metadata)
        
        # Parse Study file(s)
        study_files = list(study_dir.glob("s_*.txt"))
        for study_file in study_files:
            self._parse_study_file(study_file, metadata)
        
        # Parse Assay file(s)
        assay_files = list(study_dir.glob("a_*.txt"))
        for assay_file in assay_files:
            self._parse_assay_file(assay_file, metadata)
        
        return metadata if metadata.samples else None
    
    def _parse_investigation(
        self,
        file_path: Path,
        metadata: ISAStudyMetadata,
    ) -> None:
        """
        Parse Investigation file (i_*.txt).
        
        The Investigation file uses a key-value format, not TSV.
        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
            
            # Extract Investigation Title
            match = re.search(r'Investigation Title\t"?([^"\n]+)"?', content)
            if match:
                metadata.investigation_title = match.group(1).strip()
            
            # Extract Investigation Description
            match = re.search(r'Investigation Description\t"?([^"\n]+)"?', content)
            if match:
                metadata.investigation_description = match.group(1).strip()
            
            # Extract Study Title
            match = re.search(r'Study Title\t"?([^"\n]+)"?', content)
            if match:
                metadata.study_title = match.group(1).strip()
            
            # Extract Study Description
            match = re.search(r'Study Description\t"?([^"\n]+)"?', content)
            if match:
                metadata.study_description = match.group(1).strip()
            
            # Extract Factor Names
            match = re.search(r'Study Factor Name\t(.+)', content)
            if match:
                factors_str = match.group(1)
                factors = [f.strip().strip('"') for f in factors_str.split("\t") if f.strip()]
                metadata.factors = factors
                
        except Exception:
            pass
    
    def _parse_study_file(
        self,
        file_path: Path,
        metadata: ISAStudyMetadata,
    ) -> None:
        """
        Parse Study file (s_*.txt).
        
        The Study file is TSV format with:
        - Source Name: Subject/animal identifier
        - Sample Name: Sample identifier
        - Characteristics[*]: Subject attributes
        - Factor Value[*]: Experimental conditions
        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.reader(f, delimiter="\t")
                headers = next(reader, [])
            
            if not headers:
                return
            
            # Find column indices
            col_map = self._build_column_map(headers)
            
            # Parse data rows
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.reader(f, delimiter="\t")
                next(reader)  # Skip header
                
                for row in reader:
                    if not row or not any(row):
                        continue
                    
                    sample = self._parse_study_row(row, col_map, headers)
                    if sample:
                        metadata.samples.append(sample)
                        
        except Exception:
            pass
    
    def _build_column_map(self, headers: List[str]) -> Dict[str, int]:
        """Build a map of column types to indices."""
        col_map: Dict[str, int] = {}
        
        for i, h in enumerate(headers):
            h_lower = h.lower()
            
            if "sample name" in h_lower:
                col_map["sample_name"] = i
            elif "source name" in h_lower:
                col_map["source_name"] = i
        
        return col_map
    
    def _parse_study_row(
        self,
        row: List[str],
        col_map: Dict[str, int],
        headers: List[str],
    ) -> Optional[ISASample]:
        """Parse a single row from Study file."""
        # Get sample name
        sample_name = ""
        if "sample_name" in col_map and col_map["sample_name"] < len(row):
            sample_name = row[col_map["sample_name"]].strip()
        
        if not sample_name:
            return None
        
        # Get source name
        source_name = ""
        if "source_name" in col_map and col_map["source_name"] < len(row):
            source_name = row[col_map["source_name"]].strip()
        
        sample = ISASample(
            sample_name=sample_name,
            source_name=source_name,
        )
        
        # Extract characteristics and factor values from headers
        for i, header in enumerate(headers):
            if i >= len(row):
                continue
            
            value = row[i].strip()
            if not value:
                continue
            
            # Parse Characteristics[*]
            char_match = re.match(r"Characteristics\[([^\]]+)\]", header, re.IGNORECASE)
            if char_match:
                category = char_match.group(1)
                sample.characteristics.append(ISACharacteristic(
                    category=category,
                    value=value,
                ))
                continue
            
            # Parse Factor Value[*]
            factor_match = re.match(r"Factor Value\[([^\]]+)\]", header, re.IGNORECASE)
            if factor_match:
                factor_name = factor_match.group(1)
                sample.factor_values.append(ISAFactorValue(
                    factor_name=factor_name,
                    value=value,
                ))
        
        return sample
    
    def _parse_assay_file(
        self,
        file_path: Path,
        metadata: ISAStudyMetadata,
    ) -> None:
        """
        Parse Assay file (a_*.txt).
        
        Extracts assay type, platform, and sample list.
        """
        try:
            filename = file_path.name
            
            # Extract assay type from filename
            # e.g., "a_OSD-242_transcription-profiling_rna-sequencing_platform.txt"
            parts = filename.replace(".txt", "").split("_")
            
            measurement_type = ""
            technology_type = ""
            platform = ""
            
            if len(parts) >= 3:
                measurement_type = parts[2].replace("-", " ").title()
            if len(parts) >= 4:
                technology_type = parts[3].replace("-", " ").title()
            if len(parts) >= 5:
                platform = parts[-1]
            
            # Count samples
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.reader(f, delimiter="\t")
                headers = next(reader, [])
                
                # Find Sample Name column
                sample_col = -1
                for i, h in enumerate(headers):
                    if "sample name" in h.lower():
                        sample_col = i
                        break
                
                samples = []
                if sample_col >= 0:
                    for row in reader:
                        if sample_col < len(row):
                            sample_name = row[sample_col].strip()
                            if sample_name:
                                samples.append(sample_name)
            
            assay = ISAAssay(
                filename=filename,
                measurement_type=measurement_type,
                technology_type=technology_type,
                technology_platform=platform,
                samples=samples,
            )
            
            metadata.assays.append(assay)
            
        except Exception:
            pass
    
    def get_samples_as_dicts(self, osd_id: str) -> List[Dict[str, Any]]:
        """
        Parse ISA-Tab and return samples as dictionaries.
        
        Convenience method for integration with the enrichment pipeline.
        
        Args:
            osd_id: The OSD identifier
            
        Returns:
            List of sample dictionaries
        """
        metadata = self.parse(osd_id)
        if not metadata:
            return []
        
        return [s.to_dict() for s in metadata.samples]
    
    def merge_with_api_metadata(
        self,
        api_metadata: Dict[str, Any],
        osd_id: str,
    ) -> Dict[str, Any]:
        """
        Merge ISA-Tab parsed data with API metadata.
        
        ISA-Tab data is used to fill gaps in API metadata,
        especially for sample-level characteristics.
        
        Args:
            api_metadata: Metadata dict from OSDR API
            osd_id: The OSD identifier
            
        Returns:
            Merged metadata dict
        """
        isa_metadata = self.parse(osd_id)
        if not isa_metadata:
            return api_metadata
        
        result = api_metadata.copy()
        
        # If API has no samples, use ISA-Tab samples
        if not result.get("samples"):
            result["samples"] = [s.to_dict() for s in isa_metadata.samples]
        
        # If API has no assays, use ISA-Tab assays
        if not result.get("assays"):
            result["assays"] = [a.to_dict() for a in isa_metadata.assays]
        
        # Fill missing study-level fields
        if not result.get("title"):
            result["title"] = isa_metadata.study_title or isa_metadata.investigation_title
        
        if not result.get("description"):
            result["description"] = isa_metadata.study_description or isa_metadata.investigation_description
        
        if not result.get("factor_names"):
            result["factor_names"] = "     ".join(isa_metadata.factors)
        
        return result

