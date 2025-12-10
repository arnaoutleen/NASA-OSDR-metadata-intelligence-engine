"""
NASA OSDR Metadata Intelligence Engine - Provenance Tracking

This module provides data structures and utilities for tracking the provenance
of every enriched metadata field, ensuring full traceability and auditability.

Key components:
- ProvenanceEntry: Records where a single enriched value came from
- ConflictEntry: Records when sources disagree on a value
- ProvenanceTracker: Manages provenance logging across a pipeline run
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional
import json

from src.core.constants import ProvenanceSource, ConfidenceLevel


@dataclass
class ProvenanceEntry:
    """
    Records the provenance of a single enriched metadata field.
    
    Every field that is filled by the enrichment pipeline must have
    an associated ProvenanceEntry to maintain scientific traceability.
    
    Attributes:
        osd_id: The OSDR study identifier (e.g., "OSD-242")
        sample_id: The sample identifier, or "_study_level_" for study-wide fields
        field_name: The name of the enriched field
        value: The enriched value
        source: The provenance source (from ProvenanceSource enum)
        confidence: The confidence level of the enrichment
        evidence_path: JSON path or file reference to the source evidence
        original_value: The raw value before normalization (if applicable)
        inference_rule: The specific rule applied (if inference was used)
        timestamp: When the enrichment was performed
    """
    osd_id: str
    sample_id: str
    field_name: str
    value: Any
    source: ProvenanceSource
    confidence: ConfidenceLevel
    evidence_path: Optional[str] = None
    original_value: Optional[Any] = None
    inference_rule: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "osd_id": self.osd_id,
            "sample_id": self.sample_id,
            "field_name": self.field_name,
            "value": str(self.value) if self.value is not None else None,
            "source": self.source.value if isinstance(self.source, Enum) else self.source,
            "confidence": self.confidence.value if isinstance(self.confidence, Enum) else self.confidence,
            "evidence_path": self.evidence_path,
            "original_value": str(self.original_value) if self.original_value is not None else None,
            "inference_rule": self.inference_rule,
            "timestamp": self.timestamp.isoformat(),
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ProvenanceEntry":
        """Create ProvenanceEntry from dictionary."""
        return cls(
            osd_id=data["osd_id"],
            sample_id=data["sample_id"],
            field_name=data["field_name"],
            value=data["value"],
            source=ProvenanceSource(data["source"]) if data.get("source") else ProvenanceSource.OSDR_API_METADATA,
            confidence=ConfidenceLevel(data["confidence"]) if data.get("confidence") else ConfidenceLevel.HIGH,
            evidence_path=data.get("evidence_path"),
            original_value=data.get("original_value"),
            inference_rule=data.get("inference_rule"),
            timestamp=datetime.fromisoformat(data["timestamp"]) if data.get("timestamp") else datetime.now(),
        )


@dataclass
class ConflictEntry:
    """
    Records a conflict between data sources for a metadata field.
    
    When the OSDR API, ISA-Tab, and/or sample name inference provide
    conflicting values for the same field, the conflict is recorded
    instead of making an arbitrary choice.
    
    Attributes:
        osd_id: The OSDR study identifier
        sample_id: The sample identifier
        field_name: The field with conflicting values
        conflicting_values: Dict mapping source -> value
        resolution: How the conflict was resolved (or "unresolved")
        resolved_value: The final value chosen (if any)
        notes: Additional context about the conflict
        timestamp: When the conflict was detected
    """
    osd_id: str
    sample_id: str
    field_name: str
    conflicting_values: Dict[str, Any]
    resolution: str = "unresolved"
    resolved_value: Optional[Any] = None
    notes: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "osd_id": self.osd_id,
            "sample_id": self.sample_id,
            "field_name": self.field_name,
            "conflicting_values": {
                k: str(v) if v is not None else None 
                for k, v in self.conflicting_values.items()
            },
            "resolution": self.resolution,
            "resolved_value": str(self.resolved_value) if self.resolved_value is not None else None,
            "notes": self.notes,
            "timestamp": self.timestamp.isoformat(),
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ConflictEntry":
        """Create ConflictEntry from dictionary."""
        return cls(
            osd_id=data["osd_id"],
            sample_id=data["sample_id"],
            field_name=data["field_name"],
            conflicting_values=data["conflicting_values"],
            resolution=data.get("resolution", "unresolved"),
            resolved_value=data.get("resolved_value"),
            notes=data.get("notes"),
            timestamp=datetime.fromisoformat(data["timestamp"]) if data.get("timestamp") else datetime.now(),
        )


class ProvenanceTracker:
    """
    Manages provenance tracking across an entire pipeline run.
    
    This class aggregates ProvenanceEntry and ConflictEntry records,
    provides statistics, and exports structured provenance logs.
    """
    
    def __init__(self):
        """Initialize empty provenance tracker."""
        self._entries: List[ProvenanceEntry] = []
        self._conflicts: List[ConflictEntry] = []
        self._stats: Dict[str, int] = {}
        
        # Structured log: OSD_ID -> sample_id -> field_name -> ProvenanceEntry
        self._structured_log: Dict[str, Dict[str, Dict[str, ProvenanceEntry]]] = {}
    
    def record(
        self,
        osd_id: str,
        field_name: str,
        value: Any,
        source: ProvenanceSource,
        confidence: ConfidenceLevel = ConfidenceLevel.HIGH,
        sample_id: Optional[str] = None,
        evidence_path: Optional[str] = None,
        original_value: Optional[Any] = None,
        inference_rule: Optional[str] = None,
    ) -> ProvenanceEntry:
        """
        Record a single provenance entry.
        
        Args:
            osd_id: The OSDR study identifier
            field_name: The name of the enriched field
            value: The enriched value
            source: The provenance source
            confidence: The confidence level
            sample_id: The sample identifier (optional, defaults to "_study_level_")
            evidence_path: JSON path or file reference to source evidence
            original_value: Raw value before normalization
            inference_rule: Specific rule applied for inference
            
        Returns:
            The created ProvenanceEntry
        """
        entry = ProvenanceEntry(
            osd_id=osd_id,
            sample_id=sample_id or "_study_level_",
            field_name=field_name,
            value=value,
            source=source,
            confidence=confidence,
            evidence_path=evidence_path,
            original_value=original_value,
            inference_rule=inference_rule,
        )
        
        self._entries.append(entry)
        
        # Update stats
        stat_key = f"{field_name}:{source.value}"
        self._stats[stat_key] = self._stats.get(stat_key, 0) + 1
        
        # Update structured log
        if osd_id not in self._structured_log:
            self._structured_log[osd_id] = {}
        
        sid = sample_id or "_study_level_"
        if sid not in self._structured_log[osd_id]:
            self._structured_log[osd_id][sid] = {}
        
        self._structured_log[osd_id][sid][field_name] = entry
        
        return entry
    
    def record_conflict(
        self,
        osd_id: str,
        sample_id: str,
        field_name: str,
        conflicting_values: Dict[str, Any],
        resolution: str = "unresolved",
        resolved_value: Optional[Any] = None,
        notes: Optional[str] = None,
    ) -> ConflictEntry:
        """
        Record a conflict between data sources.
        
        Args:
            osd_id: The OSDR study identifier
            sample_id: The sample identifier
            field_name: The field with conflicting values
            conflicting_values: Dict mapping source name -> value
            resolution: How the conflict was resolved
            resolved_value: The final value chosen (if any)
            notes: Additional context
            
        Returns:
            The created ConflictEntry
        """
        conflict = ConflictEntry(
            osd_id=osd_id,
            sample_id=sample_id,
            field_name=field_name,
            conflicting_values=conflicting_values,
            resolution=resolution,
            resolved_value=resolved_value,
            notes=notes,
        )
        
        self._conflicts.append(conflict)
        return conflict
    
    @property
    def entries(self) -> List[ProvenanceEntry]:
        """Get all provenance entries."""
        return self._entries
    
    @property
    def conflicts(self) -> List[ConflictEntry]:
        """Get all conflict entries."""
        return self._conflicts
    
    def get_summary(self) -> Dict[str, Dict[str, int]]:
        """
        Get summary of enrichments by field and source.
        
        Returns:
            Nested dict: field_name -> source -> count
        """
        summary: Dict[str, Dict[str, int]] = {}
        
        for key, count in self._stats.items():
            field_name, source = key.split(":", 1)
            if field_name not in summary:
                summary[field_name] = {}
            summary[field_name][source] = count
        
        return summary
    
    def get_confidence_stats(self) -> Dict[str, int]:
        """Get counts by confidence level."""
        stats = {level.value: 0 for level in ConfidenceLevel}
        
        for entry in self._entries:
            level = entry.confidence.value if isinstance(entry.confidence, Enum) else entry.confidence
            stats[level] = stats.get(level, 0) + 1
        
        return stats
    
    def export_json(self, output_path: Path) -> None:
        """
        Export structured provenance log to JSON file.
        
        Args:
            output_path: Path to write the JSON file
        """
        export_data = {
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "total_entries": len(self._entries),
                "total_conflicts": len(self._conflicts),
            },
            "provenance": {
                osd_id: {
                    sample_id: {
                        field_name: entry.to_dict()
                        for field_name, entry in fields.items()
                    }
                    for sample_id, fields in samples.items()
                }
                for osd_id, samples in self._structured_log.items()
            },
            "conflicts": [conflict.to_dict() for conflict in self._conflicts],
            "summary": self.get_summary(),
            "confidence_stats": self.get_confidence_stats(),
        }
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(export_data, f, indent=2)
    
    def print_report(self) -> None:
        """Print a human-readable enrichment report to stdout."""
        summary = self.get_summary()
        conf_stats = self.get_confidence_stats()
        
        print("\n" + "=" * 70)
        print("PROVENANCE REPORT - Values Filled From Real Data Sources")
        print("=" * 70)
        
        total_filled = 0
        
        for field_name in sorted(summary.keys()):
            sources = summary[field_name]
            field_total = sum(sources.values())
            total_filled += field_total
            
            print(f"\n{field_name}: {field_total} cells filled")
            for source, count in sorted(sources.items()):
                print(f"    - {source}: {count}")
        
        print(f"\n{'-' * 70}")
        print("CONFIDENCE BREAKDOWN:")
        for level, count in sorted(conf_stats.items()):
            print(f"    - {level}: {count}")
        
        if self._conflicts:
            print(f"\n{'-' * 70}")
            print(f"CONFLICTS DETECTED: {len(self._conflicts)}")
            for conflict in self._conflicts[:10]:
                print(f"    - {conflict.osd_id}/{conflict.sample_id}: {conflict.field_name}")
            if len(self._conflicts) > 10:
                print(f"    ... and {len(self._conflicts) - 10} more")
        
        print(f"\n{'=' * 70}")
        print(f"TOTAL CELLS ENRICHED: {total_filled}")
        print("=" * 70)

