"""
NASA OSDR Metadata Intelligence Engine - Timeline Reconstructor

This module reconstructs experimental timelines from sample metadata,
helping to validate when_was_the_sample_collected fields.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
import re


@dataclass
class TimelineEvent:
    """A single event in an experimental timeline."""
    event_type: str  # launch, landing, sample_collection, treatment, etc.
    description: str
    date: Optional[datetime] = None
    relative_day: Optional[int] = None  # e.g., R+0, L-14
    samples: List[str] = field(default_factory=list)


@dataclass
class ExperimentTimeline:
    """Reconstructed timeline for an experiment."""
    osd_id: str
    mission_name: Optional[str] = None
    is_spaceflight: bool = False
    events: List[TimelineEvent] = field(default_factory=list)
    duration_days: Optional[int] = None
    warnings: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "osd_id": self.osd_id,
            "mission_name": self.mission_name,
            "is_spaceflight": self.is_spaceflight,
            "duration_days": self.duration_days,
            "events": [
                {
                    "type": e.event_type,
                    "description": e.description,
                    "date": e.date.isoformat() if e.date else None,
                    "relative_day": e.relative_day,
                    "sample_count": len(e.samples),
                }
                for e in self.events
            ],
            "warnings": self.warnings,
        }


class TimelineReconstructor:
    """
    Reconstructs experimental timelines from study metadata.
    
    For spaceflight studies:
    - Parses mission dates (launch, landing)
    - Calculates mission duration
    - Maps sample collection to mission timeline
    
    For ground studies:
    - Parses treatment durations
    - Groups samples by timepoint
    """
    
    # Common timepoint patterns
    TIMEPOINT_PATTERNS = [
        # Return/post-flight patterns
        (r'R\+?(\d+)', 'return', int),           # R+0, R+7, R0, R7
        (r'R\s*\+\s*(\d+)', 'return', int),      # R + 0, R + 7
        (r'(\d+)\s*days?\s+post.?return', 'return', int),
        (r'(\d+)\s*days?\s+after\s+landing', 'return', int),
        
        # Pre-launch patterns
        (r'L-(\d+)', 'launch', lambda x: -int(x)),   # L-14
        (r'(\d+)\s*days?\s+pre.?launch', 'launch', lambda x: -int(x)),
        
        # Treatment duration patterns
        (r'(\d+)\s*days?', 'treatment', int),
        (r'(\d+)\s*weeks?', 'treatment', lambda x: int(x) * 7),
        (r'(\d+)\s*months?', 'treatment', lambda x: int(x) * 30),
    ]
    
    def reconstruct(
        self,
        osd_id: str,
        metadata: Dict[str, Any],
        samples: List[Dict[str, Any]],
    ) -> ExperimentTimeline:
        """
        Reconstruct timeline from study metadata.
        
        Args:
            osd_id: The OSD identifier
            metadata: Study-level metadata
            samples: List of sample dictionaries
            
        Returns:
            ExperimentTimeline with reconstructed events
        """
        timeline = ExperimentTimeline(osd_id=osd_id)
        
        # Check if spaceflight study
        timeline.is_spaceflight = self._is_spaceflight_study(metadata)
        timeline.mission_name = metadata.get("mission_name", "")
        
        # Extract mission dates if available
        if timeline.is_spaceflight:
            self._extract_mission_dates(timeline, metadata)
        
        # Group samples by timepoint
        timepoint_groups = self._group_by_timepoint(samples)
        
        # Create events for each timepoint
        for timepoint, sample_ids in timepoint_groups.items():
            event = self._create_timepoint_event(
                timepoint,
                sample_ids,
                timeline.is_spaceflight,
            )
            if event:
                timeline.events.append(event)
        
        # Sort events by relative day
        timeline.events.sort(
            key=lambda e: e.relative_day if e.relative_day is not None else 0
        )
        
        # Calculate duration
        if timeline.events:
            days = [e.relative_day for e in timeline.events if e.relative_day is not None]
            if days:
                timeline.duration_days = max(days) - min(days)
        
        return timeline
    
    def _is_spaceflight_study(self, metadata: Dict[str, Any]) -> bool:
        """Check if study is a spaceflight study."""
        project_type = str(metadata.get("project_type", "")).lower()
        factor_names = str(metadata.get("factor_names", "")).lower()
        description = str(metadata.get("description", "")).lower()
        
        spaceflight_indicators = [
            "spaceflight", "space flight", "iss", "rodent research",
            "bion", "shuttle", "soyuz", "orbital",
        ]
        
        text = f"{project_type} {factor_names} {description}"
        
        for indicator in spaceflight_indicators:
            if indicator in text:
                return True
        
        return False
    
    def _extract_mission_dates(
        self,
        timeline: ExperimentTimeline,
        metadata: Dict[str, Any],
    ) -> None:
        """Extract mission launch and landing dates from description."""
        description = metadata.get("description", "")
        
        # Try to find date patterns
        date_patterns = [
            r'launch(?:ed)?[:\s]+(\d{1,2}[-/]\w+[-/]\d{2,4})',
            r'(\d{1,2}[-/]\w+[-/]\d{2,4})\s+launch',
            r'land(?:ed|ing)?[:\s]+(\d{1,2}[-/]\w+[-/]\d{2,4})',
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, description, re.IGNORECASE)
            if match:
                date_str = match.group(1)
                try:
                    parsed = self._parse_date(date_str)
                    if parsed:
                        if "launch" in pattern:
                            timeline.events.append(TimelineEvent(
                                event_type="launch",
                                description="Mission launch",
                                date=parsed,
                                relative_day=0,
                            ))
                        else:
                            timeline.events.append(TimelineEvent(
                                event_type="landing",
                                description="Mission landing",
                                date=parsed,
                            ))
                except Exception:
                    pass
    
    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Try to parse a date string."""
        formats = [
            "%d-%b-%Y",
            "%d/%m/%Y",
            "%m/%d/%Y",
            "%Y-%m-%d",
            "%d-%B-%Y",
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        return None
    
    def _group_by_timepoint(
        self,
        samples: List[Dict[str, Any]],
    ) -> Dict[str, List[str]]:
        """Group samples by their timepoint."""
        groups: Dict[str, List[str]] = {}
        
        for sample in samples:
            sample_id = sample.get("id", sample.get("name", ""))
            
            # Try to get timepoint from factor values
            timepoint = None
            fv = sample.get("factor_values", {})
            
            if isinstance(fv, dict):
                for key, val in fv.items():
                    key_lower = key.lower()
                    if any(t in key_lower for t in ["time", "day", "point", "collection"]):
                        timepoint = str(val)
                        break
            
            # Fall back to parsing sample ID
            if not timepoint:
                timepoint = self._extract_timepoint_from_id(sample_id)
            
            if not timepoint:
                timepoint = "unknown"
            
            if timepoint not in groups:
                groups[timepoint] = []
            groups[timepoint].append(sample_id)
        
        return groups
    
    def _extract_timepoint_from_id(self, sample_id: str) -> Optional[str]:
        """Extract timepoint from sample ID."""
        for pattern, _, _ in self.TIMEPOINT_PATTERNS:
            match = re.search(pattern, sample_id, re.IGNORECASE)
            if match:
                return match.group(0)
        
        return None
    
    def _create_timepoint_event(
        self,
        timepoint: str,
        sample_ids: List[str],
        is_spaceflight: bool,
    ) -> Optional[TimelineEvent]:
        """Create a timeline event for a timepoint."""
        if timepoint == "unknown":
            return TimelineEvent(
                event_type="sample_collection",
                description="Sample collection (timepoint unknown)",
                samples=sample_ids,
            )
        
        # Try to parse relative day
        relative_day = None
        event_type = "sample_collection"
        
        for pattern, ref_point, converter in self.TIMEPOINT_PATTERNS:
            match = re.search(pattern, timepoint, re.IGNORECASE)
            if match:
                try:
                    relative_day = converter(match.group(1))
                    if ref_point == "return":
                        event_type = "post_return_collection"
                    elif ref_point == "launch":
                        event_type = "pre_launch_collection"
                    break
                except (ValueError, TypeError):
                    pass
        
        return TimelineEvent(
            event_type=event_type,
            description=f"Sample collection: {timepoint}",
            relative_day=relative_day,
            samples=sample_ids,
        )
    
    def validate_timepoints(
        self,
        timeline: ExperimentTimeline,
        sample_timepoints: Dict[str, str],
    ) -> List[str]:
        """
        Validate sample timepoints against reconstructed timeline.
        
        Args:
            timeline: Reconstructed timeline
            sample_timepoints: Dict mapping sample_id -> timepoint string
            
        Returns:
            List of validation warnings
        """
        warnings = []
        
        # Check if samples appear in timeline events
        timeline_samples = set()
        for event in timeline.events:
            timeline_samples.update(event.samples)
        
        for sample_id, timepoint in sample_timepoints.items():
            if sample_id not in timeline_samples:
                warnings.append(
                    f"Sample {sample_id} with timepoint '{timepoint}' not found in timeline"
                )
        
        # Check for gaps in timeline
        if timeline.events:
            days = [
                e.relative_day for e in timeline.events
                if e.relative_day is not None and e.event_type != "launch"
            ]
            
            if days:
                days_sorted = sorted(days)
                for i in range(1, len(days_sorted)):
                    gap = days_sorted[i] - days_sorted[i-1]
                    if gap > 14:  # More than 2 weeks gap
                        warnings.append(
                            f"Large gap in timeline: {gap} days between timepoints"
                        )
        
        return warnings

