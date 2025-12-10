"""
NASA OSDR Metadata Intelligence Engine - Replicate Checker

This module validates replicate patterns and sample groupings,
detecting missing replicates or unusual patterns.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple
import re


@dataclass
class ReplicateGroup:
    """A group of samples that are biological or technical replicates."""
    group_name: str
    sample_ids: List[str] = field(default_factory=list)
    replicate_numbers: List[int] = field(default_factory=list)
    is_complete: bool = True
    missing_replicates: List[int] = field(default_factory=list)
    
    @property
    def count(self) -> int:
        """Number of samples in group."""
        return len(self.sample_ids)


@dataclass
class ReplicateReport:
    """Report of replicate analysis for a study."""
    osd_id: str
    groups: List[ReplicateGroup] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    @property
    def total_groups(self) -> int:
        """Total number of replicate groups."""
        return len(self.groups)
    
    @property
    def incomplete_groups(self) -> List[ReplicateGroup]:
        """Groups with missing replicates."""
        return [g for g in self.groups if not g.is_complete]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "osd_id": self.osd_id,
            "total_groups": self.total_groups,
            "incomplete_count": len(self.incomplete_groups),
            "groups": [
                {
                    "name": g.group_name,
                    "count": g.count,
                    "replicates": g.replicate_numbers,
                    "is_complete": g.is_complete,
                    "missing": g.missing_replicates,
                }
                for g in self.groups
            ],
            "warnings": self.warnings,
        }


class ReplicateChecker:
    """
    Analyzes replicate patterns in sample naming.
    
    Detects:
    - Biological replicate groups (e.g., Rep1, Rep2, Rep3)
    - Missing replicates in a sequence
    - Unusual replicate counts
    """
    
    # Patterns for extracting replicate information
    REP_PATTERNS = [
        (r'_Rep(\d+)(?:_|$)', 'Rep'),      # _Rep1, _Rep2
        (r'_rep(\d+)(?:_|$)', 'rep'),      # _rep1, _rep2
        (r'_R(\d+)(?:_|$)', 'R'),          # _R1, _R2
        (r'_B(\d+)(?:_|$)', 'B'),          # _B1, _B2 (biological)
        (r'_T(\d+)(?:_|$)', 'T'),          # _T1, _T2 (technical)
        (r'_(\d+)$', 'numeric'),           # trailing number
    ]
    
    def __init__(self, min_replicates: int = 2, max_replicates: int = 10):
        """
        Initialize replicate checker.
        
        Args:
            min_replicates: Minimum expected replicates per group
            max_replicates: Maximum expected replicates per group
        """
        self.min_replicates = min_replicates
        self.max_replicates = max_replicates
    
    def analyze_study(
        self,
        osd_id: str,
        sample_ids: List[str],
    ) -> ReplicateReport:
        """
        Analyze replicate patterns for a study.
        
        Args:
            osd_id: The OSD identifier
            sample_ids: List of sample identifiers
            
        Returns:
            ReplicateReport with analysis results
        """
        report = ReplicateReport(osd_id=osd_id)
        
        if not sample_ids:
            return report
        
        # Group samples by prefix (excluding replicate suffix)
        groups = self._group_by_prefix(sample_ids)
        
        # Analyze each group
        for group_name, samples in groups.items():
            group = self._analyze_group(group_name, samples)
            report.groups.append(group)
            
            # Add warnings for issues
            if not group.is_complete:
                report.warnings.append(
                    f"Group '{group_name}' has missing replicates: {group.missing_replicates}"
                )
            
            if group.count < self.min_replicates:
                report.warnings.append(
                    f"Group '{group_name}' has only {group.count} samples (expected >= {self.min_replicates})"
                )
            
            if group.count > self.max_replicates:
                report.warnings.append(
                    f"Group '{group_name}' has {group.count} samples (unusually high)"
                )
        
        return report
    
    def _group_by_prefix(
        self,
        sample_ids: List[str],
    ) -> Dict[str, List[Tuple[str, int]]]:
        """
        Group samples by their prefix (excluding replicate number).
        
        Returns:
            Dict mapping prefix -> list of (sample_id, replicate_number)
        """
        groups: Dict[str, List[Tuple[str, int]]] = {}
        
        for sample_id in sample_ids:
            prefix, rep_num = self._extract_replicate_info(sample_id)
            
            if prefix not in groups:
                groups[prefix] = []
            
            groups[prefix].append((sample_id, rep_num))
        
        return groups
    
    def _extract_replicate_info(
        self,
        sample_id: str,
    ) -> Tuple[str, int]:
        """
        Extract prefix and replicate number from sample ID.
        
        Args:
            sample_id: The sample identifier
            
        Returns:
            Tuple of (prefix, replicate_number)
        """
        for pattern, _ in self.REP_PATTERNS:
            match = re.search(pattern, sample_id, re.IGNORECASE)
            if match:
                rep_num = int(match.group(1))
                # Get prefix by removing the matched portion
                prefix = sample_id[:match.start()]
                return (prefix, rep_num)
        
        # No replicate pattern found - use full ID as prefix
        return (sample_id, 1)
    
    def _analyze_group(
        self,
        group_name: str,
        samples: List[Tuple[str, int]],
    ) -> ReplicateGroup:
        """
        Analyze a single replicate group.
        
        Args:
            group_name: The group prefix
            samples: List of (sample_id, replicate_number) tuples
            
        Returns:
            ReplicateGroup with analysis
        """
        group = ReplicateGroup(group_name=group_name)
        
        for sample_id, rep_num in samples:
            group.sample_ids.append(sample_id)
            group.replicate_numbers.append(rep_num)
        
        # Sort replicate numbers
        group.replicate_numbers.sort()
        
        # Check for missing replicates
        if group.replicate_numbers:
            expected = set(range(
                min(group.replicate_numbers),
                max(group.replicate_numbers) + 1
            ))
            actual = set(group.replicate_numbers)
            missing = sorted(expected - actual)
            
            if missing:
                group.is_complete = False
                group.missing_replicates = missing
        
        return group
    
    def check_balance(
        self,
        report: ReplicateReport,
    ) -> List[str]:
        """
        Check if experimental groups are balanced.
        
        Args:
            report: ReplicateReport to check
            
        Returns:
            List of balance warnings
        """
        warnings = []
        
        if len(report.groups) < 2:
            return warnings
        
        # Get counts for each group
        counts = [g.count for g in report.groups]
        
        # Check if all groups have same count
        if len(set(counts)) > 1:
            min_count = min(counts)
            max_count = max(counts)
            
            if max_count - min_count > 1:
                warnings.append(
                    f"Unbalanced groups: counts range from {min_count} to {max_count}"
                )
        
        return warnings

