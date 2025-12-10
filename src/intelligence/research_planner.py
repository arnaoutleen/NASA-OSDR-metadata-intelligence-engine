"""
NASA OSDR Metadata Intelligence Engine - Research Planner

This module converts natural language research requests into structured,
validated research plans that can be executed by the enrichment pipeline.

The planner:
1. Parses natural language input to extract key entities
2. Validates the request against project constraints
3. Generates a structured plan with required datasets and checks
4. Flags any issues or ambiguities for clarification

Usage:
    from src.intelligence.research_planner import ResearchPlanner
    
    planner = ResearchPlanner()
    plan = planner.create_plan("I want to analyze immune gene expression in flight mice")
"""

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set
from enum import Enum

from src.intelligence.context_loader import get_project_context, get_context_object
from src.intelligence.exceptions import RefinementNeeded, ValidationError
from src.core.constants import (
    CONTROLLED_ASSAY_TYPES,
    CONTROLLED_TISSUES,
    RODENT_ORGANISMS,
    MODEL_ORGANISMS,
    SPACEFLIGHT_INDICATORS,
    GROUND_CONTROL_INDICATORS,
    HLU_INDICATORS,
)


class ConfidenceLevel(str, Enum):
    """Confidence in the research plan."""
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


@dataclass
class ResearchPlan:
    """
    A structured research plan derived from natural language input.
    
    Contains all the information needed to execute a metadata
    enrichment pipeline for the user's research goal.
    """
    goal: str
    goal_summary: str
    organism: Optional[str] = None
    assay_type: Optional[str] = None
    tissue: Optional[str] = None
    condition: Optional[str] = None  # space, ground, HLU, etc.
    timepoint: Optional[str] = None
    datasets_required: List[str] = field(default_factory=list)
    checks: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    confidence: ConfidenceLevel = ConfidenceLevel.MEDIUM
    suggested_studies: List[str] = field(default_factory=list)
    is_valid: bool = True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "goal": self.goal,
            "goal_summary": self.goal_summary,
            "organism": self.organism,
            "assay_type": self.assay_type,
            "tissue": self.tissue,
            "condition": self.condition,
            "timepoint": self.timepoint,
            "datasets_required": self.datasets_required,
            "checks": self.checks,
            "warnings": self.warnings,
            "confidence": self.confidence.value,
            "suggested_studies": self.suggested_studies,
            "is_valid": self.is_valid,
        }


class ResearchPlanner:
    """
    Converts natural language research requests into structured plans.
    
    The planner uses:
    - Keyword matching for entity extraction
    - Project context for constraint validation
    - Controlled vocabularies for normalization
    """
    
    # Entity extraction patterns
    ORGANISM_PATTERNS = {
        r"\bmice?\b": "Mus musculus",
        r"\bmouse\b": "Mus musculus",
        r"\bmus musculus\b": "Mus musculus",
        r"\brat[s]?\b": "Rattus norvegicus",
        r"\brattus\b": "Rattus norvegicus",
        r"\bhuman[s]?\b": "Homo sapiens",
        r"\bhomo sapiens\b": "Homo sapiens",
        r"\brodent[s]?\b": "rodent",
    }
    
    ASSAY_PATTERNS = {
        r"\brna[-\s]?seq\b": "RNA-Seq",
        r"\btranscriptom": "RNA-Seq",
        r"\bgene expression\b": "RNA-Seq",
        r"\bmicroarray\b": "Microarray",
        r"\bproteom": "Proteomics",
        r"\bmass spec": "Mass Spectrometry",
        r"\bmetabolom": "Metabolomics",
        r"\bmethylat": "Methylation Profiling",
        r"\bchip[-\s]?seq\b": "ChIP-Seq",
        r"\batac[-\s]?seq\b": "ATAC-Seq",
        r"\bsingle[-\s]?cell\b": "scRNA-Seq",
        r"\bscrna": "scRNA-Seq",
    }
    
    CONDITION_PATTERNS = {
        r"\bspaceflight\b": "spaceflight",
        r"\bflight\b": "spaceflight",
        r"\bspace\b": "spaceflight",
        r"\biss\b": "spaceflight",
        r"\bmicrogravity\b": "spaceflight",
        r"\bground\s*control\b": "ground_control",
        r"\bground\b": "ground_control",
        r"\bhindlimb\s*unload": "HLU",
        r"\bhlu\b": "HLU",
        r"\birradiat": "irradiation",
        r"\bradiation\b": "irradiation",
    }
    
    TISSUE_KEYWORDS = {
        "brain": "Brain",
        "heart": "Heart",
        "liver": "Liver",
        "kidney": "Kidney",
        "muscle": "Skeletal Muscle",
        "soleus": "Soleus",
        "bone": "Bone",
        "spleen": "Spleen",
        "lung": "Lung",
        "eye": "Eye",
        "retina": "Retina",
        "blood": "Blood",
        "immune": None,  # Ambiguous - needs clarification
    }
    
    def __init__(self):
        """Initialize research planner."""
        self.context = get_project_context()
    
    def create_plan(self, request: str) -> ResearchPlan:
        """
        Create a research plan from a natural language request.
        
        Args:
            request: Natural language research request
            
        Returns:
            ResearchPlan with structured information
            
        Raises:
            RefinementNeeded: If the request is too vague
            ValidationError: If the request violates constraints
        """
        if not request or len(request.strip()) < 10:
            raise RefinementNeeded(
                "Request is too short to process",
                suggestions=["Please describe your research goal in more detail"],
                original_request=request,
            )
        
        request_lower = request.lower()
        
        # Extract entities
        organism = self._extract_organism(request_lower)
        assay_type = self._extract_assay_type(request_lower)
        condition = self._extract_condition(request_lower)
        tissue = self._extract_tissue(request_lower)
        
        # Build plan
        plan = ResearchPlan(
            goal=request,
            goal_summary=self._generate_summary(organism, assay_type, tissue, condition),
        )
        
        plan.organism = organism
        plan.assay_type = assay_type
        plan.condition = condition
        plan.tissue = tissue
        
        # Determine required datasets
        plan.datasets_required = self._determine_datasets(plan)
        
        # Add validation checks
        plan.checks = self._generate_checks(plan)
        
        # Validate against project constraints
        self._validate_plan(plan)
        
        # Suggest relevant studies
        plan.suggested_studies = self._suggest_studies(plan)
        
        # Determine confidence
        plan.confidence = self._assess_confidence(plan)
        
        return plan
    
    def _extract_organism(self, text: str) -> Optional[str]:
        """Extract organism from text."""
        for pattern, organism in self.ORGANISM_PATTERNS.items():
            if re.search(pattern, text, re.IGNORECASE):
                return organism
        return None
    
    def _extract_assay_type(self, text: str) -> Optional[str]:
        """Extract assay type from text."""
        for pattern, assay in self.ASSAY_PATTERNS.items():
            if re.search(pattern, text, re.IGNORECASE):
                return assay
        return None
    
    def _extract_condition(self, text: str) -> Optional[str]:
        """Extract experimental condition from text."""
        for pattern, condition in self.CONDITION_PATTERNS.items():
            if re.search(pattern, text, re.IGNORECASE):
                return condition
        return None
    
    def _extract_tissue(self, text: str) -> Optional[str]:
        """Extract tissue/organ from text."""
        for keyword, tissue in self.TISSUE_KEYWORDS.items():
            if keyword in text:
                return tissue
        return None
    
    def _generate_summary(
        self,
        organism: Optional[str],
        assay_type: Optional[str],
        tissue: Optional[str],
        condition: Optional[str],
    ) -> str:
        """Generate a concise summary of the research goal."""
        parts = []
        
        if assay_type:
            parts.append(f"Analyze {assay_type} data")
        else:
            parts.append("Analyze metadata")
        
        if tissue:
            parts.append(f"from {tissue}")
        
        if organism:
            parts.append(f"in {organism}")
        
        if condition:
            condition_desc = {
                "spaceflight": "spaceflight samples",
                "ground_control": "ground control samples",
                "HLU": "hindlimb unloading model",
                "irradiation": "irradiated samples",
            }
            parts.append(f"({condition_desc.get(condition, condition)})")
        
        return " ".join(parts) if parts else "Metadata enrichment analysis"
    
    def _determine_datasets(self, plan: ResearchPlan) -> List[str]:
        """Determine required datasets based on plan."""
        datasets = []
        
        if plan.assay_type:
            datasets.append(f"{plan.assay_type} assay data")
        else:
            datasets.append("gene expression assays")
        
        if plan.condition == "spaceflight":
            datasets.append("spaceflight samples")
            datasets.append("ground control samples (for comparison)")
        elif plan.condition == "HLU":
            datasets.append("hindlimb unloading samples")
            datasets.append("normally loaded control samples")
        elif plan.condition:
            datasets.append(f"{plan.condition} samples")
        
        if plan.organism:
            datasets.append(f"{plan.organism} metadata")
        
        if plan.tissue:
            datasets.append(f"{plan.tissue} tissue samples")
        
        return datasets
    
    def _generate_checks(self, plan: ResearchPlan) -> List[str]:
        """Generate validation checks for the plan."""
        checks = []
        
        # Organism check
        if plan.organism == "Homo sapiens":
            checks.append("⚠️ Human data requires additional ethics review")
            checks.append("Check for IRB approval and data access permissions")
        elif plan.organism:
            checks.append(f"Ensure species is {plan.organism}")
        
        # Data availability checks
        checks.append("Verify data license and access permissions")
        checks.append("Check for provenance tracking in source data")
        
        # Constraint reminders
        checks.append("No hallucination: only fill blanks with evidence")
        checks.append("Provenance required for all enriched values")
        
        return checks
    
    def _validate_plan(self, plan: ResearchPlan) -> None:
        """Validate plan against project constraints."""
        warnings = []
        
        # Check organism support
        if plan.organism == "Homo sapiens":
            warnings.append(
                "Human data has limited support - primarily rodent-focused pipeline"
            )
            plan.is_valid = True  # Warning only, not blocking
        
        # Check if plan is too vague
        if not plan.organism and not plan.assay_type and not plan.condition:
            raise RefinementNeeded(
                "Request is too vague to create a specific plan",
                suggestions=[
                    "Specify the organism (e.g., mice, rats)",
                    "Specify the assay type (e.g., RNA-Seq, proteomics)",
                    "Specify the experimental condition (e.g., spaceflight, ground control)",
                ],
                original_request=plan.goal,
            )
        
        # Check tissue ambiguity
        if plan.tissue is None and "immune" in plan.goal.lower():
            warnings.append(
                "Immune tissue not specified - could be blood, spleen, or lymph nodes"
            )
        
        plan.warnings = warnings
    
    def _suggest_studies(self, plan: ResearchPlan) -> List[str]:
        """Suggest relevant OSDR studies based on plan."""
        suggestions = []
        key_studies = self.context.get("key_studies", [])
        
        # Map conditions to known studies
        condition_studies = {
            "spaceflight": ["OSD-102", "OSD-546"],
            "HLU": ["OSD-202", "OSD-242", "OSD-661"],
            "irradiation": ["OSD-202", "OSD-242"],
        }
        
        if plan.condition and plan.condition in condition_studies:
            suggestions.extend(condition_studies[plan.condition])
        
        # If no specific matches, suggest key testing studies
        if not suggestions:
            suggestions = key_studies[:3] if key_studies else ["OSD-202", "OSD-242"]
        
        return list(set(suggestions))
    
    def _assess_confidence(self, plan: ResearchPlan) -> ConfidenceLevel:
        """Assess confidence in the plan based on specificity."""
        score = 0
        
        if plan.organism:
            score += 1
        if plan.assay_type:
            score += 1
        if plan.condition:
            score += 1
        if plan.tissue:
            score += 1
        
        if score >= 3:
            return ConfidenceLevel.HIGH
        elif score >= 2:
            return ConfidenceLevel.MEDIUM
        else:
            return ConfidenceLevel.LOW
    
    def normalize_request(self, request: str) -> Dict[str, Any]:
        """
        Normalize a request into structured components.
        
        This is a simpler version of create_plan that just extracts
        entities without full validation.
        
        Args:
            request: Natural language request
            
        Returns:
            Dictionary with extracted entities
        """
        request_lower = request.lower()
        
        return {
            "original": request,
            "organism": self._extract_organism(request_lower),
            "assay_type": self._extract_assay_type(request_lower),
            "condition": self._extract_condition(request_lower),
            "tissue": self._extract_tissue(request_lower),
        }


def create_research_plan(request: str) -> ResearchPlan:
    """
    Convenience function to create a research plan.
    
    Args:
        request: Natural language research request
        
    Returns:
        ResearchPlan with structured information
    """
    planner = ResearchPlanner()
    return planner.create_plan(request)


def normalize_request(request: str) -> Dict[str, Any]:
    """
    Convenience function to normalize a request.
    
    Args:
        request: Natural language request
        
    Returns:
        Dictionary with extracted entities
    """
    planner = ResearchPlanner()
    return planner.normalize_request(request)

