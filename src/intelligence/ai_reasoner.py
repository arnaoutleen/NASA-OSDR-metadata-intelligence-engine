"""
NASA OSDR Metadata Intelligence Engine - AI Reasoner

This module provides an interface for AI/LLM-based metadata suggestions.
It is designed to integrate with models like BioLinkBERT, PubMedBERT,
or other biomedical language models.

CRITICAL DESIGN PRINCIPLES:
1. AI components ONLY produce SUGGESTIONS - never auto-fill metadata
2. All suggestions include confidence scores and explanations
3. Human review is REQUIRED before any AI suggestion is accepted
4. Low confidence suggestions are flagged prominently
5. The system must be functional WITHOUT any AI components

This module provides the interface and data structures.
Actual model integration is left as a future enhancement.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from enum import Enum

from src.core.constants import ConfidenceLevel


class SuggestionStatus(str, Enum):
    """Status of an AI suggestion."""
    PENDING = "pending"       # Awaiting human review
    ACCEPTED = "accepted"     # Accepted by human reviewer
    REJECTED = "rejected"     # Rejected by human reviewer
    MODIFIED = "modified"     # Accepted with modifications


@dataclass
class AISuggestion:
    """
    A single AI-generated suggestion for a metadata field.
    
    All suggestions require human review before use.
    """
    field_name: str
    suggested_value: str
    confidence: float  # 0.0 to 1.0
    explanation: str
    evidence: List[str] = field(default_factory=list)
    model_name: str = "unknown"
    status: SuggestionStatus = SuggestionStatus.PENDING
    reviewer_notes: Optional[str] = None
    
    @property
    def confidence_level(self) -> ConfidenceLevel:
        """Convert numeric confidence to ConfidenceLevel."""
        if self.confidence >= 0.9:
            return ConfidenceLevel.HIGH
        elif self.confidence >= 0.7:
            return ConfidenceLevel.MEDIUM
        elif self.confidence >= 0.5:
            return ConfidenceLevel.LOW
        else:
            return ConfidenceLevel.SUGGESTION
    
    @property
    def requires_careful_review(self) -> bool:
        """Check if suggestion requires careful review."""
        return self.confidence < 0.7
    
    def accept(self, reviewer_notes: Optional[str] = None) -> None:
        """Accept the suggestion."""
        self.status = SuggestionStatus.ACCEPTED
        self.reviewer_notes = reviewer_notes
    
    def reject(self, reason: str) -> None:
        """Reject the suggestion."""
        self.status = SuggestionStatus.REJECTED
        self.reviewer_notes = reason
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "field_name": self.field_name,
            "suggested_value": self.suggested_value,
            "confidence": self.confidence,
            "confidence_level": self.confidence_level.value,
            "explanation": self.explanation,
            "evidence": self.evidence,
            "model_name": self.model_name,
            "status": self.status.value,
            "reviewer_notes": self.reviewer_notes,
            "requires_careful_review": self.requires_careful_review,
        }


@dataclass
class SampleSuggestions:
    """Collection of AI suggestions for a sample."""
    sample_id: str
    osd_id: str
    suggestions: List[AISuggestion] = field(default_factory=list)
    
    @property
    def pending_count(self) -> int:
        """Number of pending suggestions."""
        return sum(1 for s in self.suggestions if s.status == SuggestionStatus.PENDING)
    
    @property
    def low_confidence_count(self) -> int:
        """Number of low confidence suggestions."""
        return sum(1 for s in self.suggestions if s.requires_careful_review)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "sample_id": self.sample_id,
            "osd_id": self.osd_id,
            "suggestion_count": len(self.suggestions),
            "pending_count": self.pending_count,
            "low_confidence_count": self.low_confidence_count,
            "suggestions": [s.to_dict() for s in self.suggestions],
        }


class BaseAIReasoner(ABC):
    """
    Abstract base class for AI reasoners.
    
    This defines the interface that any AI/LLM integration must implement.
    Concrete implementations can use different models (BioLinkBERT, GPT, etc.)
    """
    
    @abstractmethod
    def suggest_field_value(
        self,
        field_name: str,
        sample_data: Dict[str, Any],
        study_metadata: Dict[str, Any],
        context: Optional[str] = None,
    ) -> Optional[AISuggestion]:
        """
        Generate a suggestion for a specific field.
        
        Args:
            field_name: The field to suggest a value for
            sample_data: Current sample metadata
            study_metadata: Study-level metadata for context
            context: Optional additional context
            
        Returns:
            AISuggestion if the model can make a suggestion, None otherwise
        """
        pass
    
    @abstractmethod
    def batch_suggest(
        self,
        samples: List[Dict[str, Any]],
        study_metadata: Dict[str, Any],
        fields: List[str],
    ) -> Dict[str, SampleSuggestions]:
        """
        Generate suggestions for multiple samples and fields.
        
        Args:
            samples: List of sample metadata dicts
            study_metadata: Study-level metadata
            fields: List of fields to suggest values for
            
        Returns:
            Dict mapping sample_id -> SampleSuggestions
        """
        pass
    
    @property
    @abstractmethod
    def model_name(self) -> str:
        """Name of the underlying model."""
        pass
    
    @property
    @abstractmethod
    def is_available(self) -> bool:
        """Check if the model is available for use."""
        pass


class PlaceholderReasoner(BaseAIReasoner):
    """
    Placeholder AI reasoner that returns no suggestions.
    
    This is used when no actual AI model is configured.
    The pipeline should function correctly without AI components.
    """
    
    def suggest_field_value(
        self,
        field_name: str,
        sample_data: Dict[str, Any],
        study_metadata: Dict[str, Any],
        context: Optional[str] = None,
    ) -> Optional[AISuggestion]:
        """Returns None - no AI suggestions available."""
        return None
    
    def batch_suggest(
        self,
        samples: List[Dict[str, Any]],
        study_metadata: Dict[str, Any],
        fields: List[str],
    ) -> Dict[str, SampleSuggestions]:
        """Returns empty suggestions for all samples."""
        results = {}
        for sample in samples:
            sample_id = sample.get("id", sample.get("sample_id", "unknown"))
            osd_id = sample.get("osd_id", study_metadata.get("accession", ""))
            results[sample_id] = SampleSuggestions(
                sample_id=sample_id,
                osd_id=osd_id,
            )
        return results
    
    @property
    def model_name(self) -> str:
        """Returns placeholder name."""
        return "placeholder_no_ai"
    
    @property
    def is_available(self) -> bool:
        """Always returns False."""
        return False


# Global reasoner instance (defaults to placeholder)
_active_reasoner: Optional[BaseAIReasoner] = None


def get_reasoner() -> BaseAIReasoner:
    """Get the active AI reasoner instance."""
    global _active_reasoner
    if _active_reasoner is None:
        _active_reasoner = PlaceholderReasoner()
    return _active_reasoner


def set_reasoner(reasoner: BaseAIReasoner) -> None:
    """Set the active AI reasoner instance."""
    global _active_reasoner
    _active_reasoner = reasoner


def is_ai_available() -> bool:
    """Check if AI reasoning is available."""
    return get_reasoner().is_available

