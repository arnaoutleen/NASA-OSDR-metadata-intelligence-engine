"""
NASA OSDR Metadata Intelligence Engine - Custom Exceptions

This module provides custom exceptions for agent-to-agent signaling
in the AI assistant layer. These exceptions help coordinate between
different components and provide clear error messages.

Exception Types:
- ContextLoadError: Problems loading or parsing CURSOR_CONTEXT.md
- RefinementNeeded: User request is too vague and needs clarification
- EnrichmentError: Pipeline execution failed
- ClarificationRequired: User input needs disambiguation
"""

from typing import List, Optional, Dict, Any


class ContextLoadError(Exception):
    """
    Raised when the context loader cannot properly parse CURSOR_CONTEXT.md.
    
    This signals to the agent that the project context is incomplete
    or malformed and cannot be used for grounded reasoning.
    
    Attributes:
        message: Description of what went wrong
        missing_sections: List of expected sections that were not found
    """
    
    def __init__(
        self,
        message: str,
        missing_sections: Optional[List[str]] = None,
    ):
        self.message = message
        self.missing_sections = missing_sections or []
        super().__init__(self.message)
    
    def __str__(self) -> str:
        base = f"ContextLoadError: {self.message}"
        if self.missing_sections:
            base += f"\nMissing sections: {', '.join(self.missing_sections)}"
        return base


class RefinementNeeded(Exception):
    """
    Raised when a user request is too vague to process.
    
    This signals that the agent should ask clarifying questions
    before attempting to generate a research plan.
    
    Attributes:
        message: Description of why refinement is needed
        suggestions: List of suggested clarifications to ask
        original_request: The original user request that was too vague
    """
    
    def __init__(
        self,
        message: str,
        suggestions: Optional[List[str]] = None,
        original_request: Optional[str] = None,
    ):
        self.message = message
        self.suggestions = suggestions or []
        self.original_request = original_request
        super().__init__(self.message)
    
    def __str__(self) -> str:
        base = f"RefinementNeeded: {self.message}"
        if self.suggestions:
            base += "\nSuggested clarifications:"
            for s in self.suggestions:
                base += f"\n  - {s}"
        return base
    
    def get_questions(self) -> List[str]:
        """Get the list of clarifying questions to ask."""
        return self.suggestions


class EnrichmentError(Exception):
    """
    Raised when the enrichment pipeline fails.
    
    This signals that the CLI or pipeline execution encountered
    an error that prevented successful enrichment.
    
    Attributes:
        message: Description of the error
        osd_id: The OSD study ID that failed (if applicable)
        stage: The pipeline stage that failed
        original_error: The underlying exception if any
    """
    
    def __init__(
        self,
        message: str,
        osd_id: Optional[str] = None,
        stage: Optional[str] = None,
        original_error: Optional[Exception] = None,
    ):
        self.message = message
        self.osd_id = osd_id
        self.stage = stage
        self.original_error = original_error
        super().__init__(self.message)
    
    def __str__(self) -> str:
        base = f"EnrichmentError: {self.message}"
        if self.osd_id:
            base += f" (study: {self.osd_id})"
        if self.stage:
            base += f" (stage: {self.stage})"
        if self.original_error:
            base += f"\nCaused by: {type(self.original_error).__name__}: {self.original_error}"
        return base


class ClarificationRequired(Exception):
    """
    Raised when user input is ambiguous and needs clarification.
    
    This is used by the feedback loop to signal that the assistant
    should ask the user for more specific information before proceeding.
    
    Attributes:
        message: Description of what needs clarification
        field: The specific field that is ambiguous
        options: List of possible options to present to user
        context: Additional context about why clarification is needed
    """
    
    def __init__(
        self,
        message: str,
        field: Optional[str] = None,
        options: Optional[List[str]] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        self.message = message
        self.field = field
        self.options = options or []
        self.context = context or {}
        super().__init__(self.message)
    
    def __str__(self) -> str:
        base = f"ClarificationRequired: {self.message}"
        if self.field:
            base += f" (field: {self.field})"
        if self.options:
            base += f"\nOptions: {', '.join(self.options)}"
        return base
    
    def format_question(self) -> str:
        """Format as a user-friendly question."""
        question = self.message
        if self.options:
            question += " Options: " + ", ".join(self.options)
        return question


class ValidationError(Exception):
    """
    Raised when a research plan fails validation.
    
    This signals that the plan violates project constraints
    (e.g., requesting human data when only rodent is supported).
    
    Attributes:
        message: Description of the validation failure
        constraint: The constraint that was violated
        severity: 'error' (blocking) or 'warning' (proceed with caution)
    """
    
    def __init__(
        self,
        message: str,
        constraint: Optional[str] = None,
        severity: str = "error",
    ):
        self.message = message
        self.constraint = constraint
        self.severity = severity
        super().__init__(self.message)
    
    def __str__(self) -> str:
        base = f"ValidationError [{self.severity}]: {self.message}"
        if self.constraint:
            base += f"\nViolated constraint: {self.constraint}"
        return base
    
    @property
    def is_blocking(self) -> bool:
        """Check if this error should block execution."""
        return self.severity == "error"

