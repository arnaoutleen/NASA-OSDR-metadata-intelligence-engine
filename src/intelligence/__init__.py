"""
NASA OSDR Metadata Intelligence Engine - Intelligence Module

This module provides AI-assisted and rule-based inference capabilities
for metadata enrichment. All components produce SUGGESTIONS only -
they never directly modify metadata without human review.

Key components:
- pattern_extractor: Extract structured information from sample names
- unit_inference: Infer units for numeric values (age, time)
- biological_rules: Apply biological consistency rules
- ai_reasoner: Interface for BioLLM suggestions (future)
- context_loader: Load and parse project context from CURSOR_CONTEXT.md
- research_planner: Convert natural language to structured research plans
- pipeline_connector: Bridge planner to enrichment pipeline
- feedback_loop: Generate clarification questions

CRITICAL: All AI/inference components must:
1. Never directly modify CSV data
2. Return structured suggestions with confidence scores
3. Require explicit acceptance before any value is used
"""

# Pattern extraction
from src.intelligence.pattern_extractor import (
    PatternExtractor,
    SampleNamePattern,
    extract_patterns,
)

# Unit inference
from src.intelligence.unit_inference import (
    UnitInferrer,
    UnitSuggestion,
    infer_time_units,
    infer_age_units,
)

# Biological rules
from src.intelligence.biological_rules import (
    BiologicalRuleEngine,
    apply_biological_rules,
    check_biological_consistency,
)

# Exceptions for agent signaling
from src.intelligence.exceptions import (
    ContextLoadError,
    RefinementNeeded,
    EnrichmentError,
    ClarificationRequired,
    ValidationError,
)

# Context loading
from src.intelligence.context_loader import (
    ProjectContext,
    load_project_context,
    get_project_context,
    get_context_object,
    PROJECT_CONTEXT,
)

# Research planning
from src.intelligence.research_planner import (
    ResearchPlan,
    ResearchPlanner,
    create_research_plan,
    normalize_request,
)

# Pipeline connection
from src.intelligence.pipeline_connector import (
    PipelineConnector,
    EnrichmentExecutionResult,
    execute_enrichment,
)

# Feedback loop
from src.intelligence.feedback_loop import (
    FeedbackLoop,
    FeedbackResult,
    ClarificationQuestion,
    QuestionPriority,
    evaluate_plan,
    generate_questions,
)

__all__ = [
    # Pattern extraction
    "PatternExtractor",
    "SampleNamePattern",
    "extract_patterns",
    # Unit inference
    "UnitInferrer",
    "UnitSuggestion",
    "infer_time_units",
    "infer_age_units",
    # Biological rules
    "BiologicalRuleEngine",
    "apply_biological_rules",
    "check_biological_consistency",
    # Exceptions
    "ContextLoadError",
    "RefinementNeeded",
    "EnrichmentError",
    "ClarificationRequired",
    "ValidationError",
    # Context loading
    "ProjectContext",
    "load_project_context",
    "get_project_context",
    "get_context_object",
    "PROJECT_CONTEXT",
    # Research planning
    "ResearchPlan",
    "ResearchPlanner",
    "create_research_plan",
    "normalize_request",
    # Pipeline connection
    "PipelineConnector",
    "EnrichmentExecutionResult",
    "execute_enrichment",
    # Feedback loop
    "FeedbackLoop",
    "FeedbackResult",
    "ClarificationQuestion",
    "QuestionPriority",
    "evaluate_plan",
    "generate_questions",
]
