"""
NASA OSDR Metadata Intelligence Engine - Feedback Loop

This module evaluates research plans and user inputs against project constraints,
generating clarification questions when information is ambiguous or risky.

The feedback loop:
1. Evaluates plans against PROJECT_CONTEXT constraints
2. Identifies missing or ambiguous information
3. Generates targeted clarification questions
4. Validates user responses

Usage:
    from src.intelligence.feedback_loop import FeedbackLoop
    
    feedback = FeedbackLoop()
    questions = feedback.evaluate_plan(plan)
    if questions:
        print("Please clarify:", questions)
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from enum import Enum

from src.intelligence.context_loader import get_project_context
from src.intelligence.research_planner import ResearchPlan
from src.intelligence.exceptions import ClarificationRequired


class QuestionPriority(str, Enum):
    """Priority level for clarification questions."""
    CRITICAL = "critical"  # Must be answered before proceeding
    HIGH = "high"  # Strongly recommended
    MEDIUM = "medium"  # Helpful but optional
    LOW = "low"  # Nice to have


@dataclass
class ClarificationQuestion:
    """
    A question to ask the user for clarification.
    
    Contains the question text, priority, and optional choices.
    """
    question: str
    field: str
    priority: QuestionPriority
    options: List[str] = field(default_factory=list)
    context: str = ""
    default: Optional[str] = None
    
    def format(self) -> str:
        """Format question for display."""
        text = self.question
        if self.options:
            text += f" Options: {', '.join(self.options)}"
        if self.default:
            text += f" (default: {self.default})"
        return text
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "question": self.question,
            "field": self.field,
            "priority": self.priority.value,
            "options": self.options,
            "context": self.context,
            "default": self.default,
        }


@dataclass
class FeedbackResult:
    """
    Result of evaluating a plan or input.
    
    Contains any questions that need to be asked and validation issues.
    """
    needs_clarification: bool
    questions: List[ClarificationQuestion] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    blocking_issues: List[str] = field(default_factory=list)
    
    @property
    def can_proceed(self) -> bool:
        """Check if execution can proceed (no blocking issues)."""
        return len(self.blocking_issues) == 0
    
    @property
    def critical_questions(self) -> List[ClarificationQuestion]:
        """Get only critical questions."""
        return [q for q in self.questions if q.priority == QuestionPriority.CRITICAL]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "needs_clarification": self.needs_clarification,
            "questions": [q.to_dict() for q in self.questions],
            "warnings": self.warnings,
            "blocking_issues": self.blocking_issues,
            "can_proceed": self.can_proceed,
        }


class FeedbackLoop:
    """
    Evaluates plans and inputs, generating clarification questions.
    
    Uses project context to ensure all constraints are met and
    ambiguities are resolved before pipeline execution.
    """
    
    # Known tissue options for ambiguous terms
    TISSUE_DISAMBIGUATION = {
        "immune": ["Blood", "Spleen", "Lymph Node", "Thymus", "Bone Marrow"],
        "muscle": ["Skeletal Muscle", "Soleus", "Gastrocnemius", "Heart"],
        "nervous": ["Brain", "Spinal Cord", "Peripheral Nerve"],
    }
    
    def __init__(self):
        """Initialize feedback loop."""
        self.context = get_project_context()
    
    def evaluate_plan(self, plan: ResearchPlan) -> FeedbackResult:
        """
        Evaluate a research plan and generate clarification questions.
        
        Args:
            plan: The research plan to evaluate
            
        Returns:
            FeedbackResult with questions and issues
        """
        result = FeedbackResult(needs_clarification=False)
        
        # Check organism
        self._check_organism(plan, result)
        
        # Check assay type
        self._check_assay_type(plan, result)
        
        # Check tissue/organ
        self._check_tissue(plan, result)
        
        # Check experimental condition
        self._check_condition(plan, result)
        
        # Check project constraints
        self._check_constraints(plan, result)
        
        # Set needs_clarification flag
        result.needs_clarification = len(result.questions) > 0
        
        return result
    
    def _check_organism(self, plan: ResearchPlan, result: FeedbackResult) -> None:
        """Check organism specification."""
        if not plan.organism:
            result.questions.append(ClarificationQuestion(
                question="What organism is this study focused on?",
                field="organism",
                priority=QuestionPriority.HIGH,
                options=["Mus musculus (mouse)", "Rattus norvegicus (rat)", "Other"],
                context="The pipeline is optimized for rodent studies.",
            ))
        elif plan.organism == "rodent":
            result.questions.append(ClarificationQuestion(
                question="Please specify the rodent species.",
                field="organism",
                priority=QuestionPriority.MEDIUM,
                options=["Mus musculus (mouse)", "Rattus norvegicus (rat)"],
            ))
        elif plan.organism == "Homo sapiens":
            result.warnings.append(
                "Human data support is limited. The pipeline is primarily "
                "designed for rodent studies. Proceed with caution."
            )
    
    def _check_assay_type(self, plan: ResearchPlan, result: FeedbackResult) -> None:
        """Check assay type specification."""
        if not plan.assay_type:
            result.questions.append(ClarificationQuestion(
                question="What type of assay or data are you interested in?",
                field="assay_type",
                priority=QuestionPriority.MEDIUM,
                options=[
                    "RNA-Seq",
                    "Microarray",
                    "Proteomics",
                    "Metabolomics",
                    "All available",
                ],
                default="All available",
            ))
    
    def _check_tissue(self, plan: ResearchPlan, result: FeedbackResult) -> None:
        """Check tissue/organ specification."""
        if plan.tissue is None:
            # Check if the goal mentions an ambiguous tissue term
            goal_lower = plan.goal.lower()
            
            for term, options in self.TISSUE_DISAMBIGUATION.items():
                if term in goal_lower:
                    result.questions.append(ClarificationQuestion(
                        question=f"You mentioned '{term}'. Which specific tissue/organ?",
                        field="tissue",
                        priority=QuestionPriority.HIGH,
                        options=options,
                        context="Specific tissue selection improves data filtering.",
                    ))
                    return
            
            # No specific tissue mentioned - ask if it matters
            result.questions.append(ClarificationQuestion(
                question="Do you need to filter by a specific tissue/organ?",
                field="tissue",
                priority=QuestionPriority.LOW,
                options=["Any tissue", "Brain", "Heart", "Liver", "Muscle", "Other"],
                default="Any tissue",
            ))
    
    def _check_condition(self, plan: ResearchPlan, result: FeedbackResult) -> None:
        """Check experimental condition specification."""
        if not plan.condition:
            # Check if spaceflight comparison is implied
            goal_lower = plan.goal.lower()
            
            if "comparison" in goal_lower or "compare" in goal_lower:
                result.questions.append(ClarificationQuestion(
                    question="What experimental groups do you want to compare?",
                    field="condition",
                    priority=QuestionPriority.HIGH,
                    options=[
                        "Spaceflight vs Ground Control",
                        "Hindlimb Unloading vs Control",
                        "Irradiated vs Control",
                        "Multiple conditions",
                    ],
                ))
    
    def _check_constraints(self, plan: ResearchPlan, result: FeedbackResult) -> None:
        """Check project constraints."""
        trust_policy = self.context.get("trust_policy", {})
        
        # Remind about no-hallucination policy
        if plan.confidence.value == "low":
            result.warnings.append(
                "Low confidence plan - ensure all enriched values have provenance."
            )
        
        # Check for data that might not be available
        if plan.organism == "Homo sapiens":
            result.warnings.append(
                "Human data may have restricted access. Verify permissions."
            )
    
    def generate_questions_from_request(
        self,
        request: str,
    ) -> List[ClarificationQuestion]:
        """
        Generate clarification questions from a raw request.
        
        This is used when the request is too vague to even create a plan.
        
        Args:
            request: The original user request
            
        Returns:
            List of clarification questions
        """
        questions = []
        request_lower = request.lower()
        
        # Check for missing key elements
        has_organism = any(
            term in request_lower
            for term in ["mouse", "mice", "rat", "rodent", "human"]
        )
        has_assay = any(
            term in request_lower
            for term in ["rna", "seq", "expression", "proteom", "metabol"]
        )
        has_condition = any(
            term in request_lower
            for term in ["space", "flight", "ground", "hlu", "radiation"]
        )
        
        if not has_organism:
            questions.append(ClarificationQuestion(
                question="What organism/species is your study focused on?",
                field="organism",
                priority=QuestionPriority.CRITICAL,
                options=["Mouse", "Rat", "Human", "Other"],
            ))
        
        if not has_assay:
            questions.append(ClarificationQuestion(
                question="What type of data are you looking for?",
                field="assay_type",
                priority=QuestionPriority.HIGH,
                options=["Gene expression (RNA-Seq)", "Proteomics", "Metabolomics", "Any"],
            ))
        
        if not has_condition:
            questions.append(ClarificationQuestion(
                question="Are you interested in a specific experimental condition?",
                field="condition",
                priority=QuestionPriority.MEDIUM,
                options=["Spaceflight", "Ground analog (HLU)", "Radiation", "All conditions"],
            ))
        
        return questions
    
    def validate_response(
        self,
        question: ClarificationQuestion,
        response: str,
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate a user's response to a clarification question.
        
        Args:
            question: The original question
            response: The user's response
            
        Returns:
            Tuple of (is_valid, normalized_value)
        """
        if not response or not response.strip():
            return False, None
        
        response = response.strip()
        
        # If options were provided, check against them
        if question.options:
            # Check for exact or partial match
            for option in question.options:
                if response.lower() in option.lower() or option.lower() in response.lower():
                    return True, option
            
            # No match found
            return False, None
        
        # Free-form response - accept as-is
        return True, response
    
    def should_ask_question(
        self,
        question: ClarificationQuestion,
        existing_info: Dict[str, Any],
    ) -> bool:
        """
        Check if a question should be asked given existing information.
        
        Args:
            question: The question to check
            existing_info: Dictionary of already-known information
            
        Returns:
            True if the question should be asked
        """
        # If we already have this field, don't ask
        if question.field in existing_info and existing_info[question.field]:
            return False
        
        return True


def evaluate_plan(plan: ResearchPlan) -> FeedbackResult:
    """
    Convenience function to evaluate a research plan.
    
    Args:
        plan: The research plan to evaluate
        
    Returns:
        FeedbackResult with questions and issues
    """
    feedback = FeedbackLoop()
    return feedback.evaluate_plan(plan)


def generate_questions(request: str) -> List[ClarificationQuestion]:
    """
    Convenience function to generate questions from a request.
    
    Args:
        request: The user request
        
    Returns:
        List of clarification questions
    """
    feedback = FeedbackLoop()
    return feedback.generate_questions_from_request(request)

