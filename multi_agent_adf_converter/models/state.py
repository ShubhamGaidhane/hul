"""Pydantic state models for the multi-agent ADF to Lakeflow converter.

These models define the typed data structures that flow between agents
in the LangGraph orchestration. Every agent reads from and writes to
a shared OrchestrationState object.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class ActivityType(str, Enum):
    """All supported ADF activity types."""

    COPY = "Copy"
    EXECUTE_PIPELINE = "ExecutePipeline"
    DATABRICKS_NOTEBOOK = "DatabricksNotebook"
    FOR_EACH = "ForEach"
    IF_CONDITION = "IfCondition"
    UNTIL = "Until"
    SET_VARIABLE = "SetVariable"
    APPEND_VARIABLE = "AppendVariable"
    WEB = "Web"
    WEBHOOK = "Webhook"
    STORED_PROCEDURE = "SqlServerStoredProcedure"
    LOOKUP = "Lookup"
    GET_METADATA = "GetMetadata"
    DELETE = "Delete"
    EXPRESSION = "Expression"
    SWITCH = "Switch"
    WAIT = "Wait"
    FAIL = "Fail"
    FILTER = "Filter"
    FLATTEN = "Flatten"
    VALIDATION = "Validation"
    DATA_FLOW = "ExecuteDataFlow"
    CUSTOM = "Custom"
    UNSUPPORTED = "Unsupported"


class TriggerType(str, Enum):
    SCHEDULE = "ScheduleTrigger"
    EVENT = "BlobEventsTrigger"
    CUSTOM_EVENT = "CustomEventsTrigger"
    TUMBLING_WINDOW = "TumblingWindowTrigger"
    UNKNOWN = "Unknown"


class ConversionStatus(str, Enum):
    PENDING = "pending"
    CONVERTING = "converting"
    SUCCESS = "success"
    FAILED = "failed"
    NEEDS_REVIEW = "needs_review"
    VALIDATED = "validated"


class ValidationSeverity(str, Enum):
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


# ---------------------------------------------------------------------------
# Core data nodes
# ---------------------------------------------------------------------------

class ActivityNode(BaseModel):
    """Represents a single ADF activity with its resolved context."""

    name: str = Field(..., description="Activity name from ADF")
    activity_type: ActivityType = Field(..., description="Type of the ADF activity")
    raw_json: Dict[str, Any] = Field(default_factory=dict, description="Raw JSON of this activity from ADF")
    depends_on: List[str] = Field(default_factory=list, description="List of activity names this depends on")
    inputs: List[str] = Field(default_factory=list, description="Input dataset names")
    outputs: List[str] = Field(default_factory=list, description="Output dataset names")
    linked_service: Optional[str] = Field(None, description="Linked service reference name")
    resolved_params: Dict[str, Any] = Field(default_factory=dict, description="Resolved parameters for this activity")
    nested_activities: List[ActivityNode] = Field(default_factory=list, description="Nested activities (ForEach, IfCondition, etc.)")
    note: Optional[str] = Field(None, description="Any notes or warnings")


class PipelineNode(BaseModel):
    """Represents a pipeline in the ADF catalog with all its resolved context."""

    name: str = Field(..., description="Pipeline name")
    factory: Optional[str] = Field(None, description="ADF factory name")
    activities: List[ActivityNode] = Field(default_factory=list, description="All activities (flattened including nested)")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Pipeline-level parameters with defaults")
    variables: List[str] = Field(default_factory=list, description="Pipeline variable names")
    child_pipelines: List[str] = Field(default_factory=list, description="Names of child pipelines called via ExecutePipeline")
    parent_pipeline: Optional[str] = Field(None, description="Name of parent pipeline if this is a child")
    raw_json: Dict[str, Any] = Field(default_factory=dict, description="Raw JSON for reference")


class ParameterNode(BaseModel):
    """Resolved parameter from any source (pipeline, dataset, linked service, global)."""

    name: str = Field(..., description="Parameter name")
    value: Any = Field(None, description="Resolved value (or expression)")
    source: str = Field(..., description="Source: pipeline/dataset/linked_service/global")
    source_name: Optional[str] = Field(None, description="Name of the source object")
    data_type: Optional[str] = Field(None, description="ADF data type (String, Int, etc.)")


class ConnectionNode(BaseModel):
    """Represents a linked service connection with all resolved details."""

    name: str = Field(..., description="Linked service name")
    service_kind: str = Field(..., description="Type: AzureBlobStorage, AzureSqlDatabase, etc.")
    connection_string: Optional[str] = Field(None, description="Resolved connection string")
    properties: Dict[str, Any] = Field(default_factory=dict, description="All typeProperties")
    uses_key_vault: bool = Field(False, description="Whether this LS uses AKV for secrets")
    key_vault_secrets: List[str] = Field(default_factory=list, description="Secret names if using AKV")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Linked service parameters")


class TriggerNode(BaseModel):
    """Represents a trigger with its resolved definition."""

    name: str = Field(..., description="Trigger name")
    trigger_type: TriggerType = Field(..., description="Type of trigger")
    linked_pipelines: List[str] = Field(default_factory=list, description="Pipelines this trigger starts")
    status: Optional[str] = Field(None, description="Runtime state: Started/Stopped")
    definition: Dict[str, Any] = Field(default_factory=dict, description="Full trigger definition")
    schedule: Optional[Dict[str, Any]] = Field(None, description="Schedule recurrence for schedule triggers")
    event_info: Optional[Dict[str, Any]] = Field(None, description="Event details for event triggers")


class DatasetNode(BaseModel):
    """Represents a dataset with resolved linked service reference."""

    name: str = Field(..., description="Dataset name")
    dataset_type: str = Field("", description="Type: AzureBlob, DelimitedText, etc.")
    linked_service: Optional[str] = Field(None, description="Linked service name")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Dataset parameters")
    schema_def: Optional[Dict[str, Any]] = Field(None, description="Schema if available")
    raw_json: Dict[str, Any] = Field(default_factory=dict, description="Raw JSON for reference")


# ---------------------------------------------------------------------------
# Conversion results
# ---------------------------------------------------------------------------

class ConversionResult(BaseModel):
    """Result of converting a single pipeline."""

    pipeline_name: str = Field(..., description="Name of the pipeline")
    status: ConversionStatus = Field(ConversionStatus.PENDING)
    lakeflow_code: Optional[str] = Field(None, description="Generated Lakeflow Python code")
    error_detail: Optional[str] = Field(None, description="Error message if failed")
    validation_errors: List[str] = Field(default_factory=list, description="Validation error messages")
    retry_count: int = Field(0, description="Number of retries attempted")
    warnings: List[str] = Field(default_factory=list, description="Non-fatal warnings")


class ValidationFinding(BaseModel):
    """A single validation finding."""

    severity: ValidationSeverity = Field(...)
    message: str = Field(..., description="Description of the finding")
    location: Optional[str] = Field(None, description="Where in the code this applies")


class ValidationResult(BaseModel):
    """Complete validation result for a conversion."""

    pipeline_name: str = Field(...)
    passed: bool = Field(False)
    findings: List[ValidationFinding] = Field(default_factory=list)
    syntax_valid: bool = Field(True)
    connections_resolved: bool = Field(True)
    dependencies_valid: bool = Field(True)
    parameters_resolved: bool = Field(True)


class ReviewerOutput(BaseModel):
    """Reviewer agent's final assessment."""

    pipeline_name: str = Field(...)
    approved: bool = Field(False)
    completeness_score: float = Field(0.0, ge=0.0, le=1.0)
    missing_items: List[str] = Field(default_factory=list)
    suggestions: List[str] = Field(default_factory=list)
    final_message: Optional[str] = Field(None)


# ---------------------------------------------------------------------------
# Analyzer / Context Builder outputs
# ---------------------------------------------------------------------------

class AnalyzerOutput(BaseModel):
    """Structured output from the Analyzer Agent."""

    pipeline_tree: Dict[str, PipelineNode] = Field(default_factory=dict)
    activity_types_found: List[str] = Field(default_factory=list)
    parameter_count: int = Field(0)
    has_child_pipelines: bool = Field(False)
    raw_pipeline_count: int = Field(0)
    summary: Optional[str] = Field(None)


class ContextOutput(BaseModel):
    """Structured output from the Context Builder Agent."""

    parameter_map: Dict[str, List[ParameterNode]] = Field(default_factory=dict, description="param_name -> list of resolved parameters")
    connection_map: Dict[str, ConnectionNode] = Field(default_factory=dict, description="ls_name -> ConnectionNode")
    dataset_map: Dict[str, DatasetNode] = Field(default_factory=dict, description="dataset_name -> DatasetNode")
    global_parameters: Dict[str, Any] = Field(default_factory=dict, description="Global parameters from ADF")
    trigger_map: Dict[str, TriggerNode] = Field(default_factory=dict, description="trigger_name -> TriggerNode")


# ---------------------------------------------------------------------------
# Main Orchestration State
# ---------------------------------------------------------------------------

class OrchestrationState(BaseModel):
    """The complete shared state that flows through the LangGraph orchestration.

    Every agent reads from and writes to this state.
    """

    # --- Configuration ---
    subscription_id: Optional[str] = Field(None)
    resource_group: Optional[str] = Field(None)
    factory_name: Optional[str] = Field(None)
    pipeline_names: Optional[List[str]] = Field(None, description="Specific pipelines to process, or None for all")

    # --- Scanner outputs ---
    raw_adf_json: Optional[Dict[str, Any]] = Field(None, description="Full ADF catalog JSON")
    scan_timestamp: Optional[str] = Field(None)
    cache_path: Optional[str] = Field(None, description="Path to cached JSON file")

    # --- Analyzer outputs ---
    analyzer_output: Optional[AnalyzerOutput] = Field(None)

    # --- Context Builder outputs ---
    context: Optional[ContextOutput] = Field(None)

    # --- Converter state ---
    conversion_results: Dict[str, ConversionResult] = Field(default_factory=dict)
    current_pipeline: Optional[str] = Field(None, description="Pipeline currently being converted")

    # --- Validation state ---
    validation_results: Dict[str, ValidationResult] = Field(default_factory=dict)

    # --- Reviewer state ---
    review_results: Dict[str, ReviewerOutput] = Field(default_factory=dict)

    # --- Orchestration control ---
    current_step: str = Field("start", description="Current step name for routing")
    errors: List[str] = Field(default_factory=list, description="Global error list")
    max_retries: int = Field(3, description="Maximum conversion retries per pipeline")
    trace_id: str = Field(default_factory=lambda: datetime.now().strftime("%Y%m%d_%H%M%S"))
    completed_pipelines: int = Field(0)
    total_pipelines: int = Field(0)

    # --- Output ---
    final_output_path: Optional[str] = Field(None, description="Path to final output directory")

    @field_validator("pipeline_names", mode="before")
    @classmethod
    def split_pipeline_names(cls, v: Any) -> Optional[List[str]]:
        if isinstance(v, str):
            return [p.strip() for p in v.split(",") if p.strip()]
        return v

    def get_summary(self) -> Dict[str, Any]:
        """Return a summary dict for logging/reporting."""
        return {
            "trace_id": self.trace_id,
            "pipelines_total": self.total_pipelines,
            "pipelines_completed": self.completed_pipelines,
            "success_count": sum(
                1 for r in self.conversion_results.values() if r.status == ConversionStatus.SUCCESS
            ),
            "failed_count": sum(
                1 for r in self.conversion_results.values() if r.status == ConversionStatus.FAILED
            ),
            "current_step": self.current_step,
            "error_count": len(self.errors),
        }


# ---------------------------------------------------------------------------
# Agent-specific input/output schemas
# ---------------------------------------------------------------------------

class ConverterInput(BaseModel):
    """Input for the Converter Agent — one pipeline at a time."""

    pipeline: PipelineNode
    context: ContextOutput
    all_pipelines: Dict[str, PipelineNode]
    parent_chain: List[str] = Field(default_factory=list, description="Chain of parent pipeline names")
    retry_feedback: Optional[str] = Field(None, description="Feedback from validator if retrying")
