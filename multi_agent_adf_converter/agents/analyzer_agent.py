"""Analyzer Agent - Analyzes ADF pipeline topology and extracts structure.

Takes raw ADF JSON and produces a structured pipeline tree with:
- All activities with their types, dependencies, parameters
- Nested activity resolution (ForEach, IfCondition, Switch, Until)
- Child pipeline detection and parent-child relationships
- Activity type enumeration and parameter counting
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from ..models.state import (
    ActivityNode,
    ActivityType,
    AnalyzerOutput,
    OrchestrationState,
    PipelineNode,
)
from ..utils.logger import get_logger
from .base_agent import BaseAgent

logger = get_logger(__name__)


class AnalyzerAgent(BaseAgent):
    """Agent responsible for analyzing ADF pipeline topology.

    Takes the raw ADF JSON from the Scanner and produces a structured
    pipeline tree with all activities, dependencies, parameters, and
    child pipeline relationships.
    """

    def _default_system_prompt(self) -> str:
        return (
            "You are the Analyzer Agent — the SECOND agent in the ADF-to-Databricks Lakeflow migration pipeline. "
            "Your role is to DEEPLY ANALYZE the raw ADF JSON and produce a structured, "
            "machine-readable pipeline tree. Every subsequent agent (Context Builder, Converter, Validator) "
            "depends on the accuracy of your analysis.\n\n"

            "=== YOUR RESPONSIBILITIES ===\n"
            "1. PIPELINE TOPOLOGY EXTRACTION:\n"
            "   For each pipeline, extract:\n"
            "   - Pipeline name, factory name\n"
            "   - ALL activities (no activity skipped!)\n"
            "   - Pipeline-level parameters (with types and default values)\n"
            "   - Pipeline variables\n"
            "   - Raw JSON for reference\n\n"

            "2. ACTIVITY ANALYSIS (for EVERY activity):\n"
            "   - name: Exact activity name from ADF\n"
            "   - activity_type: Map to ActivityType enum (Copy, ExecutePipeline, DatabricksNotebook, \n"
            "     ForEach, IfCondition, Until, SetVariable, AppendVariable, Web, Webhook, \n"
            "     SqlServerStoredProcedure, Lookup, GetMetadata, Delete, Switch, Wait, Fail, \n"
            "     Filter, Flatten, Validation, ExecuteDataFlow, Custom, or Unsupported)\n"
            "   - depends_on: Extract ALL dependency relationships from depends_on field\n"
            "   - inputs/outputs: Dataset references from inputs[] and outputs[]\n"
            "   - linked_service: Reference name from linkedServiceName\n"
            "   - resolved_params: The complete activity configuration from the 'config' field:\n"
            "       * Copy: source, sink, translator, enableStaging\n"
            "       * ExecutePipeline: pipeline referenceName, parameters\n"
            "       * DatabricksNotebook: notebookPath, baseParameters\n"
            "       * ForEach: items, isSequential, activities (nested!)\n"
            "       * IfCondition: expression, ifTrueActivities, ifFalseActivities (nested!)\n"
            "       * Until: expression, activities (nested!)\n"
            "       * Switch: on, cases[], defaultActivities (nested!)\n"
            "       * SetVariable: variableName, value\n"
            "       * AppendVariable: variableName, value\n"
            "       * Wait: waitTimeInSeconds\n"
            "       * Fail: message, errorCode\n"
            "       * Lookup: source, dataset, firstRowOnly\n"
            "       * GetMetadata: fieldList, dataset\n"
            "       * Delete: dataset, enableLogging\n"
            "       * Web: url, method, body, headers\n"
            "       * Webhook: url, method, body, timeout\n"
            "       * SqlServerStoredProcedure: storedProcedureName, storedProcedureParameters\n"
            "       * ExecuteDataFlow: dataflow, staging, integrationRuntime\n"
            "   - nested_activities: RECURSIVELY extract nested activities for:\n"
            "       * ForEach → activities inside the loop\n"
            "       * IfCondition → ifTrueActivities AND ifFalseActivities\n"
            "       * Until → activities inside the loop\n"
            "       * Switch → each case's activities + defaultActivities\n\n"

            "3. NESTED ACTIVITY RESOLUTION:\n"
            "   - CRITICAL: You MUST recursively traverse nested activity containers.\n"
            "   - ForEach activities contain child 'activities' that must be analyzed identically.\n"
            "   - IfCondition activities contain 'ifTrueActivities' and 'ifFalseActivities'.\n"
            "   - Until activities contain child 'activities'.\n"
            "   - Switch activities contain 'cases' (each with activities) and 'defaultActivities'.\n"
            "   - Nesting can be DEEP (e.g., ForEach containing IfCondition containing Copy).\n"
            "   - You MUST preserve this nesting structure completely.\n\n"

            "4. CHILD PIPELINE DETECTION:\n"
            "   - Detect ExecutePipeline activities and extract the child pipeline referenceName.\n"
            "   - Build a list of child_pipelines for each pipeline.\n"
            "   - After building all pipelines, RESOLVE PARENT RELATIONSHIPS:\n"
            "     If Pipeline A has child Pipeline B, set Pipeline B's parent_pipeline = A.\n"
            "   - This enables the Converter to process children FIRST (bottom-up).\n\n"

            "5. MASTER → LANDED → PROCESSED PATTERN DETECTION:\n"
            "   - Your pipelines commonly follow a pattern:\n"
            "       Master Pipeline → calls LANDED child → calls PROCESSED child\n"
            "   - Detect this pattern and note it in the analysis.\n"
            "   - The LANDED pipeline typically has Copy activity (source → landing zone).\n"
            "   - The PROCESSED pipeline typically has Copy + transformations.\n\n"

            "6. SUMMARY GENERATION:\n"
            "   - Total pipelines analyzed\n"
            "   - All unique activity types found across all pipelines\n"
            "   - Total parameter count\n"
            "   - Count of pipelines with child pipelines\n"
            "   - Any anomalies detected (pipelines with 0 activities, missing dependencies, etc.)\n\n"

            "=== EDGE CASES TO HANDLE ===\n"
            "1. Activities with UNKNOWN types: Map to ActivityType.UNSUPPORTED and add a note.\n"
            "2. Empty pipelines (0 activities): Still create a PipelineNode with empty activities list.\n"
            "3. Circular pipeline references: Detect and flag them (Pipeline A → B → A).\n"
            "4. Missing dependency references: Log a warning but don't fail.\n"
            "5. Duplicate activity names within a pipeline: Flag as a warning.\n\n"

            "=== OUTPUT ===\n"
            "You produce an AnalyzerOutput containing:\n"
            "  - pipeline_tree: Dict[str, PipelineNode] — ALL pipelines with ALL activities resolved\n"
            "  - activity_types_found: List[str] — all unique activity type names\n"
            "  - parameter_count: int — total parameters across all pipelines\n"
            "  - has_child_pipelines: bool — whether any pipeline calls child pipelines\n"
            "  - raw_pipeline_count: int — how many raw pipelines were processed\n"
            "  - summary: str — textual summary for logging/debugging\n\n"

            "=== DATA INTEGRITY ===\n"
            "- PRESERVE EVERYTHING from the raw JSON. The Converter needs full context.\n"
            "- Do NOT summarize, abbreviate, or skip any activity.\n"
            "- Every activity that exists in ADF must appear in the pipeline tree.\n"
            "- If you're unsure about an activity type, mark it as UNSUPPORTED rather than dropping it."
        )

    def run(self, state: OrchestrationState) -> Dict[str, Any]:
        """Analyze the raw ADF JSON and build a structured pipeline tree.

        Args:
            state: Current orchestration state with raw_adf_json.

        Returns:
            Dict with analyzer_output and updated current_step.
        """
        if not state.raw_adf_json:
            return {
                "errors": state.errors + ["No raw ADF JSON available for analysis."],
                "current_step": "error",
            }

        self.logger.info("Starting pipeline analysis")

        catalog = state.raw_adf_json.get("catalog", {})
        pipelines_raw = catalog.get("pipelines", [])
        datasets_raw = catalog.get("datasets", [])
        linked_services_raw = catalog.get("linked_services", [])
        triggers_raw = catalog.get("triggers", [])

        # Build pipeline tree
        pipeline_tree = self._build_pipeline_tree(pipelines_raw, datasets_raw, linked_services_raw, triggers_raw)

        # Detect activity types across all pipelines
        all_activity_types = set()
        for pipe in pipeline_tree.values():
            for act in pipe.activities:
                all_activity_types.add(act.activity_type.value)

        # Detect child pipeline relationships
        has_child_pipelines = any(
            len(pipe.child_pipelines) > 0 for pipe in pipeline_tree.values()
        )

        # Count parameters
        param_count = sum(len(pipe.parameters) for pipe in pipeline_tree.values())

        # Generate summary via LLM
        summary_prompt = (
            f"Analyzed {len(pipeline_tree)} pipelines from ADF factory '{state.factory_name}'. "
            f"Activity types found: {', '.join(sorted(all_activity_types))}. "
            f"Total parameters: {param_count}. "
            f"Has child pipelines: {has_child_pipelines}. "
            f"Pipeline names: {list(pipeline_tree.keys())[:20]}... "
            f"Provide a concise analysis summary."
        )
        summary = self._invoke_llm(summary_prompt, temperature=0.1)

        analyzer_output = AnalyzerOutput(
            pipeline_tree=pipeline_tree,
            activity_types_found=sorted(all_activity_types),
            parameter_count=param_count,
            has_child_pipelines=has_child_pipelines,
            raw_pipeline_count=len(pipelines_raw),
            summary=summary,
        )

        self.logger.info(
            "Analysis complete",
            pipelines=len(pipeline_tree),
            activity_types=list(all_activity_types),
        )

        return {
            "analyzer_output": analyzer_output,
            "current_step": "analyze_complete",
        }

    def _build_pipeline_tree(
        self,
        pipelines_raw: List[Dict[str, Any]],
        datasets_raw: List[Dict[str, Any]],
        linked_services_raw: List[Dict[str, Any]],
        triggers_raw: List[Dict[str, Any]],
    ) -> Dict[str, PipelineNode]:
        """Build a structured pipeline tree from raw ADF data.

        Args:
            pipelines_raw: List of raw pipeline dicts from ADF.
            datasets_raw: List of raw dataset dicts.
            linked_services_raw: List of raw linked service dicts.
            triggers_raw: List of raw trigger dicts.

        Returns:
            Dict mapping pipeline name -> PipelineNode.
        """
        pipeline_tree: Dict[str, PipelineNode] = {}

        for pipe_raw in pipelines_raw:
            pipe_name = pipe_raw.get("pipeline", "unknown")
            self.logger.debug("Analyzing pipeline", pipeline=pipe_name)

            # Analyze this pipeline's activities
            activities = self._analyze_activities(pipe_raw)

            # Extract child pipeline names
            child_pipelines = []
            for act in activities:
                if act.activity_type == ActivityType.EXECUTE_PIPELINE:
                    child_name = act.resolved_params.get("pipeline", "")
                    if child_name:
                        child_pipelines.append(child_name)

            pipeline_node = PipelineNode(
                name=pipe_name,
                factory=pipe_raw.get("factory", ""),
                activities=activities,
                parameters=pipe_raw.get("parameters", {}),
                variables=pipe_raw.get("variables", []),
                child_pipelines=child_pipelines,
                raw_json=pipe_raw,
            )

            pipeline_tree[pipe_name] = pipeline_node

        # Resolve parent relationships
        for pipe_name, pipe_node in pipeline_tree.items():
            for child_name in pipe_node.child_pipelines:
                if child_name in pipeline_tree:
                    pipeline_tree[child_name].parent_pipeline = pipe_name

        return pipeline_tree

    def _analyze_activities(self, pipe_raw: Dict[str, Any]) -> List[ActivityNode]:
        """Analyze all activities in a pipeline.

        Args:
            pipe_raw: Raw pipeline JSON dict.

        Returns:
            List of ActivityNode objects.
        """
        activities_detail = pipe_raw.get("activities_detail", [])
        if not activities_detail:
            return []

        activity_nodes = []
        for act_raw in activities_detail:
            act_name = act_raw.get("name", "unknown")
            act_type_str = act_raw.get("type", "Unsupported")

            # Map to ActivityType enum
            try:
                act_type = ActivityType(act_type_str)
            except ValueError:
                act_type = ActivityType.UNSUPPORTED

            # Extract dependencies
            depends_on = act_raw.get("depends_on", [])

            # Extract inputs/outputs
            inputs = []
            outputs = []
            for inp in act_raw.get("inputs", []):
                if isinstance(inp, dict):
                    inputs.append(inp.get("referenceName", ""))
                else:
                    inputs.append(str(inp))
            for out in act_raw.get("outputs", []):
                if isinstance(out, dict):
                    outputs.append(out.get("referenceName", ""))
                else:
                    outputs.append(str(out))

            # Extract linked service
            ls_ref = act_raw.get("linkedServiceName", {})
            linked_service = ls_ref.get("referenceName") if isinstance(ls_ref, dict) else None

            # Extract nested activities for ForEach, IfCondition, Until, Switch
            nested = []
            config = act_raw.get("config", {})
            if act_type == ActivityType.FOR_EACH:
                nested_raw = config.get("activities", [])
                nested = self._analyze_nested_activities(nested_raw, "ForEach")
            elif act_type == ActivityType.IF_CONDITION:
                true_raw = config.get("ifTrueActivities", [])
                false_raw = config.get("ifFalseActivities", [])
                nested = self._analyze_nested_activities(true_raw, "IfCondition.true") + \
                         self._analyze_nested_activities(false_raw, "IfCondition.false")
            elif act_type == ActivityType.UNTIL:
                nested_raw = config.get("activities", [])
                nested = self._analyze_nested_activities(nested_raw, "Until")
            elif act_type == ActivityType.SWITCH:
                cases = config.get("cases", [])
                for case in cases:
                    case_acts = case.get("activities", [])
                    nested += self._analyze_nested_activities(case_acts, f"Switch.case.{case.get('value', 'unknown')}")
                default_acts = config.get("defaultActivities", [])
                nested += self._analyze_nested_activities(default_acts, "Switch.default")

            activity_node = ActivityNode(
                name=act_name,
                activity_type=act_type,
                raw_json=act_raw,
                depends_on=depends_on,
                inputs=inputs,
                outputs=outputs,
                linked_service=linked_service,
                resolved_params=config,
                nested_activities=nested,
            )

            activity_nodes.append(activity_node)

        return activity_nodes

    def _analyze_nested_activities(
        self,
        activities_raw: List[Dict[str, Any]],
        context: str,
    ) -> List[ActivityNode]:
        """Recursively analyze nested activities.

        Args:
            activities_raw: List of raw activity dicts.
            context: Context string for logging.

        Returns:
            List of ActivityNode objects.
        """
        nodes = []
        for act_raw in activities_raw:
            act_name = act_raw.get("name", f"{context}.unknown")
            act_type_str = act_raw.get("type", "Unsupported")

            try:
                act_type = ActivityType(act_type_str)
            except ValueError:
                act_type = ActivityType.UNSUPPORTED

            depends_on = act_raw.get("depends_on", [])
            config = act_raw.get("config", act_raw.get("typeProperties", {}))

            # Recursively handle deeper nesting
            nested = []
            if act_type == ActivityType.FOR_EACH:
                nested = self._analyze_nested_activities(
                    config.get("activities", []), f"{act_name}.ForEach"
                )
            elif act_type == ActivityType.IF_CONDITION:
                nested = self._analyze_nested_activities(
                    config.get("ifTrueActivities", []), f"{act_name}.IfCondition.true"
                ) + self._analyze_nested_activities(
                    config.get("ifFalseActivities", []), f"{act_name}.IfCondition.false"
                )
            elif act_type == ActivityType.UNTIL:
                nested = self._analyze_nested_activities(
                    config.get("activities", []), f"{act_name}.Until"
                )

            node = ActivityNode(
                name=act_name,
                activity_type=act_type,
                raw_json=act_raw,
                depends_on=depends_on,
                resolved_params=config,
                nested_activities=nested,
            )
            nodes.append(node)

        return nodes
