"""LangGraph Orchestrator - Coordinates the multi-agent ADF to Lakeflow conversion.

Defines the state graph that connects all specialized agents with
conditional routing, retry loops, and error handling.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, TypedDict

from langchain_core.language_models import BaseChatModel
from langgraph.graph import END, StateGraph
from langgraph.checkpoint.memory import MemorySaver

from .agents import (
    AnalyzerAgent,
    ContextBuilderAgent,
    ConverterAgent,
    ReviewerAgent,
    ScannerAgent,
    TriggerConverterAgent,
    ValidatorAgent,
)
from .models.state import (
    ConversionResult,
    ConversionStatus,
    OrchestrationState,
    ValidationResult,
)
from .utils.logger import get_logger, setup_logging

logger = get_logger(__name__)


class Orchestrator:
    """Orchestrates the multi-agent ADF to Lakeflow conversion workflow.

    Builds and runs a LangGraph state machine that coordinates:
    Scanner -> Analyzer -> ContextBuilder -> Converter -> Validator -> Reviewer -> TriggerConverter
    With retry loops from Validator/Reviewer back to Converter/Analyzer.
    """

    def __init__(
        self,
        llm: BaseChatModel,
        output_dir: str = "output",
        cache_dir: str = "cache",
        max_retries: int = 3,
        log_level: str = "INFO",
        log_file: Optional[str] = None,
    ):
        """Initialize the orchestrator.

        Args:
            llm: LangChain chat model for all agents.
            output_dir: Directory for output files.
            cache_dir: Directory for cache files.
            max_retries: Maximum conversion retries per pipeline.
            log_level: Logging level.
            log_file: Optional log file path.
        """
        setup_logging(log_level=log_level, log_file=log_file)

        self.llm = llm
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.cache_dir = cache_dir
        self.max_retries = max_retries

        # Initialize all agents
        self.scanner = ScannerAgent(llm=llm, cache_dir=cache_dir)
        self.analyzer = AnalyzerAgent(llm=llm)
        self.context_builder = ContextBuilderAgent(llm=llm)
        self.converter = ConverterAgent(llm=llm)
        self.validator = ValidatorAgent(llm=llm)
        self.reviewer = ReviewerAgent(llm=llm)
        self.trigger_converter = TriggerConverterAgent(llm=llm)

        # Build the graph
        self.graph = self._build_graph()

    def _build_graph(self) -> StateGraph:
        """Build the LangGraph state machine.

        Returns:
            Compiled StateGraph.
        """
        # Define the state schema
        graph = StateGraph(OrchestrationState)

        # Add nodes for each agent
        graph.add_node("scanner", self.scanner.run)
        graph.add_node("analyzer", self.analyzer.run)
        graph.add_node("context_builder", self.context_builder.run)
        graph.add_node("converter", self.converter.run)
        graph.add_node("validator", self.validator.run)
        graph.add_node("reviewer", self.reviewer.run)
        graph.add_node("trigger_converter", self.trigger_converter.run)

        # Set the entry point
        graph.set_entry_point("scanner")

        # Add conditional edges
        graph.add_conditional_edges(
            "scanner",
            self._route_from_scanner,
            {
                "analyzer": "analyzer",
                "error": END,
            },
        )

        graph.add_conditional_edges(
            "analyzer",
            self._route_from_analyzer,
            {
                "context_builder": "context_builder",
                "error": END,
            },
        )

        graph.add_conditional_edges(
            "context_builder",
            self._route_from_context_builder,
            {
                "converter": "converter",
                "error": END,
            },
        )

        graph.add_conditional_edges(
            "converter",
            self._route_from_converter,
            {
                "validator": "validator",
                "error": END,
            },
        )

        graph.add_conditional_edges(
            "validator",
            self._route_from_validator,
            {
                "converter": "converter",  # Retry conversion
                "analyzer": "analyzer",    # Re-analyze with feedback
                "reviewer": "reviewer",    # Proceed to review
                "error": END,
            },
        )

        graph.add_conditional_edges(
            "reviewer",
            self._route_from_reviewer,
            {
                "converter": "converter",  # Retry conversion
                "trigger_converter": "trigger_converter",  # Proceed to triggers
                "error": END,
            },
        )

        graph.add_conditional_edges(
            "trigger_converter",
            self._route_from_trigger_converter,
            {
                END: END,
                "error": END,
            },
        )

        # Add memory saver for checkpointing
        memory = MemorySaver()

        return graph.compile(checkpointer=memory)

    def run(
        self,
        subscription_id: str,
        resource_group: str,
        factory_name: str,
        pipeline_names: Optional[List[str]] = None,
        tenant_id: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
    ) -> OrchestrationState:
        """Run the full conversion workflow.

        Args:
            subscription_id: Azure subscription ID.
            resource_group: Azure resource group name.
            factory_name: ADF factory name.
            pipeline_names: Optional list of specific pipeline names.
            tenant_id: Azure tenant ID (or set AZURE_TENANT_ID env var).
            client_id: Service principal client ID (or set AZURE_CLIENT_ID env var).
            client_secret: Service principal secret (or set AZURE_CLIENT_SECRET env var).

        Returns:
            Final OrchestrationState with all results.
        """
        # Set credentials in environment if provided
        if tenant_id:
            os.environ["AZURE_TENANT_ID"] = tenant_id
        if client_id:
            os.environ["AZURE_CLIENT_ID"] = client_id
        if client_secret:
            os.environ["AZURE_CLIENT_SECRET"] = client_secret

        # Initialize state
        initial_state = OrchestrationState(
            subscription_id=subscription_id,
            resource_group=resource_group,
            factory_name=factory_name,
            pipeline_names=pipeline_names,
            max_retries=self.max_retries,
        )

        # Generate a thread ID for checkpointing
        thread_id = initial_state.trace_id

        logger.info(
            "Starting orchestration",
            factory=factory_name,
            pipelines=pipeline_names or "ALL",
            trace_id=thread_id,
        )

        # Run the graph
        final_state = self.graph.invoke(
            initial_state,
            config={"configurable": {"thread_id": thread_id}},
        )

        # Write outputs
        self._write_outputs(final_state)

        logger.info(
            "Orchestration complete",
            summary=final_state.get_summary(),
        )

        return final_state

    def _write_outputs(self, state: OrchestrationState) -> None:
        """Write all conversion outputs to files.

        Args:
            state: Final orchestration state.
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_dir = self.output_dir / f"run_{timestamp}"
        run_dir.mkdir(parents=True, exist_ok=True)

        # Write summary report
        summary = state.get_summary()
        with open(run_dir / "summary.json", "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, default=str)

        # Write each pipeline's code
        pipelines_dir = run_dir / "pipelines"
        pipelines_dir.mkdir(exist_ok=True)

        for pipe_name, conv_result in state.conversion_results.items():
            if conv_result.lakeflow_code:
                # Sanitize filename
                safe_name = pipe_name.replace(" ", "_").replace("/", "_").replace("\\", "_")
                file_path = pipelines_dir / f"{safe_name}.py"
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(conv_result.lakeflow_code)
                logger.info("Wrote pipeline code", pipeline=pipe_name, path=str(file_path))

        # Write trigger configs
        if hasattr(state, "trigger_output") and state.trigger_output:
            triggers_path = run_dir / "triggers.json"
            with open(triggers_path, "w", encoding="utf-8") as f:
                json.dump(state.trigger_output, f, indent=2, default=str)

        # Write validation report
        if state.validation_results:
            validation_report = {}
            for pipe_name, val_result in state.validation_results.items():
                validation_report[pipe_name] = {
                    "passed": val_result.passed,
                    "findings": [
                        {"severity": f.severity.value, "message": f.message, "location": f.location}
                        for f in val_result.findings
                    ],
                }
            with open(run_dir / "validation_report.json", "w", encoding="utf-8") as f:
                json.dump(validation_report, f, indent=2, default=str)

        # Write review report
        if state.review_results:
            review_report = {}
            for pipe_name, rev_result in state.review_results.items():
                review_report[pipe_name] = {
                    "approved": rev_result.approved,
                    "completeness_score": rev_result.completeness_score,
                    "missing_items": rev_result.missing_items,
                    "suggestions": rev_result.suggestions,
                    "final_message": rev_result.final_message,
                }
            with open(run_dir / "review_report.json", "w", encoding="utf-8") as f:
                json.dump(review_report, f, indent=2, default=str)

        # Write full state for debugging
        state_path = run_dir / "full_state.json"
        with open(state_path, "w", encoding="utf-8") as f:
            # Exclude raw JSON to keep file size manageable
            state_copy = state.model_dump()
            state_copy.pop("raw_adf_json", None)
            json.dump(state_copy, f, indent=2, default=str)

        state.final_output_path = str(run_dir)
        logger.info("Outputs written", path=str(run_dir))

    # ------------------------------------------------------------------
    # Routing functions
    # ------------------------------------------------------------------

    def _route_from_scanner(
        self, state: OrchestrationState
    ) -> Literal["analyzer", "error"]:
        """Route from scanner based on success/failure."""
        if state.current_step == "error" or state.errors:
            return "error"
        return "analyzer"

    def _route_from_analyzer(
        self, state: OrchestrationState
    ) -> Literal["context_builder", "error"]:
        """Route from analyzer based on success/failure."""
        if state.current_step == "error" or state.errors:
            return "error"
        return "context_builder"

    def _route_from_context_builder(
        self, state: OrchestrationState
    ) -> Literal["converter", "error"]:
        """Route from context builder based on success/failure."""
        if state.current_step == "error" or state.errors:
            return "error"
        return "converter"

    def _route_from_converter(
        self, state: OrchestrationState
    ) -> Literal["validator", "error"]:
        """Route from converter based on success/failure."""
        if state.current_step == "error" or state.errors:
            return "error"
        return "validator"

    def _route_from_validator(
        self, state: OrchestrationState
    ) -> Literal["converter", "analyzer", "reviewer", "error"]:
        """Route from validator with retry logic.

        - If all passed -> reviewer
        - If some failed but retries remain -> converter (retry)
        - If some failed and retries exhausted -> analyzer (re-analyze)
        - If error -> end
        """
        if state.current_step == "error" or state.errors:
            return "error"

        # Check if any pipelines need retry
        needs_retry = False
        needs_reanalyze = False

        for pipe_name, conv_result in state.conversion_results.items():
            if conv_result.status == ConversionStatus.NEEDS_REVIEW:
                if conv_result.retry_count < state.max_retries:
                    needs_retry = True
                else:
                    needs_reanalyze = True

        if needs_retry:
            logger.info("Routing: validator -> converter (retry)")
            return "converter"
        elif needs_reanalyze:
            logger.info("Routing: validator -> analyzer (re-analyze)")
            return "analyzer"
        else:
            logger.info("Routing: validator -> reviewer")
            return "reviewer"

    def _route_from_reviewer(
        self, state: OrchestrationState
    ) -> Literal["converter", "trigger_converter", "error"]:
        """Route from reviewer.

        - If all approved -> trigger_converter
        - If some rejected -> converter (retry)
        - If error -> end
        """
        if state.current_step == "error" or state.errors:
            return "error"

        # Check if any pipelines need rework
        needs_rework = any(
            not r.approved for r in state.review_results.values()
        )

        if needs_rework:
            logger.info("Routing: reviewer -> converter (rework)")
            return "converter"
        else:
            logger.info("Routing: reviewer -> trigger_converter")
            return "trigger_converter"

    def _route_from_trigger_converter(
        self, state: OrchestrationState
    ) -> Literal[END, "error"]:
        """Route from trigger converter to end."""
        if state.current_step == "error" or state.errors:
            return "error"
        return END
