"""Reviewer Agent - Final quality and completeness check for converted code.

This is the SIXTH agent — the FINAL quality gate before code is considered
production-ready. The Reviewer checks completeness, code quality, best practices,
security, and ensures NOTHING was missed during conversion.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from ..models.state import (
    ConversionResult,
    ConversionStatus,
    OrchestrationState,
    ReviewerOutput,
)
from ..utils.logger import get_logger
from .base_agent import BaseAgent

logger = get_logger(__name__)


class ReviewerAgent(BaseAgent):
    """Agent responsible for final review of converted Lakeflow code.

    This is the LAST agent before code is marked as production-ready.
    It performs a comprehensive quality check including completeness,
    code quality, best practices, and produces a final approval.
    """

    def _default_system_prompt(self) -> str:
        return (
            "You are the Reviewer Agent — the SIXTH and FINAL quality gate in the ADF-to-Databricks Lakeflow migration pipeline. "
            "You are the LAST LINE OF DEFENSE before code is considered production-ready. "
            "Your approval is REQUIRED for every pipeline. Be PEDANTIC. Be THOROUGH. "
            "If you reject, the Converter must retry with your feedback.\n\n"

            "=== YOUR COMPREHENSIVE REVIEW CHECKLIST ===\n"
            "Check EVERY item below for EVERY pipeline:\n\n"

            "1. COMPLETENESS (score 0.0-1.0):\n"
            "   a) Every ADF activity has a corresponding code block.\n"
            "   b) Every pipeline parameter is a function argument with correct type.\n"
            "   c) Every linked service connection is configured.\n"
            "   d) Every dataset reference is resolved.\n"
            "   e) Every trigger is connected to its pipeline.\n"
            "   f) Child pipeline calls pass the correct parameters.\n"
            "   g) Nested activities (ForEach, IfCondition, Switch) are fully expanded.\n"
            "   h) ALL ADF expression syntax (@{...}, ${...}) is resolved.\n"
            "   SCORE 1.0 ONLY if EVERYTHING is complete.\n\n"

            "2. CORRECTNESS (score 0.0-1.0):\n"
            "   a) Copy activity source/sink mapping matches the ADF translator.\n"
            "   b) ForEach loop iterates over the correct items.\n"
            "   c) IfCondition expression produces correct branching.\n"
            "   d) Until loop condition matches ADF.\n"
            "   e) Switch cases match ADF exactly.\n"
            "   f) Wait durations match (seconds in Python vs ADF).\n"
            "   g) Stored procedure calls have correct parameters.\n"
            "   h) Web/Webhook calls have correct URLs, methods, headers, body.\n"
            "   i) Databricks notebook paths are correct.\n"
            "   j) Activity execution order matches dependency graph.\n"
            "   SCORE 1.0 ONLY if logic is EXACTLY equivalent.\n\n"

            "3. ROBUSTNESS (score 0.0-1.0):\n"
            "   a) try/except around EVERY major operation.\n"
            "   b) Exceptions are logged before being raised.\n"
            "   c) No bare 'except:' — always 'except Exception as e:'.\n"
            "   d) Timeouts are set for external calls (Web, Webhook, JDBC).\n"
            "   e) Retry logic exists for transient failures.\n"
            "   f) ForEach has bounds/limits to prevent infinite loops.\n"
            "   g) Until has max iteration count to prevent infinite loops.\n"
            "   SCORE 1.0 ONLY if error handling is COMPREHENSIVE.\n\n"

            "4. CODE QUALITY (score 0.0-1.0):\n"
            "   a) Module docstring with pipeline name, factory, conversion date.\n"
            "   b) Function docstrings with Args/Returns sections.\n"
            "   c) Type hints on ALL function parameters and return types.\n"
            "   d) F-strings used for string formatting.\n"
            "   e) Meaningful variable names (not 'df1', 'df2', 'temp').\n"
            "   f) Comments explain complex logic.\n"
            "   g) No dead code, commented-out code, or placeholder comments.\n"
            "   h) Consistent indentation (4 spaces).\n"
            "   i) Imports are organized and only what's needed.\n"
            "   j) Constants/configuration are at the top of the file.\n"
            "   SCORE 1.0 ONLY if code is CLEAN and PROFESSIONAL.\n\n"

            "5. SECURITY (score 0.0-1.0):\n"
            "   a) NO hardcoded secrets, passwords, API keys in the code.\n"
            "   b) Connection strings use dbutils.secrets.get() or environment variables.\n"
            "   c) Credentials are NEVER logged.\n"
            "   d) No hardcoded IP addresses or internal URLs.\n"
            "   e) SQL queries use parameterized statements (no string concatenation).\n"
            "   SCORE 1.0 ONLY if NO security issues exist.\n\n"

            "6. DATABRICKS COMPATIBILITY (score 0.0-1.0):\n"
            "   a) Uses SparkSession.builder.getOrCreate() — the standard Databricks pattern.\n"
            "   b) File paths use /dbfs/, /mnt/, or abfss:// — Databricks-compatible paths.\n"
            "   c) Uses dbutils.fs for file operations.\n"
            "   d) Writes to Delta Lake format.\n"
            "   e) Uses spark.sql() for SQL operations.\n"
            "   f) No local filesystem operations that would fail on Databricks.\n"
            "   g) No pip install inside the code (should be in cluster init scripts).\n"
            "   SCORE 1.0 ONLY if code will run ON Databricks without modification.\n\n"

            "7. PARENT-CHILD PIPELINE COORDINATION:\n"
            "   a) Child pipelines are defined as separate functions or notebooks.\n"
            "   b) Parent pipeline calls children with correct parameters.\n"
            "   c) Children are processed BEFORE parents (bottom-up).\n"
            "   d) Return values from children are captured.\n"
            "   e) Error from child is propagated to parent.\n\n"

            "8. TRIGGER INTEGRATION:\n"
            "   a) Schedule triggers are represented as Databricks Workflow cron schedules.\n"
            "   b) Event triggers (blob/custom) are represented as Auto Loader subscriptions.\n"
            "   c) Tumbling window triggers have correct offset/frequency.\n"
            "   d) Pipeline start/stop conditions from triggers are respected.\n\n"

            "=== DECISION CRITERIA ===\n"
            "APPROVE if:\n"
            "   - Completeness score >= 0.9\n"
            "   - All other scores >= 0.8\n"
            "   - No missing_items that are critical\n\n"
            "REJECT if:\n"
            "   - Completeness score < 0.9 (activities or details missing)\n"
            "   - Correctness score < 0.8 (logic errors)\n"
            "   - Security score < 1.0 (ANY security issue)\n"
            "   - Syntax errors exist (should have been caught by Validator, but check again)\n\n"

            "=== OUTPUT FORMAT ===\n"
            "Return a JSON object with your detailed assessment:\n"
            "{\n"
            '  "approved": true/false,\n'
            '  "completeness_score": 0.0-1.0,\n'
            '  "missing_items": ["list of activities, params, connections, etc. that are missing"],\n'
            '  "suggestions": ["actionable suggestions for improvement"],\n'
            '  "final_message": "Overall assessment summary for the pipeline"\n'
            "}\n\n"
            "Your feedback goes DIRECTLY to the Converter Agent for retry. "
            "Be specific — 'line 45: missing error handling' is better than 'error handling missing'. "
            "Your review determines if a pipeline is production-ready or needs rework."
        )

    def run(self, state: OrchestrationState) -> Dict[str, Any]:
        """Review all converted and validated pipelines."""
        if not state.conversion_results:
            return {
                "errors": state.errors + ["No conversion results to review."],
                "current_step": "error",
            }

        self.logger.info(
            "Starting final review",
            pipelines_to_review=len(state.conversion_results),
        )

        review_results: Dict[str, ReviewerOutput] = {}
        total_score = 0.0

        for pipe_name, conv_result in state.conversion_results.items():
            if conv_result.status != ConversionStatus.VALIDATED:
                review_results[pipe_name] = ReviewerOutput(
                    pipeline_name=pipe_name,
                    approved=False,
                    completeness_score=0.0,
                    missing_items=["Pipeline not validated yet."],
                )
                continue

            self.logger.info("Reviewing pipeline", pipeline=pipe_name)
            review = self._review_pipeline(pipe_name, conv_result, state)
            review_results[pipe_name] = review
            total_score += review.completeness_score

            if review.approved:
                conv_result.status = ConversionStatus.SUCCESS
                logger.info("Pipeline approved", pipeline=pipe_name)
            else:
                logger.warning(
                    "Pipeline needs rework",
                    pipeline=pipe_name,
                    score=review.completeness_score,
                    missing=review.missing_items,
                )

        avg_score = total_score / len(review_results) if review_results else 0.0
        all_approved = all(r.approved for r in review_results.values())

        self.logger.info(
            "Review complete",
            approved=sum(1 for r in review_results.values() if r.approved),
            rejected=sum(1 for r in review_results.values() if not r.approved),
            avg_score=round(avg_score, 2),
        )

        return {
            "review_results": review_results,
            "current_step": "review_complete" if all_approved else "review_failed",
        }

    def _review_pipeline(
        self,
        pipe_name: str,
        conv_result: ConversionResult,
        state: OrchestrationState,
    ) -> ReviewerOutput:
        """Review a single pipeline's converted code."""
        pipeline = state.analyzer_output.pipeline_tree.get(pipe_name) if state.analyzer_output else None
        trigger_info = self._get_trigger_info(pipe_name, state)
        validation = state.validation_results.get(pipe_name)

        prompt_parts = [
            f"## FINAL REVIEW: Pipeline '{pipe_name}'\n",
        ]

        if pipeline:
            prompt_parts.append(
                f"Original ADF had {len(pipeline.activities)} activities: "
                f"{', '.join(f'{a.name} ({a.activity_type.value})' for a in pipeline.activities)}\n"
            )
            prompt_parts.append(f"Child pipelines: {pipeline.child_pipelines}\n")

        if trigger_info:
            prompt_parts.append(f"Connected triggers: {trigger_info}\n")

        if validation:
            prompt_parts.append(
                f"Validation findings ({len(validation.findings)}):\n"
                + "\n".join(f"  - [{f.severity}] {f.message}" for f in validation.findings[:10])
                + "\n"
            )

        prompt_parts.append(
            f"\n## Converted Lakeflow Code (first 3000 chars):\n"
            f"```python\n{conv_result.lakeflow_code[:3000]}\n```\n"
        )

        prompt_parts.append(
            "\n## REVIEW CRITERIA:\n"
            "Score 0.0 to 1.0 on each:\n"
            "1. completeness - Are ALL original activities converted?\n"
            "2. correctness - Is the logic correct?\n"
            "3. robustness - Is error handling present?\n"
            "4. quality - Does the code follow best practices?\n"
            "5. security - No hardcoded secrets?\n\n"
            "Return JSON:\n"
            "{\n"
            '  "approved": true/false,\n'
            '  "completeness_score": 0.0-1.0,\n'
            '  "missing_items": ["..."],\n'
            '  "suggestions": ["..."],\n'
            '  "final_message": "..."\n'
            "}"
        )

        prompt = "\n".join(prompt_parts)

        try:
            response = self._invoke_llm(prompt, temperature=0.1)
            result = self._parse_json_response(response)

            return ReviewerOutput(
                pipeline_name=pipe_name,
                approved=result.get("approved", False),
                completeness_score=result.get("completeness_score", 0.0),
                missing_items=result.get("missing_items", []),
                suggestions=result.get("suggestions", []),
                final_message=result.get("final_message", ""),
            )
        except Exception as e:
            logger.error("Review failed", pipeline=pipe_name, error=str(e))
            return ReviewerOutput(
                pipeline_name=pipe_name,
                approved=False,
                completeness_score=0.0,
                missing_items=[f"Review process error: {str(e)}"],
            )

    def _get_trigger_info(self, pipe_name: str, state: OrchestrationState) -> Optional[str]:
        """Get trigger information for a pipeline."""
        if not state.context:
            return None

        triggers = []
        for trig_name, trig_node in state.context.trigger_map.items():
            if pipe_name in trig_node.linked_pipelines:
                triggers.append(
                    f"{trig_name} ({trig_node.trigger_type.value}, status: {trig_node.status})"
                )

        return "; ".join(triggers) if triggers else None
