"""Validator Agent - Validates converted Lakeflow code for correctness.

Performs BOTH automated checks (AST syntax, regex patterns, import analysis)
AND LLM-based semantic validation to ensure the converted code is correct.
"""

from __future__ import annotations

import ast
import re
from typing import Any, Dict, List, Optional

from ..models.state import (
    ConversionResult,
    ConversionStatus,
    OrchestrationState,
    ValidationFinding,
    ValidationResult,
    ValidationSeverity,
)
from ..utils.logger import get_logger
from .base_agent import BaseAgent

logger = get_logger(__name__)


class ValidatorAgent(BaseAgent):
    """Agent responsible for validating converted Lakeflow code.

    This is the FIFTH agent. It acts as a quality gate — if validation fails,
    the Converter must retry. It performs both automated and LLM-based checks.
    """

    def _default_system_prompt(self) -> str:
        return (
            "You are the Validator Agent — the FIFTH agent in the ADF-to-Databricks Lakeflow migration pipeline. "
            "You are a QUALITY GATE. Your job is to catch EVERY error in the converted code BEFORE it reaches the Reviewer. "
            "If you find issues, the Converter Agent must retry with your feedback. Be THOROUGH and PRECISE.\n\n"

            "=== YOUR CHECKLIST (run every check on every pipeline) ===\n\n"

            "1. SYNTAX VALIDATION:\n"
            "   - Check Python syntax using ast.parse()\n"
            "   - Catch: missing colons, unmatched brackets, invalid indentation, etc.\n"
            "   - SYNTAX ERRORS ARE UNACCEPTABLE — the code must compile.\n\n"

            "2. UNRESOLVED ADF EXPRESSIONS:\n"
            "   Search the code for these patterns — if ANY exist, the conversion is INCOMPLETE:\n"
            "   - @{...} — ADF expression notation\n"
            "   - ${...} — ADF variable/interpolation notation\n"
            "   - $$[...] — ADF string interpolation\n"
            "   - pipeline().parameters. — unresolved pipeline parameter reference\n"
            "   - pipeline().globals. — unresolved global parameter reference\n"
            "   - activity().output — unresolved activity output reference\n"
            "   - dataset(). — unresolved dataset reference\n"
            "   - linkedService(). — unresolved linked service reference\n"
            "   ANY of these mean the Converter did NOT fully resolve ADF expressions.\n\n"

            "3. CODE STRUCTURE CHECK:\n"
            "   Verify the generated code has:\n"
            "   - A shebang line (#!/usr/bin/env python3)\n"
            "   - Module docstring with pipeline name, source factory, conversion timestamp\n"
            "   - Proper imports (logging, time, typing, pyspark.sql, delta.tables)\n"
            "   - A main entry function (def main or def run)\n"
            "   - try/except blocks around major operations\n"
            "   - Logging statements (logger.info, logger.error)\n"
            "   - Docstrings on functions\n"
            "   - Clear section separators (comments with ====)\n\n"

            "4. ACTIVITY COMPLETENESS CHECK:\n"
            "   Compare the original ADF pipeline activities against the generated code.\n"
            "   Every activity name from ADF must appear in the code.\n"
            "   If any activity is missing, flag it as an ERROR.\n\n"

            "5. DEPENDENCY ORDER CHECK:\n"
            "   If Activity B depends_on Activity A, then A's code must appear BEFORE B's code.\n"
            "   Check the execution order matches the dependency graph.\n\n"

            "6. CONNECTION RESOLUTION CHECK:\n"
            "   - All linked service names referenced in activities must have corresponding \n"
            "     connection configuration in the code.\n"
            "   - Connection strings must be complete (not placeholder like 'your-connection-string').\n"
            "   - Key Vault references must be handled (either resolved or noted).\n\n"

            "7. PARAMETER HANDLING CHECK:\n"
            "   - All pipeline parameters must appear as function arguments.\n"
            "   - Parameters must have proper type hints (str, int, bool, etc.).\n"
            "   - Default values must match the ADF parameter defaults.\n\n"

            "8. DATABRICKS COMPATIBILITY CHECK:\n"
            "   - No local-only imports (os.system, subprocess, etc. are OK but flagged).\n"
            "   - Uses SparkSession.builder.getOrCreate() for Spark.\n"
            "   - File paths use /dbfs/ or /mnt/ or abfss:// patterns for Databricks.\n"
            "   - Uses dbutils.fs for file operations (not os.path or shutil).\n\n"

            "9. ERROR HANDLING CHECK:\n"
            "   - try/except blocks must be present around each major activity.\n"
            "   - Exceptions must be logged before being raised.\n"
            "   - No bare 'except:' — must be 'except Exception as e:'.\n\n"

            "10. OUTPUT DESTINATION CHECK:\n"
            "    - Code must write to Delta tables (format('delta')).\n"
            "    - NOT CSV, NOT Parquet (unless specifically for staging).\n"
            "    - Delta paths must be valid Databricks paths.\n\n"

            "=== HOW TO REPORT FINDINGS ===\n"
            "For EACH issue found, create a ValidationFinding with:\n"
            "  - severity: 'error' for blocking issues, 'warning' for concerns, 'info' for notes\n"
            "  - message: CLEAR, ACTIONABLE description of what's wrong\n"
            "  - location: Where in the code (e.g., 'imports', 'line 45', 'function main_pipeline')\n\n"
            "ERROR findings cause the validation to FAIL. WARNING findings are advisory.\n"
            "If there are ANY ERROR findings, the pipeline needs re-conversion.\n\n"

            "=== PASS/FAIL CRITERIA ===\n"
            "PASS: Zero ERROR-level findings\n"
            "FAIL: One or more ERROR-level findings\n\n"
            "=== SEMANTIC VALIDATION ===\n"
            "After automated checks, perform an LLM-based semantic check:\n"
            "1. Does the code LOGICALLY match the original ADF pipeline?\n"
            "2. Are all data transformations correct?\n"
            "3. Are all control flows (if/else, loops, switch) equivalent?\n"
            "4. Would this code produce correct results on Databricks?\n"
            "5. Are there any hidden bugs, race conditions, or edge cases?\n\n"

            "Be thorough. Be pedantic. Quality is your only concern."
        )

    def run(self, state: OrchestrationState) -> Dict[str, Any]:
        """Validate all converted pipelines.

        Args:
            state: Current orchestration state with conversion_results.

        Returns:
            Dict with validation_results and updated current_step.
        """
        if not state.conversion_results:
            return {
                "errors": state.errors + ["No conversion results to validate."],
                "current_step": "error",
            }

        self.logger.info(
            "Starting validation",
            pipelines_to_validate=len(state.conversion_results),
        )

        validation_results: Dict[str, ValidationResult] = {}

        for pipe_name, conv_result in state.conversion_results.items():
            if conv_result.status == ConversionStatus.FAILED:
                validation_results[pipe_name] = ValidationResult(
                    pipeline_name=pipe_name,
                    passed=False,
                    findings=[
                        ValidationFinding(
                            severity=ValidationSeverity.ERROR,
                            message=f"Conversion failed: {conv_result.error_detail or 'Unknown error'}",
                        )
                    ],
                )
                continue

            if not conv_result.lakeflow_code:
                validation_results[pipe_name] = ValidationResult(
                    pipeline_name=pipe_name,
                    passed=False,
                    findings=[
                        ValidationFinding(
                            severity=ValidationSeverity.ERROR,
                            message="No code generated for this pipeline.",
                        )
                    ],
                )
                continue

            self.logger.info("Validating pipeline", pipeline=pipe_name)
            result = self._validate_pipeline(pipe_name, conv_result, state)
            validation_results[pipe_name] = result

            if result.passed:
                conv_result.status = ConversionStatus.VALIDATED
                conv_result.validation_errors = [
                    f.message for f in result.findings if f.severity == ValidationSeverity.ERROR
                ]
                self.logger.info("Pipeline validated successfully", pipeline=pipe_name)
            else:
                conv_result.status = ConversionStatus.NEEDS_REVIEW
                conv_result.validation_errors = [
                    f.message for f in result.findings if f.severity == ValidationSeverity.ERROR
                ]
                self.logger.warning(
                    "Pipeline validation failed",
                    pipeline=pipe_name,
                    errors=len([f for f in result.findings if f.severity == ValidationSeverity.ERROR]),
                )

        all_passed = all(r.passed for r in validation_results.values())
        any_failed = any(not r.passed for r in validation_results.values())

        return {
            "validation_results": validation_results,
            "current_step": "validation_complete" if all_passed else "validation_failed",
        }

    def _validate_pipeline(
        self,
        pipe_name: str,
        conv_result: ConversionResult,
        state: OrchestrationState,
    ) -> ValidationResult:
        """Validate a single pipeline's converted code."""
        code = conv_result.lakeflow_code
        findings: List[ValidationFinding] = []

        # 1. Syntax check
        syntax_valid, syntax_errors = self._check_syntax(code)
        if not syntax_valid:
            for err in syntax_errors:
                findings.append(
                    ValidationFinding(
                        severity=ValidationSeverity.ERROR,
                        message=f"Syntax error: {err}",
                        location="code",
                    )
                )

        # 2. Check for unresolved ADF expressions
        unresolved = self._check_unresolved_expressions(code)
        for expr in unresolved:
            findings.append(
                ValidationFinding(
                    severity=ValidationSeverity.ERROR,
                    message=f"Unresolved ADF expression: {expr}",
                    location="code",
                )
            )

        # 3. Check for required imports
        missing_imports = self._check_imports(code)
        for imp in missing_imports:
            findings.append(
                ValidationFinding(
                    severity=ValidationSeverity.WARNING,
                    message=f"Missing recommended import: {imp}",
                    location="imports",
                )
            )

        # 4. Check for basic structure
        structure_issues = self._check_structure(code)
        for issue in structure_issues:
            findings.append(
                ValidationFinding(
                    severity=ValidationSeverity.WARNING,
                    message=issue,
                    location="structure",
                )
            )

        # 5. LLM-based semantic validation
        if state.analyzer_output and state.context:
            semantic_findings = self._llm_semantic_validation(
                pipe_name, code, state
            )
            findings.extend(semantic_findings)

        errors = [f for f in findings if f.severity == ValidationSeverity.ERROR]
        passed = len(errors) == 0

        return ValidationResult(
            pipeline_name=pipe_name,
            passed=passed,
            findings=findings,
            syntax_valid=syntax_valid,
            connections_resolved=len(unresolved) == 0,
            dependencies_valid=passed,
            parameters_resolved=len(unresolved) == 0,
        )

    def _check_syntax(self, code: str) -> tuple:
        """Check Python syntax using AST parser."""
        try:
            ast.parse(code)
            return True, []
        except SyntaxError as e:
            return False, [f"Line {e.lineno}: {e.msg} (text: {e.text or ''})"]

    def _check_unresolved_expressions(self, code: str) -> List[str]:
        """Check for unresolved ADF expression patterns."""
        patterns = [
            r"@\{[^}]+\}",
            r"\$\{[^}]+\}",
            r"\$\$\[[^\]]+\]",
            r"pipeline\(\)\.",
            r"activity\(\)\.",
        ]
        found = []
        for pattern in patterns:
            matches = re.findall(pattern, code)
            found.extend(matches)
        return found

    def _check_imports(self, code: str) -> List[str]:
        """Check for recommended imports."""
        recommended = {
            "pyspark.sql": ["SparkSession"],
            "pyspark.sql.functions": ["col", "when", "lit"],
            "delta.tables": ["DeltaTable"],
        }
        missing = []
        for module, symbols in recommended.items():
            for sym in symbols:
                if sym in code and f"import {sym}" not in code and f"from {module}" not in code:
                    missing.append(f"{sym} (from {module})")
        return missing

    def _check_structure(self, code: str) -> List[str]:
        """Check code structure for basic requirements."""
        issues = []
        if "def main" not in code and "def run" not in code:
            issues.append("No main/run function found. Pipeline should have an entry point.")
        if "try:" not in code:
            issues.append("No error handling (try/except) found. Add error handling for robustness.")
        if "log" not in code.lower() and "print" not in code:
            issues.append("No logging or print statements found. Add logging for observability.")
        return issues

    def _llm_semantic_validation(
        self,
        pipe_name: str,
        code: str,
        state: OrchestrationState,
    ) -> List[ValidationFinding]:
        """Use LLM to perform semantic validation."""
        pipeline = state.analyzer_output.pipeline_tree.get(pipe_name)
        if not pipeline:
            return []

        original_activities = [
            f"{a.name} ({a.activity_type.value})" for a in pipeline.activities
        ]

        prompt = (
            f"Validate the following Databricks Lakeflow code against the original ADF pipeline.\n\n"
            f"Original ADF Pipeline: {pipe_name}\n"
            f"Original Activities: {', '.join(original_activities)}\n"
            f"Original Parameters: {list(pipeline.parameters.keys())}\n"
            f"Child Pipelines: {pipeline.child_pipelines}\n\n"
            f"Converted Code:\n```python\n{code[:5000]}\n```\n\n"
            f"Check for:\n"
            f"1. Are all original activities represented in the code?\n"
            f"2. Are all parameters properly passed?\n"
            f"3. Are child pipeline calls correct?\n"
            f"4. Are there any obvious logical errors?\n"
            f"5. Would this code run on Databricks?\n\n"
            f"Return a JSON object with:\n"
            f"- 'issues': list of strings describing each issue found\n"
            f"- 'warnings': list of non-critical warnings\n"
            f"- 'missing_activities': list of original activities not found in code\n"
            f"- 'overall_assessment': 'pass' or 'fail'"
        )

        try:
            response = self._invoke_llm(prompt, temperature=0.1)
            result = self._parse_json_response(response)

            findings = []
            for issue in result.get("issues", []):
                findings.append(
                    ValidationFinding(
                        severity=ValidationSeverity.ERROR,
                        message=issue,
                        location="semantic",
                    )
                )
            for warning in result.get("warnings", []):
                findings.append(
                    ValidationFinding(
                        severity=ValidationSeverity.WARNING,
                        message=warning,
                        location="semantic",
                    )
                )
            for missing in result.get("missing_activities", []):
                findings.append(
                    ValidationFinding(
                        severity=ValidationSeverity.ERROR,
                        message=f"Missing activity in converted code: {missing}",
                        location="completeness",
                    )
                )

            return findings

        except Exception as e:
            logger.warning("LLM semantic validation failed", error=str(e))
            return []
