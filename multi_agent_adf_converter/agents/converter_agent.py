"""Converter Agent - Converts ADF pipeline activities to Databricks Lakeflow code.

This is the CORE conversion engine — the FOURTH agent. It takes a single pipeline
with its resolved context and generates COMPLETE, RUNNABLE Lakeflow Python code.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from ..models.state import (
    ActivityNode,
    ActivityType,
    ConversionResult,
    ConversionStatus,
    ConverterInput,
    OrchestrationState,
    PipelineNode,
)
from ..utils.logger import get_logger
from .base_agent import BaseAgent

logger = get_logger(__name__)


class ConverterAgent(BaseAgent):
    """Agent responsible for converting ADF pipelines to Lakeflow code.

    This is THE most important agent. It generates the actual Python code
    that will run on Databricks. Every detail must be correct.
    """

    def _default_system_prompt(self) -> str:
        return (
            "You are the Converter Agent — the FOURTH and MOST CRITICAL agent in the ADF-to-Databricks Lakeflow migration pipeline. "
            "Your role is to convert ADF pipeline definitions into COMPLETE, PRODUCTION-READY Databricks Lakeflow Python code. "
            "The code YOU generate will be deployed to run on Databricks. Every detail must be perfect.\n\n"

            "=== YOUR RESPONSIBILITIES ===\n"
            "Generate a complete Python module for Databricks Lakeflow that:\n"
            "1. Preserves ALL pipeline parameters as function arguments\n"
            "2. Preserves ALL activity dependencies and execution order\n"
            "3. Contains ALL connection strings and configurations from linked services\n"
            "4. Includes proper error handling (try/except blocks for EVERY activity)\n"
            "5. Includes proper logging at each step\n"
            "6. Writes output to Delta tables\n"
            "7. Documents the pipeline purpose, parameters, and activities\n"
            "8. Handles parent-child pipeline coordination\n"
            "9. INCLUDES EVERY SINGLE ACTIVITY — nothing skipped\n\n"

            "=== DETAILED CONVERSION RULES BY ACTIVITY TYPE ===\n\n"

            "1. Copy Activity:\n"
            "   - File-based sources (Blob, ADLS, S3, FileShare): Use Auto Loader\n"
            "     spark.readStream.format('cloudFiles')\n"
            "     .option('cloudFiles.format', 'csv'/'parquet'/'json'/'avro')\n"
            "     .option('cloudFiles.schemaLocation', '/path/to/schema')\n"
            "     .option('header', 'true'/'false')\n"
            "     .option('delimiter', ','/'\\t'/'|')\n"
            "     .load(source_path)\n"
            "   - Database sources (SQL, Oracle, SAP): Use JDBC or spark.sql\n"
            "     spark.read.format('jdbc')\n"
            "     .option('url', connection_string)\n"
            "     .option('query', 'SELECT * FROM table')\n"
            "     .load()\n"
            "   - Sink: Always write to Delta Lake\n"
            "     df.write.format('delta')\n"
            "     .mode('append'/'overwrite')\n"
            "     .option('mergeSchema', 'true')\n"
            "     .save(target_path)\n"
            "   - Translator (column mappings): Apply BEFORE writing\n"
            "     df.select(col('source_col').alias('target_col'), ...)\n\n"

            "2. ExecutePipeline Activity (child pipeline call):\n"
            "   - Use dbutils.notebook.run() for cross-notebook execution\n"
            "   - OR call a Python function if the child is defined in the same module\n"
            "   - Pass parameters as a dict\n"
            "   - Capture the exit value for status checking\n"
            "   Example:\n"
            "     result = dbutils.notebook.run(\n"
            "       '/path/to/child_notebook',\n"
            "       timeout_seconds=3600,\n"
            "       arguments={'param1': value1, 'param2': value2}\n"
            "     )\n\n"

            "3. DatabricksNotebook Activity:\n"
            "   - Already a notebook reference — convert to dbutils.notebook.run()\n"
            "   - Extract notebookPath and baseParameters from config\n"
            "   Example:\n"
            "     dbutils.notebook.run(\n"
            "       '/Users/me/notebook_name',\n"
            "       timeout_seconds=3600,\n"
            "       arguments=params\n"
            "     )\n\n"

            "4. ForEach Activity:\n"
            "   - Convert to a Python for loop\n"
            "   - If isSequential=False, consider using ThreadPoolExecutor for parallel execution\n"
            "   - The 'items' expression becomes the iterable\n"
            "   Example:\n"
            "     items_list = [...]  # resolved from items expression\n"
            "     for item in items_list:\n"
            "         try:\n"
            "             # nested activities go here\n"
            "             pass\n"
            "         except Exception as e:\n"
            "             logger.error(f'Error processing item {item}: {e}')\n"
            "             raise\n\n"

            "5. IfCondition Activity:\n"
            "   - Convert to Python if/else statement\n"
            "   - The 'expression' from ADF becomes the if condition\n"
            "   - ifTrueActivities go inside the 'if' block\n"
            "   - ifFalseActivities go inside the 'else' block\n"
            "   Example:\n"
            "     if eval(expression):  # or resolve the expression\n"
            "         # ifTrue activities\n"
            "         pass\n"
            "     else:\n"
            "         # ifFalse activities\n"
            "         pass\n\n"

            "6. Until Activity:\n"
            "   - Convert to while loop with the expression as the condition\n"
            "   - Include a timeout/break to prevent infinite loops\n"
            "   Example:\n"
            "     max_iterations = 100\n"
            "     iteration = 0\n"
            "     while not (expression) and iteration < max_iterations:\n"
            "         iteration += 1\n"
            "         # nested activities\n"
            "         pass\n\n"

            "7. SetVariable / AppendVariable:\n"
            "   - Convert to Python variable assignment\n"
            "   - SetVariable: variable_name = value\n"
            "   - AppendVariable: variable_list.append(value)\n\n"

            "8. Switch Activity:\n"
            "   - Convert to Python match/case or if/elif/else\n"
            "   - Each 'case' becomes a 'case' or 'elif' branch\n"
            "   - defaultActivities go in the 'else' branch\n"
            "   Example:\n"
            "     match switch_value:\n"
            "         case 'value1':\n"
            "             # case activities\n"
            "         case 'value2':\n"
            "             # case activities\n"
            "         case _:\n"
            "             # default activities\n\n"

            "9. Wait Activity:\n"
            "   - Convert to time.sleep(waitTimeInSeconds)\n"
            "   - Add logging before/after\n\n"

            "10. Fail Activity:\n"
            "    - Convert to raising an exception with the message\n"
            "    - raise Exception(f'{message} (error code: {error_code})')\n\n"

            "11. Lookup Activity:\n"
            "    - Query a source and return results\n"
            "    - Use spark.sql() for SQL sources, dbutils.fs.ls() for file listing\n"
            "    - Store results in a variable for downstream activities\n\n"

            "12. GetMetadata Activity:\n"
            "    - Use dbutils.fs.ls() to list files/folders\n"
            "    - Use dbutils.fs.info() for file metadata\n"
            "    - Return results in a structured format\n\n"

            "13. Delete Activity:\n"
            "    - Use dbutils.fs.rm() for file deletion\n"
            "    - Add logging before/after deletion\n\n"

            "14. Web Activity:\n"
            "    - Use the requests library\n"
            "    - Handle GET, POST, PUT, DELETE, PATCH, etc.\n"
            "    - Include proper headers and body\n"
            "    - Handle response and errors\n\n"

            "15. Webhook Activity:\n"
            "    - Similar to Web but with callback handling\n"
            "    - Use requests with timeout\n"
            "    - Implement callback URL listening if needed\n\n"

            "16. SqlServerStoredProcedure:\n"
            "    - Use spark.sql() or JDBC to call the stored procedure\n"
            "    - spark.read.format('jdbc').option('query', 'EXEC dbo.Proc @param=value').load()\n\n"

            "17. ExecuteDataFlow:\n"
            "    - Convert to Delta Live Tables (DLT) pipeline\n"
            "    - Or use explicit Spark transformations\n"
            "    - Include all dataflow transformations sequentially\n\n"

            "=== OUTPUT CODE STRUCTURE ===\n"
            "The generated Python code MUST follow this structure:\n\n"
            "#!/usr/bin/env python3\n"
            "\"\"\"\n"
            "Pipeline: {PIPELINE_NAME}\n"
            "Source Factory: {FACTORY_NAME}\n"
            "Converted from ADF by Multi-Agent Converter\n"
            "Parent Chain: {' -> '.join(PARENT_CHAIN)}\n"
            "\"\"\"\n\n"
            "# =============================================================================\n"
            "# IMPORTS\n"
            "# =============================================================================\n"
            "import logging\n"
            "import time\n"
            "from typing import Any, Dict, List, Optional\n\n"
            "from pyspark.sql import SparkSession, DataFrame\n"
            "from pyspark.sql.functions import col, when, lit, input_file_name\n"
            "from delta.tables import DeltaTable\n\n"
            "logger = logging.getLogger(__name__)\n\n"
            "# =============================================================================\n"
            "# CONFIGURATION - Connection strings from linked services\n"
            "# =============================================================================\n"
            "# {CONNECTION_1_NAME}\n"
            "CONNECTION_1 = \"{connection_string}\"\n\n"
            "# =============================================================================\n"
            "# MAIN PIPELINE FUNCTION\n"
            "# =============================================================================\n"
            "def main_pipeline(\n"
            "    param1: str = \"default_value\",\n"
            "    param2: int = 0,\n"
            "):\n"
            "    \"\"\"\n"
            "    Main pipeline function.\n"
            "    \n"
            "    Args:\n"
            "        param1: Description of param1\n"
            "        param2: Description of param2\n"
            "    \"\"\"\n"
            "    logger.info(f\"Starting pipeline with params: param1={param1}, param2={param2}\")\n"
            "    \n"
            "    spark = SparkSession.builder.getOrCreate()\n"
            "    \n"
            "    try:\n"
            "        # Activity 1: Copy data from source\n"
            "        logger.info(\"Starting activity: Act1_CopyData\")\n"
            "        df_source = spark.read.format('delta').load(source_path)\n"
            "        df_source.createOrReplaceTempView(\"source_data\")\n"
            "        \n"
            "        # Activity 2: Transform data\n"
            "        logger.info(\"Starting activity: Act2_Transform\")\n"
            "        df_transformed = spark.sql(\"\"\"\n"
            "            SELECT *, current_timestamp() as processed_at\n"
            "            FROM source_data\n"
            "        \"\"\")\n"
            "        \n"
            "        # Activity 3: Write to Delta\n"
            "        logger.info(\"Starting activity: Act3_WriteToDelta\")\n"
            "        df_transformed.write \\\n"
            "            .format('delta') \\\n"
            "            .mode('append') \\\n"
            "            .save(f\"/mnt/datalake/processed/{param1}\")\n"
            "        \n"
            "        logger.info(\"Pipeline completed successfully\")\n"
            "        \n"
            "    except Exception as e:\n"
            "        logger.error(f\"Pipeline failed: {e}\")\n"
            "        raise\n\n"
            "# =============================================================================\n"
            "# ENTRY POINT\n"
            "# =============================================================================\n"
            "if __name__ == \"__main__\":\n"
            "    logging.basicConfig(level=logging.INFO)\n"
            "    main_pipeline()\n\n"

            "=== QUALITY REQUIREMENTS ===\n"
            "1. EVERY activity from the ADF pipeline must appear in the code.\n"
            "2. Activity DEPENDENCIES must be preserved — if Activity B depends on A, B must execute AFTER A.\n"
            "3. ALL pipeline parameters must be function arguments with proper type hints and defaults.\n"
            "4. ALL connection strings must be included in the configuration section.\n"
            "5. ALL linked service properties must be captured.\n"
            "6. Use try/except around every major operation.\n"
            "7. Include logging before and after each activity.\n"
            "8. Code must be syntactically valid Python (check for missing colons, brackets, etc.).\n"
            "9. Code must run on Databricks (no local-only imports).\n"
            "10. Use f-strings for string formatting.\n"
            "11. Include docstrings for ALL functions.\n"
            "12. Write to Delta tables (not CSV/Parquet directly).\n\n"

            "=== CRITICAL: WHAT NOT TO DO ===\n"
            "- DO NOT skip any activity, no matter how small.\n"
            "- DO NOT use @{...} or ${{...}} expressions in the output code — resolve them first.\n"
            "- DO NOT expose credentials in log messages.\n"
            "- DO NOT assume any default paths or names — use the exact values from the context.\n"
            "- DO NOT generate placeholder code with 'TODO' — convert EVERYTHING.\n"
            "- DO NOT include markdown or explanations in the output — ONLY Python code.\n"

            "=== IF YOU CANNOT CONVERT AN ACTIVITY ===\n"
            "1. Add a detailed comment explaining what the activity does and why it couldn't be converted.\n"
            "2. Include the original ADF JSON as a comment.\n"
            "3. The Validator Agent will flag this for review.\n\n"

            "=== OUTPUT FORMAT ===\n"
            "Return ONLY valid Python code. No markdown, no code fences, no explanations. "
            "The first line must be '#!/usr/bin/env python3'. The last line must be valid Python."
        )

    def run(self, state: OrchestrationState) -> Dict[str, Any]:
        """Run the converter for the current pipeline in the state.

        Args:
            state: Current orchestration state.

        Returns:
            Dict with updated conversion_results and current_step.
        """
        if not state.analyzer_output or not state.context:
            return {
                "errors": state.errors + ["Missing analyzer output or context for conversion."],
                "current_step": "error",
            }

        pipeline_tree = state.analyzer_output.pipeline_tree
        context = state.context

        # Determine which pipeline to process next
        # Process children first (bottom-up) so parent can reference them
        pipelines_to_process = self._get_pipeline_order(pipeline_tree)

        # Process any pipeline already set as current, or the first unprocessed one
        if state.current_pipeline:
            pipelines_to_process = [p for p in pipelines_to_process if p == state.current_pipeline] or pipelines_to_process

        for pipe_name in pipelines_to_process:
            if pipe_name in state.conversion_results:
                result = state.conversion_results[pipe_name]
                if result.status in (ConversionStatus.SUCCESS, ConversionStatus.VALIDATED):
                    logger.info("Skipping already converted pipeline", pipeline=pipe_name)
                    continue
                if result.retry_count >= state.max_retries:
                    logger.warning("Pipeline exceeded max retries", pipeline=pipe_name)
                    continue

            pipeline = pipeline_tree.get(pipe_name)
            if not pipeline:
                continue

            logger.info("Converting pipeline", pipeline=pipe_name, total_activities=len(pipeline.activities))

            # Prepare converter input
            parent_chain = self._get_parent_chain(pipe_name, pipeline_tree)
            retry_feedback = None
            if pipe_name in state.validation_results:
                val_result = state.validation_results[pipe_name]
                if not val_result.passed:
                    retry_feedback = self._format_validation_feedback(val_result)

            converter_input = ConverterInput(
                pipeline=pipeline,
                context=context,
                all_pipelines=pipeline_tree,
                parent_chain=parent_chain,
                retry_feedback=retry_feedback,
            )

            # Initialize conversion result
            conversion_result = state.conversion_results.get(
                pipe_name,
                ConversionResult(pipeline_name=pipe_name),
            )
            conversion_result.status = ConversionStatus.CONVERTING

            # Invoke LLM for conversion
            try:
                lakeflow_code = self._convert_pipeline(converter_input)
                conversion_result.lakeflow_code = lakeflow_code
                conversion_result.status = ConversionStatus.SUCCESS
            except Exception as e:
                logger.error("Conversion failed", pipeline=pipe_name, error=str(e))
                conversion_result.status = ConversionStatus.FAILED
                conversion_result.error_detail = str(e)
                conversion_result.retry_count += 1

            state.conversion_results[pipe_name] = conversion_result

        # Determine next step
        all_done = all(
            r.status in (ConversionStatus.SUCCESS, ConversionStatus.VALIDATED)
            for r in state.conversion_results.values()
        )
        any_failed = any(
            r.status == ConversionStatus.FAILED for r in state.conversion_results.values()
        )

        return {
            "conversion_results": state.conversion_results,
            "current_step": "conversion_complete" if all_done else ("conversion_partial" if not any_failed else "conversion_with_errors"),
        }

    def _convert_pipeline(self, converter_input: ConverterInput) -> str:
        """Convert a single pipeline to Lakeflow code using LLM.

        Args:
            converter_input: Structured input with pipeline and context.

        Returns:
            Generated Lakeflow Python code as a string.
        """
        pipeline = converter_input.pipeline
        context = converter_input.context

        # Build the prompt with full context
        prompt = self._build_conversion_prompt(converter_input)
        code = self._invoke_llm(prompt, temperature=0.1)
        # Clean up markdown fences if any
        code = self._clean_code(code)
        return code

    def _build_conversion_prompt(self, converter_input: ConverterInput) -> str:
        """Build a comprehensive prompt for the LLM.

        Args:
            converter_input: Structured input.

        Returns:
            Prompt string.
        """
        pipeline = converter_input.pipeline
        context = converter_input.context

        # Pipeline activities details
        activities_json = []
        for act in pipeline.activities:
            act_dict = {
                "name": act.name,
                "type": act.activity_type.value,
                "depends_on": act.depends_on,
                "inputs": act.inputs,
                "outputs": act.outputs,
                "linked_service": act.linked_service,
                "config": act.resolved_params,
                "nested_activities": [
                    {
                        "name": n.name,
                        "type": n.activity_type.value,
                        "depends_on": n.depends_on,
                        "config": n.resolved_params,
                    }
                    for n in act.nested_activities
                ],
            }
            activities_json.append(act_dict)

        # Connection details
        connections_json = {}
        for ls_name, conn in context.connection_map.items():
            connections_json[ls_name] = {
                "service_kind": conn.service_kind,
                "properties": conn.properties,
                "uses_key_vault": conn.uses_key_vault,
                "parameters": conn.parameters,
            }

        # Dataset details
        datasets_json = {}
        for ds_name, ds in context.dataset_map.items():
            datasets_json[ds_name] = {
                "type": ds.dataset_type,
                "linked_service": ds.linked_service,
                "parameters": ds.parameters,
                "schema": str(ds.schema_def)[:500],
            }

        # Parameter details
        params_json = {}
        for param_name, param_nodes in context.parameter_map.items():
            params_json[param_name] = [
                {"source": p.source, "source_name": p.source_name, "value": str(p.value)[:200]}
                for p in param_nodes
            ]

        prompt_parts = [
            f"# Convert the following ADF pipeline to Databricks Lakeflow Python code\n",
            f"## Pipeline: {pipeline.name}\n",
            f"## Parent Chain: {' -> '.join(converter_input.parent_chain) if converter_input.parent_chain else 'None'}\n",
            f"## Factory: {pipeline.factory or 'Unknown'}\n",
            f"\n## Pipeline Parameters (passed as function arguments):\n",
            json.dumps(pipeline.parameters, indent=2, default=str)[:2000],
            f"\n## Pipeline Variables:\n{pipeline.variables}",
            f"\n## Child Pipelines (will be created as separate functions):\n{pipeline.child_pipelines}",
            f"\n## Activities ({len(activities_json)} total):\n",
            json.dumps(activities_json, indent=2, default=str)[:6000],
            f"\n## Linked Services / Connections:\n",
            json.dumps(connections_json, indent=2, default=str)[:3000],
            f"\n## Datasets:\n",
            json.dumps(datasets_json, indent=2, default=str)[:2000],
            f"\n## Parameters Resolution Map:\n",
            json.dumps(params_json, indent=2, default=str)[:2000],
        ]

        if converter_input.retry_feedback:
            prompt_parts.append(f"\n## PREVIOUS VALIDATION FEEDBACK (fix these issues):\n{converter_input.retry_feedback}")

        prompt_parts.append(
            "\n\n## OUTPUT REQUIREMENTS:\n"
            "Generate a complete Python module for Databricks Lakeflow with:\n"
            "1. A main function with the pipeline parameters as arguments\n"
            "2. Helper functions for each activity or logical group\n"
            "3. Proper error handling (try/except for each activity)\n"
            "4. Logging at each step\n"
            "5. All connection strings configured\n"
            "6. Auto Loader usage for file-based sources\n"
            "7. Delta Lake read/write patterns\n"
            "8. Parent-child pipeline coordination via dbutils.notebook.run() or function calls\n"
            "9. Executable code that runs without errors on Databricks\n"
            "\nReturn ONLY the Python code. No markdown, no extra text."
        )

        return "\n".join(prompt_parts)

    def _clean_code(self, code: str) -> str:
        """Clean up LLM output to extract clean Python code.

        Args:
            code: Raw LLM output.

        Returns:
            Clean Python code.
        """
        code = code.strip()
        # Remove markdown code fences
        if code.startswith("```"):
            # Remove first line (```python, ```py, or just ```)
            lines = code.split("\n")
            # Remove the first line (the fence)
            lines = lines[1:]
            # Remove the last line if it's a closing fence
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            code = "\n".join(lines)
        # Remove trailing fences
        if code.endswith("```"):
            code = code[:-3].strip()
        return code.strip()

    def _get_pipeline_order(self, pipeline_tree: Dict[str, PipelineNode]) -> List[str]:
        """Determine the order to process pipelines (children first).

        Args:
            pipeline_tree: All pipelines.

        Returns:
            List of pipeline names in processing order.
        """
        # Build dependency graph
        all_pipelines = list(pipeline_tree.keys())
        processed = set()
        order = []

        def visit(name: str):
            if name in processed:
                return
            processed.add(name)
            pipe = pipeline_tree.get(name)
            if pipe:
                for child in pipe.child_pipelines:
                    if child in pipeline_tree:
                        visit(child)
            order.append(name)

        for name in all_pipelines:
            visit(name)

        return order

    def _get_parent_chain(
        self,
        pipe_name: str,
        pipeline_tree: Dict[str, PipelineNode],
    ) -> List[str]:
        """Get the chain of parent pipeline names.

        Args:
            pipe_name: Current pipeline name.
            pipeline_tree: All pipelines.

        Returns:
            List of parent names from root to immediate parent.
        """
        chain = []
        current = pipeline_tree.get(pipe_name)
        visited = set()
        while current and current.parent_pipeline and current.parent_pipeline not in visited:
            chain.insert(0, current.parent_pipeline)
            visited.add(current.parent_pipeline)
            current = pipeline_tree.get(current.parent_pipeline)
        return chain

    def _format_validation_feedback(self, validation_result) -> str:
        """Format validation results as feedback for the converter.

        Args:
            validation_result: ValidationResult object.

        Returns:
            Formatted feedback string.
        """
        feedback = []
        if not validation_result.syntax_valid:
            feedback.append("- Syntax errors found. Fix Python syntax.")
        if not validation_result.connections_resolved:
            feedback.append("- Connection resolution issues. Ensure all connection strings are properly configured.")
        if not validation_result.dependencies_valid:
            feedback.append("- Dependency ordering issues. Ensure activities are in correct order.")
        if not validation_result.parameters_resolved:
            feedback.append("- Parameter resolution issues. Ensure all @{...} expressions are resolved.")
        for finding in validation_result.findings:
            feedback.append(f"- [{finding.severity}] {finding.message}")
        return "\n".join(feedback)
