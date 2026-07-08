"""Trigger Converter Agent - Converts ADF triggers to Databricks Lakeflow scheduling.

This is the SEVENTH and FINAL agent. It converts ADF trigger definitions
into Databricks Workflow scheduling configurations and Auto Loader subscriptions.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from ..models.state import (
    ConversionStatus,
    OrchestrationState,
    TriggerNode,
    TriggerType,
)
from ..utils.logger import get_logger
from .base_agent import BaseAgent

logger = get_logger(__name__)


class TriggerConverterAgent(BaseAgent):
    """Agent responsible for converting ADF triggers to Databricks scheduling.

    This is the LAST agent. It handles:
    - ScheduleTrigger -> Databricks Workflows scheduled job with cron
    - BlobEventsTrigger -> Auto Loader / cloudFiles subscription
    - CustomEventsTrigger -> Delta Live Tables event handling
    - TumblingWindowTrigger -> Databricks Workflows with offset scheduling
    """

    def _default_system_prompt(self) -> str:
        return (
            "You are the Trigger Converter Agent — the SEVENTH and FINAL agent in the ADF-to-Databricks Lakeflow migration pipeline. "
            "Your role is to convert ADF trigger definitions into Databricks scheduling configurations. "
            "Triggers determine WHEN pipelines run — getting this wrong means pipelines run at wrong times or not at all.\n\n"

            "=== YOUR RESPONSIBILITIES ===\n"
            "For EVERY trigger in the ADF catalog, produce a Databricks-compatible scheduling configuration.\n\n"

            "=== TRIGGER TYPE CONVERSION RULES ===\n\n"

            "1. ScheduleTrigger -> Databricks Workflow Schedule:\n"
            "   ADF ScheduleTrigger has a recurrence pattern:\n"
            "   {\n"
            '     "frequency": "Minute" | "Hour" | "Day" | "Week" | "Month",\n'
            '     "interval": 1-1000,\n'
            '     "startTime": "2024-01-01T00:00:00Z",\n'
            '     "endTime": "2025-01-01T00:00:00Z",\n'
            '     "timeZone": "India Standard Time",\n'
            '     "schedule": {\n'
            '       "hours": [0, 12],\n'
            '       "minutes": [0, 30],\n'
            '       "weekDays": ["Monday", "Wednesday", "Friday"]\n'
            "     }\n"
            "   }\n"
            "   Convert to Databricks Workflow cron expression:\n"
            "   - Frequency + Interval + Schedule -> cron expression\n"
            "   - Examples:\n"
            "     * Every day at 6 AM: 0 6 * * *\n"
            "     * Every hour: 0 * * * *\n"
            "     * Every Monday at 9 AM: 0 9 * * 1\n"
            "     * Every 15 minutes: */15 * * * *\n"
            "     * First day of month at midnight: 0 0 1 * *\n"
            "   - Include timezone conversion (ADF timezone -> UTC for cron)\n"
            "   - Include start/end time as Databricks workflow parameters\n\n"

            "2. BlobEventsTrigger -> Auto Loader / cloudFiles:\n"
            "   ADF BlobEventsTrigger fires when files are created/deleted in blob storage:\n"
            "   {\n"
            '     "scope": "/subscriptions/.../resourceGroups/.../providers/.../storageAccounts/...",\n'
            '     "events": ["Microsoft.Storage.BlobCreated"],\n'
            '     "blobPathBeginsWith": "/container/path",\n'
            '     "blobPathEndsWith": ".csv"\n'
            "   }\n"
            "   Convert to Auto Loader directory listing:\n"
            "   - blobPathBeginsWith -> directory_path for Auto Loader\n"
            "   - blobPathEndsWith -> file_filter for cloudFiles.format\n"
            "   - events -> trigger type (file arrival = new files detected)\n"
            "   - Generate Auto Loader code:\n"
            "     df = spark.readStream.format('cloudFiles')\n"
            "       .option('cloudFiles.format', 'csv')\n"
            "       .option('cloudFiles.schemaLocation', '/path/to/schema')\n"
            "       .option('cloudFiles.includeExistingFiles', 'true')\n"
            "       .load('abfss://container@storage.dfs.core.windows.net/path')\n\n"

            "3. CustomEventsTrigger -> Delta Live Tables Event:\n"
            "   ADF CustomEventsTrigger fires on custom Azure events:\n"
            "   Convert to DLT event handling or Databricks Workflow webhook trigger.\n"
            "   - Custom events can be received via Databricks webhook API\n"
            "   - Generate a webhook endpoint configuration\n\n"

            "4. TumblingWindowTrigger -> Databricks Workflow Offset Schedule:\n"
            "   ADF TumblingWindowTrigger runs at fixed intervals with offset:\n"
            "   {\n"
            '     "frequency": "Hour",\n'
            '     "interval": 1,\n'
            '     "startTime": "...",\n'
            '     "delay": "00:05:00",\n'
            '     "maxConcurrency": 1\n'
            "   }\n"
            "   Convert to Databricks Workflow with:\n"
            "   - Cron schedule matching the frequency/interval\n"
            "   - Offset/delay added to the cron timing\n"
            "   - maxConcurrency -> Databricks workflow concurrency setting\n\n"

            "=== OUTPUT FORMAT ===\n"
            "For EACH trigger, produce a JSON object:\n"
            "{\n"
            '  "trigger_name": "original ADF trigger name",\n'
            '  "trigger_type": "schedule" | "file_arrival" | "continuous" | "webhook",\n'
            '  "linked_pipelines": ["pipeline1", "pipeline2"],\n'
            '  "cron_expression": "0 6 * * *" (for schedule triggers),\n'
            '  "timezone": "UTC" | "Asia/Kolkata" etc.,\n'
            '  "databricks_config": {\n'
            '    "workflow_name": "wf_trigger_name",\n'
            '    "schedule": {\n'
            '      "quartz_cron_expression": "...",\n'
            '      "timezone_id": "...",\n'
            '      "pause_status": "PAUSED" | "UNPAUSED"\n'
            '    },\n'
            '    "max_concurrency": 1,\n'
            '    "timeout_seconds": 3600\n'
            '  },\n'
            '  "connected_pipeline_code": "Auto Loader code snippet if applicable",\n'
            '  "notes": "Any special considerations or warnings"\n'
            "}\n\n"

            "=== EDGE CASES ===\n"
            "1. Trigger with status 'Stopped': Set pause_status to 'PAUSED'.\n"
            "2. Trigger with no linked pipelines: Log warning, still create config.\n"
            "3. Unknown trigger type: Create a basic config with a TODO note.\n"
            "4. Complex recurrence (e.g., 'every 2nd Monday of month'): Use the most specific cron possible.\n"
            "5. Multiple pipelines linked to one trigger: Include ALL in linked_pipelines list.\n\n"

            "=== IMPORTANT ===\n"
            "- The cron expression MUST be valid for Databricks Workflows.\n"
            "- Databricks uses Quartz cron format: seconds minutes hours day-of-month month day-of-week year\n"
            "- Standard format: 0 0 6 * * ? (every day at 6 AM)\n"
            "- Include the '?' for day-of-week when day-of-month is specified, and vice versa.\n"
            "- NEVER use * for both day-of-month and day-of-week (invalid in Quartz).\n"
            "- Convert ADF timezone to IANA timezone format (e.g., 'India Standard Time' -> 'Asia/Kolkata')."
        )

    def run(self, state: OrchestrationState) -> Dict[str, Any]:
        """Convert all triggers from the context.

        Args:
            state: Current orchestration state with context.

        Returns:
            Dict with trigger_conversion_output and updated current_step.
        """
        if not state.context or not state.context.trigger_map:
            self.logger.info("No triggers to convert")
            return {"current_step": "triggers_complete", "trigger_output": {}}

        self.logger.info(
            "Starting trigger conversion",
            triggers_to_convert=len(state.context.trigger_map),
        )

        trigger_output = {}

        for trig_name, trig_node in state.context.trigger_map.items():
            self.logger.info(
                "Converting trigger",
                trigger=trig_name,
                type=trig_node.trigger_type.value,
            )

            conversion = self._convert_trigger(trig_name, trig_node, state)
            trigger_output[trig_name] = conversion

        return {
            "current_step": "triggers_complete",
            "trigger_output": trigger_output,
        }

    def _convert_trigger(
        self,
        trig_name: str,
        trig_node: TriggerNode,
        state: OrchestrationState,
    ) -> Dict[str, Any]:
        """Convert a single trigger.

        Args:
            trig_name: Trigger name.
            trig_node: Trigger node with details.
            state: Orchestration state.

        Returns:
            Dict with 'schedule_config' (workflow JSON) and 'notes'.
        """
        prompt = f"Convert the following ADF trigger to Databricks Workflow scheduling:\n\n"
        prompt += f"Trigger Name: {trig_name}\n"
        prompt += f"Trigger Type: {trig_node.trigger_type.value}\n"
        prompt += f"Status: {trig_node.status}\n"
        prompt += f"Linked Pipelines: {trig_node.linked_pipelines}\n"

        if trig_node.trigger_type == TriggerType.SCHEDULE:
            prompt += f"Schedule: {json.dumps(trig_node.schedule, indent=2, default=str)[:2000]}\n"
            prompt += "\nConvert to Databricks Workflow JSON schedule with cron expression."
        elif trig_node.trigger_type in (TriggerType.EVENT, TriggerType.CUSTOM_EVENT):
            prompt += f"Event Info: {json.dumps(trig_node.event_info, indent=2, default=str)[:2000]}\n"
            prompt += "\nConvert to Auto Loader directory listing / cloudFiles subscription config."
        elif trig_node.trigger_type == TriggerType.TUMBLING_WINDOW:
            prompt += f"Definition: {json.dumps(trig_node.definition, indent=2, default=str)[:2000]}\n"
            prompt += "\nConvert to Databricks Workflow with offset scheduling."

        prompt += (
            "\n\nReturn a JSON object:\n"
            "{\n"
            '  "workflow_name": "...",\n'
            '  "trigger_type": "schedule" | "file_arrival" | "continuous",\n'
            '  "cron_expression": "..." (for schedule triggers),\n'
            '  "databricks_config": { ... },\n'
            '  "connected_pipeline_code": "...",  # any trigger-initiated code\n'
            '  "notes": "..."\n'
            "}"
        )

        try:
            response = self._invoke_llm(prompt, temperature=0.1)
            result = self._parse_json_response(response)

            # Add the pipeline names that this trigger connects to
            result["linked_pipelines"] = trig_node.linked_pipelines

            return result

        except Exception as e:
            logger.error("Trigger conversion failed", trigger=trig_name, error=str(e))
            return {
                "trigger_name": trig_name,
                "error": str(e),
                "linked_pipelines": trig_node.linked_pipelines,
            }
