"""Context Builder Agent - Resolves ALL parameters, connections, datasets, and triggers.

This agent builds the COMPLETE context map that the Converter Agent needs
to generate correct Lakeflow code. It resolves:
- Parameter chains (pipeline → dataset → linked_service → global)
- Connection strings and linked service properties
- Dataset definitions with linked service references
- Global parameters from factory/IR configuration
- Trigger definitions (schedule, event, tumbling window)
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from ..models.state import (
    ConnectionNode,
    ContextOutput,
    DatasetNode,
    OrchestrationState,
    ParameterNode,
    TriggerNode,
    TriggerType,
)
from ..utils.logger import get_logger
from .base_agent import BaseAgent

logger = get_logger(__name__)


class ContextBuilderAgent(BaseAgent):
    """Agent responsible for building the COMPLETE context map.

    This is the THIRD agent in the pipeline. It takes the analyzed
    pipeline tree and produces a rich context with ALL resolved
    parameters, connections, datasets, and triggers.
    """

    def _default_system_prompt(self) -> str:
        return (
            "You are the Context Builder Agent — the THIRD agent in the ADF-to-Databricks Lakeflow migration pipeline. "
            "Your role is to BUILD THE COMPLETE CONTEXT MAP that the Converter Agent needs. "
            "Without your context, the Converter would generate incomplete or broken code.\n\n"

            "=== YOUR RESPONSIBILITIES ===\n"
            "1. CONNECTION MAP (from linked services):\n"
            "   For EVERY linked service in the catalog, extract:\n"
            "   - name: The linked service name (e.g., 'LS_ADLS_Unilever')\n"
            "   - service_kind: The type (e.g., 'AzureBlobStorage', 'AzureSqlDatabase', \n"
            "     'AzureDatabricks', 'AzureDataLakeStorage', 'AzureKeyVault', \n"
            "     'SqlServer', 'Oracle', 'SapTable', 'RestService', 'Http', etc.)\n"
            "   - connection_string: The FULL connection string (from typeProperties.connectionString).\n"
            "     If it's a nested object with 'value' key, extract the value.\n"
            "   - properties: ALL typeProperties (accountName, database, url, etc.)\n"
            "     NOT just connectionString — include everything.\n"
            "   - uses_key_vault: Check if ANY property references AzureKeyVault.\n"
            "   - key_vault_secrets: List ALL secret names referenced from Key Vault.\n"
            "   - parameters: Linked service parameters (if parameterized).\n\n"

            "2. DATASET MAP:\n"
            "   For EVERY dataset in the catalog, extract:\n"
            "   - name: The dataset name\n"
            "   - dataset_type: Type (e.g., 'AzureBlob', 'DelimitedText', 'Parquet', \n"
            "     'Avro', 'Json', 'Binary', 'AzureSqlTable', 'FileShare', etc.)\n"
            "   - linked_service: The linked service name this dataset points to\n"
            "   - parameters: Dataset-level parameters\n"
            "   - schema_def: Schema definition (columns, structure) if available\n"
            "   - raw_json: The FULL definition for reference\n\n"

            "3. PARAMETER MAP (CRITICAL for parameter resolution):\n"
            "   Build a centralized map of ALL parameters from ALL sources:\n"
            "   - Pipeline parameters: name, default values, types\n"
            "   - Dataset parameters: name, default values, types\n"
            "   - Linked service parameters: name, values, types\n"
            "   - Global parameters: factory-wide parameters\n"
            "   \n"
            "   This is how the Converter will resolve expressions like:\n"
            "   @{pipeline().parameters.SourcePath} → find 'SourcePath' in parameter_map\n"
            "   @{dataset().parameter.FilePattern} → find 'FilePattern' in parameter_map\n\n"

            "4. GLOBAL PARAMETERS:\n"
            "   Check integration runtimes and factory-level configuration for:\n"
            "   - globalParameters\n"
            "   - Factory-level configuration values\n"
            "   These are parameters available to ALL pipelines.\n\n"

            "5. TRIGGER MAP:\n"
            "   For EVERY trigger, extract:\n"
            "   - name: Trigger name\n"
            "   - trigger_type: ScheduleTrigger, BlobEventsTrigger, CustomEventsTrigger, \n"
            "     TumblingWindowTrigger, or Unknown\n"
            "   - linked_pipelines: Which pipelines this trigger starts\n"
            "   - status: 'Started', 'Stopped', or other runtime state\n"
            "   - definition: The FULL trigger JSON definition\n"
            "   - schedule: Recurrence pattern for ScheduleTrigger (frequency, interval, \n"
            "     startTime, endTime, timeZone, schedule)\n"
            "   - event_info: For BlobEventsTrigger — scope, events, blobPathBeginsWith, \n"
            "     blobPathEndsWith\n\n"

            "=== EDGE CASES TO HANDLE ===\n"
            "1. Connection strings stored in Azure Key Vault: Don't try to resolve KV secrets.\n"
            "   Just note which secrets are referenced so the Converter can handle them.\n"
            "2. Datasets without linked_service: Log a warning, create DatasetNode with null linked_service.\n"
            "3. Circular parameter references: Detect and flag them.\n"
            "4. Triggers linked to non-existent pipelines: Log warning, keep the trigger definition.\n"
            "5. Empty linked service properties: Create ConnectionNode with empty properties.\n"
            "6. Global parameters found in multiple places: Deduplicate by name.\n\n"

            "=== QUALITY CHECKS ===\n"
            "After building the context, review for:\n"
            "1. Are ALL linked services captured? (Check against datasets that reference them)\n"
            "2. Are ALL datasets captured? (Check against pipeline activities that reference them)\n"
            "3. Are ALL triggers captured?\n"
            "4. Are there parameter references in activities that can't be resolved?\n"
            "5. Flag any potential issues for the Converter Agent.\n\n"

            "=== OUTPUT ===\n"
            "You produce a ContextOutput containing:\n"
            "  - parameter_map: Dict[str, List[ParameterNode]] — all parameters by name\n"
            "  - connection_map: Dict[str, ConnectionNode] — all linked services\n"
            "  - dataset_map: Dict[str, DatasetNode] — all datasets\n"
            "  - global_parameters: Dict[str, Any] — factory-level parameters\n"
            "  - trigger_map: Dict[str, TriggerNode] — all triggers\n\n"

            "=== DATA INTEGRITY ===\n"
            "- PRESERVE ALL connection details. The Converter needs EVERY property.\n"
            "- Do NOT redact, truncate, or summarize connection strings.\n"
            "- If a linked service type is unknown, preserve ALL its properties.\n"
            "- Every dataset, linked service, and trigger in the catalog must be in the output."
        )

    def run(self, state: OrchestrationState) -> Dict[str, Any]:
        """Build the complete context map from analyzed data.

        Args:
            state: Current orchestration state with analyzer_output and raw_adf_json.

        Returns:
            Dict with context and updated current_step.
        """
        if not state.raw_adf_json or not state.analyzer_output:
            return {
                "errors": state.errors + ["Missing raw ADF JSON or analyzer output for context building."],
                "current_step": "error",
            }

        self.logger.info("Starting context building")

        catalog = state.raw_adf_json.get("catalog", {})

        # Step 1: Build connection map from linked services
        connection_map = self._build_connection_map(catalog.get("linked_services", []))

        # Step 2: Build dataset map
        dataset_map = self._build_dataset_map(catalog.get("datasets", []), connection_map)

        # Step 3: Extract global parameters
        global_params = self._extract_global_parameters(state.raw_adf_json)

        # Step 4: Build parameter map - resolve all parameter references
        parameter_map = self._build_parameter_map(
            state.analyzer_output.pipeline_tree,
            dataset_map,
            connection_map,
            global_params,
        )

        # Step 5: Extract trigger definitions
        trigger_map = self._build_trigger_map(catalog.get("triggers", []))

        # Step 6: Use LLM to add any missing context or flag issues
        validation_prompt = (
            f"Review the following context summary for completeness:\n\n"
            f"- Connected services: {len(connection_map)}\n"
            f"- Datasets: {len(dataset_map)}\n"
            f"- Global parameters: {len(global_params)}\n"
            f"- Parameter entries: {len(parameter_map)}\n"
            f"- Triggers: {len(trigger_map)}\n\n"
            f"List any potential issues or missing context that might cause "
            f"the Converter Agent to produce incorrect Lakeflow code. "
            f"Focus on unresolvable parameter references, missing linked services, "
            f"or incomplete connection strings."
        )
        issues = self._invoke_llm(validation_prompt, temperature=0.1)
        if issues:
            logger.warning("Context issues flagged by LLM", issues=issues)

        context = ContextOutput(
            parameter_map=parameter_map,
            connection_map=connection_map,
            dataset_map=dataset_map,
            global_parameters=global_params,
            trigger_map=trigger_map,
        )

        self.logger.info(
            "Context building complete",
            connections=len(connection_map),
            datasets=len(dataset_map),
            parameters=sum(len(v) for v in parameter_map.values()),
            triggers=len(trigger_map),
        )

        return {
            "context": context,
            "current_step": "context_built",
        }

    def _build_connection_map(
        self,
        linked_services_raw: List[Dict[str, Any]],
    ) -> Dict[str, ConnectionNode]:
        """Build connection map from linked service definitions."""
        connection_map = {}

        for ls in linked_services_raw:
            name = ls.get("name", "unknown")
            definition = ls.get("definition", {})
            properties = definition.get("properties", {})
            type_properties = properties.get("typeProperties", {})
            service_kind = ls.get("service_kind", properties.get("type", "Unknown"))

            # Extract connection string if present
            connection_string = type_properties.get("connectionString")
            if isinstance(connection_string, dict):
                connection_string = connection_string.get("value", str(connection_string))

            # Check for Key Vault usage
            uses_kv = ls.get("uses_key_vault", False)
            kv_secrets = []
            if uses_kv:
                for key, val in type_properties.items():
                    if isinstance(val, dict) and "azureKeyVault" in str(val).lower():
                        secret_name = val.get("secretName") or val.get("referenceName", key)
                        kv_secrets.append(str(secret_name))

            # Extract parameters
            params = ls.get("parameters", {})

            connection_node = ConnectionNode(
                name=name,
                service_kind=service_kind,
                connection_string=connection_string,
                properties=type_properties,
                uses_key_vault=uses_kv,
                key_vault_secrets=kv_secrets,
                parameters=params,
            )
            connection_map[name] = connection_node

        return connection_map

    def _build_dataset_map(
        self,
        datasets_raw: List[Dict[str, Any]],
        connection_map: Dict[str, ConnectionNode],
    ) -> Dict[str, DatasetNode]:
        """Build dataset map from raw datasets."""
        dataset_map = {}

        for ds in datasets_raw:
            name = ds.get("name", "unknown")
            ds_type = ds.get("type", "Unknown")
            linked_service = ds.get("linked_service", None)
            params = ds.get("parameters", {})
            schema = ds.get("schema", None)

            dataset_node = DatasetNode(
                name=name,
                dataset_type=ds_type,
                linked_service=linked_service,
                parameters=params,
                schema_def=schema,
                raw_json=ds.get("definition", {}),
            )
            dataset_map[name] = dataset_node

        return dataset_map

    def _extract_global_parameters(self, raw_adf_json: Dict[str, Any]) -> Dict[str, Any]:
        """Extract global parameters from ADF JSON."""
        catalog = raw_adf_json.get("catalog", {})
        global_params = {}

        for ir in catalog.get("integration_runtimes", []):
            definition = ir.get("definition", {})
            properties = definition.get("properties", {})
            gp = properties.get("globalParameters", {})
            if gp:
                for key, val in gp.items():
                    if isinstance(val, dict):
                        global_params[key] = val.get("value", str(val))
                    else:
                        global_params[key] = str(val)

        return global_params

    def _build_parameter_map(
        self,
        pipeline_tree: Dict[str, Any],
        dataset_map: Dict[str, DatasetNode],
        connection_map: Dict[str, ConnectionNode],
        global_params: Dict[str, Any],
    ) -> Dict[str, List[ParameterNode]]:
        """Build complete parameter map resolving all parameter chains."""
        parameter_map: Dict[str, List[ParameterNode]] = {}

        # Extract pipeline-level parameters
        for pipe_name, pipe_node in pipeline_tree.items():
            for param_name, param_value in pipe_node.parameters.items():
                if param_name not in parameter_map:
                    parameter_map[param_name] = []
                parameter_map[param_name].append(
                    ParameterNode(
                        name=param_name,
                        value=param_value,
                        source="pipeline",
                        source_name=pipe_name,
                        data_type=param_value.get("type") if isinstance(param_value, dict) else None,
                    )
                )

        # Extract dataset-level parameters
        for ds_name, ds_node in dataset_map.items():
            for param_name, param_value in ds_node.parameters.items():
                if param_name not in parameter_map:
                    parameter_map[param_name] = []
                parameter_map[param_name].append(
                    ParameterNode(
                        name=param_name,
                        value=param_value,
                        source="dataset",
                        source_name=ds_name,
                        data_type=param_value.get("type") if isinstance(param_value, dict) else None,
                    )
                )

        # Extract linked service parameters
        for ls_name, conn_node in connection_map.items():
            for param_name, param_value in conn_node.parameters.items():
                if param_name not in parameter_map:
                    parameter_map[param_name] = []
                parameter_map[param_name].append(
                    ParameterNode(
                        name=param_name,
                        value=param_value,
                        source="linked_service",
                        source_name=ls_name,
                        data_type=param_value.get("type") if isinstance(param_value, dict) else None,
                    )
                )

        # Extract global parameters
        for param_name, param_value in global_params.items():
            if param_name not in parameter_map:
                parameter_map[param_name] = []
            parameter_map[param_name].append(
                ParameterNode(
                    name=param_name,
                    value=param_value,
                    source="global",
                    data_type=type(param_value).__name__,
                )
            )

        return parameter_map

    def _build_trigger_map(
        self,
        triggers_raw: List[Dict[str, Any]],
    ) -> Dict[str, TriggerNode]:
        """Build trigger map from raw trigger data."""
        trigger_map = {}

        for trig in triggers_raw:
            name = trig.get("name", "unknown")
            kind = trig.get("trigger_kind", trig.get("type", "Unknown"))
            status = trig.get("status", "")
            linked_pipelines = trig.get("linked_pipelines", [])
            definition = trig.get("definition", {})
            schedule = trig.get("schedule", {})
            raw_def = trig.get("definition", {})

            # Map trigger kind to TriggerType enum
            try:
                trigger_type = TriggerType(kind)
            except ValueError:
                trigger_type = TriggerType.UNKNOWN

            # Extract event info for blob/custom event triggers
            event_info = None
            if trigger_type in (TriggerType.EVENT, TriggerType.CUSTOM_EVENT):
                props = raw_def.get("properties", {})
                type_props = props.get("typeProperties", {})
                event_info = {
                    "scope": type_props.get("scope"),
                    "events": type_props.get("events", []),
                    "blobPathBeginsWith": type_props.get("blobPathBeginsWith"),
                    "blobPathEndsWith": type_props.get("blobPathEndsWith"),
                }

            trigger_node = TriggerNode(
                name=name,
                trigger_type=trigger_type,
                linked_pipelines=linked_pipelines,
                status=status,
                definition=raw_def,
                schedule=schedule,
                event_info=event_info,
            )
            trigger_map[name] = trigger_node

        return trigger_map
