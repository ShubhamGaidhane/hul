"""Azure Data Factory scanner utility.

Wraps the Azure SDK to extract pipeline metadata, datasets,
linked services, triggers, and integration runtimes from ADF.
"""

from __future__ import annotations

import time
from collections import defaultdict
from typing import Any, Dict, List, Optional

from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient

from ..utils.logger import get_logger

logger = get_logger(__name__)


class ADFScanner:
    """Scans Azure Data Factory and returns a structured catalog."""

    def __init__(
        self,
        subscription_id: str,
        tenant_id: str,
        client_id: str,
        client_secret: str,
    ):
        """Initialize scanner with service principal credentials.

        Args:
            subscription_id: Azure subscription ID.
            tenant_id: Azure AD tenant ID.
            client_id: Service principal client ID.
            client_secret: Service principal client secret.
        """
        self.credential = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
        )
        self.client = DataFactoryManagementClient(
            self.credential,
            subscription_id,
        )
        self.catalog: Dict[str, List[Dict[str, Any]]] = {
            "pipelines": [],
            "linked_services": [],
            "triggers": [],
            "datasets": [],
            "integration_runtimes": [],
        }
        self.lineage_map: Dict[str, List[tuple]] = defaultdict(list)

    # ------------------------------------------------------------------
    # Pipeline scanning
    # ------------------------------------------------------------------
    def _normalize_dependencies(self, dep_list: Any) -> List[str]:
        """Extract dependency activity names."""
        if not dep_list:
            return []
        return [d.activity for d in dep_list]

    def _normalize_parameters(self, params: Any) -> Dict[str, Any]:
        """Normalize pipeline parameters to a dict."""
        if not params:
            return {}
        result = {}
        for k, v in params.items():
            result[k] = {
                "type": getattr(v, "type", "String"),
                "default": getattr(v, "default_value", None),
            }
        return result

    def _normalize_activity(self, act: Any) -> Dict[str, Any]:
        """Normalize a single activity to a structured dict."""
        raw = act.serialize()
        tp = raw.get("typeProperties", {})

        base = {
            "name": act.name,
            "type": act.type,
            "depends_on": self._normalize_dependencies(
                getattr(act, "depends_on", None)
            ),
        }

        if act.type == "DatabricksNotebook":
            base["config"] = {
                "notebookPath": tp.get("notebookPath"),
                "baseParameters": tp.get("baseParameters"),
            }
        elif act.type == "ExecutePipeline":
            base["config"] = {
                "pipeline": tp.get("pipeline", {}).get("referenceName"),
                "parameters": tp.get("parameters"),
            }
        elif act.type == "SetVariable":
            base["config"] = {
                "variableName": tp.get("variableName"),
                "value": tp.get("value"),
            }
        elif act.type == "AppendVariable":
            base["config"] = {
                "variableName": tp.get("variableName"),
                "value": tp.get("value"),
            }
        elif act.type == "ForEach":
            base["config"] = {
                "items": tp.get("items"),
                "isSequential": tp.get("isSequential", False),
                "activities": tp.get("activities", []),
            }
        elif act.type == "IfCondition":
            base["config"] = {
                "expression": tp.get("expression"),
                "ifTrueActivities": tp.get("ifTrueActivities", []),
                "ifFalseActivities": tp.get("ifFalseActivities", []),
            }
        elif act.type == "Until":
            base["config"] = {
                "expression": tp.get("expression"),
                "activities": tp.get("activities", []),
            }
        elif act.type == "Switch":
            base["config"] = {
                "on": tp.get("on"),
                "cases": tp.get("cases", []),
                "defaultActivities": tp.get("defaultActivities", []),
            }
        elif act.type == "Wait":
            base["config"] = {
                "waitTimeInSeconds": tp.get("waitTimeInSeconds"),
            }
        elif act.type == "Fail":
            base["config"] = {
                "message": tp.get("message"),
                "errorCode": tp.get("errorCode"),
            }
        elif act.type == "Lookup":
            base["config"] = {
                "source": tp.get("source"),
                "dataset": tp.get("dataset"),
                "firstRowOnly": tp.get("firstRowOnly", True),
            }
        elif act.type == "GetMetadata":
            base["config"] = {
                "fieldList": tp.get("fieldList"),
                "dataset": tp.get("dataset"),
            }
        elif act.type == "Delete":
            base["config"] = {
                "dataset": tp.get("dataset"),
                "enableLogging": tp.get("enableLogging", False),
            }
        elif act.type == "Web":
            base["config"] = {
                "url": tp.get("url"),
                "method": tp.get("method"),
                "body": tp.get("body"),
                "headers": tp.get("headers"),
            }
        elif act.type == "Webhook":
            base["config"] = {
                "url": tp.get("url"),
                "method": tp.get("method"),
                "body": tp.get("body"),
                "timeout": tp.get("timeout"),
            }
        elif act.type == "SqlServerStoredProcedure":
            base["config"] = {
                "storedProcedureName": tp.get("storedProcedureName"),
                "storedProcedureParameters": tp.get("storedProcedureParameters"),
            }
        elif act.type == "ExecuteDataFlow":
            base["config"] = {
                "dataflow": tp.get("dataflow"),
                "staging": tp.get("staging"),
                "integrationRuntime": tp.get("integrationRuntime"),
            }
        elif act.type == "Copy":
            base["config"] = {
                "source": tp.get("source"),
                "sink": tp.get("sink"),
                "translator": tp.get("translator"),
                "enableStaging": tp.get("enableStaging", False),
            }
        else:
            base["config"] = tp

        # Inputs / Outputs
        if hasattr(act, "inputs") and act.inputs:
            base["inputs"] = [{"referenceName": i.reference_name} for i in act.inputs]
        if hasattr(act, "outputs") and act.outputs:
            base["outputs"] = [{"referenceName": o.reference_name} for o in act.outputs]

        # Linked service
        if hasattr(act, "linked_service_name") and act.linked_service_name:
            base["linkedServiceName"] = {
                "referenceName": act.linked_service_name.reference_name
            }

        return base

    def collect_pipeline_insights(
        self,
        rg_name: str,
        factory_name: str,
        pipeline_names: Optional[List[str]] = None,
    ) -> None:
        """Collect pipeline metadata from ADF.

        Args:
            rg_name: Azure resource group name.
            factory_name: ADF factory name.
            pipeline_names: Optional list of specific pipeline names.
        """
        results = []

        if pipeline_names:
            pipelines = []
            for name in pipeline_names:
                try:
                    pipe = self.client.pipelines.get(rg_name, factory_name, name)
                    pipelines.append(pipe)
                except Exception as e:
                    logger.error("Failed to fetch pipeline", pipeline=name, error=str(e))
        else:
            pipelines = self.client.pipelines.list_by_factory(rg_name, factory_name)

        for pipe in pipelines:
            activities = pipe.activities or []
            activity_details = []
            dependencies = []

            for act in activities:
                normalized = self._normalize_activity(act)
                activity_details.append(normalized)

                dependencies.append({
                    "activity": act.name,
                    "depends_on": self._normalize_dependencies(
                        getattr(act, "depends_on", None)
                    ),
                })

                # Lineage tracking
                if hasattr(act, "inputs") and act.inputs:
                    for i in act.inputs:
                        self.lineage_map[pipe.name].append(("dataset", i.reference_name))
                if hasattr(act, "outputs") and act.outputs:
                    for o in act.outputs:
                        self.lineage_map[pipe.name].append(("dataset", o.reference_name))
                if act.type == "ExecutePipeline":
                    if hasattr(act, "pipeline") and act.pipeline:
                        self.lineage_map[pipe.name].append(
                            ("pipeline", act.pipeline.reference_name)
                        )

            results.append({
                "asset_type": "pipeline",
                "factory": factory_name,
                "pipeline": pipe.name,
                "activity_count": len(activities),
                "activity_kinds": list({a.type for a in activities}),
                "activities_detail": activity_details,
                "dependencies": dependencies,
                "parameters": self._normalize_parameters(pipe.parameters),
                "variables": list(pipe.variables.keys()) if pipe.variables else [],
                "captured_at": time.time(),
            })

        self.catalog["pipelines"] = results
        logger.info(
            "Pipeline scan complete",
            factory=factory_name,
            count=len(results),
        )

    # ------------------------------------------------------------------
    # Dataset scanning
    # ------------------------------------------------------------------
    def collect_datasets(self, rg_name: str, factory_name: str) -> None:
        """Collect dataset metadata from ADF."""
        datasets = []
        for ds in self.client.datasets.list_by_factory(rg_name, factory_name):
            datasets.append({
                "asset_type": "dataset",
                "name": ds.name,
                "type": ds.properties.type,
                "linked_service": (
                    ds.properties.linked_service_name.reference_name
                    if ds.properties.linked_service_name
                    else None
                ),
                "schema": getattr(ds.properties, "schema", None),
                "parameters": self._normalize_parameters(
                    getattr(ds.properties, "parameters", None)
                ),
                "definition": ds.serialize(),
                "captured_at": time.time(),
            })
        self.catalog["datasets"] = datasets
        logger.info("Dataset scan complete", factory=factory_name, count=len(datasets))

    # ------------------------------------------------------------------
    # Linked service scanning
    # ------------------------------------------------------------------
    def collect_linked_services(self, rg_name: str, factory_name: str) -> None:
        """Collect linked service metadata from ADF."""
        results = []
        for svc in self.client.linked_services.list_by_factory(rg_name, factory_name):
            raw_data = str(svc.serialize())
            results.append({
                "asset_type": "linked_service",
                "name": svc.name,
                "service_kind": svc.properties.type,
                "uses_key_vault": "AzureKeyVault" in raw_data,
                "definition": svc.serialize(),
                "parameters": self._normalize_parameters(
                    getattr(svc.properties, "parameters", None)
                ),
                "captured_at": time.time(),
            })
        self.catalog["linked_services"] = results
        logger.info("Linked service scan complete", factory=factory_name, count=len(results))

    # ------------------------------------------------------------------
    # Trigger scanning
    # ------------------------------------------------------------------
    def collect_triggers(self, rg_name: str, factory_name: str) -> None:
        """Collect trigger metadata from ADF."""
        results = []
        for trig in self.client.triggers.list_by_factory(rg_name, factory_name):
            pipelines = []
            if trig.properties.pipelines:
                pipelines = [
                    p.pipeline_reference.reference_name
                    for p in trig.properties.pipelines
                ]
            results.append({
                "asset_type": "trigger",
                "name": trig.name,
                "trigger_kind": trig.properties.type,
                "status": trig.properties.runtime_state,
                "linked_pipelines": pipelines,
                "schedule": getattr(trig.properties, "type_properties", {}),
                "definition": trig.serialize(),
                "captured_at": time.time(),
            })
        self.catalog["triggers"] = results
        logger.info("Trigger scan complete", factory=factory_name, count=len(results))

    # ------------------------------------------------------------------
    # Integration runtime scanning
    # ------------------------------------------------------------------
    def collect_integration_runtimes(self, rg_name: str, factory_name: str) -> None:
        """Collect integration runtime metadata from ADF."""
        irs = []
        for ir in self.client.integration_runtimes.list_by_factory(rg_name, factory_name):
            detail = self.client.integration_runtimes.get(rg_name, factory_name, ir.name)
            irs.append({
                "asset_type": "integration_runtime",
                "name": ir.name,
                "type": detail.properties.type if detail.properties else "Unknown",
                "definition": detail.serialize(),
                "captured_at": time.time(),
            })
        self.catalog["integration_runtimes"] = irs
        logger.info("IR scan complete", factory=factory_name, count=len(irs))

    # ------------------------------------------------------------------
    # Full scan
    # ------------------------------------------------------------------
    def execute_full_scan(
        self,
        rg_name: str,
        factory_name: str,
        pipeline_names: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Execute a full scan of all ADF assets.

        Args:
            rg_name: Azure resource group name.
            factory_name: ADF factory name.
            pipeline_names: Optional list of specific pipeline names.

        Returns:
            Dict with 'catalog' and 'lineage' keys.
        """
        logger.info("Starting full ADF scan", factory=factory_name)

        self.collect_pipeline_insights(rg_name, factory_name, pipeline_names)
        self.collect_datasets(rg_name, factory_name)
        self.collect_linked_services(rg_name, factory_name)
        self.collect_triggers(rg_name, factory_name)
        self.collect_integration_runtimes(rg_name, factory_name)

        logger.info("Full ADF scan complete", factory=factory_name)

        return {
            "catalog": self.catalog,
            "lineage": dict(self.lineage_map),
        }
