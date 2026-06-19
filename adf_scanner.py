import time
import json
import os
from collections import defaultdict
from datetime import datetime

from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient

def normalize_dependencies(dep_list):
    if not dep_list:
        return []
    # SDK might return objects with .activity property
    return [getattr(d, 'activity', d) for d in dep_list]


def normalize_parameters(params):
    if not params:
        return {}
    return {
        k: {
            "type": getattr(v, 'type', 'String'),
            "default": getattr(v, "default_value", None)
        }
        for k, v in params.items()
    }

def json_serializable(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj

def normalize_activity(act):
    # act is an SDK object
    raw = act.serialize()
    tp = raw.get("typeProperties", {})

    base = {
        "name": act.name,
        "type": act.type,
        "depends_on": normalize_dependencies(
            getattr(act, "depends_on", None)
        ),
        "linkedServiceName": raw.get("linkedServiceName"),
        "inputs": raw.get("inputs"),
        "outputs": raw.get("outputs")
    }

    if act.type == "DatabricksNotebook":
        base["config"] = {
            "notebook_path": tp.get("notebookPath"),
            "parameters": tp.get("baseParameters")
        }
    elif act.type == "ExecutePipeline":
        base["config"] = {
            "pipeline": tp.get("pipeline", {}).get("referenceName"),
            "parameters": tp.get("parameters")
        }
    elif act.type == "SetVariable":
        base["config"] = {
            "variable": tp.get("variableName"),
            "value": tp.get("value")
        }
    elif act.type == "IfCondition":
        # Recursively normalize nested activities
        base["config"] = {
            "expression": tp.get("expression"),
            "ifTrueActivities": [normalize_activity(a) for a in (getattr(act, "if_true_activities", []) or [])],
            "ifFalseActivities": [normalize_activity(a) for a in (getattr(act, "if_false_activities", []) or [])]
        }
    elif act.type in ["ForEach", "Until"]:
        base["config"] = {
            "activities": [normalize_activity(a) for a in (getattr(act, "activities", []) or [])]
        }
    else:
        base["config"] = tp

    return base


class UnifiedADFScanner:

    def __init__(self, subscription_id, tenant_id, client_id, client_secret):
        self.credential = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret
        )
        self.client = DataFactoryManagementClient(
            self.credential,
            subscription_id
        )
        self.catalog = {
            "pipelines": [],
            "linked_services": [],
            "triggers": [],
            "datasets": [],
            "integration_runtimes": []
        }
        self.lineage_map = defaultdict(list)

    def collect_pipeline_insights(self, rg_name, factory_name, pipeline_names=None):
        results = []
        if pipeline_names:
            pipelines = []
            for name in pipeline_names:
                try:
                    pipe = self.client.pipelines.get(rg_name, factory_name, name)
                    pipelines.append(pipe)
                except Exception as e:
                    print(f"⚠️ Failed to fetch pipeline {name}: {e}")
        else:
            pipelines = self.client.pipelines.list_by_factory(rg_name, factory_name)

        for pipe in pipelines:
            activities = pipe.activities or []
            activity_details = []
            dependencies = []

            for act in activities:
                norm_act = normalize_activity(act)
                activity_details.append(norm_act)

                dependencies.append({
                    "activity": act.name,
                    "depends_on": norm_act["depends_on"]
                })

                # Lineage
                if hasattr(act, "inputs") and act.inputs:
                    for i in act.inputs:
                        self.lineage_map[pipe.name].append(("dataset", i.reference_name))

                if hasattr(act, "outputs") and act.outputs:
                    for o in act.outputs:
                        self.lineage_map[pipe.name].append(("dataset", o.reference_name))

                if act.type == "ExecutePipeline":
                    if hasattr(act, "pipeline") and act.pipeline:
                        ref = act.pipeline.reference_name
                        self.lineage_map[pipe.name].append(("pipeline", ref))

            results.append({
                "asset_type": "pipeline",
                "factory": factory_name,
                "pipeline": pipe.name,
                "activity_count": len(activities),
                "activity_kinds": list({a.type for a in activities}),
                "activities_detail": activity_details,
                "dependencies": dependencies,
                "parameters": normalize_parameters(pipe.parameters),
                "variables": list(pipe.variables.keys()) if pipe.variables else [],
                "definition": pipe.as_dict(),
                "captured_at": time.time()
            })
        self.catalog["pipelines"] = results

    def collect_datasets(self, rg_name, factory_name):
        datasets = []
        for ds in self.client.datasets.list_by_factory(rg_name, factory_name):
            ds_dict = ds.as_dict()
            props = ds_dict.get("properties", {})
            tp = props.get("typeProperties", {})

            # Extract delimiter for DelimitedText datasets
            delimiter = tp.get("columnDelimiter") or tp.get("column_delimiter")

            datasets.append({
                "asset_type": "dataset",
                "name": ds.name,
                "type": props.get("type"),
                "linked_service": (
                    props.get("linkedServiceName", {}).get("referenceName")
                ),
                "schema": props.get("schema"),
                "delimiter": delimiter,
                "definition": ds_dict,
                "captured_at": time.time()
            })
        self.catalog["datasets"] = datasets

    def collect_linked_service_info(self, rg_name, factory_name):
        results = []
        for svc in self.client.linked_services.list_by_factory(rg_name, factory_name):
            raw = svc.serialize()
            results.append({
                "asset_type": "linked_service",
                "name": svc.name,
                "service_kind": raw.get("properties", {}).get("type"),
                "uses_key_vault": "AzureKeyVault" in str(raw),
                "definition": raw,
                "captured_at": time.time()
            })
        self.catalog["linked_services"] = results

    def collect_trigger_info(self, rg_name, factory_name):
        results = []
        for trig in self.client.triggers.list_by_factory(rg_name, factory_name):
            raw = trig.serialize()
            props = raw.get("properties", {})
            tp = props.get("typeProperties", {})

            pipelines = [p.get("pipelineReference", {}).get("referenceName") for p in (props.get("pipelines") or [])]

            trigger_time = None
            if props.get("type") == "ScheduleTrigger":
                recurrence = tp.get("recurrence", {})
                trigger_time = recurrence.get("startTime")

            results.append({
                "asset_type": "trigger",
                "name": trig.name,
                "trigger_kind": props.get("type"),
                "status": props.get("runtimeState"),
                "linked_pipelines": pipelines,
                "schedule": tp,
                "trigger_time": trigger_time,
                "definition": raw,
                "captured_at": time.time()
            })
        self.catalog["triggers"] = results

    def collect_integration_runtimes(self, rg_name, factory_name):
        irs = []
        for ir in self.client.integration_runtimes.list_by_factory(rg_name, factory_name):
            detail = self.client.integration_runtimes.get(rg_name, factory_name, ir.name)
            raw = detail.serialize()
            irs.append({
                "asset_type": "integration_runtime",
                "name": ir.name,
                "type": raw.get("properties", {}).get("type"),
                "definition": raw,
                "captured_at": time.time()
            })
        self.catalog["integration_runtimes"] = irs

    def execute_full_scan(self, rg_name, factory_name, pipeline_names=None):
        print(f"Starting ADF discovery for: {factory_name}")
        self.collect_pipeline_insights(rg_name, factory_name, pipeline_names)
        self.collect_datasets(rg_name, factory_name)
        self.collect_linked_service_info(rg_name, factory_name)
        self.collect_trigger_info(rg_name, factory_name)
        self.collect_integration_runtimes(rg_name, factory_name)
        return {
            "catalog": self.catalog,
            "lineage": dict(self.lineage_map)
        }

def main():
    tenant_id = "f66fae02-5d36-495b-bfe0-78a6ff9f8e6e"
    client_id = "370843d2-ca40-464f-86e9-005367602203"

    # ✅ Fetch from Databricks secret scope
    try:
        import IPython
        dbutils = IPython.get_ipython().user_ns.get("dbutils")
        client_secret = dbutils.secrets.get(
            "databrickskv01",
            "svc-b-da-q-901994-ina-aadprincipal"
        )
    except:
        client_secret = os.environ.get("ADF_CLIENT_SECRET", "dummy")

    subscription_id = "105cc892-0276-4b01-b5ff-426df8be49e2"
    rg_name = "bieno-da21-q-901994-rg"
    factory_name = "bieno-da21-q-901994-adf-01"

    scanner = UnifiedADFScanner(subscription_id, tenant_id, client_id, client_secret)
    result = scanner.execute_full_scan(rg_name, factory_name)

    output_path = "/Volumes/bdl_processed_rnd_qa/staging/pipelinenotebookmonitoring/adf_full_scan_output.json"

    # Ensure directory exists if not in Databricks Volume
    if not output_path.startswith("/Volumes"):
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(result, f, indent=2, default=json_serializable)

    print(f"✅ File saved at: {output_path}")

if __name__ == "__main__":
    main()
