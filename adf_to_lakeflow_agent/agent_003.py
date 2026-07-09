
#cell 1
%pip install azure-identity azure-mgmt-datafactory
%pip install langchain langchain-anthropic
%pip install -U deepagents


%pip install --upgrade langchain langchain-core langgraph
dbutils.library.restartPython()


#Cell 2

from langchain_core.tools import tool
from deepagents import create_deep_agent
from langchain_anthropic import ChatAnthropic

import time
import json
from collections import defaultdict
import re
from datetime import datetime

from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient

# Setup widgets
dbutils.widgets.text("department", "")
dbutils.widgets.text("instance", "bieno-da08-d-80011-adf-hr-01")
dbutils.widgets.text("pipelines", "PL_HR_D_TEAMENERGY_GBL_W2MODULES_MASTER")
dbutils.widgets.text("subscription_id", "8e017cde-1d7c-4842-a4a5-18f6c115cae3")
dbutils.widgets.text("resource_group", "bieno-da08-d-80011-rg")
dbutils.widgets.text("tenant_id", "f66fae02-5d36-495b-bfe0-78a6ff9f8e6e")
dbutils.widgets.text("client_id", "857458c3-549d-49dc-8e3a-993e71c1a011")

department = dbutils.widgets.get("department")
instance = dbutils.widgets.get("instance")
pipelines = dbutils.widgets.get("pipelines")
subscription_id = dbutils.widgets.get("subscription_id")
resource_group = dbutils.widgets.get("resource_group")
tenant_id = dbutils.widgets.get("tenant_id")
client_id = dbutils.widgets.get("client_id")

# Security: retrieve API key from secret scope
try:
    api_key = dbutils.secrets.get("databrickskv01", "anthropic-api-key")
except Exception:
    # Use placeholder or raise error if required secret is missing
    raise ValueError("Anthropic API Key not found. Please configure 'anthropic-api-key' in 'databrickskv01' secret scope.")

llm = ChatAnthropic(
    model="claude-sonnet-4-20250514",
    anthropic_api_key=api_key,
    base_url="https://anthropic.generative-eu.engine.capgemini.com",
)

def normalize_dependencies(dep_list):
    if not dep_list: return []
    return [d.activity for d in dep_list]

def normalize_parameters(params):
    if not params: return {}
    return {k: {"type": v.type, "default": getattr(v, "default_value", None)} for k, v in params.items()}

def normalize_activity(act):
    try:
        raw = act.serialize()
        tp = raw.get("typeProperties", {})
    except:
        raw = tp = {}

    base = {
        "name": getattr(act, "name", "unknown"),
        "type": getattr(act, "type", "unknown"),
        "depends_on": normalize_dependencies(getattr(act, "depends_on", None))
    }
    if hasattr(act, "inputs") and act.inputs: base["inputs"] = [i.reference_name for i in act.inputs]
    if hasattr(act, "outputs") and act.outputs: base["outputs"] = [o.reference_name for o in act.outputs]

    if base["type"] == "DatabricksNotebook":
        base["config"] = {"notebook_path": tp.get("notebookPath"), "parameters": tp.get("baseParameters")}
    elif base["type"] == "ExecutePipeline":
        base["config"] = {"pipeline": tp.get("pipeline", {}).get("referenceName"), "parameters": tp.get("parameters")}
    elif base["type"] == "SetVariable":
        base["config"] = {"variable": tp.get("variableName"), "value": tp.get("value")}
    elif base["type"] == "IfCondition":
        base["if_true"] = [normalize_activity(a) for a in (getattr(act, "if_true_activities", []) or [])]
        base["if_false"] = [normalize_activity(a) for a in (getattr(act, "if_false_activities", []) or [])]
    elif base["type"] in ["ForEach", "Until"]:
        base["activities"] = [normalize_activity(a) for a in (getattr(act, "activities", []) or [])]
    elif base["type"] == "Switch":
        base["cases"] = [{"value": c.value, "activities": [normalize_activity(a) for a in (c.activities or [])]} for c in (getattr(act, "cases", []) or [])]
        base["default_activities"] = [normalize_activity(a) for a in (getattr(act, "default_activities", []) or [])]

    if "config" not in base: base["config"] = tp
    return base

class UnifiedADFScanner:
    def __init__(self):
        self.factory_name = dbutils.widgets.get("instance")
        self.rg_name = dbutils.widgets.get("resource_group")
        self.subscription_id = dbutils.widgets.get("subscription_id")
        self.tenant_id = dbutils.widgets.get("tenant_id")
        self.client_id = dbutils.widgets.get("client_id")
        try:
            client_secret = dbutils.secrets.get("databrickskv01", "svc-b-da-d-80011-ina-aadprincipal")
            self.credential = ClientSecretCredential(tenant_id=self.tenant_id, client_id=self.client_id, client_secret=client_secret)
            self.client = DataFactoryManagementClient(self.credential, self.subscription_id)
        except Exception as e:
            print(f"Error initializing ADF Scanner: {e}")
            raise

    def _find_execute_pipeline_names(self, activities):
        names = []
        for act in (activities or []):
            if act.type == "ExecutePipeline" and hasattr(act, "pipeline") and act.pipeline:
                names.append(act.pipeline.reference_name)
            if act.type == "IfCondition":
                names.extend(self._find_execute_pipeline_names(act.if_true_activities))
                names.extend(self._find_execute_pipeline_names(act.if_false_activities))
            elif act.type in ["ForEach", "Until"]:
                names.extend(self._find_execute_pipeline_names(act.activities))
            elif act.type == "Switch":
                for case in (act.cases or []): names.extend(self._find_execute_pipeline_names(case.activities))
                names.extend(self._find_execute_pipeline_names(act.default_activities))
        return names

    def get_pipeline_hierarchy(self, root_pipeline_names):
        if isinstance(root_pipeline_names, str):
            root_pipeline_names = [p.strip() for p in root_pipeline_names.split(",")]
        to_process = list(root_pipeline_names)
        seen = set()
        hierarchy = []
        while to_process:
            current = to_process.pop(0)
            if current in seen: continue
            seen.add(current)
            hierarchy.append(current)
            try:
                pipe = self.client.pipelines.get(self.rg_name, self.factory_name, current)
                child_names = self._find_execute_pipeline_names(pipe.activities)
                for cn in child_names:
                    if cn not in seen: to_process.append(cn)
            except Exception as e: print(f"⚠️ Failed to fetch pipeline {current}: {e}")
        return hierarchy

    def get_pipeline_details(self, pipeline_names):
        results = []
        for name in pipeline_names:
            try:
                pipe = self.client.pipelines.get(self.rg_name, self.factory_name, name)
                results.append({
                    "asset_type": "pipeline",
                    "pipeline": pipe.name,
                    "activities": [normalize_activity(act) for act in (pipe.activities or [])],
                    "parameters": normalize_parameters(pipe.parameters),
                    "variables": list(pipe.variables.keys()) if pipe.variables else [],
                })
            except Exception as e: results.append({"error": f"Failed to fetch pipeline {name}: {str(e)}"})
        return results

    def get_dataset_details(self, dataset_names):
        results = []
        for name in dataset_names:
            try:
                ds = self.client.datasets.get(self.rg_name, self.factory_name, name)
                results.append({
                    "asset_type": "dataset",
                    "name": ds.name,
                    "type": ds.properties.type,
                    "linked_service": (ds.properties.linked_service_name.reference_name if ds.properties.linked_service_name else None),
                    "properties": ds.properties.serialize()
                })
            except Exception as e: results.append({"error": f"Failed to fetch dataset {name}: {str(e)}"})
        return results

    def get_linked_service_details(self, ls_names):
        results = []
        for name in ls_names:
            try:
                svc = self.client.linked_services.get(self.rg_name, self.factory_name, name)
                results.append({
                    "asset_type": "linked_service",
                    "name": svc.name,
                    "type": svc.properties.type,
                    "definition": svc.serialize()
                })
            except Exception as e: results.append({"error": f"Failed to fetch linked service {name}: {str(e)}"})
        return results

    def get_trigger_details(self, pipeline_names):
        results = []
        try:
            all_triggers = self.client.triggers.list_by_factory(self.rg_name, self.factory_name)
            for trig in all_triggers:
                trig_pipelines = [p.pipeline_reference.reference_name for p in (trig.properties.pipelines or [])]
                if any(p in pipeline_names for p in trig_pipelines):
                    results.append({
                        "asset_type": "trigger",
                        "name": trig.name,
                        "type": trig.properties.type,
                        "status": trig.properties.runtime_state,
                        "linked_pipelines": trig_pipelines,
                        "definition": trig.serialize()
                    })
        except Exception as e: results.append({"error": f"Failed to list triggers: {str(e)}"})
        return results

def get_pipeline_metadata_internal(pipeline_names):
    scanner = UnifiedADFScanner()
    print(f"Fetching metadata for: {pipeline_names}")
    results = {
        "pipelines": scanner.get_pipeline_details(pipeline_names),
        "triggers": scanner.get_trigger_details(pipeline_names)
    }

    def collect_ds_names(activities):
        ds = set()
        for act in activities:
            if "inputs" in act: ds.update(act["inputs"])
            if "outputs" in act: ds.update(act["outputs"])
            if "if_true" in act: ds.update(collect_ds_names(act["if_true"]))
            if "if_false" in act: ds.update(collect_ds_names(act["if_false"]))
            if "activities" in act: ds.update(collect_ds_names(act["activities"]))
            if "cases" in act:
                for case in act["cases"]: ds.update(collect_ds_names(case["activities"]))
            if "default_activities" in act: ds.update(collect_ds_names(act["default_activities"]))
        return ds

    all_ds_names = set()
    for p in results["pipelines"]:
        if "activities" in p: all_ds_names.update(collect_ds_names(p["activities"]))

    results["datasets"] = scanner.get_dataset_details(list(all_ds_names))
    ls_names = set()
    for ds in results["datasets"]:
        if "linked_service" in ds and ds["linked_service"]: ls_names.add(ds["linked_service"])
    results["linked_services"] = scanner.get_linked_service_details(list(ls_names))
    return results

@tool
def get_pipeline_hierarchy_and_metadata(pipeline_names: str):
    """
    REQUIRED TOOL. Extracts all child pipelines for the given pipeline(s)
    and then fetches complete metadata (activities, datasets, linked services, triggers)
    for the entire hierarchy by passing them to the metadata extraction logic.
    """
    scanner = UnifiedADFScanner()
    print(f"Discovering hierarchy for: {pipeline_names}")
    hierarchy = scanner.get_pipeline_hierarchy(pipeline_names)
    print(f"Full hierarchy: {hierarchy}")
    return get_pipeline_metadata_internal(hierarchy)

@tool
def get_pipeline_metadata(pipeline_names: list):
    """
    Fetches full metadata for a SPECIFIC list of pipeline names.
    Use this if you already have the list of pipelines.
    """
    return get_pipeline_metadata_internal(pipeline_names)

SYSTEM_PROMPT = """
You are a highly skilled ADF-to-Databricks Migration Engineer.
Your task is to convert ADF pipelines into Databricks Lakeflow code using Auto Loader.

CRITICAL INSTRUCTIONS:
1. Always start by calling `get_pipeline_hierarchy_and_metadata` for the user-provided pipelines.
2. Use the returned metadata to generate the Lakeflow code.
3. Return only a Python dictionary with 'lineage' and 'converted_code'.
"""

agent = create_deep_agent(
    model=llm,
    system_prompt=SYSTEM_PROMPT,
    tools=[get_pipeline_hierarchy_and_metadata, get_pipeline_metadata]
)

prompt = f"Convert these ADF pipelines to Databricks Lakeflow: {pipelines}"
print("Invoking agent...")
response = agent.invoke({"messages": [{"role": "user", "content": prompt}]})
result_content = response["messages"][-1].content
print(f"Agent Output: {result_content}")

try:
    cleaned_json = re.search(r'\{.*\}', result_content, re.DOTALL).group()
    result = json.loads(cleaned_json)
except:
    result = {"lineage": {}, "converted_code": result_content}

# Save to ADLS
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
main_pipe = pipelines.split(",")[0].strip()
file_path = f"abfss://unilever@dbstorageda08d80011adls.dfs.core.windows.net/UniversalDataLake/InternalSources/TopTier/BlobFileShare/TH/Auto_loader_test/source/generated_script_{main_pipe}_{timestamp}.py"
try:
    dbutils.fs.put(file_path, result.get("converted_code", ""), overwrite=True)
    print(f"File saved: {file_path}")
except:
    print("ADLS Save failed")

dbutils.notebook.exit(json.dumps(result))
