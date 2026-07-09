
#cell 1
%pip install azure-identity azure-mgmt-datafactory
%pip install langchain langchain-openai
%pip install -U deepagents


%pip install --upgrade langchain langchain-core langgraph
dbutils.library.restartPython()


#Cell 2

from langchain_core.tools import tool
from deepagents import create_deep_agent
from langchain_openai import ChatOpenAI

import time
import json
from collections import defaultdict
import re
from datetime import datetime
 
from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient

# Setup widgets for environment configuration
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

print(f"Department: {department}")
print(f"Instance: {instance}")
print(f"Pipelines: {pipelines}")

# Security best practice: retrieve sensitive keys from secret scope
try:
    api_key = dbutils.secrets.get("databrickskv01", "llm-api-key")
except Exception:
    # Fallback to the provided key if secret not found, though not recommended for production
    api_key = "GaVLoff71m3OtgMhl6oCe9mJiDZ24nxH5nOVQrQJ"

llm = ChatOpenAI(
    model="openai.gpt-5-mini",
    base_url="https://openai.generative.engine.capgemini.com/v1",
    api_key=api_key,
    default_headers={"x-api-key": api_key},
    temperature=0.1,
)

def normalize_dependencies(dep_list):
    if not dep_list:
        return []
    return [d.activity for d in dep_list]

def normalize_parameters(params):
    if not params:
        return {}
    return {
        k: {
            "type": v.type,
            "default": getattr(v, "default_value", None)
        }
        for k, v in params.items()
    }

def normalize_activity(act):
    try:
        raw = act.serialize()
        tp = raw.get("typeProperties", {})
    except:
        raw = {}
        tp = {}

    base = {
        "name": getattr(act, "name", "unknown"),
        "type": getattr(act, "type", "unknown"),
        "depends_on": normalize_dependencies(getattr(act, "depends_on", None))
    }

    # Extract inputs/outputs if present
    if hasattr(act, "inputs") and act.inputs:
        base["inputs"] = [i.reference_name for i in act.inputs]
    if hasattr(act, "outputs") and act.outputs:
        base["outputs"] = [o.reference_name for o in act.outputs]

    if base["type"] == "DatabricksNotebook":
        base["config"] = {
            "notebook_path": tp.get("notebookPath"),
            "parameters": tp.get("baseParameters")
        }
    elif base["type"] == "ExecutePipeline":
        base["config"] = {
            "pipeline": tp.get("pipeline", {}).get("referenceName"),
            "parameters": tp.get("parameters")
        }
    elif base["type"] == "SetVariable":
        base["config"] = {
            "variable": tp.get("variableName"),
            "value": tp.get("value")
        }
    elif base["type"] == "IfCondition":
        base["if_true"] = [normalize_activity(a) for a in (getattr(act, "if_true_activities", []) or [])]
        base["if_false"] = [normalize_activity(a) for a in (getattr(act, "if_false_activities", []) or [])]
        base["config"] = tp
    elif base["type"] == "ForEach":
        base["activities"] = [normalize_activity(a) for a in (getattr(act, "activities", []) or [])]
        base["config"] = tp
    elif base["type"] == "Until":
        base["activities"] = [normalize_activity(a) for a in (getattr(act, "activities", []) or [])]
        base["config"] = tp
    elif base["type"] == "Switch":
        base["cases"] = [
            {"value": c.value, "activities": [normalize_activity(a) for a in (c.activities or [])]}
            for c in (getattr(act, "cases", []) or [])
        ]
        base["default_activities"] = [normalize_activity(a) for a in (getattr(act, "default_activities", []) or [])]
        base["config"] = tp
    else:
        base["config"] = tp

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
            if act.type == "ExecutePipeline":
                if hasattr(act, "pipeline") and act.pipeline:
                    names.append(act.pipeline.reference_name)
            if act.type == "IfCondition":
                names.extend(self._find_execute_pipeline_names(act.if_true_activities))
                names.extend(self._find_execute_pipeline_names(act.if_false_activities))
            elif act.type in ["ForEach", "Until"]:
                names.extend(self._find_execute_pipeline_names(act.activities))
            elif act.type == "Switch":
                for case in (act.cases or []):
                    names.extend(self._find_execute_pipeline_names(case.activities))
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
            except Exception as e:
                print(f"⚠️ Failed to fetch pipeline {current}: {e}")
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
            except Exception as e:
                results.append({"error": f"Failed to fetch pipeline {name}: {str(e)}"})
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
            except Exception as e:
                results.append({"error": f"Failed to fetch dataset {name}: {str(e)}"})
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
            except Exception as e:
                results.append({"error": f"Failed to fetch linked service {name}: {str(e)}"})
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
        except Exception as e:
            results.append({"error": f"Failed to list triggers: {str(e)}"})
        return results

@tool
def discover_child_pipelines(pipeline_names: str):
    """
    REQUIRED FIRST STEP. Identifies the full hierarchy of ADF pipelines.
    Accepts a single pipeline name or a comma-separated string of names.
    Returns a list of all pipelines found in the hierarchy.
    """
    print(f"Tool discover_child_pipelines called with: {pipeline_names}")
    try:
        scanner = UnifiedADFScanner()
        res = scanner.get_pipeline_hierarchy(pipeline_names)
        print(f"Discovered: {res}")
        return res
    except Exception as e:
        return f"Error: {str(e)}"

@tool
def get_pipeline_details(pipeline_names: list):
    """
    Fetches activities, parameters, and variables for a LIST of pipeline names.
    Use this to understand the logic of the pipelines discovered.
    """
    print(f"Tool get_pipeline_details called with: {pipeline_names}")
    try:
        scanner = UnifiedADFScanner()
        return scanner.get_pipeline_details(pipeline_names)
    except Exception as e:
        return f"Error: {str(e)}"

@tool
def get_dataset_details(dataset_names: list):
    """
    Fetches connection properties and schemas for a LIST of dataset names.
    Identified from pipeline activity inputs/outputs.
    """
    print(f"Tool get_dataset_details called with: {dataset_names}")
    try:
        scanner = UnifiedADFScanner()
        return scanner.get_dataset_details(dataset_names)
    except Exception as e:
        return f"Error: {str(e)}"

@tool
def get_linked_service_details(ls_names: list):
    """
    Fetches connection definitions for a LIST of linked service names.
    Identified from dataset properties.
    """
    print(f"Tool get_linked_service_details called with: {ls_names}")
    try:
        scanner = UnifiedADFScanner()
        return scanner.get_linked_service_details(ls_names)
    except Exception as e:
        return f"Error: {str(e)}"

@tool
def get_trigger_details(pipeline_names: list):
    """
    Fetches trigger schedules and types for a LIST of pipeline names.
    """
    print(f"Tool get_trigger_details called with: {pipeline_names}")
    try:
        scanner = UnifiedADFScanner()
        return scanner.get_trigger_details(pipeline_names)
    except Exception as e:
        return f"Error: {str(e)}"

SYSTEM_PROMPT = """
You are a highly skilled ADF-to-Databricks Migration Engineer.
Your task is to convert ADF pipelines into Databricks Lakeflow code using Auto Loader.

CRITICAL INSTRUCTIONS:
1. You MUST use the provided tools to fetch ALL necessary metadata.
2. Never assume metadata is unavailable without calling the tools first.
3. Follow this specific sequence:
   a. Call `discover_child_pipelines` for the starting pipeline(s).
   b. Call `get_pipeline_details` for ALL discovered pipelines.
   c. Parse the activities to find all 'inputs' (source datasets) and 'outputs' (sink datasets).
   d. Call `get_dataset_details` for all identified datasets.
   e. Identify linked services from datasets and call `get_linked_service_details`.
   f. Call `get_trigger_details` for the hierarchy.
4. If a tool returns an error, report it and do not invent data.

Conversion Strategy:
- Use Databricks Lakeflow (DLT or Jobs) with Auto Loader for ingestion.
- Preserve all dependencies and parameters.
- Convert Linked Services to Databricks Secret Scopes and connection strings.
- Map ADF datasets to Delta tables.

Output:
Return only a Python dictionary with 'lineage' and 'converted_code' keys. No Markdown, no explanation.
"""

agent = create_deep_agent(
    model=llm,
    system_prompt=SYSTEM_PROMPT,
    tools=[
        discover_child_pipelines,
        get_pipeline_details,
        get_dataset_details,
        get_linked_service_details,
        get_trigger_details
    ]
)

prompt = f"""
Convert these ADF pipelines to Databricks Lakeflow: {pipelines}

Mandatory Workflow:
1. Discover hierarchy starting from {pipelines}.
2. Get full details for all pipelines, datasets, linked services, and triggers.
3. Generate the code.

Output format:
{{
    "lineage": {{ ... }},
    "converted_code": "..."
}}
"""

print("Invoking agent...")
response = agent.invoke({"messages": [{"role": "user", "content": prompt}]})

# Extract and process output
result_content = response["messages"][-1].content
print(f"Agent Output: {result_content}")

try:
    # Clean output if Markdown was included
    cleaned_json = re.search(r'\{.*\}', result_content, re.DOTALL).group()
    result = json.loads(cleaned_json)
except Exception as e:
    print(f"JSON parsing failed: {e}")
    result = {"lineage": {}, "converted_code": result_content}

# Save to ADLS
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
main_pipe = pipelines.split(",")[0].strip()
file_path = f"abfss://unilever@dbstorageda08d80011adls.dfs.core.windows.net/UniversalDataLake/InternalSources/TopTier/BlobFileShare/TH/Auto_loader_test/source/generated_script_{main_pipe}_{timestamp}.py"

try:
    dbutils.fs.put(file_path, result.get("converted_code", ""), overwrite=True)
    print(f"File saved: {file_path}")
except Exception as e:
    print(f"ADLS Save failed: {e}")

dbutils.notebook.exit(json.dumps(result))
