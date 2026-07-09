
#cell 1
%pip install azure-identity azure-mgmt-datafactory
%pip install langchain langchain-openai
%pip install -U deepagents


%pip install --upgrade langchain langchain-core langgraph
dbutils.library.restartPython()


#Cell 2

from langchain_core.tools import tool
# from langgraph.prebuilt import create_react_agent
from deepagents import create_deep_agent
from langchain_openai import ChatOpenAI



import time
import json
# import os
from collections import defaultdict
import re
 
from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient


dbutils.widgets.text("department", "")
dbutils.widgets.text("instance", "bieno-da08-d-80011-adf-hr-01")
dbutils.widgets.text("pipelines", "PL_HR_D_TEAMENERGY_GBL_W2MODULES_MASTER")

department = dbutils.widgets.get("department")
instance = dbutils.widgets.get("instance")
pipelines = dbutils.widgets.get("pipelines")

print (department)
print (instance)
print (pipelines)





# department = dbutils.widgets.get("department")
instance = dbutils.widgets.get("instance")      
pipeline_names = dbutils.widgets.get("pipelines").split(",")



if department == "HR":
    print("HR logic")
    ADF_fuc_to_call = "get_pipeline_metadata"
 
elif department == "RND":
    print("RND logic")
    ADF_fuc_to_call = "get_pipeline_metadata_RND"
 
else:
    print("Unknown department: hence keeping HR")
    ADF_fuc_to_call = "get_pipeline_metadata"


api_key = "GaVLoff71m3OtgMhl6oCe9mJiDZ24nxH5nOVQrQJ" #1qJwhbjKeRaAIlmhtoptb2mzLYHUZruZ3b1zngX6" #xXiPy4HlxF4J80KQArpQ16YbAaci0B1K6oxamwoi" #"u54IdLyab4aYCpDLVdaYu24AD3TZ2F2s3ohJ25Us"



llm = ChatOpenAI(
    # model=   "amazon.nova-2-lite-v1:0" , #, #"anthropic.claude-sonnet-5" , "openai.gpt-5-mini",
    model=  "openai.gpt-5-mini",
    base_url="https://openai.generative.engine.capgemini.com/v1",
    api_key=api_key,
    default_headers={"x-api-key": api_key},
    temperature=0.2,
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
    raw = act.serialize()
    tp = raw.get("typeProperties", {})

    base = {
        "name": act.name,
        "type": act.type,
        "depends_on": normalize_dependencies(
            getattr(act, "depends_on", None)
        )
    }

    # Extract inputs/outputs if present (common for Copy, Lookup, etc.)
    if hasattr(act, "inputs") and act.inputs:
        base["inputs"] = [i.reference_name for i in act.inputs]
    if hasattr(act, "outputs") and act.outputs:
        base["outputs"] = [o.reference_name for o in act.outputs]

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
        base["if_true"] = [normalize_activity(a) for a in (act.if_true_activities or [])]
        base["if_false"] = [normalize_activity(a) for a in (act.if_false_activities or [])]
        base["config"] = tp

    elif act.type == "ForEach":
        base["activities"] = [normalize_activity(a) for a in (act.activities or [])]
        base["config"] = tp

    elif act.type == "Until":
        base["activities"] = [normalize_activity(a) for a in (act.activities or [])]
        base["config"] = tp

    elif act.type == "Switch":
        base["cases"] = [
            {"value": c.value, "activities": [normalize_activity(a) for a in (c.activities or [])]}
            for c in (act.cases or [])
        ]
        base["default_activities"] = [normalize_activity(a) for a in (act.default_activities or [])]
        base["config"] = tp

    else:
        base["config"] = tp

    return base


class UnifiedADFScanner:

    def __init__(self):
        # Configuration - ideally these would be parameters or from a secure config
        self.tenant_id = "f66fae02-5d36-495b-bfe0-78a6ff9f8e6e"
        self.client_id = "857458c3-549d-49dc-8e3a-993e71c1a011"
        self.subscription_id = "8e017cde-1d7c-4842-a4a5-18f6c115cae3"
        self.rg_name = "bieno-da08-d-80011-rg"
        self.factory_name = dbutils.widgets.get("instance")

        self.client_secret = dbutils.secrets.get(
            "databrickskv01",
            "svc-b-da-d-80011-ina-aadprincipal"
        )

        self.credential = ClientSecretCredential(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret
        )

        self.client = DataFactoryManagementClient(
            self.credential,
            self.subscription_id
        )

    def _find_execute_pipeline_names(self, activities):
        names = []
        for act in (activities or []):
            if act.type == "ExecutePipeline":
                if hasattr(act, "pipeline") and act.pipeline:
                    names.append(act.pipeline.reference_name)

            # Recurse into nested activities
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

    def get_pipeline_hierarchy(self, root_pipeline_name):
        to_process = [root_pipeline_name]
        seen = set()
        hierarchy = []

        while to_process:
            current = to_process.pop(0)
            if current in seen:
                continue
            seen.add(current)
            hierarchy.append(current)

            try:
                pipe = self.client.pipelines.get(self.rg_name, self.factory_name, current)
                child_names = self._find_execute_pipeline_names(pipe.activities)
                for cn in child_names:
                    if cn not in seen:
                        to_process.append(cn)
            except Exception as e:
                print(f"⚠️ Failed to fetch pipeline {current}: {e}")

        return hierarchy

    def get_pipeline_details(self, pipeline_names):
        results = []
        for name in pipeline_names:
            try:
                pipe = self.client.pipelines.get(self.rg_name, self.factory_name, name)
                activities = pipe.activities or []
                activity_details = [normalize_activity(act) for act in activities]
                
                results.append({
                    "asset_type": "pipeline",
                    "pipeline": pipe.name,
                    "activities": activity_details,
                    "parameters": normalize_parameters(pipe.parameters),
                    "variables": list(pipe.variables.keys()) if pipe.variables else [],
                })
            except Exception as e:
                print(f"⚠️ Failed to fetch pipeline {name}: {e}")
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
                    "linked_service": (
                        ds.properties.linked_service_name.reference_name
                        if ds.properties.linked_service_name else None
                    ),
                    "schema": getattr(ds.properties, "schema", None),
                    "properties": ds.properties.serialize()
                })
            except Exception as e:
                print(f"⚠️ Failed to fetch dataset {name}: {e}")
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
                print(f"⚠️ Failed to fetch linked service {name}: {e}")
        return results

    def get_trigger_details(self, pipeline_names):
        results = []
        try:
            all_triggers = self.client.triggers.list_by_factory(self.rg_name, self.factory_name)
            for trig in all_triggers:
                trig_pipelines = [p.pipeline_reference.reference_name for p in (trig.properties.pipelines or [])]
                # Check if this trigger is associated with any of the requested pipelines
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
            print(f"⚠️ Failed to fetch triggers: {e}")
        return results


@tool
def discover_child_pipelines(pipeline_name: str):
    """
    Given a root pipeline name, recursively identifies all child pipelines called via ExecutePipeline activities (including nested ones).
    Returns a list of all unique pipeline names in the hierarchy.
    """
    scanner = UnifiedADFScanner()
    return scanner.get_pipeline_hierarchy(pipeline_name)

@tool
def get_pipeline_details(pipeline_names: list):
    """
    Fetches detailed metadata for a list of ADF pipelines, including activities, parameters, and variables.
    """
    scanner = UnifiedADFScanner()
    return scanner.get_pipeline_details(pipeline_names)

@tool
def get_dataset_details(dataset_names: list):
    """
    Fetches detailed metadata for a list of ADF datasets, including their types, linked services, and properties.
    """
    scanner = UnifiedADFScanner()
    return scanner.get_dataset_details(dataset_names)

@tool
def get_linked_service_details(ls_names: list):
    """
    Fetches detailed metadata for a list of ADF linked services, including connection definitions.
    """
    scanner = UnifiedADFScanner()
    return scanner.get_linked_service_details(ls_names)

@tool
def get_trigger_details(pipeline_names: list):
    """
    Fetches details of all triggers associated with the specified list of pipelines.
    """
    scanner = UnifiedADFScanner()
    return scanner.get_trigger_details(pipeline_names)

@tool
def get_pipeline_metadata():
    """
    Legacy tool to get ADF metadata for pipelines specified in notebook widgets.
    If 'pipelines' widget is empty, it scans all pipelines in the factory.
    """
    scanner = UnifiedADFScanner()
    pipeline_names_str = dbutils.widgets.get("pipelines")

    if not pipeline_names_str:
        # Restore "scan all" functionality
        all_pipes = scanner.client.pipelines.list_by_factory(scanner.rg_name, scanner.factory_name)
        pipeline_names = [p.name for p in all_pipes]
    else:
        pipeline_names = [p.strip() for p in pipeline_names_str.split(",")]

    all_pipelines_in_hierarchy = []
    for p in pipeline_names:
        all_pipelines_in_hierarchy.extend(scanner.get_pipeline_hierarchy(p))

    unique_pipelines = list(set(all_pipelines_in_hierarchy))

    results = {
        "pipelines": scanner.get_pipeline_details(unique_pipelines),
        "triggers": scanner.get_trigger_details(unique_pipelines)
    }

    # Recursively collect all datasets from all activities (including nested ones)
    def collect_datasets(activities):
        ds = set()
        for act in activities:
            if "inputs" in act: ds.update(act["inputs"])
            if "outputs" in act: ds.update(act["outputs"])
            if "if_true" in act: ds.update(collect_datasets(act["if_true"]))
            if "if_false" in act: ds.update(collect_datasets(act["if_false"]))
            if "activities" in act: ds.update(collect_datasets(act["activities"]))
            if "cases" in act:
                for case in act["cases"]:
                    ds.update(collect_datasets(case["activities"]))
            if "default_activities" in act: ds.update(collect_datasets(act["default_activities"]))
        return ds

    all_ds_names = set()
    for p in results["pipelines"]:
        all_ds_names.update(collect_datasets(p["activities"]))

    results["datasets"] = scanner.get_dataset_details(list(all_ds_names))

    ls_names = set()
    for ds in results["datasets"]:
        if ds["linked_service"]: ls_names.add(ds["linked_service"])

    results["linked_services"] = scanner.get_linked_service_details(list(ls_names))

    return results

   
SYSTEM_PROMPT = """
You are an expert Azure Data Factory (ADF) to Databricks Lakeflow migration agent.

Objective:
Convert ADF pipelines into equivalent Databricks Lakeflow code.

Tool Usage Rules:

1. Start by calling `discover_child_pipelines` for the main pipeline(s) to identify the full hierarchy.
2. Call `get_pipeline_details` for all identified pipelines.
3. Identify all datasets and linked services from the pipeline activities (check inputs and outputs).
4. Call `get_dataset_details` and `get_linked_service_details` to get their configurations.
5. Call `get_trigger_details` for the pipelines.
6. Use all gathered metadata to generate the conversion code.

Conversion Rules:

1. Convert ADF activities into equivalent Databricks Lakeflow patterns.
2. Use Auto Loader wherever applicable for ingestion.
3. Preserve activity dependencies and execution order.
4. Preserve pipeline parameters and variables.
5. Convert all linked services and datasets to Databricks configurations (e.g., secret scopes, catalog locations).
6. Include trigger details (Type and Schedule) in the converted code or documentation.
7. Always write to Delta tables in the processed layer.

Guardrails:
1. Never invent metadata.
2. Never expose credentials.
3. Use TODO comments for unsupported components.

Output Rules:
1. Return only executable Databricks code within the requested dictionary format.
2. Do not provide explanations outside the dictionary.
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
    ],
    skills=["/SKILLS"]
)





prompt = f"""
Convert the given ADF pipeline hierarchy (parent and child) to Databricks Lakeflow using Auto Loader.

Starting pipeline(s): {pipelines}

Follow these steps:
1. Discover all child pipelines.
2. Get details for all pipelines in the hierarchy.
3. Get details for all referenced datasets and linked services.
4. Get trigger details.
5. Generate the Lakeflow code.

The conversion must include:
1. Pipeline Logic (Activities, Dependencies, Conditions)
2. Parameters & Variables
3. Linked Service Mapping
4. Dataset Mapping
5. Trigger Semantics
6. Fidelity Validation and Artifact Coverage Report

Return the output as a Python dictionary in the following format:

{{
    "lineage": {{
        "source": [...],
        "target": [...],
        "transformations": [...],
        "dependencies": [...]
    }},
    "converted_code": "<complete Lakeflow code as string>"
}}

Rules:
- Return only the dictionary.
- Do not include explanations or markdown outside the dictionary.
"""



response = agent.invoke(
    {
        "messages": [
            {
                "role": "user",
                "content": prompt
            }
        ]
    }
)


import json
from datetime import datetime

# Extract agent output
result = response["messages"][-1].content

# If agent returns a JSON string
if isinstance(result, str):
    try:
        # Try to find JSON block if the agent included some text despite instructions
        match = re.search(r'\{.*\}', result, re.DOTALL)
        if match:
            result = json.loads(match.group())
        else:
            result = json.loads(result)
    except:
        result = {"converted_code": result, "lineage": {}}

# Get converted code
converted_code = result.get("converted_code", "")

# Generate timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Target file path
main_pipeline = pipelines.split(",")[0]
file_path = f"abfss://unilever@dbstorageda08d80011adls.dfs.core.windows.net/UniversalDataLake/InternalSources/TopTier/BlobFileShare/TH/Auto_loader_test/source/generated_script_{main_pipeline}_{timestamp}.py"

# Write code to ADLS
try:
    dbutils.fs.put(
        file_path,
        converted_code,
        overwrite=True
    )
    print(f"File saved at: {file_path}")
except Exception as e:
    print(f"Failed to save to ADLS: {e}")
    # Fallback to local /tmp
    local_path = f"/tmp/generated_script_{main_pipeline}_{timestamp}.py"
    with open(local_path, "w") as f:
        f.write(converted_code)
    print(f"File saved locally at: {local_path}")


# Return everything to Streamlit
output = {
    "lineage": result.get("lineage", {}),
    "converted_code": result.get("converted_code", "")
}

dbutils.notebook.exit(json.dumps(output))
