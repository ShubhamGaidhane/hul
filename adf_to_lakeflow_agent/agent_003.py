# Cell1:



%pip install azure-identity azure-mgmt-datafactory
%pip install langchain langchain-openai
%pip install -U deepagents
%pip install --upgrade langchain langchain-core langgraph
dbutils.library.restartPython()

#cell2

from langchain_core.tools import tool
# from langgraph.prebuilt import create_react_agent
from deepagents import create_deep_agent
from langchain_openai import ChatOpenAI



import time
# import json
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

    else:
        base["config"] = tp

    return base


class UnifiedADFScanner:

    def __init__(self, subscription_id, tenant_id, client_id, client_secret):

        # Use Service Principal authentication
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

    # --------------------------------------------------
    # PIPELINE SCAN
    # --------------------------------------------------
    def collect_pipeline_insights(self, rg_name, factory_name, pipeline_names=None):
        results = []
                
        # ✅ If specific pipelines passed
        if pipeline_names:
            pipelines = []
            for name in pipeline_names:
                try:
                    pipe = self.client.pipelines.get(rg_name, factory_name, name)
                    pipelines.append(pipe)
                except Exception as e:
                    print(f"⚠️ Failed to fetch pipeline {name}: {e}")
        else:
            # ✅ Existing behavior (all pipelines)
            pipelines = self.client.pipelines.list_by_factory(rg_name, factory_name)


        for pipe in pipelines:
            activities = pipe.activities or []

            activity_details = []
            dependencies = []

            for act in activities:
                activity_details.append(normalize_activity(act)) #activity_details.append(act.serialize())

                dependencies.append({
                    "activity": act.name,
                    "depends_on": normalize_dependencies(getattr(act, "depends_on", None))
                })

                # Lineage
                if hasattr(act, "inputs") and act.inputs:
                    for i in act.inputs:
                        self.lineage_map[pipe.name].append(
                            ("dataset", i.reference_name)
                        )

                if hasattr(act, "outputs") and act.outputs:
                    for o in act.outputs:
                        self.lineage_map[pipe.name].append(
                            ("dataset", o.reference_name)
                        )

                if act.type == "ExecutePipeline":
                    if hasattr(act, "pipeline") and act.pipeline:
                        ref = act.pipeline.reference_name
                        self.lineage_map[pipe.name].append(
                            ("pipeline", ref)
                        )

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
                #"definition": pipe.as_dict(),
                "captured_at": time.time()
            })

        self.catalog["pipelines"] = results

    # --------------------------------------------------
    # DATASET SCAN
    # --------------------------------------------------
    def collect_datasets(self, rg_name, factory_name):
            datasets = []

            for ds in self.client.datasets.list_by_factory(rg_name, factory_name):
                datasets.append({
                    "asset_type": "dataset",
                    "name": ds.name,
                    "type": ds.properties.type,
                    "linked_service": (
                        ds.properties.linked_service_name.reference_name
                        if ds.properties.linked_service_name else None
                    ),
                    "schema": getattr(ds.properties, "schema", None),
                    #"definition": ds.as_dict(),
                    "captured_at": time.time()
                })

            self.catalog["datasets"] = datasets

    # --------------------------------------------------
    # LINKED SERVICES
    # --------------------------------------------------
    def collect_linked_service_info(self, rg_name, factory_name):
        results = []

        for svc in self.client.linked_services.list_by_factory(rg_name, factory_name):
            raw_data = str(svc.serialize())

            results.append({
                "asset_type": "linked_service",
                "name": svc.name,
                "service_kind": svc.properties.type,
                "uses_key_vault": "AzureKeyVault" in raw_data,
                "definition": svc.serialize(),
                "captured_at": time.time()
            })

        self.catalog["linked_services"] = results

    # --------------------------------------------------
    # TRIGGERS
    # --------------------------------------------------
    def collect_trigger_info(self, rg_name, factory_name):
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
                "captured_at": time.time()
            })

        self.catalog["triggers"] = results

    # --------------------------------------------------
    # INTEGRATION RUNTIMES
    # --------------------------------------------------
    def collect_integration_runtimes(self, rg_name, factory_name):
        irs = []

        for ir in self.client.integration_runtimes.list_by_factory(rg_name, factory_name):
            detail = self.client.integration_runtimes.get(rg_name, factory_name, ir.name)

            irs.append({
                "asset_type": "integration_runtime",
                "name": ir.name,
                "type": detail.properties.type if detail.properties else "Unknown",
                "definition": detail.serialize(),
                "captured_at": time.time()
            })

        self.catalog["integration_runtimes"] = irs

    # --------------------------------------------------
    # FULL SCAN
    # --------------------------------------------------
    def execute_full_scan(self, rg_name, factory_name, pipeline_names=None):

        print(f"Starting ADF discovery for: {factory_name}")

        self.collect_pipeline_insights(rg_name, factory_name, pipeline_names)
        self.collect_datasets(rg_name, factory_name)
        self.collect_linked_service_info(rg_name, factory_name)
        self.collect_trigger_info(rg_name, factory_name)
        self.collect_integration_runtimes(rg_name, factory_name)

        print(f"done the scan")

        return {
            "catalog": self.catalog,
            "lineage": dict(self.lineage_map)
        }


from langchain_core.tools import tool

@tool
def get_pipeline_metadata():
    """
    Get ADF pipeline metadata using notebook parameters.
    """

    tenant_id = "f66fae02-5d36-495b-bfe0-78a6ff9f8e6e"
    client_id = "857458c3-549d-49dc-8e3a-993e71c1a011"

    client_secret = dbutils.secrets.get(
        "databrickskv01",
        "svc-b-da-d-80011-ina-aadprincipal"
    )

    subscription_id = "8e017cde-1d7c-4842-a4a5-18f6c115cae3"
    rg_name = "bieno-da08-d-80011-rg"

    # Read notebook parameters
    # factory_name = dbutils.widgets.get("factory_name")
    factory_name = dbutils.widgets.get("instance" )  
    

    # pipeline_names_str = dbutils.widgets.get("pipeline_names")
    pipeline_names_str = dbutils.widgets.get("pipelines")

    pipeline_names = (
        [p.strip() for p in pipeline_names_str.split(",")]
        if pipeline_names_str
        else None
    )

    scanner = UnifiedADFScanner(
        subscription_id,
        tenant_id,
        client_id,
        client_secret
    )

    scanner.execute_full_scan(
        rg_name,
        factory_name,
        pipeline_names
    )

    # return scanner.catalog["pipelines"]
    return scanner.catalog




   
SYSTEM_PROMPT = """
You are an expert Azure Data Factory (ADF) to Databricks Lakeflow migration agent.

Objective:
Convert ADF pipelines into equivalent Databricks Lakeflow code.

Tool Usage Rules:

Available tool:
get_pipeline_metadata

Always call get_pipeline_metadata before generating conversion code.

1. Always call the available metadata tool before generating any conversion code.

2. Read all the child pipelines, linked services, datasets, trigger ect for each pipeline.

3. Use only metadata returned by the tool.

4. Never skip tool usage when pipeline information is required.

Conversion Rules:

1. Convert ADF activities into equivalent Databricks Lakeflow patterns.

2. Use Auto Loader wherever applicable.

3. Preserve activity dependencies and execution order.

4. Preserve pipeline parameters and variables wherever possible.

5. Preserve lineage information whenever available.

6. convert all the linked services, datasets

7. Add trigger details of each pipeline (Type and Time)

8. Always write to deltalake table in processed layer


Guardrails:

1. Never invent:
   - pipelines
   - datasets
   - linked services
   - activities
   - parameters

2. Never generate conversion code if metadata is unavailable.

3. Never expose credentials, secrets, API keys, passwords, or connection strings.

4. If an ADF component is unsupported, generate a TODO comment instead of guessing.

Output Rules:

1. Return only executable Databricks code.

2. Do not provide explanations.

3. Do not provide markdown formatting.

4. Do not provide introductory text.

Return only the converted code.
"""

agent = create_deep_agent(
    model=llm,
    system_prompt=SYSTEM_PROMPT,
    tools=[get_pipeline_metadata],
    skills=["/SKILLS"]
)





prompt = f"""
Convert the given ADF pipeline/code(parent and child) to Databricks Lakeflow using Auto Loader.

Use {ADF_fuc_to_call} to extract all required ADF metadata, childpipelines, configurations, dependencies, and connection details.
CRITICAL VALIDATION REQUIREMENTS

Do not perform a code-only conversion. Before generating the Databricks notebook, fully analyze all ADF artifacts associated with the pipeline and verify that every dependency is accounted for. we need mater, landed and processed (parent and child) all the pipelines converted into a single notebook.

The conversion must include and document:

1. Pipeline Logic
   - Activities
   - Dependencies
   - Conditions (If, Switch, Until, ForEach)
   - Wait activities
   - Error handling paths
   - Retry policies
   - Timeout settings

2. Pipeline Parameters
   - Preserve parameter names
   - Preserve default values
   - Preserve runtime expressions
   - Preserve trigger-driven parameters

3. Linked Services
   - Identify every linked service used directly or indirectly by the pipeline
   - Include linked services referenced through datasets
   - Capture connection type, authentication method, Integration Runtime, Key Vault references, Managed Identity usage, and network dependencies
   - Produce a Linked Service Mapping section showing:
     ADF Linked Service -> Databricks Equivalent

4. Datasets
   - Identify all datasets used by activities
   - Capture source and target objects
   - Capture dataset parameters
   - Capture schemas, file formats, paths, and linked service references
   - Produce a Dataset Mapping section showing:
     ADF Dataset -> Databricks Implementation

5. Triggers
   - Detect all triggers associated with the pipeline
   - Preserve TriggerTime semantics
   - Preserve schedule, tumbling window, event-based, or manual trigger behavior
   - Ensure ADF RunId and Trigger metadata are passed into Databricks jobs where required

6. Child Pipeline Dependencies
   - Identify all ExecutePipeline activities
   - List all dependent pipelines
   - Verify parameter mappings between parent and child pipelines
   - Report any child pipelines that have not yet been converted

7. Integration Runtime Analysis
   - Identify any Azure IR or Self-Hosted IR dependencies
   - Highlight connectivity, VPN, private endpoint, firewall, or on-premises access requirements
   - Flag migration risks if Databricks cannot access the same resources

8. Artifact Coverage Report
   For every conversion provide a summary table:

   - Pipeline Activities
   - Parameters
   - Variables
   - Linked Services
   - Datasets
   - Triggers
   - Integration Runtimes
   - Child Pipelines
   - Stored Procedures
   - Source Systems
   - Sink Systems

   Mark each as:
   - Fully Migrated
   - Partially Migrated
   - Not Migrated

9. Fidelity Validation
   Compare the generated Databricks notebook against the original ADF pipeline and explicitly identify:
   - Missing logic
   - Added logic
   - Changed behavior
   - Assumptions made during conversion
   - Potential migration risks

10. Do Not Invent Logic
    Do not introduce new processing, ingestion, transformations, Delta writes, Auto Loader logic, audit tables, metadata tables, or lineage tracking unless they exist in the source ADF solution or are explicitly requested. Any added functionality must be clearly marked as "Enhancement" rather than "Migration".

11. Return the output as a Python dictionary in the following format:

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
- Extract and populate lineage information from the ADF metadata.
- Generate equivalent Lakeflow code using Auto Loader.
- Return only the dictionary.
- Do not include explanations, markdown, comments, or additional text outside the dictionary.
"""

# prompt = f"""
# Convert the given ADF pipeline/code to Databricks Lakeflow using Auto Loader.

# Use {ADF_fuc_to_call} to extract all required ADF metadata, childpipelines, configurations, dependencies, and connection details.

# Return the output as a Python dictionary in the following format:

# {{
#     "lineage": {{
#         "source": [...],
#         "target": [...],
#         "transformations": [...],
#         "dependencies": [...]
#     }},
#     "converted_code": "<complete Lakeflow code as string>"
# }}

# Rules:
# - Extract and populate lineage information from the ADF metadata.
# - Generate equivalent Lakeflow code using Auto Loader.
# - Return only the dictionary.
# - Do not include explanations, markdown, comments, or additional text outside the dictionary.
# """


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




# &&&&&&&&&&&&&&&&&



# Get converted code
converted_code = result.get("converted_code", "")

# Generate timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Target file path
file_path = f"abfss://unilever@dbstorageda08d80011adls.dfs.core.windows.net/UniversalDataLake/InternalSources/TopTier/BlobFileShare/TH/Auto_loader_test/source/generated_script_{pipelines}_{timestamp}.py"

# Write code to ADLS
dbutils.fs.put(
    file_path,
    converted_code,
    overwrite=True
)

print(f"File saved at: {file_path}") 


##################






# Return everything to Streamlit
output = {
    "lineage": result.get("lineage", {}),
    "converted_code": result.get("converted_code", "")
}

dbutils.notebook.exit(json.dumps(output))



