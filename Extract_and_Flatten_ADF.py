import time
import json
import os
from collections import defaultdict
 
from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient

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
                "definition": pipe.as_dict(),
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
                    "definition": ds.as_dict(),
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

        return {
            "catalog": self.catalog,
            "lineage": dict(self.lineage_map)
        }

# --------------------------------------------------
# ENTRY POINT (Databricks)
# --------------------------------------------------
def main():

    tenant_id = "f66fae02-5d36-495b-bfe0-78a6ff9f8e6e"
    client_id = "3a04829e-e5ba-498d-805d-450972cabd11"
    client_secret = dbutils.secrets.get("databrickskv01", "svc-b-da-b-80066-ina-aadprincipal")
 
    subscription_id = "7796e2b1-1c51-4a77-accf-3c035b59a22e" 
    rg_name = "bieno-da06-b-80066-rg"
    factory_name = "bieno-da06-b-80066-adf-hr-01"

    
    scanner = UnifiedADFScanner(
        subscription_id,
        tenant_id,
        client_id,
        client_secret
    )

    result = scanner.execute_full_scan(rg_name, factory_name)

    
    

    # output_path = "/Volumes/udl_landed_uat/amazon_at/asintoeanaustria/adf_full_scan_output.json"

    # with open(output_path, "w") as f:
    #     json.dump(result, f, indent=2)

    # print(f"✅ File saved at: {output_path}")


    print("✅ Done")


  

    print(result)

    # print(f"✅ Scan completed. Output saved to {output_path}")


# Run
if __name__ == "__main__":
    main()




import json 
import csv 
import os 
 
# ----------------------------------- 
# ✅ Helpers 
# ----------------------------------- 
def extract_value(val): 
    if isinstance(val, dict): 
        return val.get('value') 
    return val 
 
def extract_wildcard(params): 
    findings = [] 
    # Wildcard from SourceFileName 
    val = extract_value(params.get('SourceFileName')) 
    if val and isinstance(val, str) and ('*' in val or '?' in val): 
        findings.append(val) 
 
    # Delimiters 
    for key in ['FieldDelimiter', 'Delimeter', 'delimiter']: 
        d = extract_value(params.get(key)) 
        if d and isinstance(d, str): 
            findings.append(d) 
 
    return ", ".join(list(set(findings))) 
 
def find_activities(activities): 
    found = [] 
    if not activities: 
        return found 
 
    for activity in activities: 
        found.append(activity) 
 
        # Handle nested activities in the new 'config' structure 
        config = activity.get('config', {}) 
        if config: 
            if activity.get('type') == 'IfCondition': 
                found.extend(find_activities(config.get('ifTrueActivities', []))) 
                found.extend(find_activities(config.get('ifFalseActivities', []))) 
            elif activity.get('type') in ['ForEach', 'Until']: 
                found.extend(find_activities(config.get('activities', []))) 
 
        # Keep compatibility with old structure just in case 
        tp = activity.get('typeProperties', {}) 
        if tp: 
            if activity.get('type') == 'IfCondition': 
                found.extend(find_activities(tp.get('ifTrueActivities', []))) 
                found.extend(find_activities(tp.get('ifFalseActivities', []))) 
            elif activity.get('type') in ['ForEach', 'Until']: 
                found.extend(find_activities(tp.get('activities', []))) 
 
    return found 
 
def filter_dataset_definition(ds): 
    if not ds or 'definition' not in ds: 
        return ds 
 
    # Copy to avoid modifying original catalog 
    new_ds = ds.copy() 
    raw_def = ds['definition'] 
    if isinstance(raw_def, dict): 
        # Exclude "id", "type", "etag" from processed Dataset json 
        new_def = {k: v for k, v in raw_def.items() if k not in ["id", "type", "etag"]} 
        new_ds['definition'] = new_def 
 
    return new_ds 
 
def filter_ls_definition(ls): 
    if not ls or 'definition' not in ls: 
        return ls 
 
    new_ls = ls.copy() 
    raw_def = ls['definition'] 
    if isinstance(raw_def, dict): 
        props = raw_def.get('properties', {}) 
        if isinstance(props, dict): 
            # Exclude "encryptedCredential" 
            tp = props.get('typeProperties', {}) 
            if isinstance(tp, dict): 
                new_tp = {k: v for k, v in tp.items() if k != "encryptedCredential"} 
                new_props = props.copy() 
                new_props['typeProperties'] = new_tp 
                new_def = raw_def.copy() 
                new_def['properties'] = new_props 
                new_ls['definition'] = new_def 
 
    return new_ls 
 
def get_pipeline_info(pipeline_name, pipelines_dict, datasets_dict, ls_dict): 
    pipeline = pipelines_dict.get(pipeline_name) 
    if not pipeline: 
        return None 
 
    activities = find_activities(pipeline.get('activities_detail', [])) 
 
    ls_infos = [] 
    dataset_jsons = [] 
    copy_logics = [] 
    internal_wildcards = set() 
    seen_ls = set() 
    
    ls_names = set()
    source_dataset_jsons = []
    sink_dataset_jsons = []
    source_ls_jsons = []
    sink_ls_jsons = []
    notebook_details = []
    copy_mappings = []

    seen_source_ls = set()
    seen_sink_ls = set()

    for act in activities: 
        config = act.get('config', {}) 
        tp = act.get('typeProperties', {})

        # Linked Service from activity 
        ls_ref = act.get('linkedServiceName') 
        if ls_ref: 
            ls_name = ls_ref.get('referenceName') if isinstance(ls_ref, dict) else ls_ref 
            if ls_name:
                ls_names.add(ls_name)
                if ls_name not in seen_ls: 
                    seen_ls.add(ls_name) 
                    ls_obj = ls_dict.get(ls_name) 
                    if ls_obj: 
                        ls_infos.append(filter_ls_definition(ls_obj)) 
                    else: 
                        ls_infos.append({"name": ls_name}) 
 
        # Datasets from activity 
        inputs = act.get('inputs') or [] 
        outputs = act.get('outputs') or [] 
 
        for ds_ref in inputs:
            ds_name = ds_ref.get('referenceName')
            if ds_name:
                ds_obj = datasets_dict.get(ds_name)
                if ds_obj:
                    source_dataset_jsons.append(filter_dataset_definition(ds_obj))
                    
                    # LS from source dataset
                    ls_name = ds_obj.get('linked_service') or ds_obj.get('linkedServiceName', {}).get('referenceName')
                    if ls_name and ls_name not in seen_source_ls:
                        seen_source_ls.add(ls_name)
                        ls_obj = ls_dict.get(ls_name)
                        if ls_obj:
                            source_ls_jsons.append(filter_ls_definition(ls_obj))
                        else:
                            source_ls_jsons.append({"name": ls_name})
                else:
                    source_dataset_jsons.append({"name": ds_name})

        for ds_ref in outputs:
            ds_name = ds_ref.get('referenceName')
            if ds_name:
                ds_obj = datasets_dict.get(ds_name)
                if ds_obj:
                    sink_dataset_jsons.append(filter_dataset_definition(ds_obj))

                    # LS from sink dataset
                    ls_name = ds_obj.get('linked_service') or ds_obj.get('linkedServiceName', {}).get('referenceName')
                    if ls_name and ls_name not in seen_sink_ls:
                        seen_sink_ls.add(ls_name)
                        ls_obj = ls_dict.get(ls_name)
                        if ls_obj:
                            sink_ls_jsons.append(filter_ls_definition(ls_obj))
                        else:
                            sink_ls_jsons.append({"name": ls_name})
                else:
                    sink_dataset_jsons.append({"name": ds_name})

        for ds_ref in inputs + outputs: 
            ds_name = ds_ref.get('referenceName') 
            if ds_name: 
                ds_obj = datasets_dict.get(ds_name) 
                if ds_obj: 
                    # Filter dataset definition 
                    filtered_ds = filter_dataset_definition(ds_obj) 
                    dataset_jsons.append(filtered_ds) 
 
                    # Linked service from dataset 
                    ls_name = ds_obj.get('linked_service') or ds_obj.get('linkedServiceName', {}).get('referenceName') 
                    if ls_name:
                        ls_names.add(ls_name)
                        if ls_name not in seen_ls: 
                            seen_ls.add(ls_name) 
                            ls_obj = ls_dict.get(ls_name) 
                            if ls_obj: 
                                ls_infos.append(filter_ls_definition(ls_obj)) 
                            else: 
                                ls_infos.append({"name": ls_name}) 
 
                    # Delimiter from dataset property 
                    d = ds_obj.get('delimiter') 
                    if d: 
                        internal_wildcards.add(d) 
 
                # Delimiter/Wildcard from DatasetReference parameters 
                ds_params = ds_ref.get('parameters', {}) 
                if ds_params: 
                    w = extract_wildcard(ds_params) 
                    if w: 
                        for item in w.split(", "): 
                            internal_wildcards.add(item) 
 
        # Copy logic (translator) 
        translator = config.get('translator') or tp.get('translator') 
        if translator: 
            copy_logics.append(translator) 
            if isinstance(translator, dict) and 'mappings' in translator:
                copy_mappings.append({"translator": translator})

        # Notebook Details
        if act.get('type') == 'DatabricksNotebook':
            nb_path = config.get('notebookPath') or tp.get('notebookPath')
            nb_params = config.get('baseParameters') or tp.get('baseParameters', {})
            if nb_path:
                notebook_details.append({
                    "path": nb_path,
                    "parameters": nb_params
                })

    return { 
        "ls_configuration": ls_infos, 
        "datasets": dataset_jsons, 
        "copy_logic": copy_logics, 
        "wildcards": list(internal_wildcards),
        "ls_names": list(ls_names),
        "source_datasets": source_dataset_jsons,
        "sink_datasets": sink_dataset_jsons,
        "source_ls": source_ls_jsons,
        "sink_ls": sink_ls_jsons,
        "notebook_details": notebook_details,
        "copy_mappings": copy_mappings
    } 
 
# ----------------------------------- 
# ✅ MAIN PROCESSOR 
# ----------------------------------- 
def process_adf_json(json_file_path): 
    with open(json_file_path, 'r') as f: 
        data = json.load(f) 
 
    catalog = data.get("catalog", {}) 
    pipelines_list = catalog.get("pipelines", []) 
    datasets_list = catalog.get("datasets", []) 
    ls_list = catalog.get("linked_services", []) 
 
    pipelines_dict = {p.get('pipeline'): p for p in pipelines_list} 
    datasets_dict = {d.get('name'): d for d in datasets_list} 
    ls_dict = {l.get('name'): l for l in ls_list} 
 
    triggers_list = catalog.get("triggers", []) 
    triggers_by_pipeline = {} 
    for trig in triggers_list: 
        for p_name in trig.get('linked_pipelines', []): 
            if p_name not in triggers_by_pipeline: 
                triggers_by_pipeline[p_name] = [] 
            triggers_by_pipeline[p_name].append(trig) 
 
    results = [] 
 
    for pipe in pipelines_list: 
        master_pipeline = pipe.get('pipeline', '') 
        master_variables = json.dumps(pipe.get('parameters')) if pipe.get('parameters') else None 
 
        activities = find_activities(pipe.get('activities_detail', [])) 
 
        landed_info = {} 
        processed_info = {} 
 
        for activity in activities: 
            if activity.get('type') == 'ExecutePipeline': 
                config = activity.get('config', {}) 
                # Try new 'config' then old 'typeProperties' 
                ref_name = config.get('pipeline') or activity.get('typeProperties', {}).get('pipeline', {}).get('referenceName', '') 
                params = config.get('parameters') or activity.get('typeProperties', {}).get('parameters', {}) 
 
                if not ref_name: 
                    continue 
 
                target = None 
                if 'LANDED' in ref_name.upper(): 
                    target = landed_info 
                elif 'PROCESSED' in ref_name.upper(): 
                    target = processed_info 
 
                if target is not None: 
                    target['pipeline'] = ref_name 
                    target['variables'] = json.dumps(params) 
                    target['path'] = extract_value( 
                        params.get('UDLPath') 
                        or params.get('TargetObject') 
                        or params.get('SourceObject') 
                    ) 
 
                    # RefreshType (from processed variables)
                    if target == processed_info:
                        target['refresh_type'] = extract_value(params.get('RefreshType'))

                    wildcards = set() 
                    w = extract_wildcard(params) 
                    if w: 
                        for item in w.split(", "): 
                            wildcards.add(item) 
 
                    info = get_pipeline_info(ref_name, pipelines_dict, datasets_dict, ls_dict) 
                    if info: 
                        # Landed LS Configuration: Name + filtered properties as JSON 
                        target['ls_config'] = json.dumps(info['ls_configuration']) 
                        target['datasets'] = json.dumps(info['datasets']) 
                        
                        target['ls_names'] = json.dumps(info['ls_configuration'])
                        target['source_datasets'] = json.dumps(info['source_datasets'])
                        target['sink_datasets'] = json.dumps(info['sink_datasets'])
                        target['source_ls'] = json.dumps(info['source_ls'])
                        target['sink_ls'] = json.dumps(info['sink_ls'])
                        target['notebook_details'] = json.dumps(info['notebook_details'])

                        if target == landed_info: 
                            target['copy_logic'] = json.dumps(info['copy_logic']) 
                            target['copy_mappings'] = json.dumps(info['copy_mappings'])

                        for item in info.get('wildcards', []): 
                            wildcards.add(item) 
 
                    target['wildcard'] = ", ".join(list(wildcards)) 
 
        if not landed_info and not processed_info: 
            continue 
 
        # Triggers 
        pipeline_triggers = triggers_by_pipeline.get(master_pipeline, []) 
        trigger_names = ", ".join([t.get('name', '') for t in pipeline_triggers]) 
        trigger_types = ", ".join([t.get('trigger_kind', '') or t.get('type', '') for t in pipeline_triggers]) 
        
        trigger_statuses_list = []
        for t in pipeline_triggers:
            # Try 'runtimeState', then 'status', then 'state'
            status = t.get('runtimeState') or t.get('status') or t.get('state', '')
            trigger_statuses_list.append(status)
        trigger_statuses = ", ".join(filter(None, trigger_statuses_list))

        trigger_times = [] 
        for t in pipeline_triggers: 
            # Trigger Time: recurrence JSON if ScheduleTrigger, else full definition 
            if t.get('trigger_kind') == 'ScheduleTrigger' or t.get('type') == 'ScheduleTrigger': 
                # Try to find recurrence in definition or schedule field 
                recurrence = None 
                # Check definition structure first 
                props = t.get('definition', {}).get('properties', {}) 
                if props: 
                    recurrence = props.get('typeProperties', {}).get('recurrence') 
 
                # Fallback to schedule field 
                if not recurrence: 
                    recurrence = t.get('schedule', {}).get('recurrence') or t.get('schedule') 
 
                trigger_times.append(json.dumps(recurrence)) 
            else: 
                trigger_times.append(json.dumps(t.get('definition') or t)) 
 
        trigger_time_str = ", ".join(trigger_times) 
 
        # Combine Source/Sink datasets from both layers if they exist
        combined_sources = []
        combined_sinks = []
        for info_obj in [landed_info, processed_info]:
            s_json = info_obj.get('source_datasets')
            if s_json:
                try:
                    combined_sources.extend(json.loads(s_json))
                except:
                    pass
            si_json = info_obj.get('sink_datasets')
            if si_json:
                try:
                    combined_sinks.extend(json.loads(si_json))
                except:
                    pass
        
        # Combine Notebook Details (they are JSON strings of lists)
        combined_notebooks = []
        for info_obj in [landed_info, processed_info]:
            nb_json = info_obj.get('notebook_details')
            if nb_json:
                try:
                    combined_notebooks.extend(json.loads(nb_json))
                except:
                    pass

        results.append({ 
            "Master Pipeline": master_pipeline, 
 
            "Landed Pipeline": landed_info.get('pipeline', ''), 
            "Processed Pipeline": processed_info.get('pipeline', ''), 
 
            "Master variables json": master_variables, 
            "Landed Variables JSON": landed_info.get('variables', ''), 
            "Processed variables JSON": processed_info.get('variables', ''), 
 
            "Landed LS Configuration": landed_info.get('ls_config', ''), 
            "Landed Dataset json": landed_info.get('datasets', ''), 
            "Landed wildcard": landed_info.get('wildcard', ''), 
            "Landed path": landed_info.get('path', ''), 
            "Landed copy logic": landed_info.get('copy_logic', ''), 
 
            "processed Dataset json": processed_info.get('datasets', ''), 
            "procesed wildcard": processed_info.get('wildcard', ''), 
            "processed path": processed_info.get('path', ''), 

            # New Columns
            #"Landed_LinkedService": landed_info.get('ls_names', ''),
            #"Processed_LinkedService": processed_info.get('ls_names', ''),
            "Source_Dataset": json.dumps(combined_sources) if combined_sources else '',
            "Sink_Dataset": json.dumps(combined_sinks) if combined_sinks else '',
            
            "RefreshType": processed_info.get('refresh_type', ''),
            "Notebook_Details": json.dumps(combined_notebooks) if combined_notebooks else '',
           # "Copy_Activity_Mapping": landed_info.get('copy_mappings', ''),

            "Landed_Source_LS": landed_info.get('source_ls', ''),
            "Landed_Sink_LS": landed_info.get('sink_ls', ''),
            "Processed_Source_LS": processed_info.get('source_ls', ''),
            "Processed_Sink_LS": processed_info.get('sink_ls', ''),

            "Trigger Name": trigger_names, 
            "Trigger Time": trigger_time_str, 
            "Trigger Type": trigger_types, 
            "Trigger_Status": trigger_statuses,
            "IS_HISTORY": "Yes" if "HIST" in master_pipeline.upper() or "HISTORY" in master_pipeline.upper() else "No"
        }) 
 
    return results 
 
# ----------------------------------- 
# ✅ ENTRY POINT 
# ----------------------------------- 
def main(): 
    json_file = r"C:\Users\ssureshg\OneDrive - Capgemini\Documents\MY_task\HUL\discovery_agent\adf_full_scan_output_def_daya.json" 
    csv_file = r"C:\Users\ssureshg\OneDrive - Capgemini\Documents\MY_task\HUL\discovery_agent\FINAL_OUTPUT_19.csv" 
 
    if not os.path.exists(json_file): 
        print(f"File not found: {json_file}") 
        return 
 
    results = process_adf_json(json_file) 
 
    if not results: 
        print("No data found.") 
        return 
 
    # ✅ Write CSV 
    with open(csv_file, 'w', newline='', encoding='utf-8') as f: 
        writer = csv.DictWriter(f, fieldnames=results[0].keys()) 
        writer.writeheader() 
        writer.writerows(results) 
 
    print(f"✅ DONE! File created: {csv_file}") 
 
if __name__ == "__main__": 
    main()
