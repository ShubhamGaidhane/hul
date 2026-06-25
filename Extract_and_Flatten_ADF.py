import json
import time
from collections import defaultdict
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient

# -----------------------------------
# ✅ HELPERS & LOGIC (Merged from process_adf.py)
# -----------------------------------

def extract_value(val):
    if isinstance(val, dict):
        return val.get('value')
    return val

def extract_wildcard(params):
    findings = []
    val = extract_value(params.get('SourceFileName'))
    if val and isinstance(val, str) and ('*' in val or '?' in val):
        findings.append(val)
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
        config = activity.get('config', {})
        if config:
            if activity.get('type') == 'IfCondition':
                found.extend(find_activities(config.get('ifTrueActivities', [])))
                found.extend(find_activities(config.get('ifFalseActivities', [])))
            elif activity.get('type') in ['ForEach', 'Until']:
                found.extend(find_activities(config.get('activities', [])))
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
    new_ds = ds.copy()
    raw_def = ds['definition']
    if isinstance(raw_def, dict):
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
                    filtered_ds = filter_dataset_definition(ds_obj)
                    dataset_jsons.append(filtered_ds)
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
                    d = ds_obj.get('delimiter')
                    if d:
                        internal_wildcards.add(d)
                ds_params = ds_ref.get('parameters', {})
                if ds_params:
                    w = extract_wildcard(ds_params)
                    if w:
                        for item in w.split(", "):
                            internal_wildcards.add(item)

        translator = config.get('translator') or tp.get('translator')
        if translator:
            copy_logics.append(translator)
            if isinstance(translator, dict) and 'mappings' in translator:
                copy_mappings.append({"translator": translator})

        if act.get('type') == 'DatabricksNotebook':
            nb_path = config.get('notebookPath') or tp.get('notebookPath')
            nb_params = config.get('baseParameters') or tp.get('baseParameters', {})
            if nb_path:
                notebook_details.append({"path": nb_path, "parameters": nb_params})

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

def flatten_metadata(data):
    catalog = data.get("catalog", {})
    pipelines_list = catalog.get("pipelines", [])
    datasets_list = catalog.get("datasets", [])
    ls_list = catalog.get("linked_services", [])

    pipelines_dict = {p.get('pipeline'): p for p in pipelines_list}
    datasets_dict = {d.get('name'): d for d in datasets_list}
    ls_dict = {l.get('name'): l for l in ls_list}

    triggers_list = catalog.get("triggers", [])
    triggers_by_pipeline = defaultdict(list)
    for trig in triggers_list:
        for p_name in trig.get('linked_pipelines', []):
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
                ref_name = config.get('pipeline') or activity.get('typeProperties', {}).get('pipeline', {}).get('referenceName', '')
                params = config.get('parameters') or activity.get('typeProperties', {}).get('parameters', {})
                if not ref_name: continue

                target = None
                if 'LANDED' in ref_name.upper(): target = landed_info
                elif 'PROCESSED' in ref_name.upper(): target = processed_info

                if target is not None:
                    target['pipeline'] = ref_name
                    target['variables'] = json.dumps(params)
                    target['path'] = extract_value(params.get('UDLPath') or params.get('TargetObject') or params.get('SourceObject'))
                    if target == processed_info: target['refresh_type'] = extract_value(params.get('RefreshType'))
                    wildcards = set()
                    w = extract_wildcard(params)
                    if w:
                        for item in w.split(", "): wildcards.add(item)
                    info = get_pipeline_info(ref_name, pipelines_dict, datasets_dict, ls_dict)
                    if info:
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
                        for item in info.get('wildcards', []): wildcards.add(item)
                    target['wildcard'] = ", ".join(list(wildcards))

        if not landed_info and not processed_info: continue

        pipeline_triggers = triggers_by_pipeline.get(master_pipeline, [])
        trigger_names = ", ".join([t.get('name', '') for t in pipeline_triggers])
        trigger_types = ", ".join([t.get('trigger_kind', '') or t.get('type', '') for t in pipeline_triggers])
        trigger_statuses = ", ".join(filter(None, [t.get('runtimeState') or t.get('status') or t.get('state', '') for t in pipeline_triggers]))
        trigger_times = []
        for t in pipeline_triggers:
            if t.get('trigger_kind') == 'ScheduleTrigger' or t.get('type') == 'ScheduleTrigger':
                recurrence = t.get('definition', {}).get('properties', {}).get('typeProperties', {}).get('recurrence')
                if not recurrence: recurrence = t.get('schedule', {}).get('recurrence') or t.get('schedule')
                trigger_times.append(json.dumps(recurrence))
            else: trigger_times.append(json.dumps(t.get('definition') or t))
        trigger_time_str = ", ".join(trigger_times)

        combined_sources = []
        combined_sinks = []
        combined_notebooks = []
        for info_obj in [landed_info, processed_info]:
            for key, target_list in [('source_datasets', combined_sources), ('sink_datasets', combined_sinks), ('notebook_details', combined_notebooks)]:
                js = info_obj.get(key)
                if js:
                    try: target_list.extend(json.loads(js))
                    except: pass

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
            "Landed_LinkedService": landed_info.get('ls_names', ''),
            "Processed_LinkedService": processed_info.get('ls_names', ''),
            "Source_Dataset": json.dumps(combined_sources) if combined_sources else '',
            "Sink_Dataset": json.dumps(combined_sinks) if combined_sinks else '',
            "Trigger_Status": trigger_statuses,
            "RefreshType": processed_info.get('refresh_type', ''),
            "Notebook_Details": json.dumps(combined_notebooks) if combined_notebooks else '',
            "Copy_Activity_Mapping": landed_info.get('copy_mappings', ''),
            "Landed_Source_LS": landed_info.get('source_ls', ''),
            "Landed_Sink_LS": landed_info.get('sink_ls', ''),
            "Processed_Source_LS": processed_info.get('source_ls', ''),
            "Processed_Sink_LS": processed_info.get('sink_ls', ''),
            "Trigger Name": trigger_names,
            "Trigger Time": trigger_time_str,
            "Trigger Type": trigger_types,
            "IS_HISTORY": "Yes" if any(x in master_pipeline.upper() for x in ["HIST", "HISTORY"]) else "No"
        })
    return results

# -----------------------------------
# ✅ SCANNER CLASS (Merged from adf_scanner.py)
# -----------------------------------

class UnifiedADFScanner:
    def __init__(self, subscription_id, tenant_id=None, client_id=None, client_secret=None):
        if tenant_id and client_id and client_secret:
            self.credential = ClientSecretCredential(
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret
            )
        else:
            self.credential = DefaultAzureCredential()

        self.client = DataFactoryManagementClient(self.credential, subscription_id)
        self.catalog = {"pipelines": [], "linked_services": [], "triggers": [], "datasets": [], "integration_runtimes": []}
        self.lineage_map = defaultdict(list)

    def collect_pipeline_insights(self, rg_name, factory_name):
        for pipe in self.client.pipelines.list_by_factory(rg_name, factory_name):
            activities = pipe.activities or []
            activity_details = []
            dependencies = []
            for act in activities:
                activity_details.append(act.serialize())
                dependencies.append({"activity": act.name, "depends_on": getattr(act, "depends_on", None)})
                if hasattr(act, "inputs") and act.inputs:
                    for i in act.inputs: self.lineage_map[pipe.name].append(("dataset", i.reference_name))
                if hasattr(act, "outputs") and act.outputs:
                    for o in act.outputs: self.lineage_map[pipe.name].append(("dataset", o.reference_name))
                if act.type == "ExecutePipeline":
                    if hasattr(act, "pipeline") and act.pipeline:
                        self.lineage_map[pipe.name].append(("pipeline", act.pipeline.reference_name))
            self.catalog["pipelines"].append({
                "asset_type": "pipeline", "factory": factory_name, "pipeline": pipe.name,
                "activity_count": len(activities), "activity_kinds": list({a.type for a in activities}),
                "activities_detail": activity_details, "dependencies": dependencies,
                "parameters": pipe.parameters, "variables": pipe.variables, "definition": pipe.as_dict(),
                "captured_at": time.time()
            })

    def collect_datasets(self, rg_name, factory_name):
        for ds in self.client.datasets.list_by_factory(rg_name, factory_name):
            self.catalog["datasets"].append({
                "asset_type": "dataset", "name": ds.name, "type": ds.properties.type,
                "linked_service": ds.properties.linked_service_name.reference_name if ds.properties.linked_service_name else None,
                "schema": getattr(ds.properties, "schema", None), "definition": ds.as_dict(), "captured_at": time.time()
            })

    def collect_linked_service_info(self, rg_name, factory_name):
        for svc in self.client.linked_services.list_by_factory(rg_name, factory_name):
            self.catalog["linked_services"].append({
                "asset_type": "linked_service", "name": svc.name, "service_kind": svc.properties.type,
                "uses_key_vault": "AzureKeyVault" in str(svc.serialize()), "definition": svc.serialize(), "captured_at": time.time()
            })

    def collect_trigger_info(self, rg_name, factory_name):
        for trig in self.client.triggers.list_by_factory(rg_name, factory_name):
            pipelines = [p.pipeline_reference.reference_name for p in trig.properties.pipelines] if trig.properties.pipelines else []
            self.catalog["triggers"].append({
                "asset_type": "trigger", "name": trig.name, "trigger_kind": trig.properties.type,
                "status": trig.properties.runtime_state, "linked_pipelines": pipelines,
                "schedule": getattr(trig.properties, "type_properties", {}), "definition": trig.serialize(), "captured_at": time.time()
            })

    def collect_integration_runtimes(self, rg_name, factory_name):
        for ir in self.client.integration_runtimes.list_by_factory(rg_name, factory_name):
            detail = self.client.integration_runtimes.get(rg_name, factory_name, ir.name)
            self.catalog["integration_runtimes"].append({
                "asset_type": "integration_runtime", "name": ir.name,
                "type": detail.properties.type if detail.properties else "Unknown",
                "definition": detail.serialize(), "captured_at": time.time()
            })

    def execute_full_scan(self, rg_name, factory_name):
        self.collect_pipeline_insights(rg_name, factory_name)
        self.collect_datasets(rg_name, factory_name)
        self.collect_linked_service_info(rg_name, factory_name)
        self.collect_trigger_info(rg_name, factory_name)
        self.collect_integration_runtimes(rg_name, factory_name)
        return {"catalog": self.catalog, "lineage": dict(self.lineage_map)}

# -----------------------------------
# ✅ DATABRICKS EXECUTION
# -----------------------------------

def run_adf_scan_and_flatten(subscription_id, rg_name, factory_name, tenant_id=None, client_id=None, client_secret=None):
    # 1. Scan
    scanner = UnifiedADFScanner(subscription_id, tenant_id, client_id, client_secret)
    scan_result = scanner.execute_full_scan(rg_name, factory_name)

    # 2. Flatten
    flattened_results = flatten_metadata(scan_result)

    if not flattened_results:
        print("No data found.")
        return None

    # 3. Convert to Spark DataFrame
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        df = spark.createDataFrame(flattened_results)

        # Display/Print result
        df.show()
        return df
    except ImportError:
        # Fallback for local testing where pyspark is not available
        print(json.dumps(flattened_results, indent=2))
        return flattened_results

if __name__ == "__main__":
    # Example usage:
    # sub_id = "..."
    # rg = "..."
    # adf = "..."
    # df = run_adf_scan_and_flatten(sub_id, rg, adf)
    pass
