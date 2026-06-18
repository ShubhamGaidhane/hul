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

def get_pipeline_info(pipeline_name, pipelines_dict, datasets_dict):
    pipeline = pipelines_dict.get(pipeline_name)
    if not pipeline:
        return None

    activities = find_activities(pipeline.get('activities_detail', []))

    ls_names = set()
    dataset_jsons = []
    copy_logics = []
    internal_wildcards = set()

    for act in activities:
        # Linked Service from activity
        ls_ref = act.get('linkedServiceName')
        if ls_ref:
            if isinstance(ls_ref, dict):
                ls_names.add(ls_ref.get('referenceName'))
            else:
                ls_names.add(ls_ref)

        # Datasets from activity (Copy activity)
        # Note: inputs and outputs are now at the top level of normalized activity
        inputs = act.get('inputs') or []
        outputs = act.get('outputs') or []

        for ds_ref in inputs + outputs:
            ds_name = ds_ref.get('referenceName')
            if ds_name:
                ds_obj = datasets_dict.get(ds_name)
                if ds_obj:
                    dataset_jsons.append(ds_obj)
                    # Linked service from dataset
                    ls_name = ds_obj.get('linked_service') or ds_obj.get('linkedServiceName', {}).get('referenceName')
                    if ls_name:
                        ls_names.add(ls_name)

                    # Delimiter from dataset
                    d = ds_obj.get('delimiter')
                    if d:
                        internal_wildcards.add(d)

                # Check for wildcards and delimiters in inputs/outputs parameters
                ds_params = ds_ref.get('parameters', {})
                if ds_params:
                    w = extract_wildcard(ds_params)
                    if w:
                        for item in w.split(", "):
                            internal_wildcards.add(item)

        # Copy logic (translator) - Check both config and typeProperties
        config = act.get('config', {})
        translator = config.get('translator') or act.get('typeProperties', {}).get('translator')
        if translator:
            copy_logics.append(translator)

    return {
        "linked_services": list(filter(None, ls_names)),
        "datasets": dataset_jsons,
        "copy_logic": copy_logics,
        "wildcards": list(internal_wildcards)
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

    pipelines_dict = {p.get('pipeline'): p for p in pipelines_list}
    datasets_dict = {d.get('name'): d for d in datasets_list}

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

                # -------------------------
                # ✅ LANDED
                # -------------------------
                if 'LANDED' in ref_name.upper():
                    landed_info['pipeline'] = ref_name
                    landed_info['variables'] = json.dumps(params)

                    # ✅ path
                    landed_info['path'] = extract_value(
                        params.get('UDLPath')
                        or params.get('TargetObject')
                        or params.get('SourceObject')
                    )

                    # ✅ wildcard
                    wildcards = set()
                    w = extract_wildcard(params)
                    if w:
                        for item in w.split(", "):
                            wildcards.add(item)

                    # New Info
                    info = get_pipeline_info(ref_name, pipelines_dict, datasets_dict)
                    if info:
                        landed_info['ls_config'] = ", ".join(info['linked_services'])
                        landed_info['datasets'] = json.dumps(info['datasets'])
                        landed_info['copy_logic'] = json.dumps(info['copy_logic'])
                        for item in info.get('wildcards', []):
                            wildcards.add(item)

                    landed_info['wildcard'] = ", ".join(list(wildcards))

                # -------------------------
                # ✅ PROCESSED
                # -------------------------
                elif 'PROCESSED' in ref_name.upper():
                    processed_info['pipeline'] = ref_name
                    processed_info['variables'] = json.dumps(params)

                    processed_info['path'] = extract_value(
                        params.get('UDLPath')
                        or params.get('TargetObject')
                        or params.get('SourceObject')
                    )

                    # ✅ wildcard
                    wildcards = set()
                    w = extract_wildcard(params)
                    if w:
                        for item in w.split(", "):
                            wildcards.add(item)

                    # New Info
                    info = get_pipeline_info(ref_name, pipelines_dict, datasets_dict)
                    if info:
                        processed_info['datasets'] = json.dumps(info['datasets'])
                        for item in info.get('wildcards', []):
                            wildcards.add(item)

                    processed_info['wildcard'] = ", ".join(list(wildcards))

        # ✅ Only include valid mappings
        if not landed_info and not processed_info:
            continue

        # Triggers
        pipeline_triggers = triggers_by_pipeline.get(master_pipeline, [])
        trigger_names = ", ".join([t.get('name', '') for t in pipeline_triggers])
        trigger_types = ", ".join([t.get('trigger_kind', '') for t in pipeline_triggers])

        trigger_times = []
        for t in pipeline_triggers:
            t_time = t.get('trigger_time')
            if t_time:
                trigger_times.append(str(t_time))
            else:
                sched = t.get('schedule', {})
                if sched and 'recurrence' in sched:
                    trigger_times.append(json.dumps(sched['recurrence']))
                else:
                    trigger_times.append(json.dumps(sched))
        trigger_time_str = ", ".join(trigger_times)

        results.append({
            "Master Pipeline": master_pipeline,
            "Trigger Name": trigger_names,
            "Trigger Time": trigger_time_str,
            "Trigger Type": trigger_types,
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
            "processed path": processed_info.get('path', '')
        })

    return results

# -----------------------------------
# ✅ ENTRY POINT
# -----------------------------------
def main():
    json_file = r"C:\Users\ssureshg\OneDrive - Capgemini\Documents\MY_task\HUL\discovery_agent\adf_full_scan_output_Daya.json"
    csv_file = r"C:\Users\ssureshg\OneDrive - Capgemini\Documents\MY_task\HUL\discovery_agent\FINAL_OUTPUT_4.csv"

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
