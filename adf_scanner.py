import time
import json
from collections import defaultdict

from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient


class UnifiedADFScanner:

    def __init__(self, subscription_id):
        # Use Databricks managed identity / default credentials
        self.credential = DefaultAzureCredential()

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
    def collect_pipeline_insights(self, rg_name, factory_name):
        results = []

        for pipe in self.client.pipelines.list_by_factory(rg_name, factory_name):
            activities = pipe.activities or []

            activity_details = []
            dependencies = []

            for act in activities:
                activity_details.append(act.serialize())

                dependencies.append({
                    "activity": act.name,
                    "depends_on": getattr(act, "depends_on", None)
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
                    # For ExecutePipeline, the pipeline reference is direct on the activity object
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
                "parameters": pipe.parameters,
                "variables": pipe.variables,
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
            # In list_by_factory, ir.type is the resource type (Microsoft.DataFactory/factories/integrationRuntimes)
            # The actual IR type (Managed/Self-Hosted) is in ir.properties.type
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
    def execute_full_scan(self, rg_name, factory_name):
        print(f"Starting ADF discovery for: {factory_name}")

        self.collect_pipeline_insights(rg_name, factory_name)
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
    subscription_id = "<YOUR_SUBSCRIPTION_ID>"
    rg_name = "<YOUR_RESOURCE_GROUP>"
    factory_name = "<YOUR_ADF_NAME>"

    scanner = UnifiedADFScanner(subscription_id)

    result = scanner.execute_full_scan(rg_name, factory_name)

    # Save to DBFS
    output_path = "/dbfs/tmp/adf_full_scan_output.json"

    with open(output_path, "w") as f:
        json.dump(result, f, indent=2)

    print(f"✅ Scan completed. Output saved to {output_path}")


# Run
if __name__ == "__main__":
    main()
