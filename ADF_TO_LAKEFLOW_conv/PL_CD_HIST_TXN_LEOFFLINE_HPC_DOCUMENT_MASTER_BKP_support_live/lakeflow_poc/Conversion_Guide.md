# ADF to Databricks Lakeflow Conversion Guide (POC)

## Introduction
This document details the conversion of the `PL_CD_HIST_TXN_LEOFFLINE_HPC_DOCUMENT_MASTER_BKP` ADF pipeline into a Databricks Lakeflow-based architecture.

## 1. Component Mapping

| ADF Activity | Databricks / Lakeflow Equivalent | Details |
|--------------|---------------------------------|---------|
| `AT_LKP_InsertBatch` | `Workflow_Orchestrator.py` | JDBC call to `[EtlLog].[uspBatchStartLog]` |
| `AT_IF_ExecCheckMaster` | Workflow Task Dependency | Conditional run in Databricks Workflows |
| `AT_EXEP_..._LANDED` | `Ingest_HPC_Document_AutoLoader.py` | Uses Auto Loader (`cloudFiles`) to ingest CSVs from ADLS Gen2 to Bronze Delta Table. |
| `AT_EXEP_..._PROCESSED` | `Process_HPC_Document_Delta.py` | Performs incremental Merge from Bronze to Silver Delta Table. |
| `AT_SP_EndBatch` | `Workflow_Orchestrator.py` | JDBC call to `[EtlLog].[uspBatchEndLog]` |

## 2. Ingestion Strategy (Auto Loader)
The original ADF pipeline used a `ForEach` loop to copy yearly partitions of CSV data. In the Lakeflow POC, we use **Auto Loader** with the following benefits:
- **Incremental Processing**: Auto Loader tracks new files automatically using checkpoints.
- **Schema Enforcement**: We defined the schema explicitly based on the ADF dataset metadata to ensure data quality.
- **Performance**: Auto Loader is more efficient than a standard Spark read for large numbers of files in ADLS.

## 3. Transformation Strategy (Delta Lake)
The transformation replaces the legacy notebook with a structured Delta Merge logic:
- **Bronze Layer**: Raw data with audit columns.
- **Silver Layer**: Cleaned, deduplicated data using primary keys (`CompanyCode`, `DistributorCode`, `DocumentNumber`, `ProductCode`, `DetailID`).
- **Upsert Logic**: `whenMatchedUpdateAll().whenNotMatchedInsertAll()` handles incremental updates.

## 4. Setup Instructions
1. **Upload Notebooks**: Import the Python files in the `lakeflow_poc` directory into your Databricks workspace.
2. **Configure Secrets**: Set up Databricks Secrets for SQL Server credentials and ADLS access keys.
3. **Create Workflow**:
   - Create a new Databricks Job.
   - **Task 1**: Start Logging (Notebook: `Workflow_Orchestrator`).
   - **Task 2**: Ingest Data (Notebook: `Ingest_HPC_Document_AutoLoader`).
   - **Task 3**: Process Data (Notebook: `Process_HPC_Document_Delta`).
   - **Task 4**: End Logging (Notebook: `Workflow_Orchestrator`).
4. **Trigger**: Schedule the job or run it on-demand for the POC.

## 5. GCP Migration Path
Once the logic is validated in Azure:
- **Storage**: Change `abfss://` paths to `gs://`.
- **Compute**: Databricks logic remains identical on GCP.
- **Logging**: If moving away from SQL Server, replace JDBC calls with BigQuery or Cloud Logging.
