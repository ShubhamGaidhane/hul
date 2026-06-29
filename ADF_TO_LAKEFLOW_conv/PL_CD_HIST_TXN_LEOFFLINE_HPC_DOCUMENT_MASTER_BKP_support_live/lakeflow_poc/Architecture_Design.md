# Lakeflow Architecture Design for POC (ADF to Databricks)

## Overview
This architecture replaces the Azure Data Factory (ADF) pipeline orchestration with Databricks Workflows and uses Auto Loader for efficient data ingestion.

## Components

### 1. Ingestion (Bronze Layer)
- **Source**: ADLS Gen2 (CSV files, pipe-delimited).
- **Mechanism**: Databricks Auto Loader (`cloudFiles`).
- **Target**: `bronze_hpc_document` (Delta Table).
- **Features**:
  - Schema inference and evolution.
  - Efficient incremental processing using checkpoints.
  - Audit columns (`_rescued_data`, `_ingestion_time`, `_source_file`).

### 2. Transformation (Silver Layer)
- **Source**: `bronze_hpc_document`.
- **Mechanism**: Structured Streaming with `foreachBatch` or a DLT (Delta Live Tables) pipeline. For this POC, we will use a Spark Notebook with `merge` logic.
- **Primary Keys**: `CompanyCode`, `DistributorCode`, `DocumentNumber`, `ProductCode`, `DetailID`.
- **Target**: `silver_hpc_document` (Delta Table).
- **Logic**: Deduplication and incremental merge.

### 3. Metadata & Logging (SQL Server)
- **Integration**: Use JDBC to read/write to the existing SQL Server `EtlLog` schema to maintain compatibility with the legacy monitoring system.
- **Actions**:
  - Call `uspBatchStartLog` at the beginning of the workflow.
  - Call `uspExecutionStartLog` and `uspExecutionEndLogV1` for each task.

### 4. Orchestration
- **Tool**: Databricks Workflows.
- **Job Tasks**:
  - `Task_Ingest_Landed_To_Bronze`: Runs the Auto Loader notebook.
  - `Task_Process_Bronze_To_Silver`: Runs the transformation notebook.

## Mapping from ADF

| ADF Component | Lakeflow/Databricks Equivalent |
|---------------|--------------------------------|
| Copy Activity (SQL to ADLS) | Auto Loader (for existing files) or JDBC Ingestion |
| Databricks Notebook Activity | Databricks Workflow Task (Notebook) |
| Lookup/Stored Procedure | JDBC connection via Spark/Python |
| IfCondition / ForEach | Workflow conditional tasks or Python logic within notebooks |
| Pipeline Parameters | Workflow Job Parameters |
