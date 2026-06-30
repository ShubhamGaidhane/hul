# Databricks notebook source
# Landed Layer: Auto Loader for Excel to Bronze (Improved)
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, current_timestamp, lit
import os

# Import logging utility
from logging_utils import UCLogger

# Parameters (usually passed via widgets)
dbutils.widgets.text("batch_id", "0")
dbutils.widgets.text("source_path", "abfss://unilever@dbstorageda06b80066adls.dfs.core.windows.net/UniversalDataLake/InternalSources/TopTier/BlobFileShare/TH/TargetMasterPSD/Source_file_1/")
dbutils.widgets.text("target_path", "abfss://unilever@dbstorageda06b80066adls.dfs.core.windows.net/UniversalDataLake/InternalSources/TopTier/BlobFileShare/TH/TargetMasterPSD/Landed_001/")
dbutils.widgets.text("checkpoint_path", "abfss://unilever@dbstorageda06b80066adls.dfs.core.windows.net/UniversalDataLake/InternalSources/TopTier/BlobFileShare/TH/TargetMasterPSD/checkpoints/targetmasterpsd_landed/")

batch_id = int(dbutils.widgets.get("batch_id"))
source_path = dbutils.widgets.get("source_path")
target_path = dbutils.widgets.get("target_path")
checkpoint_path = dbutils.widgets.get("checkpoint_path")

activity_name = "AT_CPY_CD_A_AURORA_TH_TOPTIER_TGT_M_PSD_LANDED"

# Logging start
logger = UCLogger()
detail_id = logger.start_batch_detail(batch_id, activity_name)

def process_excel(batch_df, batch_id_inner):
    # Fix: Alias _metadata.file_path to allow easy access in the Row object
    # collect() is okay here assuming the number of files per trigger is not massive
    files_to_process = batch_df.selectExpr("_metadata.file_path as file_path").collect()

    for row in files_to_process:
        path = row["file_path"]
        print(f"Processing: {path}")

        try:
            # Read Excel file
            df_excel = (
                spark.read.format("com.crealytics.spark.excel")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(path)
                .withColumn("batch_id", lit(batch_id))
                .withColumn("source_file", lit(path))
                .withColumn("processing_timestamp", current_timestamp())
            )

            # Write to Delta with schema merging enabled
            (df_excel.write.format("delta")
             .mode("append")
             .option("mergeSchema", "true")
             .save(target_path)
            )

        except Exception as e:
            print(f"Error processing file {path}: {str(e)}")
            # Depending on requirements, you might want to raise here or continue
            # For now, we log and raise to ensure the batch is marked as failed
            raise e

try:
    # Auto Loader configuration using binaryFile to get file paths
    files_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("pathGlobFilter", "*.xlsx")
        .load(source_path)
    )

    # Use foreachBatch to handle the Excel reading (since Spark cannot natively stream Excel files directly)
    query = (
        files_df.writeStream
        .foreachBatch(process_excel)
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()

    logger.end_batch_detail(detail_id, status="Succeeded", source_object=source_path, target_object=target_path)

except Exception as e:
    logger.end_batch_detail(detail_id, status="Failed", error_message=str(e))
    raise e
