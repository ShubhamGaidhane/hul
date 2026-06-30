# Databricks notebook source
# Landed Layer: Auto Loader for Excel to Bronze (Improved)
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

# Import logging utility
from logging_utils import UCLogger

# Parameters (usually passed via widgets)
dbutils.widgets.text("batch_id", "0")
dbutils.widgets.text("source_path", "abfss://unilever@dbstorageda06b80066adls.dfs.core.windows.net/UniversalDataLake/InternalSources/TopTier/BlobFileShare/TH/TargetMasterPSD/Source_file_1/")
dbutils.widgets.text("target_path", "abfss://unilever@dbstorageda06b80066adls.dfs.core.windows.net/UniversalDataLake/InternalSources/TopTier/BlobFileShare/TH/TargetMasterPSD/Landed_001/")
dbutils.widgets.text("checkpoint_path", "abfss://unilever@dbstorageda06b80066adls.dfs.core.windows.net/UniversalDataLake/InternalSources/TopTier/BlobFileShare/TH/TargetMasterPSD/checkpoints/targetmasterpsd_landed/")

batch_id_val = int(dbutils.widgets.get("batch_id"))
source_path_val = dbutils.widgets.get("source_path")
target_path_val = dbutils.widgets.get("target_path")
checkpoint_path_val = dbutils.widgets.get("checkpoint_path")

activity_name = "AT_CPY_CD_A_AURORA_TH_TOPTIER_TGT_M_PSD_LANDED"

# Logging start
logger = UCLogger()
detail_id = logger.start_batch_detail(batch_id_val, activity_name)

def process_excel(batch_df, batch_id_inner):
    # Use the spark session from the batch dataframe to support Spark Connect
    spark_inner = batch_df.sparkSession

    # In cloudFiles (binaryFile), the file path is in the 'path' column
    files_to_process = batch_df.select("path").collect()

    for row in files_to_process:
        file_path = row["path"]
        print(f"Processing: {file_path}")

        try:
            # Read Excel file
            df_excel = (
                spark_inner.read.format("com.crealytics.spark.excel")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(file_path)
                .withColumn("batch_id", lit(batch_id_val))
                .withColumn("source_file", lit(file_path))
                .withColumn("processing_timestamp", current_timestamp())
            )

            # Write to Delta with schema merging enabled
            (df_excel.write.format("delta")
             .mode("append")
             .option("mergeSchema", "true")
             .save(target_path_val)
            )

        except Exception as e:
            print(f"Error processing file {file_path}: {str(e)}")
            raise e

try:
    # Auto Loader configuration using binaryFile to get file paths
    # Note: binaryFile format provides: path, modificationTime, length, content
    files_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("pathGlobFilter", "*.xlsx")
        .load(source_path_val)
    )

    # Use foreachBatch to handle the Excel reading
    query = (
        files_df.writeStream
        .foreachBatch(process_excel)
        .option("checkpointLocation", checkpoint_path_val)
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()

    logger.end_batch_detail(detail_id, status="Succeeded", source_object=source_path_val, target_object=target_path_val)

except Exception as e:
    logger.end_batch_detail(detail_id, status="Failed", error_message=str(e))
    raise e
