# Databricks notebook source
# Landed Layer: Auto Loader for Excel to Bronze
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, current_timestamp, lit
# Import logging utility
# In Databricks, if they are in the same folder, we can use %run
# %run ./logging_utils

# Parameters (usually passed via widgets)
dbutils.widgets.text("batch_id", "0")
dbutils.widgets.text("source_path", "abfss://cd-aurora-th@dbstorageda06b80066.blob.core.windows.net/toptier/targetmasterpsd/")
dbutils.widgets.text("target_path", "abfss://unilever@dbstorageda06b80066adls.dfs.core.windows.net/UniversalDataLake/InternalSources/TopTier/BlobFileShare/TH/TargetMasterPSD/LandedFiles/")
dbutils.widgets.text("checkpoint_path", "abfss://unilever@dbstorageda06b80066adls.dfs.core.windows.net/checkpoints/targetmasterpsd_landed/")

batch_id = int(dbutils.widgets.get("batch_id"))
source_path = dbutils.widgets.get("source_path")
target_path = dbutils.widgets.get("target_path")
checkpoint_path = dbutils.widgets.get("checkpoint_path")

activity_name = "AT_CPY_CD_A_AURORA_TH_TOPTIER_TGT_M_PSD_LANDED"

# Logging start
from logging_utils import UCLogger
logger = UCLogger()
detail_id = logger.start_batch_detail(batch_id, activity_name)

try:
    # Auto Loader configuration
    df = (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "binaryFile")
          .option("pathGlobFilter", "*.xlsx")
          .load(source_path)
          .withColumn("processing_timestamp", current_timestamp())
          .withColumn("source_file", input_file_name())
         )

    # Use trigger(availableNow=True) for batch-style streaming
    query = (df.writeStream
             .format("delta")
             .option("checkpointLocation", checkpoint_path)
             .outputMode("append")
             .trigger(availableNow=True)
             .start(target_path)
            )

    query.awaitTermination()

    logger.end_batch_detail(detail_id, status="Succeeded", source_object=source_path, target_object=target_path)

except Exception as e:
    logger.end_batch_detail(detail_id, status="Failed", error_message=str(e))
    raise e
