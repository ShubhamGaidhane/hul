# Databricks notebook source
# Processed Layer: Landed (Bronze) to Processed (Silver)
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from logging_utils import UCLogger

# Parameters
dbutils.widgets.text("batch_id", "0")
dbutils.widgets.text("source_path", "abfss://unilever@dbstorageda06b80066adls.dfs.core.windows.net/UniversalDataLake/InternalSources/TopTier/BlobFileShare/TH/TargetMasterPSD/LandedFiles/")
dbutils.widgets.text("target_path", "abfss://unilever@dbstorageda06b80066adls.dfs.core.windows.net/UniversalDataLake/InternalSources/TopTier/BlobFileShare/TH/TargetMasterPSD/Processed_Parquet/")

batch_id = int(dbutils.widgets.get("batch_id"))
source_path = dbutils.widgets.get("source_path")
target_path = dbutils.widgets.get("target_path")

activity_name = "AT_CPY_CD_A_AURORA_TH_TOPTIER_TGT_M_PSD_PROCESSED"

# Logging start
logger = UCLogger()
detail_id = logger.start_batch_detail(batch_id, activity_name)

try:
    # Read from Landed (Delta/Bronze)
    df_landed = spark.read.format("delta").load(source_path)

    # Write to Processed (Silver)
    (df_landed.write
     .format("delta")
     .mode("append")
     .save(target_path)
    )

    row_count = df_landed.count()

    logger.end_batch_detail(detail_id, status="Succeeded", source_object=source_path,
                           target_object=target_path, source_row_count=row_count, target_row_count=row_count)

except Exception as e:
    logger.end_batch_detail(detail_id, status="Failed", error_message=str(e))
    raise e
