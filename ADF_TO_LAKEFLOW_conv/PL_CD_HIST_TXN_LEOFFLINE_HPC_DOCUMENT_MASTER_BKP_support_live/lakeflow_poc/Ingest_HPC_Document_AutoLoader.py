# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest_HPC_Document_AutoLoader
# MAGIC This notebook ingests CSV files from ADLS Gen2 using Auto Loader and writes to a Bronze Delta table.

# COMMAND ----------

# DBTITLE 1, Define Widgets (Parameters)
dbutils.widgets.text("source_path", "abfss://unilever@dbstorageda06b80066adls.dfs.core.windows.net/UniversalDataLake/InternalSources/LeveredgeOffline/AWS/SQLServer/HPC/Indonesia/VDLDocument/LandedFiles", "Source Path")
dbutils.widgets.text("checkpoint_path", "/mnt/lakeflow_poc/checkpoints/hpc_document_bronze", "Checkpoint Path")
dbutils.widgets.text("target_table", "bronze_hpc_document", "Target Bronze Table")

source_path = dbutils.widgets.get("source_path")
checkpoint_path = dbutils.widgets.get("checkpoint_path")
target_table = dbutils.widgets.get("target_table")

# COMMAND ----------

# DBTITLE 1, Auto Loader Ingestion Logic
from pyspark.sql.functions import input_file_name, current_timestamp

# Define the schema based on the ADF dataset
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DecimalType, IntegerType, LongType

schema = StructType([
    StructField("CompanyCode", StringType(), True),
    StructField("DistributorCode", StringType(), True),
    StructField("DocumentNumber", StringType(), True),
    StructField("DocumentType", StringType(), True),
    StructField("ProductCode", StringType(), True),
    StructField("ProductType", StringType(), True),
    StructField("BatchCode", StringType(), True),
    StructField("DetailID", StringType(), True),
    StructField("OutletCode", StringType(), True),
    StructField("RouteCode", StringType(), True),
    StructField("DeliverRouteCode", StringType(), True),
    StructField("DocumentDate", TimestampType(), True),
    StructField("DeliverDate", TimestampType(), True),
    StructField("DeliverQuanitity", DecimalType(32, 10), True),
    StructField("GrossAmount", DecimalType(18, 2), True),
    StructField("TaxAmount", DecimalType(18, 2), True),
    StructField("DiscountAmount", DecimalType(20, 2), True),
    StructField("FreeGoodsIndicator", IntegerType(), True),
    StructField("DiscountCode", StringType(), True),
    StructField("ReasonCode", StringType(), True),
    StructField("ReferenceDocumentNumber", StringType(), True),
    StructField("iRowVersion", LongType(), True),
    StructField("ModifyUser", StringType(), True),
    StructField("ModifyDate", TimestampType(), True),
    StructField("DocumentStatus", StringType(), True)
])

# Read using Auto Loader
df = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("delimiter", "|")
      .option("header", "true")
      .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
      .schema(schema)
      .load(source_path)
      .withColumn("_source_file", input_file_name())
      .withColumn("_ingestion_time", current_timestamp())
     )

# Write to Bronze Delta Table
query = (df.writeStream
         .format("delta")
         .option("checkpointLocation", checkpoint_path)
         .trigger(availableNow=True)
         .outputMode("append")
         .table(target_table)
         .start()
        )

# Ensure the query completes before exiting the notebook
query.awaitTermination()
