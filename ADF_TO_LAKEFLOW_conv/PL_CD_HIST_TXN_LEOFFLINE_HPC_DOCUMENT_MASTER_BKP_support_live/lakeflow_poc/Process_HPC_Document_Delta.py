# Databricks notebook source
# MAGIC %md
# MAGIC # Process_HPC_Document_Delta
# MAGIC This notebook processes data from the Bronze table to the Silver table, applying deduplication and incremental merge.

# COMMAND ----------

# DBTITLE 1, Define Widgets (Parameters)
dbutils.widgets.text("source_table", "bronze_hpc_document", "Source Bronze Table")
dbutils.widgets.text("target_table", "silver_hpc_document", "Target Silver Table")
dbutils.widgets.text("primary_keys", "CompanyCode,DistributorCode,DocumentNumber,ProductCode,DetailID", "Primary Keys")

source_table = dbutils.widgets.get("source_table")
target_table = dbutils.widgets.get("target_table")
pk_list = dbutils.widgets.get("primary_keys").split(",")

# COMMAND ----------

# DBTITLE 1, Transformation and Merge Logic
from delta.tables import DeltaTable
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

# Read from Bronze
bronze_df = spark.table(source_table)

# Deduplicate (taking the latest version based on iRowVersion or _ingestion_time)
window_spec = Window.partitionBy(*pk_list).orderBy(bronze_df["iRowVersion"].desc(), bronze_df["_ingestion_time"].desc())
dedup_df = bronze_df.withColumn("rn", row_number().over(window_spec)).filter("rn = 1").drop("rn")

# COMMAND ----------

# DBTITLE 1, Merge into Silver
if not spark.catalog.tableExists(target_table):
    # Initial load
    dedup_df.write.format("delta").saveAsTable(target_table)
else:
    # Merge
    silver_table = DeltaTable.forName(spark, target_table)

    merge_condition = " AND ".join([f"s.{pk} = t.{pk}" for pk in pk_list])

    silver_table.alias("t").merge(
        dedup_df.alias("s"),
        merge_condition
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
