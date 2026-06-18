# ================================
# 1. Date Logic (Excluding Today)
# ================================
from datetime import datetime, timedelta

today = datetime.today()
yesterday = today - timedelta(days=1)

def get_days_in_month(y, m):
    if m == 12:
        return (datetime(y+1, 1, 1) - datetime(y, m, 1)).days
    return (datetime(y, m+1, 1) - datetime(y, m, 1)).days

# Current Month Logic (up to yesterday)
cm_y, cm_m = yesterday.year, yesterday.month
cm_days = [f"{d:02d}" for d in range(1, yesterday.day + 1)]
cm_glob = "{" + ",".join(cm_days) + "}"

# Previous Month Logic (full month)
first_of_cm = today.replace(day=1)
last_of_pm = first_of_cm - timedelta(days=1)
pm_y, pm_m = last_of_pm.year, last_of_pm.month
pm_days = [f"{d:02d}" for d in range(1, get_days_in_month(pm_y, pm_m) + 1)]
pm_glob = "{" + ",".join(pm_days) + "}"

print(f"Current Month Glob: {cm_y}-{cm_m:02d}/d={cm_glob}")
print(f"Previous Month Glob: {pm_y}-{pm_m:02d}/d={pm_glob}")

# ================================
# 2. ABFSS Source Paths
# ================================

base_paths = [
    "abfss://insights-logs-activityruns@dbstorageda05p80010adls.dfs.core.windows.net/",
    "abfss://insights-logs-activityruns@dbstorageda09p901404adls.dfs.core.windows.net/",
    "abfss://insights-logs-activityruns@dbstorageda18p80049adls.dfs.core.windows.net/",
    "abfss://insights-logs-activityruns@dbstorageda18p80083adls.dfs.core.windows.net/",
    "abfss://insights-logs-activityruns@dbstorageda18p80115adls.dfs.core.windows.net/",
    "abfss://insights-logs-activityruns@dbstorageda18p80195adls.dfs.core.windows.net/",
    "abfss://insights-logs-activityruns@dbstorageda18p901996adls.dfs.core.windows.net/",
    "abfss://insights-logs-activityruns@dbstorageda18p902655adls.dfs.core.windows.net/",
    "abfss://insights-logs-activityruns@dbstorageda18p902664adls.dfs.core.windows.net/"
]

# ================================
# 3. Path Discovery and Reading
# ================================
from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([
    StructField("pipelineName", StringType(), True),
    StructField("pipelineRunId", StringType(), True),
    StructField("activityName", StringType(), True),
    StructField("activityRunId", StringType(), True),
    StructField("activityType", StringType(), True),
    StructField("correlationId", StringType(), True),
    StructField("operationName", StringType(), True),
    StructField("status", StringType(), True),
    StructField("resourceId", StringType(), True),
    StructField("properties", StringType(), True),
    StructField("start", StringType(), True),
    StructField("end", StringType(), True)
])

final_df = None

for base in base_paths:
    # Check if resourceId directory exists to avoid PATH_NOT_FOUND
    # Since we can't use spark.sql.files.ignoreMissingFiles, we must be sure the root exists
    try:
        # In Databricks, dbutils.fs.ls returns an error if the path doesn't exist
        dbutils.fs.ls(f"{base.rstrip('/')}/resourceId=")
    except:
        print(f"Skipping {base} - resourceId folder not found.")
        continue

    # Build paths for Current and Previous Months
    paths = [
        f"{base.rstrip('/')}/resourceId=/*/*/*/*/*/*/*/*/y={cm_y}/m={cm_m:02d}/d={cm_glob}/*/*/*.json",
        f"{base.rstrip('/')}/resourceId=/*/*/*/*/*/*/*/*/y={pm_y}/m={pm_m:02d}/d={pm_glob}/*/*/*.json"
    ]

    try:
        temp_df = spark.read.schema(schema).json(paths)
        if final_df is None:
            final_df = temp_df
        else:
            final_df = final_df.unionByName(temp_df)
    except Exception as e:
        # This handles cases where specific months/days are missing for this account
        print(f"No logs found for {base} in the requested period.")

if final_df is None:
    raise Exception("No logs found in any of the provided storage accounts.")

df = final_df.repartition(128)

# ================================
# 4. JSON Extraction Logic (Recursive iget)
# ================================
import json
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, MapType

def iget(d, k):
    if d is None: return None
    if isinstance(d, str):
        try: d = json.loads(d)
        except: return None
    if isinstance(d, list):
        if d: d = d[0]
        else: return None
    if not isinstance(d, dict): return None

    if len(k) == 1: return d.get(k[0])
    else: return iget(d.get(k[0]), k[1:])

def get_activityPipelineRunId(properties):
    return iget(properties, ['Output', 'pipelineRunId'])

def get_notebookpath(properties):
    return iget(properties, ['Input', 'notebookPath'])

def get_runpageURL(properties):
    return iget(properties, ['Output', 'runPageUrl'])

def get_baseParameters(properties):
    return iget(properties, ['Input', 'baseParameters'])

def get_error(properties):
    return iget(properties, ['Error'])

def get_logical_jobname(properties):
    mapping = [
        ['Input', 'baseParameters', 'LogicalJobName'],
        ['Input', 'baseParameters', 'Logical_JobName'],
        ['Input', 'baseParameters', 'logical_jobname'],
        ['Input', 'baseParameters', 'inParam_LogicalPLName'],
        ['Input', 'baseParameters', 'inParam_LogicalJobName'],
        ['Input', 'baseParameters', 'inParamFileDetailsJSON', 'LogicalJobName'],
        ['Input', 'baseParameters', 'inParamFileDetailsJSON', 'Logical_Jobname'],
        ['Input', 'baseParameters', 'inParamFileDetailsJSON', 'logical_jobname'],
    ]
    try:
        data = json.loads(properties) if isinstance(properties, str) else properties
    except:
        return None
    for m in mapping:
        output = iget(data, m)
        if output: return output
    return None

activityPipelineRunIdUDF = udf(get_activityPipelineRunId)
notebookPathUDF = udf(get_notebookpath)
runPageUrlUDF = udf(get_runpageURL)
baseParametersUDF = udf(get_baseParameters, MapType(StringType(), StringType()))
errorUDF = udf(get_error, MapType(StringType(), StringType()))
logicalJobNameUDF = udf(get_logical_jobname)

# ================================
# 5. Apply Transformations
# ================================
df2 = df.select(
    "*",
    activityPipelineRunIdUDF(col("properties")).alias("activityPipelineRunId"),
    notebookPathUDF(col("properties")).alias("ADB_notebookPath"),
    logicalJobNameUDF(col("properties")).alias("ADB_logicalJobName"),
    runPageUrlUDF(col("properties")).alias("ADB_runPageUrl"),
    baseParametersUDF(col("properties")).alias("ADB_notebook_baseParameters"),
    errorUDF(col("properties")).alias("error")
)

# ================================
# 6. Add Metadata
# ================================
from pyspark.sql.functions import lit, current_timestamp

df3 = df2.withColumn("data_load_for", lit(f"PM: {pm_y}-{pm_m:02d}, CM: {cm_y}-{cm_m:02d} up to day {yesterday.day}"))
df3 = df3.withColumn("record_update_time", current_timestamp())

df3.filter(df3.activityType == "DatabricksNotebook").show()

# ================================
# 7. Write to Delta (External Location)
# ================================
target_path = "abfss://<your-target-container>@<storage-account>.dfs.core.windows.net/<path>/ADFActivityRunLogs"

df3.repartition(16).write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(target_path)

# ================================
# 8. Create Table (Unity Catalog)
# ================================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS <catalog>.<schema>.ADFActivityRunLogs
USING DELTA
LOCATION '{target_path}'
""")
