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
# 2. ABFSS Source Paths (ADLS)
# ================================

base_paths = [
    "abfss://insights-logs-pipelineruns@dbstorageda05p80010adls.dfs.core.windows.net/",
    "abfss://insights-logs-pipelineruns@dbstorageda09p901404adls.dfs.core.windows.net/",
    "abfss://insights-logs-pipelineruns@dbstorageda18p80049adls.dfs.core.windows.net/",
    "abfss://insights-logs-pipelineruns@dbstorageda09p901404adls.dfs.core.windows.net/", # Retained original account list
    "abfss://insights-logs-pipelineruns@dbstorageda18p80115adls.dfs.core.windows.net/",
    "abfss://insights-logs-pipelineruns@dbstorageda18p80195adls.dfs.core.windows.net/",
    "abfss://insights-logs-pipelineruns@dbstorageda18p901996adls.dfs.core.windows.net/",
    "abfss://insights-logs-pipelineruns@dbstorageda18p902655adls.dfs.core.windows.net/",
    "abfss://insights-logs-pipelineruns@dbstorageda18p902664adls.dfs.core.windows.net/"
]

# ================================
# 3. Path Discovery and Reading
# ================================
from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([
      StructField("pipelineName",StringType(),True),
      StructField("runId",StringType(),True),
      StructField("resourceId",StringType(),True),
      StructField("correlationId",StringType(),True),
      StructField("status",StringType(),True),
      StructField("groupId",StringType(),True),
      StructField("properties",StringType(),True),
      StructField("start",StringType(),True),
      StructField("end",StringType(),True)
      ])

final_df = None

for base in base_paths:
    try:
        # Programmatic check for resourceId folder to avoid PATH_NOT_FOUND
        dbutils.fs.ls(f"{base.rstrip('/')}/resourceId=")
    except:
        print(f"Skipping {base} - resourceId folder not found.")
        continue

    # Using explicit wildcards for the 8-level resourceId hierarchy
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

def get_logical_jobname(properties):
  # Mapping for Pipeline Runs
  mapping = [
    ['Parameters', 'LogicalJobName'],
    ['Parameters', 'Logical_JobName'],
    ['Parameters', 'logical_jobname'],
    ['Parameters', 'inParam_LogicalPLName'],
    ['Parameters', 'inParam_LogicalJobName'],
    ['Parameters', 'inParamFileDetailsJSON', 'LogicalJobName'],
    ['Parameters', 'inParamFileDetailsJSON', 'Logical_Jobname'],
    ['Parameters', 'inParamFileDetailsJSON', 'logical_jobname']
  ]
  try:
      data = json.loads(properties) if isinstance(properties, str) else properties
  except:
      return None
  for m in mapping:
      output = iget(data, m)
      if output: return output
  return None

def get_invokedByType(properties):
  return iget(properties, ['Predecessors', 'InvokedByType'])

def get_parentPipelineRunId(properties):
  return iget(properties, ['Predecessors', 'PipelineRunId'])

logicalJobNameUDF = udf(get_logical_jobname)
invokedByTypeUDF = udf(get_invokedByType)
parentPipelineRunIdUDF = udf(get_parentPipelineRunId)

# ================================
# 5. Apply Transformations
# ================================
df2 = df.select(
    "*",
    logicalJobNameUDF(col("properties")).alias("ADB_logicalJobName"),
    invokedByTypeUDF(col("properties")).alias("invokedByType"),
    parentPipelineRunIdUDF(col("properties")).alias("invokedByPipelineRunId")
)

# ================================
# 6. Add Metadata
# ================================
from pyspark.sql.functions import lit, current_timestamp

date_list = [f"{cm_y}-{cm_m:02d}", f"{pm_y}-{pm_m:02d}"]
df3 = df2.withColumn("data_load_for", lit(str(date_list)))
df3 = df3.withColumn("record_update_time", current_timestamp())

# ================================
# 7. Write to Delta
# ================================
target_path = '/mnt/adls/centrallake/BusinessDataLake/SC/Logs/ADFPipelineRunLogs'
table_name = 'devops.ADFPipelineRunLogs'
numFiles = 16

df3.repartition(numFiles).write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(target_path)

# ================================
# 8. Create Table (Unity Catalog)
# ================================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {table_name}
USING DELTA
LOCATION '{target_path}'
""")
