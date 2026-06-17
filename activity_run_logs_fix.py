# ================================
# 1. Date Logic (Same as before)
# ================================
from datetime import datetime, timedelta

today = datetime.today()

CM = today.strftime("%Y-%m")  # current month
first = today.replace(day=1)
prev_month = first - timedelta(days=1)
PM = prev_month.strftime("%Y-%m")  # previous month

date_list = [CM, PM]


# ================================
# 2. ABFSS Source Paths (All Environments)
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
# 3. Build Paths (Fixed Wildcards & Slashes)
# ================================

paths = []

for date in date_list:
    yyyy = int(date[0:4])
    mm = int(date[5:7])

    for base in base_paths:
        # rstrip("/") prevents double slashes.
        # resourceId=** allows Spark to find the nested y= and m= folders regardless of depth.
        path = f"{base.rstrip('/')}/resourceId=**/y={yyyy}/m={format(mm, '02d')}/*"
        paths.append(path)

print(f"Total source paths: {len(paths)}")


# ================================
# 4. Read JSON Logs (With Error Handling)
# ================================
from pyspark.sql.types import StructType, StructField, StringType

# CRITICAL: Ignore missing paths so the whole job doesn't fail if one account is empty
spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")
spark.conf.set("spark.sql.files.ignoreEmptyFiles", "true")

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

df = spark.read.schema(schema).json(paths)
df = df.repartition(128)


# ================================
# 5. JSON Extraction Logic (Optimized for correctness)
# ================================
import json
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, MapType

def iget(d, k):
    if d is None:
        return None

    if isinstance(d, str):
        try:
            d = json.loads(d)
        except (ValueError, TypeError):
            return None

    if isinstance(d, list):
        if d:
            d = d[0]
        else:
            return None

    if not isinstance(d, dict):
        return None

    if len(k) == 1:
        return d.get(k[0])
    else:
        return iget(d.get(k[0]), k[1:])


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

    # To avoid multiple json.loads of the root properties string,
    # we can parse it once if it's a string.
    try:
        data = json.loads(properties) if isinstance(properties, str) else properties
    except:
        return None

    output = None
    for m in mapping:
        output = iget(data, m)
        if output:
            break

    return output


activityPipelineRunIdUDF = udf(get_activityPipelineRunId)
notebookPathUDF = udf(get_notebookpath)
runPageUrlUDF = udf(get_runpageURL)
baseParametersUDF = udf(get_baseParameters, MapType(StringType(), StringType()))
errorUDF = udf(get_error, MapType(StringType(), StringType()))
logicalJobNameUDF = udf(get_logical_jobname)


# ================================
# 6. Apply Transformations
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
# 7. Add Metadata
# ================================
from pyspark.sql.functions import lit, current_timestamp

df3 = df2.withColumn("data_load_for", lit(str(date_list)))
df3 = df3.withColumn("record_update_time", current_timestamp())

df3.filter(df3.activityType == "DatabricksNotebook").show()


# ================================
# 8. Write to Delta (External Location)
# ================================

target_path = "abfss://<your-target-container>@<storage-account>.dfs.core.windows.net/<path>/ADFActivityRunLogs"

df3.repartition(16).write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(target_path)


# ================================
# 9. Create Table (Unity Catalog)
# ================================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS <catalog>.<schema>.ADFActivityRunLogs
USING DELTA
LOCATION '{target_path}'
""")
