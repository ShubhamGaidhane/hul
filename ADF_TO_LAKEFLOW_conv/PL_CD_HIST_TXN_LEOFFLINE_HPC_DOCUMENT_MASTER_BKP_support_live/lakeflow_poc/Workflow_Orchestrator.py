# Databricks notebook source
# MAGIC %md
# MAGIC # Workflow_Orchestrator
# MAGIC This notebook acts as a helper to manage logging to the SQL Server legacy system from Databricks using JDBC.

# COMMAND ----------

# DBTITLE 1, Define Widgets
dbutils.widgets.text("sp_name", "", "Stored Procedure Name")
dbutils.widgets.text("sp_params", "{}", "Stored Procedure Parameters (JSON)")
dbutils.widgets.text("secret_scope", "kv", "Databricks Secret Scope")
dbutils.widgets.text("secret_key", "mcsConnectionString", "Secret Key for Connection String")

# COMMAND ----------

# DBTITLE 1, JDBC Connection and Execution Logic
import json

def get_connection_details():
    scope = dbutils.widgets.get("secret_scope")
    key = dbutils.widgets.get("secret_key")
    # In a real environment, the secret would contain the full JDBC string or components
    # connection_string = dbutils.secrets.get(scope, key)

    # Placeholder for POC demonstration
    jdbc_url = "jdbc:sqlserver://bnldmuw200016:1635;database=Centegy_SnDPro_UID_HPC"
    connection_properties = {
        "user": "UDLPOC",
        "password": "REDACTED", # Should be fetched from secrets
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    return jdbc_url, connection_properties

def execute_stored_procedure(sp_name, params):
    if not sp_name:
        print("No SP name provided, skipping.")
        return

    jdbc_url, connection_properties = get_connection_details()

    # Construct the SQL call
    # e.g., EXEC [EtlLog].[uspBatchStartLog] @JobName='...', @RunID='...'
    param_str = ", ".join([f"@{k}='{v}'" if isinstance(v, str) else f"@{k}={v}" for k, v in params.items()])
    sql = f"EXEC {sp_name} {param_str}"

    print(f"Executing SQL: {sql}")

    try:
        # Use Spark to execute the SP via JDBC
        # Note: For SPs that don't return a result set, you might need a direct JDBC connection via jaydebeapi or similar
        # or use a dummy select to wrap the EXEC if using spark.read.jdbc
        spark.read.jdbc(url=jdbc_url, table=f"({sql}) as result", properties=connection_properties).collect()
        print("SP executed successfully.")
    except Exception as e:
        print(f"Error executing SP: {e}")

# COMMAND ----------

# DBTITLE 1, Main Execution
sp_name = dbutils.widgets.get("sp_name")
sp_params = json.loads(dbutils.widgets.get("sp_params"))

if sp_name:
    execute_stored_procedure(sp_name, sp_params)
else:
    print("Orchestrator called without SP name. Used for module import or manual runs.")
