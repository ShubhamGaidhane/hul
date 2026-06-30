# Databricks notebook source
# Master Orchestration: Replaces PL_CD_A_AURORA_TH_TOPTIER_TGT_M_PSD_MASTER
import os
from logging_utils import UCLogger

# Parameters
dbutils.widgets.text("SliceStartTime", "2018-01-01")
slice_start_time = dbutils.widgets.get("SliceStartTime")
job_name = "PL_CD_A_AURORA_TH_TOPTIER_TGT_M_PSD_MASTER"

# Try to get run_id from context
try:
    run_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().jobRunId().getOrElse(lambda: "local")
except:
    run_id = "local"

logger = UCLogger()
batch_id = logger.start_batch(job_name, run_id, slice_date_time=slice_start_time)

try:
    # 1. Execute Landed Task
    print("Starting Landed Task...")
    dbutils.notebook.run("landed_autoloader", 3600, {"batch_id": str(batch_id)})

    # 2. Wait (Simulating AT_WAIT_3)
    import time
    time.sleep(3)

    # 3. Execute Processed Task
    print("Starting Processed Task...")
    dbutils.notebook.run("processed_task", 3600, {"batch_id": str(batch_id)})

    logger.end_batch(batch_id, status="Succeeded")
    print("Master Workflow Completed Successfully.")

except Exception as e:
    logger.end_batch(batch_id, status="Failed")
    print(f"Master Workflow Failed: {str(e)}")
    raise e
