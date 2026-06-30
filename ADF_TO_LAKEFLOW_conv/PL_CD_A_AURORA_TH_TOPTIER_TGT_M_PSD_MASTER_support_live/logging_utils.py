import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

class UCLogger:
    def __init__(self, catalog="main", schema="etl_log"):
        self.spark = SparkSession.builder.getOrCreate()
        self.catalog = catalog
        self.schema = schema
        self.batch_table = f"{catalog}.{schema}.batch"
        self.batch_detail_table = f"{catalog}.{schema}.batch_detail"

    def start_batch(self, job_name, run_id, slice_date_time=None, parent_batch_id=None):
        now = datetime.datetime.now()
        # Inserting and getting identity might be tricky with Delta if not using a specific approach
        # For simplicity in this conversion, we assume the table has an identity column
        query = f"""
            INSERT INTO {self.batch_table} (job_name, run_id, start_time, status, slice_date_time, parent_batch_id)
            VALUES ('{job_name}', '{run_id}', current_timestamp(), 'Running', '{slice_date_time or now}', {parent_batch_id or 'NULL'})
        """
        self.spark.sql(query)
        # Get the last generated batch_id
        batch_id = self.spark.sql(f"SELECT max(batch_id) FROM {self.batch_table} WHERE run_id = '{run_id}'").collect()[0][0]
        return batch_id

    def end_batch(self, batch_id, status="Succeeded"):
        query = f"""
            UPDATE {self.batch_table}
            SET end_time = current_timestamp(), status = '{status}'
            WHERE batch_id = {batch_id}
        """
        self.spark.sql(query)

    def start_batch_detail(self, batch_id, activity_name):
        query = f"""
            INSERT INTO {self.batch_detail_table} (batch_id, activity_name, start_time, status)
            VALUES ({batch_id}, '{activity_name}', current_timestamp(), 'Running')
        """
        self.spark.sql(query)
        batch_detail_id = self.spark.sql(f"SELECT max(batch_detail_id) FROM {self.batch_detail_table} WHERE batch_id = {batch_id} AND activity_name = '{activity_name}'").collect()[0][0]
        return batch_detail_id

    def end_batch_detail(self, batch_detail_id, status="Succeeded", source_name=None, source_object=None,
                         target_object=None, source_row_count=0, target_row_count=0, error_row_count=0,
                         error_message=None):

        err_msg = f"'{error_message}'" if error_message else "NULL"
        src_name = f"'{source_name}'" if source_name else "NULL"
        src_obj = f"'{source_object}'" if source_object else "NULL"
        tgt_obj = f"'{target_object}'" if target_object else "NULL"

        query = f"""
            UPDATE {self.batch_detail_table}
            SET end_time = current_timestamp(),
                status = '{status}',
                source_name = {src_name},
                source_object = {src_obj},
                target_object = {tgt_obj},
                source_row_count = {source_row_count},
                target_row_count = {target_row_count},
                error_row_count = {error_row_count},
                error_message = {err_msg}
            WHERE batch_detail_id = {batch_detail_id}
        """
        self.spark.sql(query)
