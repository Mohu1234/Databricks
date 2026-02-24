"""
Audit Logger Module for SCD Jobs

Provides robust audit logging functionality with explicit schema definition
to avoid Spark type inference issues.
"""

from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, LongType


def log_audit(
    spark, 
    job_name, 
    batch_id, 
    table_name, 
    operation_type, 
    start_time, 
    end_time, 
    source_count, 
    inserted, 
    updated, 
    deleted, 
    active_count, 
    historical_count, 
    status, 
    error_msg, 
    user,
    audit_table="workspace.default.scd_job_audit"
):
    """
    Log SCD job execution details to audit table.
    
    Args:
        spark: SparkSession
        job_name: Name of the SCD job
        batch_id: Batch identifier
        table_name: Target table name
        operation_type: Type of operation (SCD2, INCREMENTAL, etc.)
        start_time: Job start timestamp
        end_time: Job end timestamp
        source_count: Number of source records
        inserted: Number of records inserted
        updated: Number of records updated
        deleted: Number of records deleted
        active_count: Total active records in target
        historical_count: Total historical records in target
        status: Job status (SUCCESS/FAILED)
        error_msg: Error message if failed (None if success)
        user: User who executed the job
        audit_table: Audit table name (default: workspace.default.scd_job_audit)
    
    Returns:
        None
    """
    duration = (end_time - start_time).total_seconds()
    
    # Define explicit schema to avoid type inference issues
    # This prevents [CANNOT_DETERMINE_TYPE] errors when values are 0 or None
    schema = StructType([
        StructField("job_name", StringType(), True),
        StructField("batch_id", StringType(), True),
        StructField("table_name", StringType(), True),
        StructField("operation_type", StringType(), True),
        StructField("start_time", TimestampType(), True),
        StructField("end_time", TimestampType(), True),
        StructField("duration_seconds", DoubleType(), True),
        StructField("source_record_count", LongType(), True),
        StructField("records_inserted", LongType(), True),
        StructField("records_updated", LongType(), True),
        StructField("records_deleted", LongType(), True),
        StructField("total_active_records", LongType(), True),
        StructField("total_historical_records", LongType(), True),
        StructField("status", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("execution_user", StringType(), True),
        StructField("created_timestamp", TimestampType(), True)
    ])
    
    audit_data = [(
        job_name, batch_id, table_name, operation_type,
        start_time, end_time, duration,
        source_count, inserted, updated, deleted,
        active_count, historical_count,
        status, error_msg, user, datetime.now()
    )]
    
    try:
        audit_df = spark.createDataFrame(audit_data, schema)
        audit_df.write.format("delta").mode("append").saveAsTable(audit_table)
    except Exception as e:
        # If audit logging fails, print error but don't fail the job
        print(f"⚠️ Warning: Failed to write audit log: {str(e)}")
        import traceback
        traceback.print_exc()