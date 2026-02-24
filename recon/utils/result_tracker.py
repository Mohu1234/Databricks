"""
Result Tracker Module

Stores reconciliation results in Delta tables:
- Summary results table (high-level metrics)
- Details table (sample mismatches, missing records)
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from datetime import datetime
from typing import Dict, Any
import json


class ResultTracker:
    """Tracks and stores reconciliation results"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def create_results_table(self, table_name: str):
        """Create the reconciliation results summary table"""
        schema = StructType([
            StructField("recon_id", StringType(), False),
            StructField("recon_name", StringType(), False),
            StructField("batch_id", StringType(), False),
            StructField("execution_time", TimestampType(), False),
            StructField("status", StringType(), False),
            StructField("source_table", StringType(), False),
            StructField("target_table", StringType(), False),
            StructField("source_count", IntegerType(), True),
            StructField("target_count", IntegerType(), True),
            StructField("matched_count", IntegerType(), True),
            StructField("match_rate", DoubleType(), True),
            StructField("missing_count", IntegerType(), True),
            StructField("extra_count", IntegerType(), True),
            StructField("mismatch_count", IntegerType(), True),
            StructField("source_duplicates", IntegerType(), True),
            StructField("target_duplicates", IntegerType(), True),
            StructField("schema_status", StringType(), True),
            StructField("data_status", StringType(), True),
            StructField("summary", StringType(), True),
            StructField("duration_seconds", DoubleType(), True)
        ])
        
        # Create empty DataFrame with schema
        empty_df = self.spark.createDataFrame([], schema)
        
        # Create table if not exists
        if not self.spark.catalog.tableExists(table_name):
            empty_df.write.format("delta").saveAsTable(table_name)
            print(f"✓ Created results table: {table_name}")
        else:
            print(f"✓ Results table already exists: {table_name}")
    
    def create_details_table(self, table_name: str):
        """Create the reconciliation details table for storing samples"""
        schema = StructType([
            StructField("recon_id", StringType(), False),
            StructField("recon_name", StringType(), False),
            StructField("batch_id", StringType(), False),
            StructField("execution_time", TimestampType(), False),
            StructField("issue_type", StringType(), False),  # MISSING, EXTRA, MISMATCH, DUPLICATE
            StructField("record_keys", StringType(), True),  # JSON of key values
            StructField("source_values", StringType(), True),  # JSON of source values
            StructField("target_values", StringType(), True),  # JSON of target values
            StructField("details", StringType(), True)  # Additional details
        ])
        
        empty_df = self.spark.createDataFrame([], schema)
        
        if not self.spark.catalog.tableExists(table_name):
            empty_df.write.format("delta").saveAsTable(table_name)
            print(f"✓ Created details table: {table_name}")
        else:
            print(f"✓ Details table already exists: {table_name}")
    
    def log_results(
        self,
        results_table: str,
        recon_id: str,
        recon_name: str,
        batch_id: str,
        schema_results: Dict[str, Any],
        data_results: Dict[str, Any],
        duration_seconds: float
    ):
        """Log reconciliation results to summary table"""
        execution_time = datetime.now()
        
        # Determine overall status
        overall_status = "PASSED"
        if schema_results["status"] == "FAILED" or data_results["status"] == "FAILED":
            overall_status = "FAILED"
        
        # Prepare data
        result_data = [(
            recon_id,
            recon_name,
            batch_id,
            execution_time,
            overall_status,
            data_results["source_table"],
            data_results["target_table"],
            data_results["source_count"],
            data_results["target_count"],
            data_results["matched_count"],
            data_results["match_rate"],
            data_results["missing_records"]["count"],
            data_results["extra_records"]["count"],
            data_results["mismatched_records"]["count"],
            data_results["source_duplicates"]["count"],
            data_results["target_duplicates"]["count"],
            schema_results["status"],
            data_results["status"],
            f"Schema: {schema_results['summary']}; Data: {data_results['summary']}",
            duration_seconds
        )]
        
        # Create DataFrame
        result_df = self.spark.createDataFrame(result_data, schema=self._get_results_schema())
        
        # Append to table
        result_df.write.format("delta").mode("append").saveAsTable(results_table)
        print(f"✓ Logged results to {results_table}")
    
    def log_details(
        self,
        details_table: str,
        recon_id: str,
        recon_name: str,
        batch_id: str,
        data_results: Dict[str, Any]
    ):
        """Log detailed mismatch samples to details table"""
        execution_time = datetime.now()
        details_data = []
        
        # Log missing records
        for sample in data_results["missing_records"]["samples"]:
            details_data.append((
                recon_id,
                recon_name,
                batch_id,
                execution_time,
                "MISSING",
                json.dumps(sample),
                json.dumps(sample),
                None,
                "Record exists in source but not in target"
            ))
        
        # Log extra records
        for sample in data_results["extra_records"]["samples"]:
            details_data.append((
                recon_id,
                recon_name,
                batch_id,
                execution_time,
                "EXTRA",
                json.dumps(sample),
                None,
                json.dumps(sample),
                "Record exists in target but not in source"
            ))
        
        # Log mismatched records
        for sample in data_results["mismatched_records"]["samples"]:
            details_data.append((
                recon_id,
                recon_name,
                batch_id,
                execution_time,
                "MISMATCH",
                json.dumps({k: v for k, v in sample.items() if k not in ["source_hash", "target_hash"]}),
                sample.get("source_hash"),
                sample.get("target_hash"),
                "Record values differ between source and target"
            ))
        
        # Log source duplicates
        for sample in data_results["source_duplicates"]["samples"]:
            details_data.append((
                recon_id,
                recon_name,
                batch_id,
                execution_time,
                "SOURCE_DUPLICATE",
                json.dumps(sample),
                json.dumps(sample),
                None,
                f"Duplicate key found in source (count: {sample.get('cnt', 0)})"
            ))
        
        # Log target duplicates
        for sample in data_results["target_duplicates"]["samples"]:
            details_data.append((
                recon_id,
                recon_name,
                batch_id,
                execution_time,
                "TARGET_DUPLICATE",
                json.dumps(sample),
                None,
                json.dumps(sample),
                f"Duplicate key found in target (count: {sample.get('cnt', 0)})"
            ))
        
        if details_data:
            details_df = self.spark.createDataFrame(details_data, schema=self._get_details_schema())
            details_df.write.format("delta").mode("append").saveAsTable(details_table)
            print(f"✓ Logged {len(details_data)} detail records to {details_table}")
        else:
            print("✓ No details to log (perfect match)")
    
    def _get_results_schema(self):
        """Get schema for results table"""
        return StructType([
            StructField("recon_id", StringType(), False),
            StructField("recon_name", StringType(), False),
            StructField("batch_id", StringType(), False),
            StructField("execution_time", TimestampType(), False),
            StructField("status", StringType(), False),
            StructField("source_table", StringType(), False),
            StructField("target_table", StringType(), False),
            StructField("source_count", IntegerType(), True),
            StructField("target_count", IntegerType(), True),
            StructField("matched_count", IntegerType(), True),
            StructField("match_rate", DoubleType(), True),
            StructField("missing_count", IntegerType(), True),
            StructField("extra_count", IntegerType(), True),
            StructField("mismatch_count", IntegerType(), True),
            StructField("source_duplicates", IntegerType(), True),
            StructField("target_duplicates", IntegerType(), True),
            StructField("schema_status", StringType(), True),
            StructField("data_status", StringType(), True),
            StructField("summary", StringType(), True),
            StructField("duration_seconds", DoubleType(), True)
        ])
    
    def _get_details_schema(self):
        """Get schema for details table"""
        return StructType([
            StructField("recon_id", StringType(), False),
            StructField("recon_name", StringType(), False),
            StructField("batch_id", StringType(), False),
            StructField("execution_time", TimestampType(), False),
            StructField("issue_type", StringType(), False),
            StructField("record_keys", StringType(), True),
            StructField("source_values", StringType(), True),
            StructField("target_values", StringType(), True),
            StructField("details", StringType(), True)
        ])
