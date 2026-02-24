"""
SCD Job Runner Module

Orchestrates SCD job execution:
- Reads YAML configuration
- Filters tables based on parameters
- Processes each table using SCDProcessor
- Logs results using audit_logger
"""

import yaml
from datetime import datetime
from typing import Optional, List
from .audit_logger import log_audit
from .scd_processor import SCDProcessor


class SCDJobRunner:
    """Orchestrates SCD job execution"""
    
    def __init__(self, spark, config_path: str):
        self.spark = spark
        self.config_path = config_path
        self.config = self._load_config()
        self.current_user = spark.sql("SELECT current_user() as user").collect()[0]["user"]
    
    def _load_config(self):
        with open(self.config_path) as f:
            return yaml.safe_load(f)
    
    def filter_tables(self, table_names: Optional[List[str]] = None):
        if table_names:
            original_count = len(self.config["tables"])
            self.config["tables"] = [t for t in self.config["tables"] if t["name"] in table_names]
            print(f"🎯 Filtered: {original_count} → {len(self.config['tables'])} table(s)")
            return len(self.config["tables"])
        else:
            print(f"🎯 Running all {len(self.config['tables'])} configured tables")
            return len(self.config["tables"])
    
    def run(self, table_names: Optional[List[str]] = None):
        table_count = self.filter_tables(table_names)
        if table_count == 0:
            raise ValueError("❌ No tables to process!")
        
        batch_id = self.config["job"]["batch_id"]
        job_name = self.config["job"]["name"]
        processor = SCDProcessor(self.spark, batch_id)
        job_start_time = datetime.now()
        
        print("="*80)
        print(f"SCD Job: {job_name} | Batch: {batch_id}")
        print(f"Tables: {table_count} | Start: {job_start_time}")
        print("="*80)
        
        results = {"tables_processed": 0, "tables_succeeded": 0, "tables_failed": 0}
        
        for table_cfg in self.config["tables"]:
            table_result = self._process_table(processor, table_cfg, job_name, batch_id)
            results["tables_processed"] += 1
            if table_result["status"] == "SUCCESS":
                results["tables_succeeded"] += 1
            else:
                results["tables_failed"] += 1
        
        print(f"\n{'='*80}")
        print(f"✓ Succeeded: {results['tables_succeeded']} | ✗ Failed: {results['tables_failed']}")
        print(f"{'='*80}")
        return results
    
    def _process_table(self, processor, table_cfg, job_name, batch_id):
        table_start_time = datetime.now()
        table_name = table_cfg["target"]["table_name"]
        load_type = table_cfg.get("load_type", "scd2")
        
        print(f"\nProcessing: {table_cfg['name']} ({load_type})")
        
        status = "SUCCESS"
        error_message = None
        stats = {}
        
        try:
            stats = processor.process_table(table_cfg)
            print(f"  ✓ Complete: {stats['active_count']} active, {stats['historical_count']} historical")
            print(f"  📊 Changes: +{stats['inserted']} inserted | ~{stats['updated']} updated | -{stats['deleted']} deleted")
        except Exception as e:
            status = "FAILED"
            error_message = str(e)
            print(f"  ✗ Error: {error_message}")
        
        table_end_time = datetime.now()
        
        try:
            log_audit(
                spark=self.spark, job_name=job_name, batch_id=batch_id,
                table_name=table_name, operation_type=load_type.upper(),
                start_time=table_start_time, end_time=table_end_time,
                source_count=stats.get('source_count', 0),
                inserted=stats.get('inserted', 0),
                updated=stats.get('updated', 0),
                deleted=stats.get('deleted', 0),
                active_count=stats.get('active_count', 0),
                historical_count=stats.get('historical_count', 0),
                status=status, error_msg=error_message, user=self.current_user
            )
        except Exception as audit_error:
            print(f"  ⚠️  Audit logging failed: {audit_error}")
        
        return {"status": status, "stats": stats}


def run_scd_job(spark, config_path: str, table_names: Optional[List[str]] = None):
    """
    Convenience function to run SCD job
    
    Example:
        # Run all tables
        results = run_scd_job(spark, "/path/to/config.yaml")
        
        # Run specific table
        results = run_scd_job(spark, "/path/to/config.yaml", ["product_current"])
    """
    runner = SCDJobRunner(spark, config_path)
    return runner.run(table_names)
