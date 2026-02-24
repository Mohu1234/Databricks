"""
Reconciliation Runner Module

Main orchestrator that:
- Reads YAML configuration
- Runs schema validation
- Runs data reconciliation
- Stores results in tracking tables
"""

import yaml
from datetime import datetime
from typing import Dict, Any, List
import uuid

from .schema_validator import SchemaValidator
from .data_reconciler import DataReconciler
from .result_tracker import ResultTracker


class ReconRunner:
    """Orchestrates reconciliation execution"""
    
    def __init__(self, spark, config_path: str):
        self.spark = spark
        self.config_path = config_path
        self.config = self._load_config()
        
        # Initialize components
        self.schema_validator = SchemaValidator(spark)
        self.data_reconciler = DataReconciler(spark)
        self.result_tracker = ResultTracker(spark)
    
    def _load_config(self) -> Dict[str, Any]:
        """Load YAML configuration"""
        with open(self.config_path) as f:
            return yaml.safe_load(f)
    
    def setup_tables(self):
        """Create result tracking tables if they don't exist"""
        results_table = self.config["job"]["results_table"]
        details_table = self.config["job"]["details_table"]
        
        print("Setting up result tracking tables...")
        self.result_tracker.create_results_table(results_table)
        self.result_tracker.create_details_table(details_table)
        print()
    
    def run_all(self, recon_names: List[str] = None) -> Dict[str, Any]:
        """
        Run all reconciliations defined in config
        
        Args:
            recon_names: Optional list of specific reconciliation names to run
        
        Returns:
            Summary of all reconciliation results
        """
        batch_id = self.config["job"]["batch_id"]
        job_name = self.config["job"]["name"]
        
        # Filter reconciliations if specific names provided
        recons = self.config["reconciliations"]
        if recon_names:
            recons = [r for r in recons if r["name"] in recon_names]
        
        if not recons:
            raise ValueError(f"No reconciliations found matching: {recon_names}")
        
        print("="*80)
        print(f"RECONCILIATION JOB: {job_name}")
        print(f"Batch ID: {batch_id}")
        print(f"Reconciliations to run: {len(recons)}")
        print("="*80)
        
        results_summary = {
            "job_name": job_name,
            "batch_id": batch_id,
            "total": len(recons),
            "passed": 0,
            "failed": 0,
            "results": []
        }
        
        for recon_config in recons:
            result = self.run_single(recon_config)
            results_summary["results"].append(result)
            
            if result["overall_status"] == "PASSED":
                results_summary["passed"] += 1
            else:
                results_summary["failed"] += 1
        
        # Print summary
        print("\n" + "="*80)
        print("JOB SUMMARY")
        print("="*80)
        print(f"Total: {results_summary['total']}")
        print(f"✓ Passed: {results_summary['passed']}")
        print(f"✗ Failed: {results_summary['failed']}")
        print("="*80)
        
        return results_summary
    
    def run_single(self, recon_config: Dict[str, Any]) -> Dict[str, Any]:
        """Run a single reconciliation"""
        recon_name = recon_config["name"]
        recon_id = str(uuid.uuid4())
        start_time = datetime.now()
        
        print(f"\n{'='*80}")
        print(f"RECONCILIATION: {recon_name}")
        print(f"ID: {recon_id}")
        print(f"{'='*80}")
        
        source_table = recon_config["source"]["table"]
        target_table = recon_config["target"]["table"]
        keys = recon_config["keys"]
        
        # Schema validation
        schema_results = {}
        if recon_config["validations"].get("schema_check", True):
            print("\n📋 Running schema validation...")
            schema_results = self.schema_validator.compare_schemas(source_table, target_table)
            self.schema_validator.print_schema_comparison(schema_results)
        else:
            schema_results = {"status": "SKIPPED", "summary": "Schema check disabled"}
        
        # Data reconciliation
        data_results = {}
        if recon_config["validations"].get("data_comparison", True):
            print("\n📊 Running data reconciliation...")
            data_results = self.data_reconciler.reconcile(
                source_table=source_table,
                target_table=target_table,
                keys=keys,
                columns_to_compare=recon_config.get("columns_to_compare"),
                exclude_columns=recon_config.get("exclude_columns", []),
                sample_size=self.config["settings"].get("sample_mismatches", 10)
            )
            self.data_reconciler.print_reconciliation_results(data_results)
        else:
            data_results = {"status": "SKIPPED", "summary": "Data comparison disabled"}
        
        # Calculate duration
        end_time = datetime.now()
        duration_seconds = (end_time - start_time).total_seconds()
        
        # Check thresholds
        threshold_breached = self._check_thresholds(recon_config, data_results)
        
        # Determine overall status
        overall_status = "PASSED"
        if schema_results.get("status") == "FAILED" or data_results.get("status") == "FAILED":
            overall_status = "FAILED"
        if threshold_breached:
            overall_status = "THRESHOLD_BREACHED"
        
        # Store results
        if self.config["settings"].get("enable_detailed_logging", True):
            print(f"\n💾 Storing results...")
            self.result_tracker.log_results(
                results_table=self.config["job"]["results_table"],
                recon_id=recon_id,
                recon_name=recon_name,
                batch_id=self.config["job"]["batch_id"],
                schema_results=schema_results,
                data_results=data_results,
                duration_seconds=duration_seconds
            )
            
            self.result_tracker.log_details(
                details_table=self.config["job"]["details_table"],
                recon_id=recon_id,
                recon_name=recon_name,
                batch_id=self.config["job"]["batch_id"],
                data_results=data_results
            )
        
        print(f"\n✓ Reconciliation complete: {overall_status} ({duration_seconds:.2f}s)")
        
        return {
            "recon_id": recon_id,
            "recon_name": recon_name,
            "overall_status": overall_status,
            "schema_status": schema_results.get("status"),
            "data_status": data_results.get("status"),
            "duration_seconds": duration_seconds,
            "threshold_breached": threshold_breached
        }
    
    def _check_thresholds(self, recon_config: Dict[str, Any], data_results: Dict[str, Any]) -> bool:
        """Check if any thresholds are breached"""
        if "thresholds" not in recon_config:
            return False
        
        thresholds = recon_config["thresholds"]
        source_count = data_results.get("source_count", 0)
        
        if source_count == 0:
            return False
        
        # Check mismatch threshold
        mismatch_percent = (data_results.get("mismatched_records", {}).get("count", 0) / source_count) * 100
        if mismatch_percent > thresholds.get("max_mismatch_percent", 100):
            print(f"⚠️  Mismatch threshold breached: {mismatch_percent:.2f}% > {thresholds['max_mismatch_percent']}%")
            return True
        
        # Check missing threshold
        missing_percent = (data_results.get("missing_records", {}).get("count", 0) / source_count) * 100
        if missing_percent > thresholds.get("max_missing_percent", 100):
            print(f"⚠️  Missing threshold breached: {missing_percent:.2f}% > {thresholds['max_missing_percent']}%")
            return True
        
        return False


def run_reconciliation(spark, config_path: str, recon_names: List[str] = None):
    """
    Convenience function to run reconciliation
    
    Args:
        spark: SparkSession
        config_path: Path to YAML config file
        recon_names: Optional list of specific reconciliations to run
    
    Example:
        # Run all reconciliations
        results = run_reconciliation(spark, "/path/to/config.yaml")
        
        # Run specific reconciliation
        results = run_reconciliation(spark, "/path/to/config.yaml", ["customer_recon"])
    """
    runner = ReconRunner(spark, config_path)
    runner.setup_tables()
    return runner.run_all(recon_names)
