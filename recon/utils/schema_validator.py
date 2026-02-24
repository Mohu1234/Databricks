"""
Schema Validator Module

Validates and compares schemas between two tables:
- Column name differences
- Data type mismatches
- Nullability differences
- Missing/extra columns
"""

from pyspark.sql import SparkSession
from typing import Dict, List, Tuple, Any


class SchemaValidator:
    """Validates schema compatibility between two tables"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def get_schema_info(self, table_name: str) -> Dict[str, Any]:
        """
        Get schema information for a table
        
        Returns:
            Dict with column name -> {type, nullable} mapping
        """
        df = self.spark.read.table(table_name)
        schema_info = {}
        
        for field in df.schema.fields:
            schema_info[field.name] = {
                "type": str(field.dataType),
                "nullable": field.nullable
            }
        
        return schema_info
    
    def compare_schemas(self, source_table: str, target_table: str) -> Dict[str, Any]:
        """
        Compare schemas between source and target tables
        
        Returns:
            Dict with comparison results
        """
        source_schema = self.get_schema_info(source_table)
        target_schema = self.get_schema_info(target_table)
        
        source_cols = set(source_schema.keys())
        target_cols = set(target_schema.keys())
        
        # Find differences
        common_cols = source_cols & target_cols
        missing_in_target = source_cols - target_cols
        extra_in_target = target_cols - source_cols
        
        # Check data type mismatches in common columns
        type_mismatches = []
        nullable_mismatches = []
        
        for col in common_cols:
            if source_schema[col]["type"] != target_schema[col]["type"]:
                type_mismatches.append({
                    "column": col,
                    "source_type": source_schema[col]["type"],
                    "target_type": target_schema[col]["type"]
                })
            
            if source_schema[col]["nullable"] != target_schema[col]["nullable"]:
                nullable_mismatches.append({
                    "column": col,
                    "source_nullable": source_schema[col]["nullable"],
                    "target_nullable": target_schema[col]["nullable"]
                })
        
        # Determine overall status
        has_issues = (
            len(missing_in_target) > 0 or 
            len(extra_in_target) > 0 or 
            len(type_mismatches) > 0
        )
        
        return {
            "status": "FAILED" if has_issues else "PASSED",
            "source_table": source_table,
            "target_table": target_table,
            "source_column_count": len(source_cols),
            "target_column_count": len(target_cols),
            "common_columns": list(common_cols),
            "missing_in_target": list(missing_in_target),
            "extra_in_target": list(extra_in_target),
            "type_mismatches": type_mismatches,
            "nullable_mismatches": nullable_mismatches,
            "summary": self._generate_summary(
                missing_in_target, extra_in_target, type_mismatches, nullable_mismatches
            )
        }
    
    def _generate_summary(self, missing, extra, type_mismatches, nullable_mismatches) -> str:
        """Generate human-readable summary"""
        issues = []
        
        if missing:
            issues.append(f"{len(missing)} columns missing in target")
        if extra:
            issues.append(f"{len(extra)} extra columns in target")
        if type_mismatches:
            issues.append(f"{len(type_mismatches)} type mismatches")
        if nullable_mismatches:
            issues.append(f"{len(nullable_mismatches)} nullable mismatches")
        
        if not issues:
            return "Schemas are compatible"
        
        return "; ".join(issues)
    
    def print_schema_comparison(self, comparison: Dict[str, Any]):
        """Print schema comparison results in a readable format"""
        print("="*80)
        print("SCHEMA VALIDATION RESULTS")
        print("="*80)
        print(f"Status: {comparison['status']}")
        print(f"Source: {comparison['source_table']} ({comparison['source_column_count']} columns)")
        print(f"Target: {comparison['target_table']} ({comparison['target_column_count']} columns)")
        print(f"Summary: {comparison['summary']}")
        
        if comparison['missing_in_target']:
            print(f"\n❌ Missing in target ({len(comparison['missing_in_target'])}):")
            for col in comparison['missing_in_target']:
                print(f"   - {col}")
        
        if comparison['extra_in_target']:
            print(f"\n➕ Extra in target ({len(comparison['extra_in_target'])}):")
            for col in comparison['extra_in_target']:
                print(f"   - {col}")
        
        if comparison['type_mismatches']:
            print(f"\n⚠️  Type mismatches ({len(comparison['type_mismatches'])}):")
            for mismatch in comparison['type_mismatches']:
                print(f"   - {mismatch['column']}: {mismatch['source_type']} → {mismatch['target_type']}")
        
        if comparison['nullable_mismatches']:
            print(f"\n⚠️  Nullable mismatches ({len(comparison['nullable_mismatches'])}):")
            for mismatch in comparison['nullable_mismatches']:
                print(f"   - {mismatch['column']}: {mismatch['source_nullable']} → {mismatch['target_nullable']}")
        
        print("="*80)
