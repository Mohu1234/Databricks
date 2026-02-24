"""
Data Reconciliation Engine

Compares data between two tables:
- Row count comparison
- Missing records (in source but not in target)
- Extra records (in target but not in source)
- Mismatched records (different values)
- Duplicate key detection
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, lit, concat_ws, sha2, when
from typing import Dict, List, Any, Optional


class DataReconciler:
    """Reconciles data between source and target tables"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def reconcile(
        self,
        source_table: str,
        target_table: str,
        keys: List[str],
        columns_to_compare: Optional[List[str]] = None,
        exclude_columns: Optional[List[str]] = None,
        sample_size: int = 10
    ) -> Dict[str, Any]:
        """
        Perform full data reconciliation
        
        Args:
            source_table: Source table name
            target_table: Target table name
            keys: List of key columns for matching
            columns_to_compare: Specific columns to compare (None = all)
            exclude_columns: Columns to exclude from comparison
            sample_size: Number of sample mismatches to return
        
        Returns:
            Dict with reconciliation results
        """
        # Load tables
        source_df = self.spark.read.table(source_table)
        target_df = self.spark.read.table(target_table)
        
        # Determine columns to compare
        compare_cols = self._determine_compare_columns(
            source_df, target_df, columns_to_compare, exclude_columns, keys
        )
        
        # Row counts
        source_count = source_df.count()
        target_count = target_df.count()
        
        # Check for duplicates
        source_dups = self._check_duplicates(source_df, keys)
        target_dups = self._check_duplicates(target_df, keys)
        
        # Find missing and extra records
        missing_records = self._find_missing_records(source_df, target_df, keys)
        extra_records = self._find_extra_records(source_df, target_df, keys)
        
        # Find mismatched records
        mismatches = self._find_mismatches(
            source_df, target_df, keys, compare_cols, sample_size
        )
        
        # Calculate match rate
        matched_count = source_count - missing_records["count"] - mismatches["count"]
        match_rate = (matched_count / source_count * 100) if source_count > 0 else 0
        
        # Determine overall status
        has_issues = (
            source_dups["count"] > 0 or
            target_dups["count"] > 0 or
            missing_records["count"] > 0 or
            extra_records["count"] > 0 or
            mismatches["count"] > 0
        )
        
        return {
            "status": "FAILED" if has_issues else "PASSED",
            "source_table": source_table,
            "target_table": target_table,
            "source_count": source_count,
            "target_count": target_count,
            "matched_count": matched_count,
            "match_rate": round(match_rate, 2),
            "source_duplicates": source_dups,
            "target_duplicates": target_dups,
            "missing_records": missing_records,
            "extra_records": extra_records,
            "mismatched_records": mismatches,
            "columns_compared": compare_cols,
            "summary": self._generate_summary(
                source_count, target_count, matched_count, 
                missing_records["count"], extra_records["count"], 
                mismatches["count"]
            )
        }
    
    def _determine_compare_columns(
        self, 
        source_df: DataFrame, 
        target_df: DataFrame,
        columns_to_compare: Optional[List[str]],
        exclude_columns: Optional[List[str]],
        keys: List[str]
    ) -> List[str]:
        """Determine which columns to compare"""
        source_cols = set(source_df.columns)
        target_cols = set(target_df.columns)
        common_cols = source_cols & target_cols
        
        # Remove keys from comparison (they're used for matching)
        compare_cols = common_cols - set(keys)
        
        # Apply column filters
        if columns_to_compare:
            compare_cols = compare_cols & set(columns_to_compare)
        
        if exclude_columns:
            compare_cols = compare_cols - set(exclude_columns)
        
        return sorted(list(compare_cols))
    
    def _check_duplicates(self, df: DataFrame, keys: List[str]) -> Dict[str, Any]:
        """Check for duplicate keys"""
        dup_df = df.groupBy(*keys).agg(count("*").alias("cnt")).filter("cnt > 1")
        dup_count = dup_df.count()
        
        samples = []
        if dup_count > 0:
            samples = [row.asDict() for row in dup_df.limit(5).collect()]
        
        return {
            "count": dup_count,
            "samples": samples
        }
    
    def _find_missing_records(
        self, 
        source_df: DataFrame, 
        target_df: DataFrame, 
        keys: List[str]
    ) -> Dict[str, Any]:
        """Find records in source but not in target"""
        # Anti-join: records in source not in target
        missing_df = source_df.select(*keys).subtract(target_df.select(*keys))
        missing_count = missing_df.count()
        
        samples = []
        if missing_count > 0:
            samples = [row.asDict() for row in missing_df.limit(10).collect()]
        
        return {
            "count": missing_count,
            "samples": samples
        }
    
    def _find_extra_records(
        self, 
        source_df: DataFrame, 
        target_df: DataFrame, 
        keys: List[str]
    ) -> Dict[str, Any]:
        """Find records in target but not in source"""
        # Anti-join: records in target not in source
        extra_df = target_df.select(*keys).subtract(source_df.select(*keys))
        extra_count = extra_df.count()
        
        samples = []
        if extra_count > 0:
            samples = [row.asDict() for row in extra_df.limit(10).collect()]
        
        return {
            "count": extra_count,
            "samples": samples
        }
    
    def _find_mismatches(
        self,
        source_df: DataFrame,
        target_df: DataFrame,
        keys: List[str],
        compare_cols: List[str],
        sample_size: int
    ) -> Dict[str, Any]:
        """Find records with matching keys but different values"""
        if not compare_cols:
            return {"count": 0, "samples": []}
        
        # Create hash of compare columns
        source_with_hash = source_df.withColumn(
            "_hash",
            sha2(concat_ws("||", *[col(c).cast("string") for c in compare_cols]), 256)
        )
        
        target_with_hash = target_df.withColumn(
            "_hash",
            sha2(concat_ws("||", *[col(c).cast("string") for c in compare_cols]), 256)
        )
        
        # Join on keys and compare hashes
        join_condition = [source_with_hash[k] == target_with_hash[k] for k in keys]
        
        mismatched_df = (
            source_with_hash.alias("s")
            .join(target_with_hash.alias("t"), join_condition, "inner")
            .filter(col("s._hash") != col("t._hash"))
            .select(
                *[col(f"s.{k}").alias(k) for k in keys],
                col("s._hash").alias("source_hash"),
                col("t._hash").alias("target_hash")
            )
        )
        
        mismatch_count = mismatched_df.count()
        
        samples = []
        if mismatch_count > 0:
            samples = [row.asDict() for row in mismatched_df.limit(sample_size).collect()]
        
        return {
            "count": mismatch_count,
            "samples": samples
        }
    
    def _generate_summary(
        self,
        source_count: int,
        target_count: int,
        matched_count: int,
        missing_count: int,
        extra_count: int,
        mismatch_count: int
    ) -> str:
        """Generate human-readable summary"""
        issues = []
        
        if missing_count > 0:
            issues.append(f"{missing_count} missing in target")
        if extra_count > 0:
            issues.append(f"{extra_count} extra in target")
        if mismatch_count > 0:
            issues.append(f"{mismatch_count} mismatched")
        
        if not issues:
            return f"Perfect match: {matched_count}/{source_count} records"
        
        return f"{matched_count}/{source_count} matched; " + "; ".join(issues)
    
    def print_reconciliation_results(self, results: Dict[str, Any]):
        """Print reconciliation results in a readable format"""
        print("="*80)
        print("DATA RECONCILIATION RESULTS")
        print("="*80)
        print(f"Status: {results['status']}")
        print(f"Source: {results['source_table']} ({results['source_count']} records)")
        print(f"Target: {results['target_table']} ({results['target_count']} records)")
        print(f"Match Rate: {results['match_rate']}%")
        print(f"Summary: {results['summary']}")
        
        if results['source_duplicates']['count'] > 0:
            print(f"\n❌ Source duplicates: {results['source_duplicates']['count']}")
        
        if results['target_duplicates']['count'] > 0:
            print(f"❌ Target duplicates: {results['target_duplicates']['count']}")
        
        if results['missing_records']['count'] > 0:
            print(f"\n⚠️  Missing in target: {results['missing_records']['count']}")
            if results['missing_records']['samples']:
                print("   Samples:", results['missing_records']['samples'][:3])
        
        if results['extra_records']['count'] > 0:
            print(f"\n➕ Extra in target: {results['extra_records']['count']}")
            if results['extra_records']['samples']:
                print("   Samples:", results['extra_records']['samples'][:3])
        
        if results['mismatched_records']['count'] > 0:
            print(f"\n🔄 Mismatched records: {results['mismatched_records']['count']}")
            if results['mismatched_records']['samples']:
                print("   Samples:", results['mismatched_records']['samples'][:3])
        
        print("="*80)
