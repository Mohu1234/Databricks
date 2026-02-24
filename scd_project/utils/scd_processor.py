"""
SCD Processor Module - Production Ready

Handles all SCD load types:
- incremental: Append-only with optional deduplication
- scd1: Upsert (no history)
- scd2: Full history tracking with effective dates
- full_refresh: Complete table overwrite
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, current_timestamp, sha2, concat_ws, col
from datetime import datetime
from typing import Dict, Any, Optional


class SCDProcessor:
    """Processes tables using different SCD strategies"""
    
    def __init__(self, spark: SparkSession, batch_id: str):
        self.spark = spark
        self.batch_id = batch_id
    
    def load_source(self, source_cfg: Dict[str, Any]) -> DataFrame:
        """Load source data from table or custom SQL query"""
        if "sql" in source_cfg:
            print(f"  Loading from custom SQL query")
            return self.spark.sql(source_cfg["sql"])
        elif "table" in source_cfg:
            print(f"  Loading from table: {source_cfg['table']}")
            return self.spark.read.table(source_cfg["table"])
        else:
            raise ValueError("Source must have either 'table' or 'sql' key")
    
    def process_incremental(self, source_df: DataFrame, table_name: str, keys: list) -> Dict[str, int]:
        """Process incremental load"""
        print(f"  Mode: Incremental (append only)")
        source_count = source_df.count()
        inserted_count = 0
        table_exists = self.spark.catalog.tableExists(table_name)
        
        if not table_exists:
            source_df.write.format("delta").saveAsTable(table_name)
            inserted_count = source_count
            print(f"  ✓ Created table with {inserted_count} records")
        else:
            if keys:
                source_df.createOrReplaceTempView("source_temp")
                join_condition = " AND ".join([f"s.{k} = t.{k}" for k in keys])
                new_records_sql = f"""
                SELECT s.* FROM source_temp s
                LEFT JOIN {table_name} t ON {join_condition}
                WHERE t.{keys[0]} IS NULL
                """
                new_records_df = self.spark.sql(new_records_sql)
                inserted_count = new_records_df.count()
                if inserted_count > 0:
                    new_records_df.write.format("delta").mode("append").saveAsTable(table_name)
                    print(f"  ✓ Appended {inserted_count} new records (deduplicated)")
                else:
                    print(f"  ✓ No new records to append")
            else:
                source_df.write.format("delta").mode("append").saveAsTable(table_name)
                inserted_count = source_count
                print(f"  ✓ Appended {inserted_count} records")
        
        active_count = self.spark.sql(f"SELECT COUNT(*) as cnt FROM {table_name}").collect()[0]["cnt"]
        return {"source_count": source_count, "inserted": inserted_count, "updated": 0, "deleted": 0, "active_count": active_count, "historical_count": 0}
    
    def process_scd1(self, source_df: DataFrame, table_name: str, keys: list, columns: list) -> Dict[str, int]:
        """Process SCD Type 1 (upsert)"""
        print(f"  Mode: SCD Type 1 (upsert - no history)")
        source_count = source_df.count()
        table_exists = self.spark.catalog.tableExists(table_name)
        
        if "created_at" not in source_df.columns:
            source_df = source_df.withColumn("created_at", current_timestamp())
        if "updated_at" not in source_df.columns:
            source_df = source_df.withColumn("updated_at", current_timestamp())
        
        if not table_exists:
            source_df.write.format("delta").saveAsTable(table_name)
            print(f"  ✓ Created table with {source_count} records")
        else:
            source_df.createOrReplaceTempView("source_temp")
            join_condition = " AND ".join([f"t.{k} = s.{k}" for k in keys])
            set_clause = ", ".join([f"t.{c} = s.{c}" for c in columns]) + ", t.updated_at = CURRENT_TIMESTAMP()"
            all_columns = keys + columns + ["created_at", "updated_at"]
            insert_columns = ", ".join(all_columns)
            insert_values = ", ".join([f"s.{c}" for c in all_columns])
            merge_sql = f"""
            MERGE INTO {table_name} t USING source_temp s ON {join_condition}
            WHEN MATCHED THEN UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
            """
            self.spark.sql(merge_sql)
            print(f"  ✓ Upsert complete")
        
        active_count = self.spark.sql(f"SELECT COUNT(*) as cnt FROM {table_name}").collect()[0]["cnt"]
        return {"source_count": source_count, "inserted": source_count, "updated": 0, "deleted": 0, "active_count": active_count, "historical_count": 0}
    
    def process_scd2(self, source_df: DataFrame, table_name: str, keys: list, columns: list, delete_handling: Optional[Dict[str, Any]] = None) -> Dict[str, int]:
        """Process SCD Type 2 (history tracking)"""
        print(f"  Mode: SCD Type 2 (history tracking)")
        source_count = source_df.count()
        table_exists = self.spark.catalog.tableExists(table_name)
        has_is_deleted = "is_deleted" in source_df.columns
        delete_enabled = delete_handling and delete_handling.get("enabled", False)
        delete_column = delete_handling.get("column") if delete_handling else None
        delete_value = delete_handling.get("value") if delete_handling else None
        all_source_columns = keys + columns + ([delete_column] if has_is_deleted and delete_column else [])
        all_target_columns = all_source_columns + ["_hash", "batch_id", "effective_start_date", "effective_end_date", "is_active", "updated_timestamp"]
        
        if not table_exists:
            prepared_df = (source_df.withColumn("_hash", sha2(concat_ws("||", *[col(c) for c in columns]), 256))
                .withColumn("batch_id", lit(self.batch_id)).withColumn("effective_start_date", current_timestamp())
                .withColumn("effective_end_date", lit(None).cast("timestamp")).withColumn("is_active", lit("Y"))
                .withColumn("updated_timestamp", current_timestamp()))
            prepared_df.select(*all_target_columns).write.format("delta").saveAsTable(table_name)
            print(f"  ✓ Created with {source_count} records")
            return {"source_count": source_count, "inserted": source_count, "updated": 0, "deleted": 0, "active_count": source_count, "historical_count": 0}
        
        before_counts = self.spark.sql(f"SELECT COUNT(*) as total, SUM(CASE WHEN is_active = 'Y' THEN 1 ELSE 0 END) as active, SUM(CASE WHEN is_active = 'N' THEN 1 ELSE 0 END) as historical FROM {table_name}").collect()[0]
        source_with_hash = source_df.withColumn("_hash", sha2(concat_ws("||", *[col(c) for c in columns]), 256)).withColumn("batch_id", lit(self.batch_id))
        source_with_hash.createOrReplaceTempView("source_temp")
        join_condition = " AND ".join([f"t.{k} = s.{k}" for k in keys])
        insert_columns = ", ".join(all_target_columns)
        insert_values = ", ".join([f"s.{c}" if c in all_source_columns else "s.batch_id" if c == "batch_id" else "current_timestamp()" if c in ["effective_start_date", "updated_timestamp"] else "NULL" if c == "effective_end_date" else "'Y'" if c == "is_active" else "s._hash" if c == "_hash" else f"s.{c}" for c in all_target_columns])
        
        if delete_enabled and delete_column and delete_value:
            merge_sql = f"MERGE INTO {table_name} t USING source_temp s ON {join_condition} AND t.is_active = 'Y' WHEN MATCHED AND s.{delete_column} = '{delete_value}' THEN UPDATE SET t.effective_end_date = current_timestamp(), t.is_active = 'N', t.updated_timestamp = current_timestamp() WHEN MATCHED AND s._hash != t._hash THEN UPDATE SET t.effective_end_date = current_timestamp(), t.is_active = 'N', t.updated_timestamp = current_timestamp() WHEN NOT MATCHED AND s.{delete_column} != '{delete_value}' THEN INSERT ({insert_columns}) VALUES ({insert_values})"
        else:
            merge_sql = f"MERGE INTO {table_name} t USING source_temp s ON {join_condition} AND t.is_active = 'Y' WHEN MATCHED AND s._hash != t._hash THEN UPDATE SET t.effective_end_date = current_timestamp(), t.is_active = 'N', t.updated_timestamp = current_timestamp() WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})"
        self.spark.sql(merge_sql)
        
        # Simplified: Use INNER JOIN to avoid complex nested subquery
        # Only insert new versions for records that were JUST closed (hash changed)
        subquery_join = ' AND '.join([f't.{k} = s.{k}' for k in keys])
        
        if delete_enabled and delete_column and delete_value:
            new_versions_sql = f"INSERT INTO {table_name} SELECT {insert_values} FROM source_temp s INNER JOIN {table_name} t ON {subquery_join} WHERE s.{delete_column} != '{delete_value}' AND t.is_active = 'N' AND t.effective_end_date IS NOT NULL AND s._hash != t._hash"
        else:
            new_versions_sql = f"INSERT INTO {table_name} SELECT {insert_values} FROM source_temp s INNER JOIN {table_name} t ON {subquery_join} WHERE t.is_active = 'N' AND t.effective_end_date IS NOT NULL AND s._hash != t._hash"
        self.spark.sql(new_versions_sql)
        
        after_counts = self.spark.sql(f"SELECT COUNT(*) as total, SUM(CASE WHEN is_active = 'Y' THEN 1 ELSE 0 END) as active, SUM(CASE WHEN is_active = 'N' THEN 1 ELSE 0 END) as historical FROM {table_name}").collect()[0]
        inserted_count = after_counts["active"] - before_counts["active"] if before_counts["active"] else after_counts["active"]
        updated_count = before_counts["active"] - after_counts["active"] + inserted_count if before_counts["active"] else 0
        deleted_count = max(0, before_counts["active"] - after_counts["active"] - updated_count) if before_counts["active"] else 0
        print(f"  ✓ Complete: {int(after_counts['active']) if after_counts['active'] else 0} active, {int(after_counts['historical']) if after_counts['historical'] else 0} historical")
        return {"source_count": source_count, "inserted": max(0, inserted_count), "updated": max(0, updated_count), "deleted": max(0, deleted_count), "active_count": int(after_counts["active"]) if after_counts["active"] else 0, "historical_count": int(after_counts["historical"]) if after_counts["historical"] else 0}
    
    def process_full_refresh(self, source_df: DataFrame, table_name: str) -> Dict[str, int]:
        """Process full refresh"""
        print(f"  Mode: Full Refresh (complete overwrite)")
        source_count = source_df.count()
        source_df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        print(f"  ✓ Table overwritten with {source_count} records")
        return {"source_count": source_count, "inserted": source_count, "updated": 0, "deleted": 0, "active_count": source_count, "historical_count": 0}
    
    def process_table(self, table_cfg: Dict[str, Any]) -> Dict[str, int]:
        """Main dispatcher"""
        source_df = self.load_source(table_cfg["source"])
        table_name = table_cfg["target"]["table_name"]
        load_type = table_cfg.get("load_type", "scd2")
        scd_cfg = table_cfg.get("scd", {})
        keys = scd_cfg.get("keys", [])
        columns = scd_cfg.get("columns", [])
        delete_handling = table_cfg.get("delete_handling")
        
        if load_type == "incremental":
            return self.process_incremental(source_df, table_name, keys)
        elif load_type == "scd1":
            return self.process_scd1(source_df, table_name, keys, columns)
        elif load_type == "scd2":
            return self.process_scd2(source_df, table_name, keys, columns, delete_handling)
        elif load_type == "full_refresh":
            return self.process_full_refresh(source_df, table_name)
        else:
            raise ValueError(f"Unknown load_type: {load_type}")
