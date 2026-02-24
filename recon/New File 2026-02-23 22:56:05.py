# Databricks notebook source
# MAGIC %md
# MAGIC # 🔍 Data Reconciliation Framework - Complete Test
# MAGIC 
# MAGIC **Just run all cells to see the framework in action!**
# MAGIC 
# MAGIC This notebook will:
# MAGIC 1. ✅ Create sample test data
# MAGIC 2. ✅ Run reconciliation using config file
# MAGIC 3. ✅ Display results and analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Step 1: Install Dependencies

# COMMAND ----------

%pip install pyyaml

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🗄️ Step 2: Create Sample Test Data

# COMMAND ----------

# Create source table (4 customers)
spark.sql("""
CREATE OR REPLACE TABLE workspace.default.customer_source AS
SELECT * FROM (VALUES
    (1, 'Alice', 'alice@example.com', 'LA'),
    (2, 'Bob', 'bob@example.com', 'SF'),
    (3, 'Charlie', 'charlie@example.com', 'LA'),
    (4, 'David', 'david@example.com', 'Chicago')
) AS t(customer_id, customer_name, email, city)
""")

print("✓ Created customer_source (4 records)")
display(spark.read.table("workspace.default.customer_source"))

# COMMAND ----------

# Create target table with differences:
# - Missing: David (customer_id 4)
# - Extra: Eve (customer_id 5)
# - Mismatch: Bob's city (SF → NYC)
spark.sql("""
CREATE OR REPLACE TABLE workspace.default.customer_target AS
SELECT * FROM (VALUES
    (1, 'Alice', 'alice@example.com', 'LA'),
    (2, 'Bob', 'bob@example.com', 'NYC'),
    (3, 'Charlie', 'charlie@example.com', 'LA'),
    (5, 'Eve', 'eve@example.com', 'Boston')
) AS t(customer_id, customer_name, email, city)
""")

print("✓ Created customer_target (4 records with differences)")
display(spark.read.table("workspace.default.customer_target"))

# COMMAND ----------

# Create product tables (matching data for comparison)
spark.sql("""
CREATE OR REPLACE TABLE workspace.default.product_source AS
SELECT * FROM (VALUES
    (101, 'Laptop', 'Electronics', 999.99, 'High-performance laptop'),
    (102, 'Mouse', 'Electronics', 29.99, 'Wireless mouse'),
    (103, 'Keyboard', 'Electronics', 79.99, 'Mechanical keyboard'),
    (104, 'Monitor', 'Electronics', 299.99, '27-inch monitor')
) AS t(product_id, product_name, category, price, description)
""")

# Create matching target (perfect match scenario)
spark.sql("""
CREATE OR REPLACE TABLE workspace.default.product_current AS
SELECT * FROM workspace.default.product_source
""")

print("✓ Created product_source and product_current (perfect match)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Step 3: Run Reconciliation Framework
# MAGIC 
# MAGIC This calls all utils and processes based on config

# COMMAND ----------

import sys
sys.path.append("/Workspace/Users/mohu.tera@gmail.com/recon")

from utils.recon_runner import run_reconciliation

# Run all reconciliations from config
results = run_reconciliation(
    spark, 
    "/Workspace/Users/mohu.tera@gmail.com/recon/configs/recon_config.yaml"
)

print(f"\n{'='*80}")
print(f"✅ JOB COMPLETE!")
print(f"{'='*80}")
print(f"Total: {results['total']}")
print(f"Passed: {results['passed']}")
print(f"Failed: {results['failed']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Step 4: View Results Summary

# COMMAND ----------

# Query results table
results_df = spark.sql("""
SELECT 
    recon_name,
    status,
    source_count,
    target_count,
    matched_count,
    match_rate,
    missing_count,
    extra_count,
    mismatch_count,
    summary
FROM workspace.default.recon_results
ORDER BY execution_time DESC
LIMIT 10
""")

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Step 5: View Detailed Mismatches

# COMMAND ----------

# Query details table
details_df = spark.sql("""
SELECT 
    recon_name,
    issue_type,
    record_keys,
    details
FROM workspace.default.recon_details
ORDER BY execution_time DESC
LIMIT 20
""")

display(details_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 Step 6: Analyze by Issue Type

# COMMAND ----------

# Count issues by type
spark.sql("""
SELECT 
    recon_name,
    issue_type,
    COUNT(*) as count
FROM workspace.default.recon_details
WHERE DATE(execution_time) = CURRENT_DATE()
GROUP BY recon_name, issue_type
ORDER BY recon_name, issue_type
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Step 7: Run Specific Reconciliation

# COMMAND ----------

# Run only customer reconciliation
print("Running customer_recon only...")

results = run_reconciliation(
    spark, 
    "/Workspace/Users/mohu.tera@gmail.com/recon/configs/recon_config.yaml",
    ["customer_recon"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 Step 8: Use Individual Components

# COMMAND ----------

from utils.schema_validator import SchemaValidator
from utils.data_reconciler import DataReconciler

# Schema validation
validator = SchemaValidator(spark)
schema_results = validator.compare_schemas(
    "workspace.default.customer_source",
    "workspace.default.customer_target"
)
validator.print_schema_comparison(schema_results)

# COMMAND ----------

# Data reconciliation
reconciler = DataReconciler(spark)
data_results = reconciler.reconcile(
    source_table="workspace.default.customer_source",
    target_table="workspace.default.customer_target",
    keys=["customer_id"],
    sample_size=10
)
reconciler.print_reconciliation_results(data_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Expected Results
# MAGIC 
# MAGIC ### Customer Reconciliation (FAILED):
# MAGIC - **Missing**: 1 record (David, customer_id=4)
# MAGIC - **Extra**: 1 record (Eve, customer_id=5)
# MAGIC - **Mismatch**: 1 record (Bob's city: SF vs NYC)
# MAGIC - **Match rate**: ~25%
# MAGIC 
# MAGIC ### Product Reconciliation (PASSED):
# MAGIC - **Perfect match**: 100%
# MAGIC - All 4 products match exactly
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## ✅ Framework Features Demonstrated:
# MAGIC - ✅ Schema validation
# MAGIC - ✅ Row count comparison
# MAGIC - ✅ Missing record detection
# MAGIC - ✅ Extra record detection
# MAGIC - ✅ Data mismatch detection
# MAGIC - ✅ Result storage in Delta tables
# MAGIC - ✅ Sample record capture
# MAGIC - ✅ Config-driven execution
# MAGIC 
# MAGIC ## 🚀 Next Steps:
# MAGIC 1. Update `recon_config.yaml` with your table pairs
# MAGIC 2. Adjust thresholds
# MAGIC 3. Schedule as Databricks Job
# MAGIC 4. Set up alerts

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Cleanup (Optional)

# COMMAND ----------

# Uncomment to clean up test data
# spark.sql("DROP TABLE IF EXISTS workspace.default.customer_source")
# spark.sql("DROP TABLE IF EXISTS workspace.default.customer_target")
# spark.sql("DROP TABLE IF EXISTS workspace.default.product_source")
# spark.sql("DROP TABLE IF EXISTS workspace.default.product_current")
# spark.sql("DROP TABLE IF EXISTS workspace.default.recon_results")
# spark.sql("DROP TABLE IF EXISTS workspace.default.recon_details")
# print("✓ Cleanup complete")
