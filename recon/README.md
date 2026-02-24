# Data Reconciliation Framework

A comprehensive, config-driven framework for validating and reconciling data between tables in Databricks.

## 📋 Features

- ✅ **Schema Validation**: Compare table schemas and identify differences
- ✅ **Data Reconciliation**: Find missing, extra, and mismatched records
- ✅ **Result Tracking**: Store results in Delta tables for historical analysis
- ✅ **Config-Driven**: Define reconciliations in YAML
- ✅ **Threshold Alerts**: Configure acceptable mismatch levels
- ✅ **Sample Storage**: Keep examples of mismatches for investigation
- ✅ **Duplicate Detection**: Identify duplicate keys in source/target

## 📁 Project Structure

```
recon/
├── configs/
│   └── recon_config.yaml          # Configuration file
├── utils/
│   ├── __init__.py                # Package init
│   ├── schema_validator.py       # Schema comparison
│   ├── data_reconciler.py        # Data comparison
│   ├── result_tracker.py         # Result storage
│   └── recon_runner.py           # Main orchestrator
└── README.md                      # This file
```

## 🚀 Quick Start

### 1. Install Dependencies

```python
%pip install pyyaml
```

### 2. Run Reconciliation

```python
import sys
sys.path.append("/Workspace/Users/mohu.tera@gmail.com/recon")

from utils.recon_runner import run_reconciliation

# Run all reconciliations
results = run_reconciliation(
    spark, 
    "/Workspace/Users/mohu.tera@gmail.com/recon/configs/recon_config.yaml"
)
```

### 3. Run Specific Reconciliation

```python
# Run only customer reconciliation
results = run_reconciliation(
    spark, 
    "/Workspace/Users/mohu.tera@gmail.com/recon/configs/recon_config.yaml",
    ["customer_recon"]
)
```

## ⚙️ Configuration

Edit `configs/recon_config.yaml` to define your reconciliations:

```yaml
job:
  name: data_reconciliation_job
  batch_id: "2026-02-23"
  results_table: workspace.default.recon_results
  details_table: workspace.default.recon_details

reconciliations:
  - name: customer_recon
    source:
      table: workspace.default.customer_source
    target:
      table: workspace.default.customer_target
    
    keys:
      - customer_id
    
    columns_to_compare: []  # Empty = all columns
    
    exclude_columns:
      - created_at
      - updated_at
    
    validations:
      schema_check: true
      row_count_check: true
      data_comparison: true
      null_check: true
      duplicate_check: true
    
    thresholds:
      max_mismatch_percent: 5.0
      max_missing_percent: 2.0
```

## 📊 View Results

### Query Results Table

```sql
SELECT * 
FROM workspace.default.recon_results
ORDER BY execution_time DESC
LIMIT 20
```

### Query Details Table (Mismatches)

```sql
SELECT * 
FROM workspace.default.recon_details
WHERE recon_name = 'customer_recon'
  AND issue_type = 'MISMATCH'
ORDER BY execution_time DESC
```

### Analyze Match Rate Trend

```sql
SELECT 
    recon_name,
    DATE(execution_time) as date,
    AVG(match_rate) as avg_match_rate,
    COUNT(*) as run_count
FROM workspace.default.recon_results
GROUP BY recon_name, DATE(execution_time)
ORDER BY date DESC
```

## 🔧 Advanced Usage

### Use Individual Components

```python
from utils.schema_validator import SchemaValidator
from utils.data_reconciler import DataReconciler

# Schema validation only
validator = SchemaValidator(spark)
schema_results = validator.compare_schemas(
    "workspace.default.source_table",
    "workspace.default.target_table"
)

# Data reconciliation only
reconciler = DataReconciler(spark)
data_results = reconciler.reconcile(
    source_table="workspace.default.source_table",
    target_table="workspace.default.target_table",
    keys=["id"],
    exclude_columns=["timestamp"],
    sample_size=10
)
```

## 📈 Result Tables Schema

### Results Table (Summary)

| Column | Type | Description |
|--------|------|-------------|
| recon_id | String | Unique reconciliation ID |
| recon_name | String | Name from config |
| batch_id | String | Batch identifier |
| execution_time | Timestamp | When reconciliation ran |
| status | String | PASSED/FAILED |
| source_count | Integer | Source record count |
| target_count | Integer | Target record count |
| matched_count | Integer | Matching records |
| match_rate | Double | Match percentage |
| missing_count | Integer | Records missing in target |
| extra_count | Integer | Extra records in target |
| mismatch_count | Integer | Mismatched records |
| source_duplicates | Integer | Duplicate keys in source |
| target_duplicates | Integer | Duplicate keys in target |
| schema_status | String | Schema validation status |
| data_status | String | Data validation status |
| summary | String | Human-readable summary |
| duration_seconds | Double | Execution time |

### Details Table (Samples)

| Column | Type | Description |
|--------|------|-------------|
| recon_id | String | Links to results table |
| recon_name | String | Name from config |
| batch_id | String | Batch identifier |
| execution_time | Timestamp | When reconciliation ran |
| issue_type | String | MISSING/EXTRA/MISMATCH/DUPLICATE |
| record_keys | String | JSON of key values |
| source_values | String | JSON of source values |
| target_values | String | JSON of target values |
| details | String | Additional information |

## 🎯 Use Cases

### 1. Data Migration Validation
Validate that data migrated correctly from old to new system.

### 2. ETL Pipeline Validation
Ensure ETL transformations produce expected results.

### 3. Replication Monitoring
Monitor data replication between systems.

### 4. Data Quality Checks
Identify data quality issues between environments.

### 5. Compliance Auditing
Track data consistency for compliance requirements.

## 🔄 Scheduling

Schedule as a Databricks Job:

1. Create a new Job in Databricks
2. Add a notebook task with this code:
```python
from utils.recon_runner import run_reconciliation
results = run_reconciliation(spark, "/path/to/config.yaml")
```
3. Set schedule (daily, hourly, etc.)
4. Configure alerts based on results

## 🐛 Troubleshooting

### Issue: Module not found
**Solution**: Ensure path is added:
```python
import sys
sys.path.append("/Workspace/Users/mohu.tera@gmail.com/recon")
```

### Issue: Table not found
**Solution**: Verify table names in config are fully qualified (catalog.schema.table)

### Issue: Results table doesn't exist
**Solution**: Run `runner.setup_tables()` first or let `run_reconciliation()` create them automatically

## 📝 Best Practices

1. **Use fully qualified table names**: `catalog.schema.table`
2. **Exclude timestamp columns**: They often differ and cause false positives
3. **Set realistic thresholds**: Based on your data quality expectations
4. **Review samples regularly**: Investigate patterns in mismatches
5. **Schedule regular runs**: Monitor data quality over time
6. **Archive old results**: Partition results tables by date for performance

## 🚀 Next Steps

1. Update `recon_config.yaml` with your table pairs
2. Run reconciliation job
3. Query results tables for analysis
4. Schedule as a Databricks Job
5. Set up alerts for threshold breaches

## 📞 Support

For issues or questions, refer to the inline documentation in each module.
