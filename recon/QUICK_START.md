# 🚀 Reconciliation Framework - Quick Start

## 📝 One-Line Usage

```python
from utils.recon_runner import run_reconciliation
results = run_reconciliation(spark, "/path/to/recon_config.yaml")
```

## 📁 Files Created

```
recon/
├── configs/
│   └── recon_config.yaml              # ← Edit this with your tables
├── utils/
│   ├── schema_validator.py           # Schema comparison
│   ├── data_reconciler.py            # Data comparison
│   ├── result_tracker.py             # Result storage
│   └── recon_runner.py               # Main orchestrator
├── New File 2026-02-23 22:56:05.py   # ← Run this to test
├── README.md                          # Full documentation
└── QUICK_START.md                     # This file
```

## 🎯 Test the Framework

**Open and run:** `New File 2026-02-23 22:56:05.py`

Just click "Run All" - it will:
1. Create sample test data
2. Run reconciliation
3. Show results

## ⚙️ Configure Your Tables

Edit `configs/recon_config.yaml`:

```yaml
reconciliations:
  - name: my_recon
    source:
      table: catalog.schema.source_table
    target:
      table: catalog.schema.target_table
    keys:
      - id
    thresholds:
      max_mismatch_percent: 5.0
```

## 📊 View Results

```sql
-- Summary
SELECT * FROM workspace.default.recon_results 
ORDER BY execution_time DESC;

-- Details
SELECT * FROM workspace.default.recon_details 
WHERE recon_name = 'my_recon';
```

## 🔧 Advanced Usage

### Run Specific Reconciliation
```python
results = run_reconciliation(spark, config_path, ["customer_recon"])
```

### Use Components Directly
```python
from utils.schema_validator import SchemaValidator
from utils.data_reconciler import DataReconciler

validator = SchemaValidator(spark)
schema_results = validator.compare_schemas("source_table", "target_table")

reconciler = DataReconciler(spark)
data_results = reconciler.reconcile("source_table", "target_table", keys=["id"])
```

## 📋 What Gets Checked

✅ Schema compatibility (columns, types, nullability)
✅ Row counts
✅ Missing records (in source but not target)
✅ Extra records (in target but not source)
✅ Mismatched values (different data)
✅ Duplicate keys

## 🎨 Output Tables

### recon_results (Summary)
- recon_name, status, match_rate
- source_count, target_count, matched_count
- missing_count, extra_count, mismatch_count

### recon_details (Samples)
- issue_type (MISSING/EXTRA/MISMATCH/DUPLICATE)
- record_keys, source_values, target_values

## 🚨 Common Issues

**Module not found?**
```python
import sys
sys.path.append("/Workspace/Users/mohu.tera@gmail.com/recon")
```

**Table not found?**
- Use fully qualified names: `catalog.schema.table`

**No results?**
- Tables are created automatically on first run

## 📞 Need Help?

See `README.md` for full documentation.
