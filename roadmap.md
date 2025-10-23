# Synapse Enhancement Roadmap

## 🎯 Priority 1: Core Functionality Fixes

### 1. Enhanced SQL Support (core/sql_engine.py)
**Current**: Only basic WHERE filtering
**Target**: Full SQL support including:
- JOINs (INNER, LEFT, RIGHT, FULL, CROSS)
- GROUP BY with HAVING
- Window functions (ROW_NUMBER, RANK, LAG, LEAD)
- CTEs (WITH clauses)
- UNION/INTERSECT/EXCEPT
- Subqueries

### 2. Real DAG Orchestration (core/orchestrator.py)
**Current**: Only parses SCHEDULE/TASK
**Target**: 
- Execute tasks in dependency order
- Parallel execution where possible
- Retry logic with exponential backoff
- Task timeout handling
- Checkpoint/resume capability

### 3. Data Source Connectors (core/connectors/)
**Current**: Only CSV/Parquet
**Target**:
- PostgreSQL, MySQL, SQLite
- Snowflake, BigQuery, Redshift
- S3, GCS, Azure Blob
- REST APIs with pagination
- MongoDB, Redis
- Kafka streams

### 4. Schema Management (core/schema.py)
**New Feature**:
- Type inference and validation
- Schema evolution tracking
- Data quality constraints
- Auto-generate schema from data

---

## 🚀 Priority 2: Advanced Features

### 5. Data Quality Framework (core/data_quality.py)
- Null checks, uniqueness constraints
- Range validation (min/max)
- Referential integrity
- Custom validation rules
- Profiling and statistics

### 6. Incremental Loading (core/incremental.py)
- Change Data Capture (CDC)
- Watermark-based loading
- Merge/Upsert operations
- Slowly Changing Dimensions (SCD Type 2)

### 7. Caching Layer (core/cache.py)
- In-memory caching for repeated queries
- Disk-based cache for large datasets
- Cache invalidation strategies
- Query result memoization

### 8. Extended Python Integration
- Support for popular libraries (numpy, sklearn, plotly)
- Async function execution
- Multi-processing for CPU-bound tasks
- Better error propagation

---

## 🎨 Priority 3: Developer Experience

### 9. CLI Enhancements (bin/synapse_cli.py)
- `synapse init` - Project scaffolding
- `synapse validate` - Syntax checking
- `synapse test` - Run test suites
- `synapse dag` - Visualize DAG
- `synapse profile` - Performance analysis
- `synapse docs` - Generate documentation

### 10. Configuration Management (config/)
- Environment-based configs (dev/staging/prod)
- Secret management (encrypted credentials)
- Connection pooling settings
- Resource limits

### 11. Logging & Monitoring (core/observability.py)
- Structured logging (JSON format)
- Metrics collection (execution time, row counts)
- Integration with Prometheus/Grafana
- Alerting on failures

### 12. Testing Framework (tests/)
- Unit tests for all core modules
- Integration tests with real databases
- Mock data generators
- Performance benchmarks

---

## 📊 Priority 4: Language Extensions

### 13. New Synapse Keywords
```synapse
# Incremental loading
LOAD INCREMENTAL "source" AS df
    WATERMARK timestamp_col
    CHECKPOINT "s3://checkpoints/job1"

# Merge operations
MERGE INTO target_table
USING source_df
ON target.id = source.id
WHEN MATCHED THEN UPDATE
WHEN NOT MATCHED THEN INSERT

# Data quality
VALIDATE df
    EXPECT column > 0
    EXPECT NOT NULL email
    EXPECT UNIQUE customer_id
    ON_FAILURE STOP

# Parallel processing
PARALLEL 4
    RUN chunk WITH PYTHON process_batch()

# Conditional execution
IF ROW_COUNT(df) > 1000
    SAVE df TO "large_dataset.parquet"
ELSE
    SAVE df TO "small_dataset.csv"
```

### 14. Built-in Functions
- Date/time functions: `DATE_TRUNC()`, `DATE_ADD()`
- String functions: `REGEXP_EXTRACT()`, `SPLIT()`
- Aggregate functions: `MEDIAN()`, `PERCENTILE()`
- Window functions: `DENSE_RANK()`, `NTILE()`

---

## 🏗️ Project Structure Improvements

### Recommended New Structure
```
synapse/
├── bin/
│   └── synapse_cli.py
├── core/
│   ├── __init__.py
│   ├── parser.py (enhanced)
│   ├── executor.py (enhanced)
│   ├── sql_engine.py (new)
│   ├── orchestrator.py (new)
│   ├── schema.py (new)
│   ├── data_quality.py (new)
│   ├── incremental.py (new)
│   ├── cache.py (new)
│   └── observability.py (new)
├── connectors/
│   ├── __init__.py
│   ├── base.py
│   ├── database.py
│   ├── cloud_storage.py
│   ├── api.py
│   └── streaming.py
├── user_functions/
│   ├── __init__.py
│   ├── metrics.py
│   └── transformations.py
├── config/
│   ├── __init__.py
│   ├── default.yaml
│   └── secrets.yaml.example
├── tests/
│   ├── unit/
│   ├── integration/
│   └── fixtures/
├── examples/
│   ├── basic_etl.synapse
│   ├── complex_pipeline.synapse
│   ├── incremental_load.synapse
│   └── data_quality.synapse
├── docs/
│   ├── getting_started.md
│   ├── language_reference.md
│   └── api_docs.md
├── requirements.txt
├── setup.py
├── README.md
└── .gitignore
```

---

## 📝 Implementation Priority Order

### Week 1-2: Foundation
1. Enhanced SQL engine with JOIN support
2. Basic orchestrator with dependency resolution
3. PostgreSQL connector
4. Schema validation

### Week 3-4: Core Features
5. Data quality framework
6. S3 connector
7. Incremental loading
8. Enhanced error handling

### Week 5-6: Developer Experience
9. CLI improvements
10. Configuration system
11. Logging framework
12. Test suite

### Week 7-8: Advanced Features
13. Caching layer
14. API connector
15. Performance optimizations
16. Documentation

---

## 🐛 Critical Bugs to Fix

1. **Parser**: Doesn't handle multi-line SQL correctly
2. **Executor**: No rollback on failures
3. **TRANSFORM**: SQL variable scoping issues
4. **RUN**: Doesn't handle functions returning None
5. **File paths**: Relative path handling is inconsistent

---

## 💡 Quick Wins (Easy to Implement)

1. Add `--dry-run` flag to CLI
2. Add `--verbose` flag for detailed logging
3. Support for `.env` files
4. Pretty-print DataFrames in CLI output
5. Add `SHOW TABLES` command
6. Add `DESCRIBE <table>` command
7. Support for inline comments in SQL blocks
8. Better error messages with line numbers

---

## 🎓 Example Use Cases to Support

### 1. E-commerce Analytics Pipeline
```synapse
LOAD "s3://raw/orders.parquet" AS orders
LOAD "postgres://db/customers" AS customers

TRANSFORM orders AS enriched_orders
    SELECT 
        o.*,
        c.customer_name,
        c.segment
    FROM orders o
    LEFT JOIN customers c ON o.customer_id = c.id
    WHERE o.order_date >= DATE_ADD(CURRENT_DATE, -30)

RUN enriched_orders WITH PYTHON calculate_rfm_score()

SAVE enriched_orders TO "snowflake://warehouse/analytics.orders"
```

### 2. Real-time Data Quality Monitoring
```synapse
LOAD STREAM "kafka://events" AS events

VALIDATE events
    EXPECT NOT NULL user_id, event_type, timestamp
    EXPECT event_type IN ('click', 'view', 'purchase')
    EXPECT timestamp > DATE_ADD(CURRENT_TIMESTAMP, -1h)
    ON_FAILURE LOG_AND_CONTINUE

SAVE events TO "bigquery://project/events_validated"
```

### 3. Slowly Changing Dimension (SCD Type 2)
```synapse
LOAD "s3://raw/customers_today.csv" AS source
LOAD "postgres://dwh/dim_customer" AS target

MERGE INTO target
USING source
ON target.customer_id = source.customer_id 
    AND target.is_current = true
WHEN MATCHED AND source.email != target.email THEN
    UPDATE SET is_current = false, end_date = CURRENT_DATE
    INSERT new row with is_current = true
WHEN NOT MATCHED THEN INSERT
```
