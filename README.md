# pyql - SQL-Python Hybrid Framework for Data Engineering

pyql is a framework that combines the power of SQL for data manipulation with Python for orchestration, creating a simplified and efficient approach to ETL pipelines and data engineering tasks.

## Features

- **SQL Templating with Python Embedding**: Write SQL queries with embedded Python expressions
- **Pipeline Orchestration**: Define ETL workflows with dependency management
- **Schema Validation**: Define and enforce data schemas with automatic validation
- **Transform Chains**: Build reusable transformation pipelines
- **DuckDB Integration**: High-performance in-process SQL engine
- **Type Safety**: Strong typing with automatic type inference

## Installation

```bash
pip install -r requirements.txt
pip install -e .
```

## Quick Start

### 1. Basic SQL with Python Variables

```python
from pyql import sql
from pyql.sql_engine import execute_sql

# Create a SQL template with Python embedding
query = sql('''
    SELECT *
    FROM customers
    WHERE signup_date > {cutoff_date}
    AND status IN {python: get_active_statuses()}
    AND total_purchases >= {min_purchases}
''')

# Execute with context
result = execute_sql(
    query,
    context={
        'cutoff_date': '2024-01-01',
        'get_active_statuses': lambda: ('active', 'premium'),
        'min_purchases': 10
    }
)
```

### 2. Building ETL Pipelines

```python
from pyql import Pipeline, sql

pipeline = Pipeline("customer_etl")

@pipeline.extract("load_customers")
def load_customers(ctx, engine):
    return sql('''
        SELECT * FROM raw_customers
        WHERE updated_at > {last_run}
    ''')

@pipeline.transform("clean_data", depends_on=["load_customers"])
def clean_data(ctx, engine):
    df = ctx['results']['load_customers']
    return engine.execute(
        sql('''
            SELECT 
                TRIM(name) as name,
                LOWER(email) as email,
                CAST(signup_date AS DATE) as signup_date
            FROM {df: input}
        '''),
        dataframes={'input': df}
    )

@pipeline.load("save_results", depends_on=["clean_data"])
def save_results(ctx, engine):
    final_df = ctx['results']['clean_data']
    engine.create_table("clean_customers", final_df)
    return f"Saved {len(final_df)} records"

# Run the pipeline
summary = pipeline.run()
```

### 3. Schema Validation

```python
from pyql import Schema, Column
from pyql.schema import DataType

# Define schema
customer_schema = (Schema("customers")
    .integer("id", nullable=False, primary_key=True)
    .string("name", nullable=False, max_length=100)
    .string("email", nullable=False, pattern=r'^[\w\.-]+@[\w\.-]+\.\w+$')
    .float("revenue", min_value=0)
    .string("status", allowed_values=['active', 'inactive', 'pending'])
)

# Validate data
validation = customer_schema.validate(df)
if not validation['valid']:
    for error in validation['errors']:
        print(f"Error: {error}")

# Generate SQL
print(customer_schema.to_create_table_sql())
```

### 4. Transform Chains

```python
from pyql.transforms import TransformChain

# Build transformation pipeline
transform = (TransformChain()
    .clean_names()              # Standardize column names
    .trim()                     # Trim whitespace
    .lower(['email'])           # Lowercase specific columns
    .fill({'age': 0})          # Fill missing values
    .filter(lambda df: df['active'] == True)  # Filter rows
    .compute('age_group', lambda df: pd.cut(df['age'], bins=[0, 18, 35, 50, 100]))
    .validate_not_null(['id', 'name'])
)

cleaned_df = transform.apply(raw_df)
```

### 5. Working with DataFrames and SQL

```python
from pyql.sql_engine import DuckDBEngine
import pandas as pd

engine = DuckDBEngine()

# Load data
orders = pd.DataFrame(...)
customers = pd.DataFrame(...)

# Execute SQL on DataFrames
result = engine.execute(
    sql('''
        SELECT 
            c.name,
            COUNT(o.order_id) as num_orders,
            SUM(o.amount) as total_spent
        FROM {df: customers} c
        JOIN {df: orders} o ON c.id = o.customer_id
        GROUP BY c.name
        HAVING COUNT(*) > {min_orders}
    '''),
    context={'min_orders': 5},
    dataframes={'customers': customers, 'orders': orders}
)
```

## Core Components

### SQL Template Engine

The SQL template engine allows you to:
- Embed Python variables: `{variable_name}`
- Execute Python expressions: `{python: expression}`
- Reference DataFrames as tables: `{df: dataframe_name}`

### Pipeline Orchestration

Pipelines provide:
- Dependency resolution (automatic execution order)
- Error handling with retry logic
- Execution tracking and logging
- Context management for sharing data between steps

### Schema Management

Schemas support:
- Type definitions with constraints
- Automatic validation
- Schema inference from data
- SQL DDL generation
- JSON serialization

### Transform Utilities

Built-in transformations include:
- Column name cleaning
- Null handling
- Type casting
- Filtering and validation
- Custom transformations

## Architecture

```
Synpase Framework
├── sql_engine.py      # SQL templating and DuckDB integration
├── pipeline.py        # ETL orchestration
├── transforms.py      # Reusable transformations
├── schema.py          # Schema validation
└── config.py          # Configuration management
```

## Best Practices

1. **Use SQL for Set Operations**: Leverage SQL's power for joins, aggregations, and filtering
2. **Use Python for Logic**: Embed Python for complex business logic and dynamic queries
3. **Define Schemas Early**: Create schemas to catch data quality issues early
4. **Chain Transformations**: Build reusable transform chains for common patterns
5. **Monitor Pipelines**: Use logging to track pipeline execution and debug issues

## Examples

See the `examples/` directory for:
- Complete ETL pipelines
- Data cleaning workflows
- Schema validation patterns
- Advanced SQL templating

## Running the Example

```bash
cd pyql_framework
python -m examples.complete_example
```

## License

MIT License

## Contributing

Contributions are welcome! Please read the contributing guidelines before submitting pull requests.
