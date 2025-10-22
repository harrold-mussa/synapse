# Synapse: The Ultimate Data Engineering Language

**Synapse** is designed to be a high-level, declarative language that combines the intuitive, set-based logic of **SQL** with the vast ecosystem and power of **Python** libraries (mainly Pandas and Airflow concepts). It's designed to abstract away boilerplate code and streamline data workflow orchestration (ETL/ELT).

## 🚀 Why choose Synapse? 

Pain Points & Solutions

| Data Engineering Pain Point | Synapse Solution |
| :--- | :--- |
| **Context Switching Overhead** | Synapse provides a unified syntax. You write one script that handles all steps (SQL, Python, Orchestration), eliminating the need for separate `.py` and `.sql` files. |
| **Boilerplate/Scaffolding** | Simple, declarative keywords (`LOAD`, `SAVE`, `RUN`) handle all connection and library scaffolding automatically. |
| **Complex Workflow Dependencies** | The `SCHEDULE` and `DEPENDS ON` keywords provide a clear, readable, and in-script way to define your Directed Acyclic Graph (DAG) logic. |
| **Inability to Inject Custom Logic** | The `RUN ... WITH PYTHON` block allows direct, inline execution of any defined Python function on your current data frame. |

---

## 📚 Synapse Core Syntax & Function Map

Synapse scripts operate on named **DataFrames** (which map to Pandas DataFrames in the execution engine).

### 1. Data I/O and Setup

| Synapse Function | Description | Maps to Python Logic (e.g., Pandas) |
| :--- | :--- | :--- |
| **`LOAD`** `<source>` **AS** `<var>` | Reads data from a path (S3, local, DB connection) into a named DataFrame variable. | `pd.read_csv()`, `pd.read_sql()`, etc. |
| **`SAVE`** `<var>` **TO** `<destination>` | Writes the specified DataFrame variable to a path or connection. | `df.to_csv()`, `df.to_sql()`, `df.to_parquet()`, etc. |
| **`DEFINE_CONN`** `<name>` **URL** `<url>` | Defines a reusable connection string (e.g., for a PostgreSQL database). | Internal connection management dictionary. |

### 2. Data Transformation (SQL-Inspired)

| Synapse Function | Description | Maps to Python Logic (e.g., Pandas) |
| :--- | :--- | :--- |
| **`TRANSFORM`** `<in_var>` **AS** `<out_var>` | The core transformation block. All code indented underneath must be standard SQL. | Uses a library like `pandasql` or internal logic to execute SQL against the DataFrame. |
| **`PIVOT`** `<var>` **ON** `<col>` | Performs a pivot operation (similar to SQL's `PIVOT` or Pandas `pivot_table`). | `df.pivot_table()` |
| **`JOIN`** `<var1>` **WITH** `<var2>` **ON** `<condition>` | Performs a data join (INNER, LEFT, RIGHT, FULL). | `pd.merge()` |
| **`FILTER`** `<var>` **WHERE** `<condition>` | A simple shorthand for a `WHERE` clause selection. | `df.query()` or boolean indexing. |

### 3. Custom Logic and Orchestration

| Synapse Function | Description | Maps to Python Logic (e.g., Python/Airflow) |
| :--- | :--- | :--- |
| **`RUN`** `<var>` **WITH PYTHON** `<function_call>` | Executes a pre-defined Python function, passing the DataFrame variable as the first argument. | Direct function call (`custom_func(df, *args)`). |
| **`LOG`** `<message>` | Records an informational message to the console or log file. | `logging.info()` |
| **`SCHEDULE`** `<job_name>` **DEPENDS ON** `<jobs>` | Defines an orchestration step. Used to create the DAG. | Internal DAG definition/execution. |
| **`TASK`** `<step_name>` | Used inside a `SCHEDULE` block to list the execution order of I/O and transformation steps. | Defines the node in the DAG. |

---

## ⚙️ Step-by-Step Implementation Guide

### Step 1: Write Your Synapse Script

Create a file (e.g., `etl_pipeline.synapse`) using the defined syntax.

```synapse
# etl_pipeline.synapse

# 1. Load Data
LOAD "s3://raw/customers.csv" AS RawCustomers

# 2. Transform Data (using SQL logic)
TRANSFORM RawCustomers AS ActiveCustomers
    SELECT customer_id, name, email
    FROM RawCustomers
    WHERE status = 'active' AND email IS NOT NULL

# 3. Apply custom Python logic (e.g., Data Quality Checks)
RUN ActiveCustomers WITH PYTHON validate_customer_emails(column='email')

# 4. Save Final Output
SAVE ActiveCustomers TO "snowflake://dwh/active_user_table"

# 5. Define Workflow Orchestration
SCHEDULE CustomerETL
DEPENDS ON None
TASK 1: LOAD RawCustomers
TASK 2: TRANSFORM ActiveCustomers
TASK 3: RUN ActiveCustomers
TASK 4: SAVE ActiveCustomers
