# Synapse: The Ultimate Data Engineering Language

**Synapse** is designed to be a high-level, declarative language that combines the intuitive, set-based logic of **SQL** with the vast ecosystem and power of **Python** libraries (mainly Pandas and Airflow concepts). It's designed to abstract away boilerplate code and streamline data workflow orchestration (ETL/ELT).

## 🚀 Why choose Synapse? 

| Data Engineering Pain Point | Synapse Solution |
| :--- | :--- |
| **Context Switching Overhead** | Synapse provides a unified syntax. You write one script that handles all steps (SQL, Python, Orchestration), eliminating the need for separate `.py` and `.sql` files. |
| **Boilerplate/Scaffolding** | Simple, declarative keywords (`LOAD`, `SAVE`, `RUN`) handle all connection and library scaffolding automatically. |
| **Inconsistent Data Quality** | The **`VALIDATE`** command provides a first-class, declarative step to enforce data contracts, enabling **fail-fast** pipelines. |
| **Complex Workflow Dependencies** | The **`SCHEDULE`** and **`TASK`** keywords provide a clear, readable way to define your Directed Acyclic Graph (DAG) logic. |
| **Security & Environment Parity** | **`DEFINE_CONN`** and environment variable resolution ensure security and portability across environments. |
| **Monolithic Codebases** | **`IMPORT`** commands allow breaking large projects into reusable script components. |

---

## 📚 Synapse Core Syntax & Function Map

Synapse scripts operate on named **DataFrames** (which map to Pandas DataFrames in the execution engine).

### 1. Project Setup, Modularity, and Data I/O

| Synapse Function | Description | Maps to Python Logic (e.g., Pandas) |
| :--- | :--- | :--- |
| **`IMPORT`** `<path>` | **Imports commands** (connections, rulesets) from another `.synapse` file. | Script pre-processing/command merger. |
| **`IMPORT_PYTHON`** `<module_name>` | **Dynamically loads a Python module**, making its functions available to `RUN` commands. | `importlib.import_module()` |
| **`DEFINE_CONN`** `<name>` **URL** `<url>` | **Defines a reusable connection alias** where the URL can resolve secrets from environment variables (`${VAR}`). | Internal connection management dictionary. |
| **`LOAD`** `<source/alias>` **AS** `<var>` | Reads data from a file path or connection alias (`CONN::path/table`). | `pd.read_csv()`, `pd.read_sql()`, etc. |
| **`SAVE`** `<var>` **TO** `<destination/alias>` | Writes the specified DataFrame variable to a file path or connection alias. | `df.to_csv()`, `df.to_sql()`, etc. |
| **`CONFIG`** `LOGGING_TABLE` **TO** `<destination>` | Sets a global parameter, typically for storing pipeline execution metadata. | Internal configuration store. |

### 2. Data Transformation (SQL & Declarative Pandas)

| Synapse Function | Description | Maps to Python Logic (e.g., Pandas) |
| :--- | :--- | :--- |
| **`TRANSFORM`** `<in_var>` **AS** `<out_var>` | Executes **standard SQL** (including JOINs, GROUP BY) against the data store. | Uses `pandasql` library. |
| **`PIVOT`** `<var>` **AS** `<out_var>` **INDEX** `<col>`... | Performs a Pandas-style **pivot table** operation with aggregation. | `df.pivot_table()` |
| **`WINDOW`** `<var>` **AS** `<out_var>` **FUNCTION** `RANK()`... | Calculates a new column using common SQL **window functions**. | `df.groupby().rank()` |
| **`RUN`** `<var>` **WITH PYTHON** `<function_call>` | Executes a pre-defined Python function. | Direct function call (`custom_func(df, *args)`). |

### 3. Data Quality and Operational Excellence

| Synapse Function | Description | Maps to Python Logic (e.g., Data Quality/Airflow) |
| :--- | :--- | :--- |
| **`DEFINE RULESET`** `<name>` **RULE** `<check>`... | Defines a reusable set of **data quality rules** (e.g., uniqueness, value ranges). | Internal ruleset dictionary. |
| **`VALIDATE`** `<var>` **WITH RULESET** `<name>` **ON FAIL** `ABORT` | Executes a ruleset. **`ON FAIL ABORT`** stops the pipeline, ensuring fail-fast execution. | Placeholder integration for Great Expectations/Deeque. |
| **`SCHEDULE`** `<job_name>` | Defines the workflow orchestration job. | Internal DAG definition. |
| **`TASK`** `<step_name>` | Defines a node in the DAG. | DAG node. |
| **`ALERT`** **ON FAILURE TASK** `<name>` **TO** `<target>` | Configures a **notification** (e.g., Slack) for a specific task failure. | Orchestrator/Monitoring hook. |

---

## ⚙️ Example Synapse Script with New Syntax

```synapse
# project_config.synapse - Defines reusable components
DEFINE_CONN DataWarehouse URL "postgresql://${DW_USER}:${DW_PASS}@postgres-host:5432/db"

DEFINE RULESET Sales_DQ_Rules
    RULE "customer_id must be unique"
    RULE "revenue must be greater than 0"
    RULE "country must be in ('USA', 'CAN', 'MEX')"


# main_pipeline.synapse - Main logic
IMPORT "project_config.synapse"
IMPORT_PYTHON "data_wrangling.cleaning_funcs"

# 1. Load Data using Connection Alias (Security/Portability)
LOAD DataWarehouse::raw.sales_data AS RawSales

# 2. Declarative Transformation (Window Function)
WINDOW RawSales AS RankedSales
    FUNCTION ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY sale_date DESC) AS rnk

# 3. Filter using SQL
TRANSFORM RankedSales AS LatestSales
    SELECT *
    FROM RankedSales
    WHERE rnk = 1

# 4. Data Quality Check (Fail-Fast Logic)
VALIDATE LatestSales WITH RULESET Sales_DQ_Rules ON FAIL ABORT

# 5. Declarative Transformation (Pivot Table)
PIVOT LatestSales AS MonthlyRevenue
    INDEX customer_id
    COLUMNS month
    VALUES revenue AGG sum

# 6. Save Final Output
SAVE MonthlyRevenue TO DataWarehouse::analytics.monthly_summary

# 7. Orchestration and Monitoring
CONFIG LOGGING_TABLE TO DataWarehouse::meta.pipeline_runs

SCHEDULE Monthly_Revenue_Job
DEPENDS ON None
TASK 1: LOAD RawSales
TASK 2: TRANSFORM LatestSales
TASK 3: VALIDATE LatestSales
TASK 4: PIVOT MonthlyRevenue
TASK 5: SAVE MonthlyRevenue

ALERT ON FAILURE TASK VALIDATE LatestSales TO Slack(channel="#data-alerts")