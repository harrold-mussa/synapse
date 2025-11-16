"""
synapse Example: Complete ETL Pipeline

This example demonstrates how to use the synapse framework to create a 
SQL-Python hybrid data engineering workflow.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

# Import synapse components
from synapse import Pipeline, sql, Schema, Column
from synapse.transforms import TransformChain, clean_column_names, validate_not_null
from synapse.sql_engine import DuckDBEngine
from synapse.schema import DataType


# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_sample_data():
    """Create sample data for demonstration."""
    
    # Raw customers data (messy)
    customers = pd.DataFrame({
        'Customer ID': [1, 2, 3, 4, 5, 6],
        'NAME': ['  John Doe  ', 'JANE SMITH', 'bob wilson', 'Alice Brown', 'Charlie Davis', 'Eve Miller'],
        'Email': ['john@email.com', 'JANE@EMAIL.COM', 'bob@email.com', None, 'charlie@email.com', 'eve@email.com'],
        'Phone': ['555-0101', '555-0102', '555-0103', '555-0104', '555-0105', '555-0106'],
        'signup_date': ['2024-01-15', '2024-02-20', '2024-01-10', '2024-03-05', '2024-02-28', '2024-01-25'],
        'Status': ['active', 'ACTIVE', 'inactive', 'active', 'Active', 'pending']
    })
    
    # Raw orders data
    orders = pd.DataFrame({
        'order_id': [101, 102, 103, 104, 105, 106, 107, 108],
        'customer_id': [1, 2, 1, 3, 4, 2, 5, 1],
        'product_name': ['Widget A', 'Widget B', 'Widget A', 'Gadget X', 'Widget A', 'Gadget Y', 'Widget B', 'Gadget X'],
        'quantity': [2, 1, 3, 1, 5, 2, 1, 2],
        'unit_price': [29.99, 49.99, 29.99, 99.99, 29.99, 149.99, 49.99, 99.99],
        'order_date': pd.to_datetime([
            '2024-02-01', '2024-02-15', '2024-03-01', '2024-02-20',
            '2024-03-10', '2024-03-15', '2024-03-20', '2024-04-01'
        ])
    })
    
    return customers, orders


def example_1_simple_pipeline():
    """Example 1: Simple ETL Pipeline with SQL and Python."""
    
    print("\n" + "="*60)
    print("EXAMPLE 1: Simple ETL Pipeline")
    print("="*60)
    
    # Create sample data
    customers, orders = create_sample_data()
    
    # Create pipeline
    pipeline = Pipeline("customer_orders_etl")
    
    # Set context variables
    pipeline.set_variable('min_order_quantity', 2)
    pipeline.set_variable('status_filter', ['active', 'pending'])
    pipeline.add_dataframe('raw_customers', customers)
    pipeline.add_dataframe('raw_orders', orders)
    
    @pipeline.extract("load_customers")
    def load_customers(ctx, engine):
        """Load and clean customer data using SQL."""
        raw_df = ctx['dataframes']['raw_customers']
        
        # Use SQL to clean and transform
        return engine.execute(
            sql('''
                SELECT 
                    "Customer ID" as customer_id,
                    TRIM(NAME) as name,
                    LOWER(Email) as email,
                    Phone as phone,
                    CAST(signup_date AS DATE) as signup_date,
                    LOWER(Status) as status
                FROM {df: customers}
                WHERE Email IS NOT NULL
            '''),
            dataframes={'customers': raw_df}
        )
    
    @pipeline.extract("load_orders")
    def load_orders(ctx, engine):
        """Load orders data."""
        return ctx['dataframes']['raw_orders']
    
    @pipeline.transform("filter_active_customers", depends_on=["load_customers"])
    def filter_active_customers(ctx, engine):
        """Filter to only active/pending customers."""
        customers_df = ctx['results']['load_customers']
        status_filter = ctx['variables']['status_filter']
        
        # Embed Python list into SQL
        return engine.execute(
            sql('''
                SELECT *
                FROM {df: customers}
                WHERE status IN {status_list}
            '''),
            context={'status_list': tuple(status_filter)},
            dataframes={'customers': customers_df}
        )
    
    @pipeline.transform("aggregate_orders", depends_on=["load_orders"])
    def aggregate_orders(ctx, engine):
        """Aggregate orders by customer."""
        orders_df = ctx['results']['load_orders']
        min_qty = ctx['variables']['min_order_quantity']
        
        return engine.execute(
            sql('''
                SELECT 
                    customer_id,
                    COUNT(*) as total_orders,
                    SUM(quantity) as total_items,
                    SUM(quantity * unit_price) as total_revenue,
                    AVG(quantity * unit_price) as avg_order_value,
                    MIN(order_date) as first_order,
                    MAX(order_date) as last_order
                FROM {df: orders}
                WHERE quantity >= {min_qty}
                GROUP BY customer_id
            '''),
            context={'min_qty': min_qty},
            dataframes={'orders': orders_df}
        )
    
    @pipeline.transform("join_customer_orders", depends_on=["filter_active_customers", "aggregate_orders"])
    def join_customer_orders(ctx, engine):
        """Join customers with their order summaries."""
        customers_df = ctx['results']['filter_active_customers']
        orders_df = ctx['results']['aggregate_orders']
        
        return engine.execute(
            sql('''
                SELECT 
                    c.*,
                    COALESCE(o.total_orders, 0) as total_orders,
                    COALESCE(o.total_items, 0) as total_items,
                    COALESCE(o.total_revenue, 0) as total_revenue,
                    o.avg_order_value,
                    o.first_order,
                    o.last_order
                FROM {df: customers} c
                LEFT JOIN {df: orders} o ON c.customer_id = o.customer_id
            '''),
            dataframes={'customers': customers_df, 'orders': orders_df}
        )
    
    @pipeline.load("save_results", depends_on=["join_customer_orders"])
    def save_results(ctx, engine):
        """Save final results."""
        final_df = ctx['results']['join_customer_orders']
        engine.create_table("customer_summary", final_df)
        return f"Saved {len(final_df)} customer records"
    
    # Run the pipeline
    summary = pipeline.run()
    
    # Display results
    print("\nPipeline Execution Summary:")
    print(f"  Total Steps: {summary['total_steps']}")
    print(f"  Successful: {summary['successful_steps']}")
    print(f"  Duration: {summary['total_duration']:.2f}s")
    
    print("\nFinal Customer Summary:")
    final_result = pipeline.engine.read_table("customer_summary")
    print(final_result.to_string())
    
    return pipeline, final_result


def example_2_transform_chain():
    """Example 2: Using TransformChain for complex transformations."""
    
    print("\n" + "="*60)
    print("EXAMPLE 2: TransformChain for Data Cleaning")
    print("="*60)
    
    # Create messy data
    data = pd.DataFrame({
        'USER_NAME': ['  JOHN  ', 'jane', 'BOB SMITH', 'Alice', 'Charlie'],
        'user email': ['JOHN@EMAIL.COM', 'jane@email.com', 'bob@email', 'alice@email.com', None],
        'Age': [25, 30, None, 28, 35],
        'Revenue': [1000.50, 2000.00, 1500.75, 3000.00, 500.25],
        'signup_date': ['2024-01-01', '2024-02-15', '2024-03-01', '2024-01-20', '2024-02-28']
    })
    
    print("Original Data:")
    print(data)
    print()
    
    # Create transformation chain
    transform = (TransformChain()
        .clean_names()  # Standardize column names
        .trim()  # Trim whitespace from strings
        .lower(['user_name'])  # Lowercase names
        .fill({'age': data['Age'].median()})  # Fill missing ages with median
        .compute('revenue_per_year', lambda df: df['revenue'] * 12)  # Add computed column
        .filter(lambda df: df['user_email'].notna())  # Remove rows with no email
        .custom(lambda df: standardize_dates(df, ['signup_date']))  # Custom transformation
    )
    
    # Apply transformations
    cleaned = transform.apply(data)
    
    print("Cleaned Data:")
    print(cleaned)
    
    return cleaned


def standardize_dates(df, date_columns):
    """Helper function to standardize date columns."""
    df = df.copy()
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    return df


def example_3_schema_validation():
    """Example 3: Schema definition and validation."""
    
    print("\n" + "="*60)
    print("EXAMPLE 3: Schema Validation")
    print("="*60)
    
    # Define expected schema
    customer_schema = (Schema("customer_data")
        .integer("customer_id", nullable=False, unique=True, primary_key=True)
        .string("name", nullable=False, min_length=1, max_length=100)
        .string("email", nullable=False, pattern=r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$')
        .integer("age", nullable=True, min_value=18, max_value=120)
        .float("revenue", nullable=False, min_value=0)
        .string("status", nullable=False, allowed_values=['active', 'inactive', 'pending'])
    )
    
    print("Schema Definition:")
    print(customer_schema)
    print()
    
    # Create test data (with some issues)
    test_data = pd.DataFrame({
        'customer_id': [1, 2, 3, 4],
        'name': ['John Doe', 'Jane Smith', '', 'Bob Wilson'],  # Empty name
        'email': ['john@email.com', 'invalid-email', 'bob@email.com', 'alice@email.com'],  # Invalid email
        'age': [25, 150, 30, 17],  # Age out of range
        'revenue': [1000.0, 2000.0, -500.0, 3000.0],  # Negative revenue
        'status': ['active', 'inactive', 'unknown', 'pending']  # Unknown status
    })
    
    print("Test Data:")
    print(test_data)
    print()
    
    # Validate
    validation_result = customer_schema.validate(test_data)
    
    print("Validation Result:")
    print(f"  Valid: {validation_result['valid']}")
    if validation_result['errors']:
        print("  Errors:")
        for error in validation_result['errors']:
            print(f"    - {error}")
    
    print()
    
    # Generate CREATE TABLE SQL
    print("Generated SQL:")
    print(customer_schema.to_create_table_sql("customers"))
    
    return customer_schema, validation_result


def example_4_advanced_sql_templating():
    """Example 4: Advanced SQL templating with Python embedding."""
    
    print("\n" + "="*60)
    print("EXAMPLE 4: Advanced SQL Templating")
    print("="*60)
    
    # Create engine
    engine = DuckDBEngine()
    
    # Create sample data
    products = pd.DataFrame({
        'product_id': [1, 2, 3, 4, 5],
        'product_name': ['Widget A', 'Widget B', 'Gadget X', 'Gadget Y', 'Tool Z'],
        'category': ['widgets', 'widgets', 'gadgets', 'gadgets', 'tools'],
        'price': [29.99, 49.99, 99.99, 149.99, 199.99],
        'stock': [100, 50, 30, 20, 10]
    })
    
    # Define Python functions that can be embedded
    def get_price_tiers():
        return (0, 50, 100, 200)
    
    def calculate_discount(category):
        discounts = {'widgets': 0.1, 'gadgets': 0.15, 'tools': 0.05}
        return discounts.get(category, 0)
    
    # Context with Python functions and values
    context = {
        'get_price_tiers': get_price_tiers,
        'min_stock': 25,
        'target_date': datetime.now().date()
    }
    
    # Query with embedded Python
    query = sql('''
        WITH price_categorized AS (
            SELECT 
                *,
                CASE 
                    WHEN price < 50 THEN 'budget'
                    WHEN price < 100 THEN 'mid-range'
                    ELSE 'premium'
                END as price_tier,
                -- Use Python variable directly
                CASE 
                    WHEN stock > {min_stock} THEN 'in_stock'
                    ELSE 'low_stock'
                END as stock_status
            FROM {df: products}
        )
        SELECT 
            product_name,
            category,
            price,
            price_tier,
            stock,
            stock_status,
            -- Use Python function result
            {python: target_date} as report_date
        FROM price_categorized
        WHERE stock >= 10
        ORDER BY price DESC
    ''')
    
    result = engine.execute(query, context, {'products': products})
    
    print("Products Report:")
    print(result.to_string())
    
    # More complex example with aggregation
    print("\n\nCategory Analysis:")
    
    agg_query = sql('''
        SELECT 
            category,
            COUNT(*) as product_count,
            AVG(price) as avg_price,
            SUM(stock) as total_stock,
            SUM(price * stock) as inventory_value
        FROM {df: products}
        GROUP BY category
        HAVING COUNT(*) >= 1
        ORDER BY inventory_value DESC
    ''')
    
    agg_result = engine.execute(agg_query, dataframes={'products': products})
    print(agg_result.to_string())
    
    return result, agg_result


def example_5_schema_inference():
    """Example 5: Automatic schema inference."""
    
    print("\n" + "="*60)
    print("EXAMPLE 5: Schema Inference")
    print("="*60)
    
    # Create some data
    data = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'score': [85.5, 92.0, 78.5, 88.0, 95.5],
        'passed': [True, True, False, True, True],
        'test_date': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05'])
    })
    
    print("Sample Data:")
    print(data)
    print()
    
    # Infer schema
    inferred_schema = Schema.infer_from_dataframe(data, "student_scores")
    
    print("Inferred Schema:")
    print(inferred_schema)
    print()
    
    # Export schema as SQL
    print("SQL CREATE TABLE:")
    print(inferred_schema.to_create_table_sql())
    print()
    
    # Export schema as dictionary (for serialization)
    print("Schema as JSON:")
    import json
    print(json.dumps(inferred_schema.to_dict(), indent=2, default=str))
    
    return inferred_schema


def example_6_fluent_pipeline_builder():
    """Example 6: Fluent API for pipeline building."""
    
    print("\n" + "="*60)
    print("EXAMPLE 6: Fluent Pipeline Builder")
    print("="*60)
    
    from synapse.pipeline import PipelineBuilder
    
    # Sample data
    sales_data = pd.DataFrame({
        'sale_id': range(1, 11),
        'product': ['A', 'B', 'A', 'C', 'B', 'A', 'C', 'A', 'B', 'C'],
        'amount': [100, 200, 150, 300, 250, 120, 280, 180, 220, 310],
        'date': pd.date_range('2024-01-01', periods=10, freq='D')
    })
    
    # Build pipeline using fluent API
    pipeline = (PipelineBuilder("sales_analysis")
        .extract("load_sales", lambda ctx, engine: sales_data)
        .transform("calculate_totals", '''
            SELECT 
                product,
                COUNT(*) as num_sales,
                SUM(amount) as total_revenue,
                AVG(amount) as avg_sale
            FROM {df: input}
            GROUP BY product
            ORDER BY total_revenue DESC
        ''')
        .load("display_results", lambda ctx, engine: print("\nResults:\n", ctx['results']['calculate_totals']))
        .build()
    )
    
    # Need to register the sales_data for the SQL query
    @pipeline.transform("calculate_totals", depends_on=["load_sales"])
    def calculate_totals(ctx, engine):
        input_df = ctx['results']['load_sales']
        return engine.execute(
            sql('''
                SELECT 
                    product,
                    COUNT(*) as num_sales,
                    SUM(amount) as total_revenue,
                    AVG(amount) as avg_sale
                FROM {df: input}
                GROUP BY product
                ORDER BY total_revenue DESC
            '''),
            dataframes={'input': input_df}
        )
    
    summary = pipeline.run()
    
    print(f"\nPipeline completed in {summary['total_duration']:.2f}s")
    
    return pipeline, summary


def main():
    """Run all examples."""
    
    print("synapse Framework - SQL-Python Hybrid for Data Engineering")
    print("="*60)
    
    # Run examples
    example_1_simple_pipeline()
    example_2_transform_chain()
    example_3_schema_validation()
    example_4_advanced_sql_templating()
    example_5_schema_inference()
    # example_6_fluent_pipeline_builder()  # Needs refinement
    
    print("\n" + "="*60)
    print("All examples completed successfully!")
    print("="*60)


if __name__ == "__main__":
    main()
