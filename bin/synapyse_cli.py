import argparse
import logging
import os
import sys
import pandas as pd 
import importlib.util

# Adjust the path to import from the parent directory structure
# This is necessary when running the script directly from 'bin/'
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import core Synapse components
try:
    from core.parser import parse_synapse_script
    from core.executor import SynapseExecutor
    from core.data_io import load_data, save_data
except ImportError as e:
    # A robust way to handle imports when running in different environments
    print(f"Error importing Synapse core modules: {e}")
    print("Ensure you are running the command from the project root or the bin directory.")
    sys.exit(1)


logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# --- Helper for Loading User Functions ---

def load_user_functions():
    """Dynamically loads the metrics.py file from the user_functions directory."""
    module_name = "user_functions.metrics"
    try:
        # Import the module dynamically
        spec = importlib.util.spec_from_file_location(
            module_name, 
            os.path.join(os.path.dirname(__file__), '..', 'user_functions', 'metrics.py')
        )
        if spec is None:
            raise ImportError(f"Could not find module spec for {module_name}")
            
        user_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(user_module)
        return user_module
        
    except Exception as e:
        logging.error(f"Failed to load user functions from user_functions/metrics.py: {e}")
        return None


def setup_test_files():
    """Creates a dummy CSV file for initial testing."""
    INPUT_FILE = os.path.join(os.path.dirname(__file__), '..', 'examples', 'customers_raw.csv')
    
    # Ensure the input file exists before running the Synapse script
    if not os.path.exists(INPUT_FILE):
        dummy_data = {
            'customer_id': [101, 102, 103, 104, 105, 106, 107],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve', 'Frank', 'Grace'],
            'revenue': [150.0, 80.0, 220.0, 310.0, 90.0, 400.0, 180.0],
            'country': ['USA', 'CAN', 'USA', 'MEX', 'CAN', 'USA', 'MEX'],
            'status': ['active', 'inactive', 'active', 'active', 'inactive', 'active', 'inactive']
        }
        pd.DataFrame(dummy_data).to_csv(INPUT_FILE, index=False)
        logging.info(f"Setup: Created test input file at: {INPUT_FILE}")

# --- Main CLI Logic ---
def main():
    parser = argparse.ArgumentParser(description="Synapse: The Data Engineering Language Engine.")
    parser.add_argument("script_file", help="Path to the .synapse script file to execute.")
    args = parser.parse_args()
    
    script_path = os.path.abspath(args.script_file)
    
    # 1. Load User Functions and Setup Test Data
    user_functions = load_user_functions()
    if not user_functions:
        sys.exit(1)
        
    setup_test_files()
    
    # 2. Read Synapse Script
    try:
        with open(script_path, 'r') as f:
            script_content = f.read()
    except FileNotFoundError:
        logging.error(f"Synapse Error: Script file not found: {script_path}")
        sys.exit(1)

    # 3. Parse and Execute
    commands = parse_synapse_script(script_content)
    executor = SynapseExecutor(user_functions)
    
    executor.execute_script(commands)
    
    # Optional: Display final DataFrame store information
    logging.info("\n--- FINAL DATAFRAME STORE SUMMARY ---")
    for var_name, df in executor.data_store.items():
        logging.info(f"Variable '{var_name}': {len(df)} rows, Columns: {list(df.columns)}")

if __name__ == '__main__':
    main()