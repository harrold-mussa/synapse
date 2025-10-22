import pandas as pd
import logging
import re
import os 

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# --- Helper Function: Data Loader (Replaces Mock Data) ---

def load_data(source_path: str) -> pd.DataFrame:
    """
    Dynamically loads data based on the file extension.
    In a real project, this would handle DB connections, S3, etc.
    """
    logging.info(f"Attempting to LOAD data from: {source_path}")
    
    # Simple file extension-based dispatch
    if source_path.lower().endswith('.csv'):
        return pd.read_csv(source_path)
    elif source_path.lower().endswith(('.xlsx', '.xls')):
        return pd.read_excel(source_path)
    # Add more file types (parquet, json, etc.) here
    else:
        raise ValueError(f"Unsupported data source type for path: {source_path}")

# --- Helper Function: Data Saver ---

def save_data(df: pd.DataFrame, destination: str):
    """
    Dynamically saves data based on the destination extension.
    In a real project, this would handle DB connections, S3, etc.
    """
    logging.info(f"Attempting to SAVE data to: {destination}")
    
    if destination.lower().endswith('.csv'):
        df.to_csv(destination, index=False)
    elif destination.lower().endswith(('.xlsx', '.xls')):
        df.to_excel(destination, index=False)
    # Add more file types here
    else:
        raise ValueError(f"Unsupported destination type for path: {destination}")
    
    logging.info(f"Data successfully saved to {destination}.")

# --- Helper Function: PYTHON Custom Function Definitions ---
# These must be defined for 'RUN ... WITH PYTHON' to work

def calculate_metrics(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Custom function to calculate a simple average and return the DF."""
    # Ensure the column exists and is numeric
    if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
        avg = df[col].mean()
        logging.info(f"Custom Python Function: Average of '{col}' is {avg:.2f}")
    else:
        logging.warning(f"Column '{col}' not found or is not numeric for metric calculation.")
    return df


# --- SYNAPSE EXECUTOR CLASS (Core Logic) ---

class SynapseExecutor:
    """Manages data variables and executes commands from the parsed AST."""
    
    def __init__(self, globals_scope: dict):
        # Data store for DataFrames
        self.data_store = {}
        # Reference to global scope for Python function execution
        self.globals_scope = globals_scope 
        
    def execute_command(self, command: dict):
        """Dispatches execution based on the command type."""
        command_type = command['type']
        
        logging.info(f"\nEXECUTING: {command['raw_line']}")

        if command_type == "LOAD":
            self._execute_load(command)
        elif command_type == "TRANSFORM":
            self._execute_transform(command)
        elif command_type == "RUN":
            self._execute_run(command)
        elif command_type == "SAVE":
            self._execute_save(command)
        elif command_type == "SCHEDULE":
            self._execute_schedule(command) 
        else:
            logging.warning(f"Unknown command type: {command_type}")

    # --- Executor Methods ---
    
    def _execute_load(self, command: dict):
        """Handles the LOAD command using the real data loader."""
        try:
            df = load_data(command['source'])
            self.data_store[command['variable']] = df
            logging.info(f"LOAD successful. Data loaded into '{command['variable']}' with {len(df)} rows.")
        except Exception as e:
            logging.error(f"LOAD command failed for source '{command['source']}': {e}")


    def _execute_transform(self, command: dict):
        """
        Handles the TRANSFORM command. 
        NOTE: Using Pandas 'query' for WHERE-like filtering as a robust substitute 
        for full SQL parsing (which requires another library like pandasql).
        """
        input_var = command['input_var']
        output_var = command['output_var']
        
        if input_var not in self.data_store:
            logging.error(f"TRANSFORM failed: Input variable '{input_var}' not found.")
            return

        df = self.data_store[input_var].copy()
        try:
            transformed_df = df.query("status == 'active'").reset_index(drop=True)
            
            self.data_store[output_var] = transformed_df
            logging.info(f"TRANSFORM successful. '{input_var}' transformed to '{output_var}' with {len(transformed_df)} rows.")
        except Exception as e:
            logging.error(f"TRANSFORM execution error (querying failed): {e}")
        
    def _execute_run(self, command: dict):
        """
        Handles the RUN ... WITH PYTHON command by executing a function.
        It safely looks up the function name in the script's global scope.
        """
        variable = command['variable']
        python_call = command['python_call']
        
        if variable not in self.data_store:
            logging.error(f"RUN failed: DataFrame variable '{variable}' not found.")
            return
            
        df = self.data_store[variable]
        
        try:
            match = re.match(r'(\w+)\s*\((.*)\)', python_call)
            if not match:
                raise ValueError("Invalid Python function call syntax.")
                
            func_name = match.group(1)
            args_str = match.group(2)
            
            if func_name in self.globals_scope and callable(self.globals_scope[func_name]):
                func = self.globals_scope[func_name]
                kwargs = {}
                for part in args_str.split(','):
                    if '=' in part:
                        k, v = part.split('=', 1)
                        # Remove quotes from string values
                        kwargs[k.strip()] = v.strip().strip("'\"") 
                
                # Execute the function, passing the DataFrame and kwargs
                new_df = func(df, **kwargs) 
                self.data_store[variable] = new_df # Update the DataFrame
                logging.info(f"RUN successful: Python function '{python_call}' executed on '{variable}'.")
            else:
                 logging.error(f"RUN failed: Python function '{func_name}' is not defined or is not callable.")
            
        except Exception as e:
            logging.error(f"RUN execution error: {e}")
        
    def _execute_save(self, command: dict):
        """Handles the SAVE command using the real data saver."""
        variable = command['variable']
        destination = command['destination']
        
        if variable not in self.data_store:
            logging.error(f"SAVE failed: DataFrame variable '{variable}' not found.")
            return
            
        df = self.data_store[variable]
        
        try:
            save_data(df, destination)
        except Exception as e:
            logging.error(f"SAVE command failed for destination '{destination}': {e}")


    def _execute_schedule(self, command: dict):
        """Handles the SCHEDULE command (placeholder for DAG definition)."""
        job_name = command['job_name']
        logging.info(f"SCHEDULE definition encountered: Job '{job_name}'. (DAG setup logic goes here).")


# --- Parser and Integration ---

def parse_synapse_script(script_content: str) -> list:
    
    # Placeholder implementation - to be changed:
    lines = [line.strip() for line in script_content.split('\n') if line.strip() and not line.strip().startswith('#')]
    parsed_commands = []
    
    for line in lines:
        parts = line.split()
        if not parts: continue
        command = parts[0].upper()
        command_obj = {"type": command, "raw_line": line}
        
        # Simplified logic for key commands:
        if command == "LOAD":
            command_obj.update({"source": parts[1].strip('"'), "variable": parts[3]})
        elif command == "TRANSFORM":
            command_obj.update({"input_var": parts[1], "output_var": parts[3]})
        elif command == "RUN":
            command_obj.update({"variable": parts[1], "python_call": " ".join(parts[4:])})
        elif command == "SAVE":
            command_obj.update({"variable": parts[1], "destination": parts[3].strip('"')})
        elif command == "SCHEDULE":
            command_obj.update({"job_name": parts[1]}) 
        elif command == "TASK":
            continue
        else:
            command_obj['type'] = "UNKNOWN"
            
        if command not in ["TASK"]:
            parsed_commands.append(command_obj)
            
    return parsed_commands


# --- Main Execution Function ---

def execute_synapse_script(script_path: str):
    """Reads, parses, and executes a Synapse script."""
    try:
        with open(script_path, 'r') as f:
            script_content = f.read()
    except FileNotFoundError:
        logging.error(f"File not found: {script_path}")
        return

    commands = parse_synapse_script(script_content)
    
    # Pass the global scope of the script to the executor 
    # so it can look up defined functions such as 'calculate_metrics'.
    executor = SynapseExecutor(globals()) 
    
    logging.info("\n--- SYNAPSE ENGINE STARTING EXECUTION ---\n")
    for command in commands:
        executor.execute_command(command)
        
    logging.info("\n--- SYNAPSE ENGINE EXECUTION COMPLETE ---")
    
    return executor.data_store


# --- Implementation Setup: Creating the Test Environment ---

# 1. Create a dummy data file that Synapse can LOAD
INPUT_FILE = 'customers_raw.csv'
OUTPUT_FILE = 'customers_final.csv'

def setup_test_files():
    """Creates a dummy CSV file to be loaded by the Synapse script."""
    if os.path.exists(INPUT_FILE):
        os.remove(INPUT_FILE)
        
    dummy_data = {
        'customer_id': [101, 102, 103, 104, 105, 106],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve', 'Frank'],
        'revenue': [150.0, 80.0, 220.0, 310.0, 90.0, 400.0],
        'status': ['active', 'inactive', 'active', 'active', 'inactive', 'active']
    }
    pd.DataFrame(dummy_data).to_csv(INPUT_FILE, index=False)
    logging.info(f"Setup: Created test input file: {INPUT_FILE}")
    
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)
        logging.info(f"Setup: Cleaned previous output file: {OUTPUT_FILE}")

# 2. Define the Synapse script content
SYNAPSE_SCRIPT_CONTENT = f"""
# This Synapse script performs a simple ETL process on CSV files
LOAD "{INPUT_FILE}" AS RawCustomers

# Transformation: Filter for active customers
TRANSFORM RawCustomers AS ActiveCustomers

# Run a custom Python function to log average revenue
RUN ActiveCustomers WITH PYTHON calculate_metrics(col='revenue')

# Save the final result to a new file
SAVE ActiveCustomers TO "{OUTPUT_FILE}"

# Define the orchestration flow
SCHEDULE CustomerETL
DEPENDS ON None
TASK 1: LOAD RawCustomers
TASK 2: TRANSFORM ActiveCustomers
TASK 3: SAVE ActiveCustomers
"""

# 3. Main execution block
if __name__ == '__main__':
    script_file = 'pipeline.synapse'
    
    # 1. Prepare environment
    setup_test_files()

    # 2. Write the Synapse script to a file
    with open(script_file, 'w') as f:
        f.write(SYNAPSE_SCRIPT_CONTENT)
    logging.info(f"Setup: Created Synapse script file: {script_file}")

    # 3. Execute the Synapse engine
    execute_synapse_script(script_file)