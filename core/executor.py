import pandas as pd
import logging
import re
from pandasql import sqldf
import importlib
import inspect

from .data_io import load_data, save_data

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

class SynapseExecutor:
    """Manages data variables and executes commands from the parsed AST."""
    
    def __init__(self, user_functions_module):
        # Dictionary to hold all DataFrames, mapping variable name to DataFrame object
        self.data_store = {}
        # The user-defined module (e.g., user_functions.metrics)
        self.user_functions = user_functions_module
        # Stores DAG structure for orchestration (job_name: [task_list])
        self.orchestration_dag = {}
        
    def execute_script(self, commands: list):
        """Executes a list of parsed Synapse commands sequentially."""
        
        logging.info("\n--- SYNAPSE ENGINE STARTING EXECUTION ---")
        
        for command in commands:
            command_type = command['type']
            
            # Skip orchestration definitions during linear execution
            if command_type in ("SCHEDULE", "TASK"): 
                self._execute_schedule(command) 
                continue

            logging.info(f"\nEXECUTING: {command['raw_line']}")

            try:
                if command_type == "LOAD":
                    self._execute_load(command)
                elif command_type == "TRANSFORM":
                    self._execute_transform(command)
                elif command_type == "RUN":
                    self._execute_run(command)
                elif command_type == "SAVE":
                    self._execute_save(command)
                
            except Exception as e:
                logging.error(f"Execution failed for command {command_type}: {e}")
                break
                
        logging.info("\n--- SYNAPSE ENGINE EXECUTION COMPLETE ---")

    # --- Executor Methods ---
    
    def _execute_load(self, command: dict):
        """Handles the LOAD command using the real data loader."""
        df = load_data(command['source'])
        self.data_store[command['variable']] = df
        logging.info(f"LOAD successful. Data loaded into '{command['variable']}' with {len(df)} rows.")

    def _execute_transform(self, command: dict):
        """Handles the TRANSFORM command using pandasql to execute SQL."""
        input_var = command['input_var']
        output_var = command['output_var']
        sql_query = command['sql_query']
        
        if input_var not in self.data_store:
            raise ValueError(f"Input variable '{input_var}' not found in data store.")

        # Create a dictionary of DataFrames to make them accessible to sqldf
        # sqldf requires DataFrames to be passed in its global scope
        sqldf_scope = {var_name: df for var_name, df in self.data_store.items()}

        # 1. Format SQL Query: Ensure 'FROM' clause uses the input variable
        # We assume the user wrote a complete SQL query in the block
        final_query = sql_query

        # 2. Execute SQL
        transformed_df = sqldf(final_query, sqldf_scope)
        
        self.data_store[output_var] = transformed_df
        logging.info(f"TRANSFORM successful. '{input_var}' transformed to '{output_var}' with {len(transformed_df)} rows.")

    def _execute_run(self, command: dict):
        """
        Handles the RUN ... WITH PYTHON command by executing a user-defined function.
        """
        variable = command['variable']
        python_call = command['python_call']
        
        if variable not in self.data_store:
            raise ValueError(f"DataFrame variable '{variable}' not found for RUN command.")
            
        df = self.data_store[variable]
        
        # Simple regex to extract function name and arguments
        match = re.match(r'(\w+)\s*\((.*)\)', python_call)
        if not match:
            raise ValueError("Invalid Python function call syntax.")
            
        func_name = match.group(1)
        args_str = match.group(2)
        
        # Look up the function in the user_functions module
        func = getattr(self.user_functions, func_name, None)

        if func and callable(func):
            # Parse keyword arguments
            kwargs = {}
            for part in args_str.split(','):
                if '=' in part:
                    k, v = part.split('=', 1)
                    # Simple value cleaning
                    kwargs[k.strip()] = v.strip().strip("'\"") 
            
            # Use inspect to check function signature and pass DataFrame
            sig = inspect.signature(func)
            if not sig.parameters:
                # If function takes no arguments, just call it (e.g., custom_api_call())
                new_df = func(**kwargs) 
            else:
                # Assume first argument is the DataFrame
                new_df = func(df, **kwargs) 
                
            self.data_store[variable] = new_df 
            logging.info(f"RUN successful: Python function '{python_call}' executed on '{variable}'.")
        else:
             raise NameError(f"Python function '{func_name}' is not defined or is not callable in user_functions.")
        
    def _execute_save(self, command: dict):
        """Handles the SAVE command using the real data saver."""
        variable = command['variable']
        destination = command['destination']
        
        if variable not in self.data_store:
            raise ValueError(f"DataFrame variable '{variable}' not found for SAVE command.")
            
        df = self.data_store[variable]
        save_data(df, destination)

    def _execute_schedule(self, command: dict):
        """Handles the SCHEDULE and TASK definition to build the DAG."""
        
        if command['type'] == 'SCHEDULE':
            job_name = command['job_name']
            self.current_schedule = job_name 
            self.orchestration_dag[job_name] = []
            logging.info(f"DAG: Defining new job '{job_name}'...")
        
        elif command['type'] == 'TASK' and hasattr(self, 'current_schedule'):
            step = command['step']
            self.orchestration_dag[self.current_schedule].append(step)
            logging.info(f"DAG: Added task '{step}' to job '{self.current_schedule}'")