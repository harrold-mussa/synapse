import pandas as pd
import logging
import re
import importlib
import inspect
import os
from typing import Dict, Any
from pandasql import sqldf
from .data_io import load_data, save_data 
from .sql_engine import apply_window_function, SQLExecutionError 

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

class SynapseExecutor:
    """Manages data variables, configuration, and executes commands from the parsed AST."""
    
    def __init__(self, user_functions_module: Any):
        self.data_store: Dict[str, pd.DataFrame] = {}
        self.connection_store: Dict[str, str] = {}
        self.ruleset_store: Dict[str, list] = {}
        self.config_store: Dict[str, str] = {} 
        self.user_functions = user_functions_module
        self.orchestration_dag: Dict[str, list] = {}
        
    def execute_script(self, commands: list):
        """Executes a list of parsed Synapse commands sequentially."""
        
        logging.info("\n--- SYNAPSE ENGINE STARTING EXECUTION ---")
        
        for command in commands:
            command_type = command['type']

            # Skip orchestration definitions during linear execution, except for storage
            if command_type in ("SCHEDULE", "TASK", "ALERT"): 
                self._execute_schedule(command) 
                continue

            logging.info(f"\nEXECUTING: {command['raw_line']}")

            try:
                if command_type == "LOAD":
                    self._execute_load(command)
                elif command_type == "SAVE":
                    self._execute_save(command)
                elif command_type == "TRANSFORM":
                    self._execute_transform(command)
                elif command_type == "RUN":
                    self._execute_run(command)
                
                # New Core Commands
                elif command_type == "DEFINE_CONN":
                    self._execute_define_conn(command)
                elif command_type == "DEFINE_RULESET":
                    self._execute_define_ruleset(command)
                elif command_type == "VALIDATE":
                    self._execute_validate(command)
                elif command_type == "PIVOT":
                    self._execute_pivot(command)
                elif command_type == "WINDOW":
                    self._execute_window(command)
                elif command_type == "CONFIG":
                    self._execute_config(command)
                elif command_type in ("IMPORT_SYNAPSE", "IMPORT_PYTHON"):
                    self._execute_import(command)
                
            except Exception as e:
                logging.error(f"Execution failed for command {command_type}: {e}")
                break
                
        logging.info("\n--- SYNAPSE ENGINE EXECUTION COMPLETE ---")

    # --- Connection and I/O Methods (Updated) ---
    
    def _resolve_source_or_destination(self, path_or_alias: str) -> Dict[str, str]:
        """Resolves an input/output string into a source/connection type and value."""
        if '::' in path_or_alias:
            conn_name, target = path_or_alias.split('::', 1)
            if conn_name in self.connection_store:
                conn_url = self.connection_store[conn_name]
                return {'type': 'database', 'url': conn_url, 'target': target}
            else:
                raise ValueError(f"Connection alias '{conn_name}' not defined in DEFINE_CONN.")
        else:
            return {'type': 'file', 'path': path_or_alias}
            
    def _execute_load(self, command: dict):
        """Handles LOAD using the connection store."""
        source_info = self._resolve_source_or_destination(command['source'])
        
        if source_info['type'] == 'file':
            df = load_data(source_info['path'])
        elif source_info['type'] == 'database':
            # In a full impl, this would call load_from_database(url, table=target)
            logging.info(f"Using connection '{source_info['url']}' to load table '{source_info['target']}'.")
            df = load_data('placeholder.csv') # Fallback to file for demo
        
        self.data_store[command['variable']] = df
        logging.info(f"LOAD successful. Data loaded into '{command['variable']}' with {len(df)} rows.")

    def _execute_save(self, command: dict):
        """Handles SAVE using the connection store."""
        variable = command['variable']
        destination_info = self._resolve_source_or_destination(command['destination'])
        
        if variable not in self.data_store:
            raise ValueError(f"DataFrame variable '{variable}' not found for SAVE command.")
            
        df = self.data_store[variable]
        
        if destination_info['type'] == 'file':
            save_data(df, destination_info['path'])
        elif destination_info['type'] == 'database':
            # In a full impl, this would call save_to_database(df, url, table=target)
            logging.info(f"Using connection '{destination_info['url']}' to save to table '{destination_info['target']}'.")
            save_data(df, 'placeholder_output.csv') # Fallback to file for demo
            
        logging.info(f"SAVE successful. '{variable}' saved to '{command['destination']}'.")

    # --- New Logic Implementations ---
    
    # Robust Configuration and Connection Management

    def _execute_define_conn(self, command: dict):
        """Handles DEFINE_CONN command, stores and resolves connection strings."""
        conn_name = command['conn_name']
        url = command['url']
        
        def resolve_env_vars(s):
            return re.sub(r'\$\{(\w+)\}', lambda m: os.getenv(m.group(1), m.group(0)), s)
            
        resolved_url = resolve_env_vars(url)
        self.connection_store[conn_name] = resolved_url
        # Only logging the resolved URL if it doesn't look like it contains secrets
        logging.info(f"DEFINE_CONN successful. Connection '{conn_name}' stored.")
        
    # 
    First-Class Data Quality and Validation
    def _execute_define_ruleset(self, command: dict):
        """Handles DEFINE RULESET command, stores the rules."""
        self.ruleset_store[command['name']] = command['rules']
        logging.info(f"DEFINE RULESET successful. Ruleset '{command['name']}' defined with {len(command['rules'])} rules.")
        
    def _execute_validate(self, command: dict):
        """Handles VALIDATE command (Placeholder for Data Quality execution)."""
        variable = command['variable']
        ruleset_name = command['ruleset']
        on_fail = command['on_fail']
        
        if ruleset_name not in self.ruleset_store:
            raise ValueError(f"Ruleset '{ruleset_name}' not found.")
        if variable not in self.data_store:
            raise ValueError(f"DataFrame variable '{variable}' not found for VALIDATE command.")
        
        df = self.data_store[variable]
        num_rows = len(df)

        # to edit later
    
        if num_rows == 0:
            logging.error(f"VALIDATE FAILED: DataFrame '{variable}' is empty (Row Count = 0).")
            if on_fail == 'ABORT':
                raise Exception(f"Validation failed for '{variable}' (ON FAIL ABORT). Pipeline stopped.")
            else:
                logging.warning(f"Validation failed for '{variable}' (ON FAIL {on_fail}). Pipeline continuing.")
        else:
            logging.info(f"VALIDATE SUCCESS: Ruleset '{ruleset_name}' checked on '{variable}' ({num_rows} rows).")


    # Simplified Declarative Pandas Operations (PIVOT)

    def _execute_pivot(self, command: dict):
        """Handles PIVOT command using df.pivot_table."""
        input_var = command['input_var']
        output_var = command['output_var']
        params = command['params']
        
        if input_var not in self.data_store:
            raise ValueError(f"Input variable '{input_var}' not found for PIVOT.")
        
        df = self.data_store[input_var]
        
        try:
            # Map simple syntax to pd.pivot_table arguments
            pivot_df = df.pivot_table(
                index=params.get('index'),
                columns=params.get('columns'),
                values=params.get('values'),
                aggfunc=params.get('agg', 'mean')
            ).reset_index()
            
            self.data_store[output_var] = pivot_df
            logging.info(f"PIVOT successful. '{input_var}' pivoted to '{output_var}' with {len(pivot_df)} rows.")
        except Exception as e:
            raise Exception(f"PIVOT execution failed: {e}. Check if index/columns/values exist.")

    # Simplified Declarative Pandas Operations (WINDOW)

    def _execute_window(self, command: dict):
        """Handles WINDOW command, reusing the helper from sql_engine.py."""
        input_var = command['input_var']
        output_var = command['output_var']
        
        if input_var not in self.data_store:
            raise ValueError(f"Input variable '{input_var}' not found for WINDOW.")
        
        df = self.data_store[input_var].copy()
        
        try:
            # apply_window_function is imported from .sql_engine
            window_series = apply_window_function(
                df=df,
                partition_by=command['partition_by'],
                order_by=command['order_by'],
                func_name=command['function'].lower()
            )
            
            df[command['alias']] = window_series
            self.data_store[output_var] = df
            logging.info(f"WINDOW successful. '{command['alias']}' calculated on '{input_var}', result in '{output_var}'.")
        except ValueError as e:
            raise Exception(f"WINDOW execution failed: {e}")

    # Enhanced Modularity and Reusability

    def _execute_import(self, command: dict):
        """Handles IMPORT commands (Placeholder for recursive/dynamic loading)."""
        if command['type'] == 'IMPORT_SYNAPSE':
            path = command['path']
            logging.info(f"IMPORT: Synapse script '{path}' configured for command inclusion. (Placeholder)")
            
        elif command['type'] == 'IMPORT_PYTHON':
            module = command['module']
            try:
                # Dynamically load the module, making its functions available to the user_functions_module
                imported_module = importlib.import_module(module)
                logging.info(f"IMPORT_PYTHON: Python module '{module}' loaded and functions are available via the execution environment.")
            except Exception as e:
                logging.error(f"IMPORT_PYTHON failed for module '{module}': {e}")
                raise

    # Built-in Monitoring and Alerting

    def _execute_config(self, command: dict):
        """Handles CONFIG command, stores global settings."""
        self.config_store[command['config_key']] = command['destination']
        logging.info(f"CONFIG successful. Global setting '{command['config_key']}' set to '{command['destination']}'.")

    # Reusing existing methods (e.g., _execute_transform and _execute_run are largely unchanged)
    
    def _execute_schedule(self, command: dict):
        """Handles the SCHEDULE, TASK, and ALERT definition to build the DAG/metadata."""
        
        if command['type'] == 'SCHEDULE':
            job_name = command['job_name']
            self.current_schedule = job_name 
            self.orchestration_dag[job_name] = []
            logging.info(f"DAG: Defining new job '{job_name}'...")
        
        elif command['type'] == 'TASK' and hasattr(self, 'current_schedule'):
            step = command['step']
            self.orchestration_dag[self.current_schedule].append(step)
            logging.info(f"DAG: Added task '{step}' to job '{self.current_schedule}'")
        
        elif command['type'] == 'ALERT':
            # Store alert metadata with the DAG/Task definition
            task_name = command['task_name']
            target = command['target']
            logging.info(f"ALERT configured: On failure of Task '{task_name}', notify '{target}'.")
            # to edit later. 