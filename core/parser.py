import logging
import re

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def parse_synapse_script(script_content: str) -> list:
    """
    Parses a Synapse script into a list of structured command objects (AST).
    
    This implementation uses simple line-based tokenization and handles multi-line blocks 
    for TRANSFORM, DEFINE RULESET, PIVOT, and WINDOW commands.
    """
    lines = [
        line.strip() 
        for line in script_content.split('\n') 
        if line.strip() and not line.strip().startswith('#')
    ]
    
    parsed_commands = []
    
    i = 0
    while i < len(lines):
        line = lines[i]
        parts = line.split()
        if not parts:
            i += 1
            continue
            
        command = parts[0].upper()
        command_obj = {"type": command, "raw_line": line}
        
        try:
            
            # --- Existing Commands ---

            if command == "LOAD":
                command_obj.update({"source": parts[1].strip('"'), "variable": parts[3]})
            
            elif command == "SAVE":
                command_obj.update({"variable": parts[1], "destination": parts[3].strip('"')})

            elif command == "TRANSFORM":
                input_var = parts[1]
                output_var = parts[3]
                sql_lines = []
                j = i + 1
                while j < len(lines) and lines[j].startswith((' ', '\t')):
                    sql_lines.append(lines[j].strip())
                    j += 1
                
                command_obj.update({
                    "input_var": input_var, 
                    "output_var": output_var,
                    "sql_query": " ".join(sql_lines)
                })
                i = j - 1

            elif command == "RUN":
                variable = parts[1]
                python_call = " ".join(parts[4:])
                command_obj.update({"variable": variable, "python_call": python_call})

            # --- New Commands: Configuration & Modularity ---

            elif command == "DEFINE_CONN":
                # DEFINE_CONN DW_Prod URL "postgresql://..."
                command_obj.update({"conn_name": parts[1], "url": parts[3].strip('"')})
            
            elif command == "DEFINE" and parts[1].upper() == "RULESET":
                # DEFINE RULESET Customer_DQ_Rules \n RULE ...
                ruleset_name = parts[2]
                rules = []
                j = i + 1
                while j < len(lines):
                    sub_line = lines[j].strip()
                    rule_match = re.match(r'RULE\s+(.*)', sub_line, re.IGNORECASE)
                    if not sub_line or rule_match:
                        if rule_match:
                            rules.append(rule_match.group(1).strip())
                        j += 1
                    else:
                        break # End of RULESET block
                
                command_obj.update({"type": "DEFINE_RULESET", "name": ruleset_name, "rules": rules})
                i = j - 1

            elif command == "VALIDATE":
                # VALIDATE CleanedCustomers WITH RULESET Customer_DQ_Rules ON FAIL ABORT
                command_obj.update({
                    "variable": parts[1], 
                    "ruleset": parts[3],
                    "on_fail": parts[5].upper() if len(parts) > 5 and parts[4].upper() == "ON" else "WARN"
                })

            elif command == "IMPORT":
                # IMPORT "common_definitions.synapse"
                command_obj.update({"type": "IMPORT_SYNAPSE", "path": parts[1].strip('"')})

            elif command == "IMPORT_PYTHON":
                # IMPORT_PYTHON "data_quality.email_utils"
                command_obj.update({"module": parts[1].strip('"')})

            # --- New Commands: Declarative Pandas ---

            elif command == "PIVOT":
                # PIVOT SalesData AS MonthlySummary \n INDEX...
                output_var = parts[3]
                input_var = parts[1]
                pivot_params = {}
                j = i + 1
                while j < len(lines) and lines[j].startswith((' ', '\t')):
                    sub_line = lines[j].strip()
                    sub_parts = sub_line.split()
                    if sub_parts and sub_parts[0].upper() in ("INDEX", "COLUMNS", "VALUES"):
                        key = sub_parts[0].lower()
                        if key == "values":
                            value_col = sub_parts[1]
                            agg_func = sub_parts[3].lower() if len(sub_parts) > 3 and sub_parts[2].upper() == "AGG" else "mean"
                            pivot_params["values"] = value_col
                            pivot_params["agg"] = agg_func
                        else:
                            pivot_params[key] = sub_parts[1]
                    j += 1
                
                command_obj.update({
                    "input_var": input_var, 
                    "output_var": output_var,
                    "params": pivot_params
                })
                i = j - 1

            elif command == "WINDOW":
                # WINDOW SalesData AS RankedSales \n FUNCTION ROW_NUMBER() OVER (...) AS rnk
                output_var = parts[3]
                input_var = parts[1]
                
                # Combine current line with indented lines for full window definition parsing
                window_text = line + " " + " ".join([lines[j].strip() for j in range(i + 1, len(lines)) if lines[j].startswith((' ', '\t'))])
                
                window_parts = re.search(r'FUNCTION\s+(\w+)\s*\((.*)\)\s+OVER\s+\((.*)\)\s+AS\s+(\w+)', window_text, re.IGNORECASE)
                
                if window_parts:
                    func_name, _, over_clause, alias = window_parts.groups()
                    
                    partition_by_match = re.search(r'PARTITION BY\s+(.*?)(?:\s+ORDER BY|$)', over_clause, re.IGNORECASE | re.DOTALL)
                    order_by_match = re.search(r'ORDER BY\s+(.*)', over_clause, re.IGNORECASE | re.DOTALL)

                    command_obj.update({
                        "input_var": input_var, 
                        "output_var": output_var,
                        "function": func_name,
                        "alias": alias,
                        "partition_by": [p.strip() for p in partition_by_match.group(1).split(',')] if partition_by_match else [],
                        "order_by": order_by_match.group(1).strip() if order_by_match else ""
                    })
                
                # Advance index past indented lines if any
                j = i + 1
                while j < len(lines) and lines[j].startswith((' ', '\t')):
                    j += 1
                i = j - 1

            # --- New Commands: Orchestration & Monitoring ---

            elif command == "CONFIG" and parts[1].upper() == "LOGGING_TABLE":
                # CONFIG LOGGING_TABLE TO DW_Prod::meta.etl_runs
                command_obj.update({
                    "config_key": "LOGGING_TABLE", 
                    "destination": parts[3]
                })
            
            elif command == "ALERT" and parts[1].upper() == "ON" and parts[2].upper() == "FAILURE" and parts[3].upper() == "TASK":
                # ALERT ON FAILURE TASK TransformData TO Slack(channel="#alerts")
                command_obj.update({
                    "task_name": parts[4], 
                    "target": " ".join(parts[6:])
                })

            # --- Existing Orchestration ---
            elif command == "SCHEDULE":
                command_obj.update({"job_name": parts[1]})
            
            elif command == "TASK":
                command_obj.update({"step": " ".join(parts[1:])}) 
            
            else:
                command_obj['type'] = "UNKNOWN"
            
            parsed_commands.append(command_obj)
            
        except IndexError:
            logging.error(f"Synapse Parser Error: Malformed command: {line}")
            command_obj['type'] = "MALFORMED" 
        
        i += 1
            
    return parsed_commands