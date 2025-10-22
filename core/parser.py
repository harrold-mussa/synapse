import logging
import re

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def parse_synapse_script(script_content: str) -> list:
    """
    Parses a Synapse script into a list of structured command objects (AST).
    
    This implementation uses simple line-based tokenization. A more robust solution 
    would use a formal grammar tool like Lark or PLY.
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
            if command == "LOAD":
                command_obj.update({"source": parts[1].strip('"'), "variable": parts[3]})
            
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

            elif command == "SAVE":
                command_obj.update({"variable": parts[1], "destination": parts[3].strip('"')})
            
            elif command == "SCHEDULE":
                command_obj.update({"job_name": parts[1]})
            
            elif command == "TASK":
                command_obj.update({"step": " ".join(parts[1:])}) 
            
            else:
                command_obj['type'] = "UNKNOWN"
            
            parsed_commands.append(command_obj)
            
        except IndexError:
            logging.error(f"Synapse Parser Error: Malformed command: {line}")

        i += 1
            
    return parsed_commands