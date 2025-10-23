"""
DAG Orchestrator for Synapse
Executes tasks in dependency order with parallel processing, retries, and checkpointing.
"""

import logging
from typing import Dict, List, Set, Callable, Any, Optional
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


class TaskStatus(Enum):
    """Possible states of a task."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    RETRYING = "retrying"


@dataclass
class Task:
    """Represents a single task in the DAG."""
    name: str
    func: Callable
    depends_on: List[str] = field(default_factory=list)
    status: TaskStatus = TaskStatus.PENDING
    retries: int = 0
    max_retries: int = 3
    timeout: Optional[int] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    error: Optional[str] = None
    result: Any = None
    
    def __hash__(self):
        return hash(self.name)
    
    def duration(self) -> float:
        """Return task duration in seconds."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0


class DAGOrchestrator:
    """
    Manages and executes a Directed Acyclic Graph of tasks.
    
    Features:
    - Automatic dependency resolution
    - Parallel execution of independent tasks
    - Retry logic with exponential backoff
    - Checkpointing and resume capability
    - Task timeout handling
    """
    
    def __init__(self, name: str, max_workers: int = 4, checkpoint_dir: str = None):
        self.name = name
        self.tasks: Dict[str, Task] = {}
        self.max_workers = max_workers
        self.checkpoint_dir = checkpoint_dir or './checkpoints'
        self.execution_log: List[Dict] = []
        
    def add_task(self, name: str, func: Callable, depends_on: List[str] = None, 
                 max_retries: int = 3, timeout: int = None):
        """
        Add a task to the DAG.
        
        Args:
            name: Unique task identifier
            func: Function to execute
            depends_on: List of task names this task depends on
            max_retries: Maximum retry attempts
            timeout: Task timeout in seconds
        """
        if name in self.tasks:
            raise ValueError(f"Task '{name}' already exists in DAG")
        
        task = Task(
            name=name,
            func=func,
            depends_on=depends_on or [],
            max_retries=max_retries,
            timeout=timeout
        )
        self.tasks[name] = task
        logging.info(f"Added task '{name}' with dependencies: {depends_on or 'None'}")
    
    def validate_dag(self) -> bool:
        """
        Validate that the DAG is acyclic and all dependencies exist.
        
        Returns:
            True if DAG is valid
        """
        # Check all dependencies exist
        for task in self.tasks.values():
            for dep in task.depends_on:
                if dep not in self.tasks:
                    raise ValueError(f"Task '{task.name}' depends on non-existent task '{dep}'")
        
        # Check for cycles
        if self._has_cycle():
            raise ValueError(f"DAG '{self.name}' contains cycles")
        
        logging.info(f"DAG validation passed for '{self.name}'")
        return True
    
    def _has_cycle(self) -> bool:
        """Detect cycles using DFS."""
        visited = set()
        rec_stack = set()
        
        def visit(task_name: str) -> bool:
            visited.add(task_name)
            rec_stack.add(task_name)
            
            for dep in self.tasks[task_name].depends_on:
                if dep not in visited:
                    if visit(dep):
                        return True
                elif dep in rec_stack:
                    return True
            
            rec_stack.remove(task_name)
            return False
        
        for task_name in self.tasks:
            if task_name not in visited:
                if visit(task_name):
                    return True
        return False
    
    def get_execution_order(self) -> List[Set[str]]:
        """
        Determine execution order using topological sort.
        Returns list of sets, where each set contains tasks that can run in parallel.
        
        Returns:
            List of task name sets representing execution levels
        """
        # Calculate in-degree for each task
        in_degree = {name: 0 for name in self.tasks}
        for task in self.tasks.values():
            for dep in task.depends_on:
                in_degree[task.name] += 1
        
        levels = []
        remaining = set(self.tasks.keys())
        
        while remaining:
            # Find all tasks with in-degree 0
            ready = {name for name in remaining if in_degree[name] == 0}
            
            if not ready:
                raise ValueError("Circular dependency detected")
            
            levels.append(ready)
            
            # Remove ready tasks and update in-degrees
            for task_name in ready:
                remaining.remove(task_name)
                for other_task in self.tasks.values():
                    if task_name in other_task.depends_on:
                        in_degree[other_task.name] -= 1
        
        return levels
    
    def execute(self, resume: bool = False) -> Dict[str, Any]:
        """
        Execute the DAG with parallel processing where possible.
        
        Args:
            resume: Whether to resume from last checkpoint
            
        Returns:
            Execution summary dictionary
        """
        self.validate_dag()
        
        if resume:
            self._load_checkpoint()
        
        execution_levels = self.get_execution_order()
        
        logging.info(f"\n{'='*60}")
        logging.info(f"Starting DAG execution: '{self.name}'")
        logging.info(f"Total tasks: {len(self.tasks)}")
        logging.info(f"Execution levels: {len(execution_levels)}")
        logging.info(f"Max parallel workers: {self.max_workers}")
        logging.info(f"{'='*60}\n")
        
        start_time = datetime.now()
        
        for level_idx, level_tasks in enumerate(execution_levels, 1):
            logging.info(f"\n--- Level {level_idx}: {len(level_tasks)} parallel tasks ---")
            
            # Filter out already completed tasks if resuming
            tasks_to_run = [
                name for name in level_tasks 
                if self.tasks[name].status != TaskStatus.SUCCESS
            ]
            
            if not tasks_to_run:
                logging.info("All tasks in this level already completed (resume mode)")
                continue
            
            # Execute tasks in parallel
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_task = {
                    executor.submit(self._execute_task, name): name 
                    for name in tasks_to_run
                }
                
                for future in as_completed(future_to_task):
                    task_name = future_to_task[future]
                    try:
                        future.result()
                    except Exception as e:
                        logging.error(f"Task '{task_name}' failed: {e}")
            
            # Save checkpoint after each level
            self._save_checkpoint()
            
            # Check if any task failed
            failed_tasks = [
                name for name in level_tasks 
                if self.tasks[name].status == TaskStatus.FAILED
            ]
            if failed_tasks:
                logging.error(f"DAG execution stopped due to failures: {failed_tasks}")
                break
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Generate execution summary
        summary = self._generate_summary(duration)
        self._log_summary(summary)
        
        return summary
    
    def _execute_task(self, task_name: str):
        """Execute a single task with retry logic."""
        task = self.tasks[task_name]
        
        # Check dependencies completed successfully
        for dep in task.depends_on:
            if self.tasks[dep].status != TaskStatus.SUCCESS:
                task.status = TaskStatus.SKIPPED
                logging.warning(f"Task '{task_name}' skipped due to failed dependency '{dep}'")
                return
        
        task.status = TaskStatus.RUNNING
        task.start_time = datetime.now()
        
        logging.info(f"▶ Starting task: '{task_name}'")
        
        while task.retries <= task.max_retries:
            try:
                # Execute the task function
                task.result = task.func()
                task.status = TaskStatus.SUCCESS
                task.end_time = datetime.now()
                
                logging.info(f"✓ Task '{task_name}' completed successfully in {task.duration():.2f}s")
                
                self.execution_log.append({
                    'task': task_name,
                    'status': 'success',
                    'duration': task.duration(),
                    'timestamp': datetime.now().isoformat()
                })
                return
                
            except Exception as e:
                task.retries += 1
                task.error = str(e)
                
                if task.retries <= task.max_retries:
                    wait_time = 2 ** task.retries  # Exponential backoff
                    logging.warning(
                        f"✗ Task '{task_name}' failed (attempt {task.retries}/{task.max_retries}). "
                        f"Retrying in {wait_time}s... Error: {e}"
                    )
                    task.status = TaskStatus.RETRYING
                    time.sleep(wait_time)
                else:
                    task.status = TaskStatus.FAILED
                    task.end_time = datetime.now()
                    logging.error(f"✗✗ Task '{task_name}' failed after {task.max_retries} retries. Error: {e}")
                    
                    self.execution_log.append({
                        'task': task_name,
                        'status': 'failed',
                        'error': str(e),
                        'retries': task.retries,
                        'timestamp': datetime.now().isoformat()
                    })
                    raise
    
    def _save_checkpoint(self):
        """Save current execution state to disk."""
        if not self.checkpoint_dir:
            return
        
        os.makedirs(self.checkpoint_dir, exist_ok=True)
        checkpoint_path = os.path.join(self.checkpoint_dir, f"{self.name}_checkpoint.json")
        
        checkpoint_data = {
            'dag_name': self.name,
            'timestamp': datetime.now().isoformat(),
            'tasks': {
                name: {
                    'status': task.status.value,
                    'retries': task.retries,
                    'duration': task.duration()
                }
                for name, task in self.tasks.items()
            }
        }
        
        with open(checkpoint_path, 'w') as f:
            json.dump(checkpoint_data, f, indent=2)
        
        logging.debug(f"Checkpoint saved to {checkpoint_path}")
    
    def _load_checkpoint(self):
        """Load execution state from checkpoint."""
        checkpoint_path = os.path.join(self.checkpoint_dir, f"{self.name}_checkpoint.json")
        
        if not os.path.exists(checkpoint_path):
            logging.info("No checkpoint found, starting fresh execution")
            return
        
        with open(checkpoint_path, 'r') as f:
            checkpoint_data = json.load(f)
        
        for task_name, task_data in checkpoint_data['tasks'].items():
            if task_name in self.tasks:
                self.tasks[task_name].status = TaskStatus(task_data['status'])
                self.tasks[task_name].retries = task_data['retries']
        
        logging.info(f"Resumed from checkpoint: {checkpoint_path}")
    
    def _generate_summary(self, duration: float) -> Dict[str, Any]:
        """Generate execution summary statistics."""
        status_counts = {status: 0 for status in TaskStatus}
        for task in self.tasks.values():
            status_counts[task.status] += 1
        
        summary = {
            'dag_name': self.name,
            'total_duration': duration,
            'total_tasks': len(self.tasks),
            'successful': status_counts[TaskStatus.SUCCESS],
            'failed': status_counts[TaskStatus.FAILED],
            'skipped': status_counts[TaskStatus.SKIPPED],
            'task_details': {
                name: {
                    'status': task.status.value,
                    'duration': task.duration(),
                    'retries': task.retries,
                    'error': task.error
                }
                for name, task in self.tasks.items()
            }
        }
        return summary
    
    def _log_summary(self, summary: Dict[str, Any]):
        """Pretty print execution summary."""
        logging.info(f"\n{'='*60}")
        logging.info(f"DAG EXECUTION SUMMARY: '{self.name}'")
        logging.info(f"{'='*60}")
        logging.info(f"Total Duration: {summary['total_duration']:.2f}s")
        logging.info(f"Total Tasks: {summary['total_tasks']}")
        logging.info(f"✓ Successful: {summary['successful']}")
        logging.info(f"✗ Failed: {summary['failed']}")
        logging.info(f"⊘ Skipped: {summary['skipped']}")
        logging.info(f"{'='*60}\n")
    
    def visualize(self) -> str:
        """Generate a simple text-based visualization of the DAG."""
        lines = [f"DAG: {self.name}", "=" * 40]
        
        for task_name, task in self.tasks.items():
            status_symbol = {
                TaskStatus.SUCCESS: "✓",
                TaskStatus.FAILED: "✗",
                TaskStatus.PENDING: "○",
                TaskStatus.RUNNING: "▶",
                TaskStatus.SKIPPED: "⊘"
            }.get(task.status, "?")
            
            deps = f" <- {', '.join(task.depends_on)}" if task.depends_on else ""
            lines.append(f"{status_symbol} {task_name}{deps}")
        
        return "\n".join(lines)


# --- Example Usage ---
if __name__ == '__main__':
    # Create a sample DAG
    dag = DAGOrchestrator("sample_etl", max_workers=2)
    
    # Define some sample tasks
    def load_data():
        logging.info("Loading data...")
        time.sleep(1)
        return {"rows": 1000}
    
    def transform_data():
        logging.info("Transforming data...")
        time.sleep(2)
        return {"rows": 950}
    
    def validate_data():
        logging.info("Validating data...")
        time.sleep(1)
        return {"valid": True}
    
    def save_data():
        logging.info("Saving data...")
        time.sleep(1)
        return {"saved": True}
    
    # Build the DAG
    dag.add_task("load", load_data)
    dag.add_task("transform", transform_data, depends_on=["load"])
    dag.add_task("validate", validate_data, depends_on=["transform"])
    dag.add_task("save", save_data, depends_on=["validate"])
    
    # Visualize before execution
    print(dag.visualize())
    
    # Execute
    summary = dag.execute()
    
    # Visualize after execution
    print("\n" + dag.visualize())