"""
Pipeline - ETL orchestration with dependency management and execution tracking.
"""

import pandas as pd
from typing import Any, Callable, Dict, List, Optional, Union
from datetime import datetime
from functools import wraps
import logging
import traceback
from enum import Enum

from .sql_engine import DuckDBEngine, SQLTemplate, sql, get_engine


class StepStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class PipelineStep:
    """Represents a single step in the pipeline."""
    
    def __init__(self, 
                 name: str, 
                 func: Callable, 
                 step_type: str,
                 depends_on: List[str] = None,
                 retry_count: int = 0,
                 timeout: Optional[int] = None):
        self.name = name
        self.func = func
        self.step_type = step_type
        self.depends_on = depends_on or []
        self.retry_count = retry_count
        self.timeout = timeout
        self.status = StepStatus.PENDING
        self.result = None
        self.error = None
        self.start_time = None
        self.end_time = None
        self.duration = None
    
    def execute(self, context: Dict[str, Any], engine: DuckDBEngine) -> Any:
        """Execute this pipeline step."""
        self.status = StepStatus.RUNNING
        self.start_time = datetime.now()
        
        attempts = 0
        max_attempts = self.retry_count + 1
        
        while attempts < max_attempts:
            try:
                # Pass engine and context to the function
                result = self.func(context, engine)
                
                # If result is a SQLTemplate, execute it
                if isinstance(result, SQLTemplate):
                    result = engine.execute(result, context.get('variables', {}), 
                                           context.get('dataframes', {}))
                
                self.result = result
                self.status = StepStatus.SUCCESS
                self.end_time = datetime.now()
                self.duration = (self.end_time - self.start_time).total_seconds()
                return result
                
            except Exception as e:
                attempts += 1
                if attempts >= max_attempts:
                    self.error = str(e)
                    self.status = StepStatus.FAILED
                    self.end_time = datetime.now()
                    self.duration = (self.end_time - self.start_time).total_seconds()
                    raise
                else:
                    logging.warning(f"Step {self.name} failed (attempt {attempts}/{max_attempts}): {e}")
    
    def __repr__(self):
        return f"PipelineStep(name={self.name}, type={self.step_type}, status={self.status.value})"


class Pipeline:
    """
    ETL Pipeline orchestrator that combines SQL and Python operations.
    
    Example:
        pipeline = Pipeline("customer_etl")
        
        @pipeline.extract("load_customers")
        def load_customers(ctx, engine):
            return sql('''
                SELECT * FROM raw.customers
                WHERE updated_at > {last_run}
            ''')
        
        @pipeline.transform("clean_data", depends_on=["load_customers"])
        def clean_data(ctx, engine):
            df = ctx['results']['load_customers']
            return sql('''
                SELECT 
                    TRIM(name) as name,
                    LOWER(email) as email
                FROM {df: input_df}
            ''').render(dataframes={'input_df': df})
        
        pipeline.run()
    """
    
    def __init__(self, 
                 name: str, 
                 database: str = ":memory:",
                 log_level: int = logging.INFO):
        self.name = name
        self.engine = DuckDBEngine(database)
        self.steps: Dict[str, PipelineStep] = {}
        self.execution_order: List[str] = []
        self.context = {
            'variables': {},
            'dataframes': {},
            'results': {},
            'metadata': {
                'pipeline_name': name,
                'created_at': datetime.now(),
                'last_run': None,
                'run_count': 0
            }
        }
        
        # Setup logging
        self.logger = logging.getLogger(f"synapse.pipeline.{name}")
        self.logger.setLevel(log_level)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
    def _add_step(self, 
                  name: str, 
                  func: Callable, 
                  step_type: str,
                  depends_on: List[str] = None,
                  retry_count: int = 0,
                  timeout: Optional[int] = None) -> Callable:
        """Internal method to add a step to the pipeline."""
        step = PipelineStep(name, func, step_type, depends_on, retry_count, timeout)
        self.steps[name] = step
        return func
    
    def extract(self, 
                name: str = None, 
                depends_on: List[str] = None,
                retry_count: int = 0,
                timeout: Optional[int] = None):
        """Decorator to add an extract step to the pipeline."""
        def decorator(func):
            step_name = name or func.__name__
            self._add_step(step_name, func, "extract", depends_on, retry_count, timeout)
            return func
        return decorator
    
    def transform(self, 
                  name: str = None, 
                  depends_on: List[str] = None,
                  retry_count: int = 0,
                  timeout: Optional[int] = None):
        """Decorator to add a transform step to the pipeline."""
        def decorator(func):
            step_name = name or func.__name__
            self._add_step(step_name, func, "transform", depends_on, retry_count, timeout)
            return func
        return decorator
    
    def load(self, 
             name: str = None, 
             depends_on: List[str] = None,
             retry_count: int = 0,
             timeout: Optional[int] = None):
        """Decorator to add a load step to the pipeline."""
        def decorator(func):
            step_name = name or func.__name__
            self._add_step(step_name, func, "load", depends_on, retry_count, timeout)
            return func
        return decorator
    
    def step(self, 
             name: str = None,
             step_type: str = "custom",
             depends_on: List[str] = None,
             retry_count: int = 0,
             timeout: Optional[int] = None):
        """Decorator to add a custom step to the pipeline."""
        def decorator(func):
            step_name = name or func.__name__
            self._add_step(step_name, func, step_type, depends_on, retry_count, timeout)
            return func
        return decorator
    
    def _resolve_execution_order(self) -> List[str]:
        """Resolve step execution order based on dependencies using topological sort."""
        # Build dependency graph
        in_degree = {name: 0 for name in self.steps}
        graph = {name: [] for name in self.steps}
        
        for name, step in self.steps.items():
            for dep in step.depends_on:
                if dep not in self.steps:
                    raise ValueError(f"Step '{name}' depends on unknown step '{dep}'")
                graph[dep].append(name)
                in_degree[name] += 1
        
        # Topological sort (Kahn's algorithm)
        queue = [name for name, degree in in_degree.items() if degree == 0]
        order = []
        
        while queue:
            current = queue.pop(0)
            order.append(current)
            
            for neighbor in graph[current]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)
        
        if len(order) != len(self.steps):
            raise ValueError("Circular dependency detected in pipeline")
        
        return order
    
    def set_variable(self, name: str, value: Any):
        """Set a context variable available to all steps."""
        self.context['variables'][name] = value
    
    def set_variables(self, **kwargs):
        """Set multiple context variables."""
        self.context['variables'].update(kwargs)
    
    def add_dataframe(self, name: str, df: pd.DataFrame):
        """Add a DataFrame to the context."""
        self.context['dataframes'][name] = df
    
    def run(self, 
            variables: Dict[str, Any] = None,
            dry_run: bool = False) -> Dict[str, Any]:
        """
        Execute the pipeline.
        
        Args:
            variables: Additional variables to pass to steps
            dry_run: If True, only validate without executing
        
        Returns:
            Dictionary containing results and metadata
        """
        # Update context with provided variables
        if variables:
            self.context['variables'].update(variables)
        
        # Add current timestamp
        self.context['variables']['run_timestamp'] = datetime.now()
        
        # Resolve execution order
        self.execution_order = self._resolve_execution_order()
        self.logger.info(f"Pipeline '{self.name}' starting with {len(self.steps)} steps")
        self.logger.info(f"Execution order: {' -> '.join(self.execution_order)}")
        
        if dry_run:
            self.logger.info("Dry run mode - skipping execution")
            return {'execution_order': self.execution_order, 'steps': self.steps}
        
        # Execute steps in order
        start_time = datetime.now()
        failed_steps = []
        
        for step_name in self.execution_order:
            step = self.steps[step_name]
            
            # Check if dependencies succeeded
            deps_ok = all(
                self.steps[dep].status == StepStatus.SUCCESS 
                for dep in step.depends_on
            )
            
            if not deps_ok:
                step.status = StepStatus.SKIPPED
                self.logger.warning(f"Skipping step '{step_name}' due to failed dependencies")
                continue
            
            self.logger.info(f"Executing step '{step_name}' ({step.step_type})")
            
            try:
                result = step.execute(self.context, self.engine)
                self.context['results'][step_name] = result
                
                if isinstance(result, pd.DataFrame):
                    self.logger.info(
                        f"Step '{step_name}' completed successfully "
                        f"({len(result)} rows, {len(result.columns)} columns) "
                        f"in {step.duration:.2f}s"
                    )
                else:
                    self.logger.info(
                        f"Step '{step_name}' completed successfully in {step.duration:.2f}s"
                    )
                    
            except Exception as e:
                self.logger.error(f"Step '{step_name}' failed: {e}")
                self.logger.debug(traceback.format_exc())
                failed_steps.append(step_name)
        
        # Update metadata
        end_time = datetime.now()
        total_duration = (end_time - start_time).total_seconds()
        self.context['metadata']['last_run'] = end_time
        self.context['metadata']['run_count'] += 1
        
        # Generate summary
        summary = {
            'pipeline_name': self.name,
            'total_steps': len(self.steps),
            'successful_steps': sum(1 for s in self.steps.values() if s.status == StepStatus.SUCCESS),
            'failed_steps': len(failed_steps),
            'skipped_steps': sum(1 for s in self.steps.values() if s.status == StepStatus.SKIPPED),
            'total_duration': total_duration,
            'start_time': start_time,
            'end_time': end_time,
            'results': self.context['results'],
            'step_details': {
                name: {
                    'status': step.status.value,
                    'duration': step.duration,
                    'error': step.error
                }
                for name, step in self.steps.items()
            }
        }
        
        if failed_steps:
            self.logger.error(f"Pipeline completed with {len(failed_steps)} failed steps: {failed_steps}")
        else:
            self.logger.info(f"Pipeline '{self.name}' completed successfully in {total_duration:.2f}s")
        
        return summary
    
    def get_status(self) -> Dict[str, str]:
        """Get the current status of all steps."""
        return {name: step.status.value for name, step in self.steps.items()}
    
    def reset(self):
        """Reset all steps to pending status."""
        for step in self.steps.values():
            step.status = StepStatus.PENDING
            step.result = None
            step.error = None
            step.start_time = None
            step.end_time = None
            step.duration = None
        self.context['results'] = {}
    
    def __repr__(self):
        return f"Pipeline(name={self.name}, steps={len(self.steps)})"


class PipelineBuilder:
    """Fluent API for building pipelines."""
    
    def __init__(self, name: str):
        self.pipeline = Pipeline(name)
        self._current_step = None
    
    def extract(self, name: str, query: Union[str, SQLTemplate, Callable]):
        """Add an extract step."""
        if callable(query):
            self.pipeline.extract(name)(query)
        else:
            @self.pipeline.extract(name)
            def _extract(ctx, engine):
                if isinstance(query, SQLTemplate):
                    return engine.execute(query, ctx['variables'], ctx['dataframes'])
                return engine.execute(sql(query), ctx['variables'], ctx['dataframes'])
        self._current_step = name
        return self
    
    def transform(self, name: str, query: Union[str, SQLTemplate, Callable], depends_on: List[str] = None):
        """Add a transform step."""
        if depends_on is None and self._current_step:
            depends_on = [self._current_step]
        
        if callable(query):
            self.pipeline.transform(name, depends_on=depends_on)(query)
        else:
            @self.pipeline.transform(name, depends_on=depends_on)
            def _transform(ctx, engine):
                if isinstance(query, SQLTemplate):
                    return engine.execute(query, ctx['variables'], ctx['dataframes'])
                return engine.execute(sql(query), ctx['variables'], ctx['dataframes'])
        self._current_step = name
        return self
    
    def load(self, name: str, func: Callable, depends_on: List[str] = None):
        """Add a load step."""
        if depends_on is None and self._current_step:
            depends_on = [self._current_step]
        self.pipeline.load(name, depends_on=depends_on)(func)
        self._current_step = name
        return self
    
    def build(self) -> Pipeline:
        """Return the built pipeline."""
        return self.pipeline
