"""
Transforms - Reusable transformation functions and utilities.
"""

import pandas as pd
from typing import Any, Callable, Dict, List, Optional, Union
from functools import wraps


class Transform:
    """Base class for reusable transformations."""
    
    def __init__(self, name: str = None):
        self.name = name or self.__class__.__name__
    
    def apply(self, df: pd.DataFrame, context: Dict[str, Any] = None) -> pd.DataFrame:
        """Apply the transformation to a DataFrame."""
        raise NotImplementedError("Subclasses must implement apply()")
    
    def __call__(self, df: pd.DataFrame, context: Dict[str, Any] = None) -> pd.DataFrame:
        return self.apply(df, context)


class Extract:
    """Base class for data extraction."""
    
    def __init__(self, name: str = None):
        self.name = name or self.__class__.__name__
    
    def extract(self, context: Dict[str, Any] = None) -> pd.DataFrame:
        """Extract data and return as DataFrame."""
        raise NotImplementedError("Subclasses must implement extract()")
    
    def __call__(self, context: Dict[str, Any] = None) -> pd.DataFrame:
        return self.extract(context)


class Load:
    """Base class for data loading."""
    
    def __init__(self, name: str = None):
        self.name = name or self.__class__.__name__
    
    def load(self, df: pd.DataFrame, context: Dict[str, Any] = None) -> Any:
        """Load data to destination."""
        raise NotImplementedError("Subclasses must implement load()")
    
    def __call__(self, df: pd.DataFrame, context: Dict[str, Any] = None) -> Any:
        return self.load(df, context)


# Common transformation functions

def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Clean column names: lowercase, replace spaces with underscores."""
    df = df.copy()
    df.columns = [col.lower().strip().replace(' ', '_').replace('-', '_') for col in df.columns]
    return df


def remove_duplicates(df: pd.DataFrame, subset: List[str] = None, keep: str = 'first') -> pd.DataFrame:
    """Remove duplicate rows."""
    return df.drop_duplicates(subset=subset, keep=keep)


def fill_nulls(df: pd.DataFrame, fill_value: Any = None, method: str = None) -> pd.DataFrame:
    """Fill null values."""
    if method:
        return df.fillna(method=method)
    elif fill_value is not None:
        return df.fillna(fill_value)
    return df


def cast_columns(df: pd.DataFrame, type_map: Dict[str, str]) -> pd.DataFrame:
    """Cast columns to specified types."""
    df = df.copy()
    for col, dtype in type_map.items():
        if col in df.columns:
            df[col] = df[col].astype(dtype)
    return df


def rename_columns(df: pd.DataFrame, rename_map: Dict[str, str]) -> pd.DataFrame:
    """Rename columns."""
    return df.rename(columns=rename_map)


def filter_rows(df: pd.DataFrame, condition: Callable[[pd.DataFrame], pd.Series]) -> pd.DataFrame:
    """Filter rows based on a condition function."""
    return df[condition(df)]


def add_computed_column(df: pd.DataFrame, name: str, computation: Callable[[pd.DataFrame], pd.Series]) -> pd.DataFrame:
    """Add a new column based on computation."""
    df = df.copy()
    df[name] = computation(df)
    return df


def standardize_dates(df: pd.DataFrame, date_columns: List[str], format: str = None) -> pd.DataFrame:
    """Standardize date columns to datetime."""
    df = df.copy()
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format=format, errors='coerce')
    return df


def trim_strings(df: pd.DataFrame, columns: List[str] = None) -> pd.DataFrame:
    """Trim whitespace from string columns."""
    df = df.copy()
    if columns is None:
        columns = df.select_dtypes(include=['object']).columns.tolist()
    for col in columns:
        if col in df.columns and df[col].dtype == 'object':
            df[col] = df[col].str.strip()
    return df


def lowercase_strings(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """Convert string columns to lowercase."""
    df = df.copy()
    for col in columns:
        if col in df.columns and df[col].dtype == 'object':
            df[col] = df[col].str.lower()
    return df


def validate_not_null(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """Validate that specified columns have no null values."""
    for col in columns:
        if col in df.columns:
            null_count = df[col].isnull().sum()
            if null_count > 0:
                raise ValueError(f"Column '{col}' has {null_count} null values")
    return df


def validate_unique(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """Validate that specified columns (or combination) are unique."""
    duplicates = df.duplicated(subset=columns, keep=False)
    if duplicates.any():
        raise ValueError(f"Duplicate values found in columns {columns}: {duplicates.sum()} rows")
    return df


def validate_range(df: pd.DataFrame, column: str, min_val: Any = None, max_val: Any = None) -> pd.DataFrame:
    """Validate that column values are within a range."""
    if min_val is not None:
        violations = df[column] < min_val
        if violations.any():
            raise ValueError(f"Column '{column}' has {violations.sum()} values below {min_val}")
    if max_val is not None:
        violations = df[column] > max_val
        if violations.any():
            raise ValueError(f"Column '{column}' has {violations.sum()} values above {max_val}")
    return df


# Transformation pipeline builder

class TransformChain:
    """Chain multiple transformations together."""
    
    def __init__(self):
        self.transforms: List[Callable] = []
    
    def add(self, transform: Callable) -> 'TransformChain':
        """Add a transformation to the chain."""
        self.transforms.append(transform)
        return self
    
    def clean_names(self) -> 'TransformChain':
        """Add column name cleaning."""
        return self.add(clean_column_names)
    
    def remove_dups(self, subset: List[str] = None, keep: str = 'first') -> 'TransformChain':
        """Add duplicate removal."""
        return self.add(lambda df: remove_duplicates(df, subset, keep))
    
    def fill(self, value: Any = None, method: str = None) -> 'TransformChain':
        """Add null filling."""
        return self.add(lambda df: fill_nulls(df, value, method))
    
    def cast(self, type_map: Dict[str, str]) -> 'TransformChain':
        """Add type casting."""
        return self.add(lambda df: cast_columns(df, type_map))
    
    def rename(self, rename_map: Dict[str, str]) -> 'TransformChain':
        """Add column renaming."""
        return self.add(lambda df: rename_columns(df, rename_map))
    
    def filter(self, condition: Callable) -> 'TransformChain':
        """Add row filtering."""
        return self.add(lambda df: filter_rows(df, condition))
    
    def compute(self, name: str, computation: Callable) -> 'TransformChain':
        """Add computed column."""
        return self.add(lambda df: add_computed_column(df, name, computation))
    
    def trim(self, columns: List[str] = None) -> 'TransformChain':
        """Add string trimming."""
        return self.add(lambda df: trim_strings(df, columns))
    
    def lower(self, columns: List[str]) -> 'TransformChain':
        """Add string lowercasing."""
        return self.add(lambda df: lowercase_strings(df, columns))
    
    def validate_not_null(self, columns: List[str]) -> 'TransformChain':
        """Add not-null validation."""
        return self.add(lambda df: validate_not_null(df, columns))
    
    def validate_unique(self, columns: List[str]) -> 'TransformChain':
        """Add uniqueness validation."""
        return self.add(lambda df: validate_unique(df, columns))
    
    def validate_range(self, column: str, min_val: Any = None, max_val: Any = None) -> 'TransformChain':
        """Add range validation."""
        return self.add(lambda df: validate_range(df, column, min_val, max_val))
    
    def custom(self, func: Callable[[pd.DataFrame], pd.DataFrame]) -> 'TransformChain':
        """Add a custom transformation function."""
        return self.add(func)
    
    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply all transformations in sequence."""
        result = df
        for transform in self.transforms:
            result = transform(result)
        return result
    
    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.apply(df)


# Decorators for creating transformations

def transform_step(name: str = None):
    """Decorator to mark a function as a transform step."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        wrapper._is_transform = True
        wrapper._transform_name = name or func.__name__
        return wrapper
    return decorator


def validate_input(schema: Dict[str, type] = None, required_columns: List[str] = None):
    """Decorator to validate input DataFrame before transformation."""
    def decorator(func):
        @wraps(func)
        def wrapper(df: pd.DataFrame, *args, **kwargs):
            if required_columns:
                missing = set(required_columns) - set(df.columns)
                if missing:
                    raise ValueError(f"Missing required columns: {missing}")
            
            if schema:
                for col, expected_type in schema.items():
                    if col in df.columns:
                        actual_type = df[col].dtype
                        # Basic type checking (can be extended)
                        if expected_type == str and actual_type != 'object':
                            raise TypeError(f"Column '{col}' expected string, got {actual_type}")
                        elif expected_type == int and not pd.api.types.is_integer_dtype(actual_type):
                            raise TypeError(f"Column '{col}' expected int, got {actual_type}")
                        elif expected_type == float and not pd.api.types.is_float_dtype(actual_type):
                            raise TypeError(f"Column '{col}' expected float, got {actual_type}")
            
            return func(df, *args, **kwargs)
        return wrapper
    return decorator


def log_transform(logger=None):
    """Decorator to log transformation details."""
    def decorator(func):
        @wraps(func)
        def wrapper(df: pd.DataFrame, *args, **kwargs):
            import logging
            log = logger or logging.getLogger(__name__)
            
            log.info(f"Starting transformation: {func.__name__}")
            log.info(f"Input shape: {df.shape}")
            
            result = func(df, *args, **kwargs)
            
            log.info(f"Output shape: {result.shape}")
            log.info(f"Completed transformation: {func.__name__}")
            
            return result
        return wrapper
    return decorator
