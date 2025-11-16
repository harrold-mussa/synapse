"""
Schema - Data validation, type inference, and schema management.
"""

import pandas as pd
from typing import Any, Dict, List, Optional, Union
from datetime import datetime
from enum import Enum


class DataType(Enum):
    """Supported data types for schema definition."""
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"
    JSON = "json"
    ARRAY = "array"


class Column:
    """
    Column definition with type, constraints, and metadata.
    """
    
    def __init__(self,
                 name: str,
                 data_type: Union[DataType, str],
                 nullable: bool = True,
                 unique: bool = False,
                 primary_key: bool = False,
                 default: Any = None,
                 min_value: Any = None,
                 max_value: Any = None,
                 min_length: int = None,
                 max_length: int = None,
                 pattern: str = None,
                 allowed_values: List[Any] = None,
                 description: str = None):
        self.name = name
        
        if isinstance(data_type, str):
            self.data_type = DataType(data_type.lower())
        else:
            self.data_type = data_type
        
        self.nullable = nullable
        self.unique = unique
        self.primary_key = primary_key
        self.default = default
        self.min_value = min_value
        self.max_value = max_value
        self.min_length = min_length
        self.max_length = max_length
        self.pattern = pattern
        self.allowed_values = allowed_values
        self.description = description
    
    def validate_series(self, series: pd.Series) -> List[str]:
        """Validate a pandas Series against this column definition."""
        errors = []
        
        # Check nullability
        if not self.nullable and series.isnull().any():
            null_count = series.isnull().sum()
            errors.append(f"Column '{self.name}' has {null_count} null values but is not nullable")
        
        # Check uniqueness
        if self.unique:
            duplicates = series.duplicated(keep=False)
            if duplicates.any():
                errors.append(f"Column '{self.name}' has {duplicates.sum()} non-unique values but must be unique")
        
        # Type-specific validations on non-null values
        non_null = series.dropna()
        
        if len(non_null) > 0:
            # Value range checks
            if self.min_value is not None:
                violations = non_null < self.min_value
                if violations.any():
                    errors.append(f"Column '{self.name}' has {violations.sum()} values below minimum {self.min_value}")
            
            if self.max_value is not None:
                violations = non_null > self.max_value
                if violations.any():
                    errors.append(f"Column '{self.name}' has {violations.sum()} values above maximum {self.max_value}")
            
            # String length checks
            if self.data_type == DataType.STRING:
                if self.min_length is not None:
                    str_lengths = non_null.astype(str).str.len()
                    violations = str_lengths < self.min_length
                    if violations.any():
                        errors.append(f"Column '{self.name}' has {violations.sum()} values shorter than {self.min_length}")
                
                if self.max_length is not None:
                    str_lengths = non_null.astype(str).str.len()
                    violations = str_lengths > self.max_length
                    if violations.any():
                        errors.append(f"Column '{self.name}' has {violations.sum()} values longer than {self.max_length}")
                
                # Pattern matching
                if self.pattern:
                    import re
                    pattern_re = re.compile(self.pattern)
                    matches = non_null.astype(str).str.match(self.pattern)
                    violations = ~matches
                    if violations.any():
                        errors.append(f"Column '{self.name}' has {violations.sum()} values not matching pattern '{self.pattern}'")
            
            # Allowed values check
            if self.allowed_values is not None:
                violations = ~non_null.isin(self.allowed_values)
                if violations.any():
                    errors.append(f"Column '{self.name}' has {violations.sum()} values not in allowed set")
        
        return errors
    
    def to_pandas_dtype(self) -> str:
        """Convert to pandas dtype string."""
        mapping = {
            DataType.STRING: 'object',
            DataType.INTEGER: 'Int64',  # Nullable integer
            DataType.FLOAT: 'float64',
            DataType.BOOLEAN: 'boolean',
            DataType.DATE: 'datetime64[ns]',
            DataType.DATETIME: 'datetime64[ns]',
            DataType.JSON: 'object',
            DataType.ARRAY: 'object'
        }
        return mapping.get(self.data_type, 'object')
    
    def to_sql_type(self) -> str:
        """Convert to SQL type string."""
        mapping = {
            DataType.STRING: 'VARCHAR',
            DataType.INTEGER: 'INTEGER',
            DataType.FLOAT: 'DOUBLE',
            DataType.BOOLEAN: 'BOOLEAN',
            DataType.DATE: 'DATE',
            DataType.DATETIME: 'TIMESTAMP',
            DataType.JSON: 'JSON',
            DataType.ARRAY: 'VARCHAR[]'
        }
        return mapping.get(self.data_type, 'VARCHAR')
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert column definition to dictionary."""
        return {
            'name': self.name,
            'data_type': self.data_type.value,
            'nullable': self.nullable,
            'unique': self.unique,
            'primary_key': self.primary_key,
            'default': self.default,
            'min_value': self.min_value,
            'max_value': self.max_value,
            'min_length': self.min_length,
            'max_length': self.max_length,
            'pattern': self.pattern,
            'allowed_values': self.allowed_values,
            'description': self.description
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Column':
        """Create Column from dictionary."""
        return cls(**data)
    
    def __repr__(self):
        return f"Column(name={self.name}, type={self.data_type.value}, nullable={self.nullable})"


class Schema:
    """
    Schema definition for a dataset with validation capabilities.
    """
    
    def __init__(self, name: str, columns: List[Column] = None):
        self.name = name
        self.columns: Dict[str, Column] = {}
        self.created_at = datetime.now()
        self.version = "1.0"
        
        if columns:
            for col in columns:
                self.add_column(col)
    
    def add_column(self, column: Column) -> 'Schema':
        """Add a column to the schema."""
        self.columns[column.name] = column
        return self
    
    def column(self,
               name: str,
               data_type: Union[DataType, str],
               **kwargs) -> 'Schema':
        """Fluent API to add a column."""
        col = Column(name, data_type, **kwargs)
        return self.add_column(col)
    
    def string(self, name: str, **kwargs) -> 'Schema':
        """Add a string column."""
        return self.column(name, DataType.STRING, **kwargs)
    
    def integer(self, name: str, **kwargs) -> 'Schema':
        """Add an integer column."""
        return self.column(name, DataType.INTEGER, **kwargs)
    
    def float(self, name: str, **kwargs) -> 'Schema':
        """Add a float column."""
        return self.column(name, DataType.FLOAT, **kwargs)
    
    def boolean(self, name: str, **kwargs) -> 'Schema':
        """Add a boolean column."""
        return self.column(name, DataType.BOOLEAN, **kwargs)
    
    def date(self, name: str, **kwargs) -> 'Schema':
        """Add a date column."""
        return self.column(name, DataType.DATE, **kwargs)
    
    def datetime(self, name: str, **kwargs) -> 'Schema':
        """Add a datetime column."""
        return self.column(name, DataType.DATETIME, **kwargs)
    
    def validate(self, df: pd.DataFrame, strict: bool = False) -> Dict[str, Any]:
        """
        Validate a DataFrame against this schema.
        
        Args:
            df: DataFrame to validate
            strict: If True, require exact column match
        
        Returns:
            Dictionary with validation results
        """
        errors = []
        warnings = []
        
        # Check for missing columns
        expected_cols = set(self.columns.keys())
        actual_cols = set(df.columns)
        
        missing_cols = expected_cols - actual_cols
        extra_cols = actual_cols - expected_cols
        
        if missing_cols:
            errors.append(f"Missing columns: {missing_cols}")
        
        if extra_cols:
            if strict:
                errors.append(f"Unexpected columns: {extra_cols}")
            else:
                warnings.append(f"Extra columns not in schema: {extra_cols}")
        
        # Validate each column
        for col_name, col_def in self.columns.items():
            if col_name in df.columns:
                col_errors = col_def.validate_series(df[col_name])
                errors.extend(col_errors)
        
        # Check primary key uniqueness
        pk_columns = [col.name for col in self.columns.values() if col.primary_key]
        if pk_columns:
            pk_cols_present = [col for col in pk_columns if col in df.columns]
            if pk_cols_present:
                duplicates = df.duplicated(subset=pk_cols_present, keep=False)
                if duplicates.any():
                    errors.append(f"Primary key columns {pk_cols_present} have {duplicates.sum()} duplicate rows")
        
        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings,
            'column_count': len(df.columns),
            'row_count': len(df),
            'schema_columns': len(self.columns)
        }
    
    def enforce(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Enforce schema on DataFrame by casting types and reordering columns.
        """
        result = df.copy()
        
        # Reorder and select only schema columns
        schema_cols = [col for col in self.columns.keys() if col in df.columns]
        result = result[schema_cols]
        
        # Cast types
        for col_name, col_def in self.columns.items():
            if col_name in result.columns:
                try:
                    target_dtype = col_def.to_pandas_dtype()
                    result[col_name] = result[col_name].astype(target_dtype)
                except Exception as e:
                    raise TypeError(f"Cannot cast column '{col_name}' to {target_dtype}: {e}")
        
        # Add missing columns with defaults
        for col_name, col_def in self.columns.items():
            if col_name not in result.columns:
                if col_def.default is not None:
                    result[col_name] = col_def.default
                elif col_def.nullable:
                    result[col_name] = None
                else:
                    raise ValueError(f"Missing required column '{col_name}' with no default value")
        
        return result
    
    def to_create_table_sql(self, table_name: str = None) -> str:
        """Generate CREATE TABLE SQL statement."""
        table_name = table_name or self.name
        
        column_defs = []
        pk_columns = []
        
        for col in self.columns.values():
            col_sql = f"    {col.name} {col.to_sql_type()}"
            if not col.nullable:
                col_sql += " NOT NULL"
            if col.default is not None:
                if isinstance(col.default, str):
                    col_sql += f" DEFAULT '{col.default}'"
                else:
                    col_sql += f" DEFAULT {col.default}"
            if col.unique and not col.primary_key:
                col_sql += " UNIQUE"
            column_defs.append(col_sql)
            
            if col.primary_key:
                pk_columns.append(col.name)
        
        if pk_columns:
            column_defs.append(f"    PRIMARY KEY ({', '.join(pk_columns)})")
        
        sql = f"CREATE TABLE {table_name} (\n"
        sql += ",\n".join(column_defs)
        sql += "\n);"
        
        return sql
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert schema to dictionary."""
        return {
            'name': self.name,
            'version': self.version,
            'created_at': self.created_at.isoformat(),
            'columns': [col.to_dict() for col in self.columns.values()]
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Schema':
        """Create Schema from dictionary."""
        schema = cls(data['name'])
        schema.version = data.get('version', '1.0')
        if 'created_at' in data:
            schema.created_at = datetime.fromisoformat(data['created_at'])
        for col_data in data.get('columns', []):
            schema.add_column(Column.from_dict(col_data))
        return schema
    
    @classmethod
    def infer_from_dataframe(cls, df: pd.DataFrame, name: str = "inferred_schema") -> 'Schema':
        """
        Infer schema from an existing DataFrame.
        """
        schema = cls(name)
        
        for col_name in df.columns:
            series = df[col_name]
            dtype = series.dtype
            
            # Determine data type
            if pd.api.types.is_integer_dtype(dtype):
                data_type = DataType.INTEGER
            elif pd.api.types.is_float_dtype(dtype):
                data_type = DataType.FLOAT
            elif pd.api.types.is_bool_dtype(dtype):
                data_type = DataType.BOOLEAN
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                data_type = DataType.DATETIME
            else:
                data_type = DataType.STRING
            
            # Determine constraints
            nullable = series.isnull().any()
            unique = series.nunique() == len(series.dropna())
            
            # Get value range for numeric types
            min_val = None
            max_val = None
            if data_type in (DataType.INTEGER, DataType.FLOAT):
                min_val = series.min() if not series.isnull().all() else None
                max_val = series.max() if not series.isnull().all() else None
            
            column = Column(
                name=col_name,
                data_type=data_type,
                nullable=nullable,
                unique=unique,
                min_value=min_val,
                max_value=max_val
            )
            schema.add_column(column)
        
        return schema
    
    def __repr__(self):
        return f"Schema(name={self.name}, columns={len(self.columns)})"
    
    def __str__(self):
        lines = [f"Schema: {self.name} (v{self.version})"]
        lines.append("-" * 50)
        for col in self.columns.values():
            nullable = "NULL" if col.nullable else "NOT NULL"
            pk = " [PK]" if col.primary_key else ""
            unique = " [UNIQUE]" if col.unique and not col.primary_key else ""
            lines.append(f"  {col.name}: {col.data_type.value} {nullable}{pk}{unique}")
        return "\n".join(lines)
