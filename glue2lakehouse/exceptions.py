"""
Custom exceptions for the Glue2Databricks migration framework.
Provides granular error handling for production environments.
"""

from typing import Optional, Dict, Any


class Glue2DatabricksError(Exception):
    """Base exception for all Glue2Databricks errors."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        self.message = message
        self.details = details or {}
        super().__init__(self.message)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to dictionary for structured logging."""
        return {
            'error_type': self.__class__.__name__,
            'message': self.message,
            'details': self.details
        }


class ValidationError(Glue2DatabricksError):
    """Raised when input validation fails."""
    pass


class ParseError(Glue2DatabricksError):
    """Raised when code parsing fails."""
    pass


class TransformationError(Glue2DatabricksError):
    """Raised when code transformation fails."""
    pass


class FileSystemError(Glue2DatabricksError):
    """Raised when file system operations fail."""
    pass


class ConfigurationError(Glue2DatabricksError):
    """Raised when configuration is invalid."""
    pass


class DependencyError(Glue2DatabricksError):
    """Raised when dependency resolution fails."""
    pass


class StateError(Glue2DatabricksError):
    """Raised when state management encounters issues."""
    pass


class MigrationError(Glue2DatabricksError):
    """Raised when migration process fails."""
    pass


class AnalysisError(Glue2DatabricksError):
    """Raised when code analysis fails."""
    pass


class BackupError(Glue2DatabricksError):
    """Raised when backup operations fail."""
    pass


class RollbackError(Glue2DatabricksError):
    """Raised when rollback operations fail."""
    pass
