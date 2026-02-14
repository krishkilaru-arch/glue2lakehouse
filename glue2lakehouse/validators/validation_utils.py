"""
Input validation and pre-migration checks.
Ensures data integrity and prevents common errors.
"""

import os
import ast
from pathlib import Path
from typing import List, Dict, Any, Tuple
import logging

from glue2lakehouse.exceptions import ValidationError


logger = logging.getLogger(__name__)


class Validator:
    """Comprehensive validation for migration inputs."""
    
    @staticmethod
    def validate_python_file(file_path: str) -> Tuple[bool, List[str]]:
        """
        Validate that a file is valid Python code.
        
        Args:
            file_path: Path to Python file
            
        Returns:
            Tuple of (is_valid, list of errors)
        """
        errors = []
        
        # Check file exists
        if not os.path.exists(file_path):
            errors.append(f"File does not exist: {file_path}")
            return False, errors
        
        # Check file extension
        if not file_path.endswith('.py'):
            errors.append(f"Not a Python file: {file_path}")
            return False, errors
        
        # Check file is readable
        if not os.access(file_path, os.R_OK):
            errors.append(f"File is not readable: {file_path}")
            return False, errors
        
        # Try to parse Python code
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                code = f.read()
            ast.parse(code)
        except SyntaxError as e:
            errors.append(f"Syntax error in {file_path}: {str(e)}")
            return False, errors
        except UnicodeDecodeError as e:
            errors.append(f"Encoding error in {file_path}: {str(e)}")
            return False, errors
        except Exception as e:
            errors.append(f"Unable to parse {file_path}: {str(e)}")
            return False, errors
        
        return True, []
    
    @staticmethod
    def validate_directory(dir_path: str, must_contain_python: bool = True) -> Tuple[bool, List[str]]:
        """
        Validate directory for migration.
        
        Args:
            dir_path: Path to directory
            must_contain_python: Whether directory must contain .py files
            
        Returns:
            Tuple of (is_valid, list of errors)
        """
        errors = []
        
        # Check directory exists
        if not os.path.exists(dir_path):
            errors.append(f"Directory does not exist: {dir_path}")
            return False, errors
        
        # Check is directory
        if not os.path.isdir(dir_path):
            errors.append(f"Not a directory: {dir_path}")
            return False, errors
        
        # Check directory is readable
        if not os.access(dir_path, os.R_OK):
            errors.append(f"Directory is not readable: {dir_path}")
            return False, errors
        
        # Check for Python files if required
        if must_contain_python:
            python_files = list(Path(dir_path).rglob("*.py"))
            if not python_files:
                errors.append(f"No Python files found in: {dir_path}")
                return False, errors
        
        return True, []
    
    @staticmethod
    def validate_output_path(output_path: str, allow_overwrite: bool = False) -> Tuple[bool, List[str]]:
        """
        Validate output path for migration.
        
        Args:
            output_path: Path for output
            allow_overwrite: Whether to allow overwriting existing files
            
        Returns:
            Tuple of (is_valid, list of errors)
        """
        errors = []
        
        # Check parent directory exists
        parent_dir = os.path.dirname(output_path) or '.'
        if not os.path.exists(parent_dir):
            errors.append(f"Parent directory does not exist: {parent_dir}")
            return False, errors
        
        # Check parent directory is writable
        if not os.access(parent_dir, os.W_OK):
            errors.append(f"Parent directory is not writable: {parent_dir}")
            return False, errors
        
        # Check if output already exists
        if os.path.exists(output_path) and not allow_overwrite:
            errors.append(f"Output path already exists: {output_path}")
            errors.append("Use --force to overwrite")
            return False, errors
        
        return True, []
    
    @staticmethod
    def validate_glue_code(file_path: str) -> Tuple[bool, List[str], Dict[str, Any]]:
        """
        Validate that code contains Glue-specific imports.
        
        Args:
            file_path: Path to Python file
            
        Returns:
            Tuple of (is_valid, warnings, metadata)
        """
        warnings = []
        metadata = {
            'has_glue_context': False,
            'has_dynamic_frame': False,
            'has_glue_imports': False,
            'glue_imports': []
        }
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                code = f.read()
            
            # Check for Glue imports
            glue_patterns = [
                'awsglue.context',
                'awsglue.transforms',
                'awsglue.utils',
                'awsglue.job',
                'awsglue.dynamicframe'
            ]
            
            for pattern in glue_patterns:
                if pattern in code:
                    metadata['has_glue_imports'] = True
                    metadata['glue_imports'].append(pattern)
            
            # Check for GlueContext
            if 'GlueContext' in code:
                metadata['has_glue_context'] = True
            
            # Check for DynamicFrame
            if 'DynamicFrame' in code:
                metadata['has_dynamic_frame'] = True
            
            # Warning if no Glue code detected
            if not metadata['has_glue_imports']:
                warnings.append(
                    f"No Glue imports detected in {file_path}. "
                    "This may not be a Glue script."
                )
        
        except Exception as e:
            warnings.append(f"Could not analyze file {file_path}: {str(e)}")
        
        return True, warnings, metadata
    
    @staticmethod
    def pre_migration_check(
        source: str,
        target: str,
        force: bool = False
    ) -> Tuple[bool, List[str], List[str]]:
        """
        Comprehensive pre-migration validation.
        
        Args:
            source: Source file or directory
            target: Target file or directory
            force: Allow overwriting existing files
            
        Returns:
            Tuple of (is_valid, errors, warnings)
        """
        errors = []
        warnings = []
        
        logger.info(f"Running pre-migration checks...")
        logger.info(f"Source: {source}")
        logger.info(f"Target: {target}")
        
        # Validate source
        if os.path.isfile(source):
            valid, errs = Validator.validate_python_file(source)
            if not valid:
                errors.extend(errs)
            else:
                # Check if it's Glue code
                _, warns, metadata = Validator.validate_glue_code(source)
                warnings.extend(warns)
                if not metadata['has_glue_imports']:
                    warnings.append(
                        "Source file may not contain Glue code. "
                        "Migration may not be necessary."
                    )
        elif os.path.isdir(source):
            valid, errs = Validator.validate_directory(source)
            if not valid:
                errors.extend(errs)
        else:
            errors.append(f"Source path does not exist: {source}")
        
        # Validate target
        valid, errs = Validator.validate_output_path(target, allow_overwrite=force)
        if not valid:
            errors.extend(errs)
        
        # Check for common issues
        if source == target:
            errors.append("Source and target cannot be the same")
        
        # Check disk space (basic check)
        try:
            import shutil
            stat = shutil.disk_usage(os.path.dirname(target) or '.')
            if stat.free < 100 * 1024 * 1024:  # Less than 100MB
                warnings.append(
                    f"Low disk space: {stat.free / (1024**2):.1f} MB available"
                )
        except Exception as e:
            warnings.append(f"Could not check disk space: {str(e)}")
        
        is_valid = len(errors) == 0
        
        logger.info(f"Pre-migration check: {'PASSED' if is_valid else 'FAILED'}")
        if errors:
            logger.error(f"Validation errors: {len(errors)}")
            for err in errors:
                logger.error(f"  - {err}")
        if warnings:
            logger.warning(f"Validation warnings: {len(warnings)}")
            for warn in warnings:
                logger.warning(f"  - {warn}")
        
        return is_valid, errors, warnings
