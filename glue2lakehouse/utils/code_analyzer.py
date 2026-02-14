"""Code analysis utilities for Glue scripts"""

import ast
from typing import Dict, List, Set, Any
from collections import defaultdict


class GlueCodeAnalyzer:
    """Analyzes Glue code to identify patterns and dependencies"""
    
    def __init__(self, source_code: str):
        self.source_code = source_code
        self.tree = None
        self.imports = []
        self.glue_context_usage = []
        self.dynamic_frame_usage = []
        self.catalog_references = []
        self.transforms_used = []
        self.s3_paths = []
        self.job_arguments = []
        
    def analyze(self) -> Dict:
        """Perform comprehensive analysis of the Glue code"""
        try:
            self.tree = ast.parse(self.source_code)
        except SyntaxError as e:
            return {
                'success': False,
                'error': f"Syntax error in source code: {str(e)}"
            }
        
        self._analyze_imports()
        self._analyze_glue_context()
        self._analyze_dynamic_frames()
        self._analyze_catalog_references()
        self._analyze_transforms()
        self._analyze_s3_paths()
        self._analyze_job_arguments()
        
        return {
            'success': True,
            'imports': self.imports,
            'glue_context_usage': len(self.glue_context_usage),
            'dynamic_frame_count': len(self.dynamic_frame_usage),
            'catalog_references': self.catalog_references,
            'transforms_used': self.transforms_used,
            's3_paths': self.s3_paths,
            'job_arguments': self.job_arguments,
            'complexity_score': self._calculate_complexity(),
        }
    
    def _analyze_imports(self):
        """Extract all import statements"""
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    self.imports.append({
                        'type': 'import',
                        'module': alias.name,
                        'alias': alias.asname
                    })
            elif isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    self.imports.append({
                        'type': 'from',
                        'module': node.module,
                        'name': alias.name,
                        'alias': alias.asname
                    })
    
    def _analyze_glue_context(self):
        """Find GlueContext instantiation and usage"""
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Call):
                if self._is_glue_context_call(node):
                    self.glue_context_usage.append(node)
    
    def _analyze_dynamic_frames(self):
        """Find DynamicFrame creation and operations"""
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Call):
                if self._is_dynamic_frame_operation(node):
                    self.dynamic_frame_usage.append(node)
    
    def _analyze_catalog_references(self):
        """Find Glue Data Catalog references"""
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Call):
                catalog_ref = self._extract_catalog_reference(node)
                if catalog_ref:
                    self.catalog_references.append(catalog_ref)
    
    def _analyze_transforms(self):
        """Identify Glue transforms being used"""
        transform_names = [
            'ApplyMapping', 'DropFields', 'RenameField', 'SelectFields',
            'Filter', 'Join', 'SplitFields', 'Relationalize'
        ]
        
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Call):
                func_name = self._get_function_name(node)
                if any(transform in func_name for transform in transform_names):
                    self.transforms_used.append(func_name)
    
    def _analyze_s3_paths(self):
        """Extract S3 paths from the code"""
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Str):
                if node.s.startswith('s3://') or node.s.startswith('s3a://'):
                    self.s3_paths.append(node.s)
            elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                if node.value.startswith('s3://') or node.value.startswith('s3a://'):
                    self.s3_paths.append(node.value)
    
    def _analyze_job_arguments(self):
        """Find job argument usage (getResolvedOptions)"""
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Call):
                func_name = self._get_function_name(node)
                if 'getResolvedOptions' in func_name:
                    if len(node.args) >= 2:
                        self.job_arguments.append(node.args[1])
    
    def _is_glue_context_call(self, node) -> bool:
        """Check if node is a GlueContext call"""
        func_name = self._get_function_name(node)
        return 'GlueContext' in func_name
    
    def _is_dynamic_frame_operation(self, node) -> bool:
        """Check if node is a DynamicFrame operation"""
        func_name = self._get_function_name(node)
        dynamic_frame_ops = [
            'create_dynamic_frame',
            'DynamicFrame',
            'toDF',
            'fromDF'
        ]
        return any(op in func_name for op in dynamic_frame_ops)
    
    def _extract_catalog_reference(self, node) -> Dict:
        """Extract database and table name from catalog operations"""
        func_name = self._get_function_name(node)
        
        if 'from_catalog' in func_name:
            db_name = None
            table_name = None
            
            # Check keyword arguments
            for keyword in node.keywords:
                if keyword.arg == 'database':
                    db_name = self._get_string_value(keyword.value)
                elif keyword.arg == 'table_name':
                    table_name = self._get_string_value(keyword.value)
            
            if db_name and table_name:
                return {'database': db_name, 'table': table_name}
        
        return None
    
    def _get_function_name(self, node) -> str:
        """Extract function name from a Call node"""
        if isinstance(node.func, ast.Name):
            return node.func.id
        elif isinstance(node.func, ast.Attribute):
            parts = []
            current = node.func
            while isinstance(current, ast.Attribute):
                parts.append(current.attr)
                current = current.value
            if isinstance(current, ast.Name):
                parts.append(current.id)
            return '.'.join(reversed(parts))
        return ''
    
    def _get_string_value(self, node):
        """Extract string value from AST node"""
        if isinstance(node, ast.Str):
            return node.s
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        return None
    
    def _calculate_complexity(self) -> int:
        """Calculate migration complexity score"""
        score = 0
        score += len(self.glue_context_usage) * 2
        score += len(self.dynamic_frame_usage) * 3
        score += len(self.catalog_references) * 2
        score += len(self.transforms_used) * 4
        score += len(self.s3_paths)
        score += len(self.job_arguments)
        return score


class CodeAnalyzer:
    """
    Wrapper class for analyzing Glue code files.
    Provides a simple interface for file-based analysis.
    """
    
    def analyze_file(self, file_path: str) -> Dict[str, Any]:
        """
        Analyze a Glue Python file.
        
        Args:
            file_path: Path to the file to analyze
            
        Returns:
            Dictionary with analysis results
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                code = f.read()
            
            analyzer = GlueCodeAnalyzer(code)
            result = analyzer.analyze()
            
            if result.get('success'):
                return {
                    'complexity_score': analyzer.calculate_complexity_score(),
                    'dynamic_frames': analyzer.dynamic_frame_usage,
                    'catalog_references': analyzer.catalog_references,
                    'transforms': analyzer.transforms_used,
                    'imports': analyzer.imports,
                    's3_paths': analyzer.s3_paths,
                    'job_arguments': analyzer.job_arguments,
                }
            else:
                return {'error': result.get('error')}
        
        except Exception as e:
            return {'error': str(e)}
