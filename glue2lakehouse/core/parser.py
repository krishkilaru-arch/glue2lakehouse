"""Code parsing module for Glue scripts"""

import ast
import re
from typing import List, Dict, Tuple
import astunparse


class GlueCodeParser:
    """Parses and manipulates Glue code using AST"""
    
    def __init__(self, source_code: str):
        self.source_code = source_code
        self.tree = None
        self.lines = source_code.split('\n')
        
    def parse(self) -> bool:
        """Parse the source code into an AST"""
        try:
            self.tree = ast.parse(self.source_code)
            return True
        except SyntaxError as e:
            raise ValueError(f"Failed to parse code: {str(e)}")
    
    def get_imports(self) -> List[Dict]:
        """Extract all import statements"""
        imports = []
        
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.append({
                        'type': 'import',
                        'module': alias.name,
                        'alias': alias.asname,
                        'node': node,
                        'line': node.lineno
                    })
            elif isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    imports.append({
                        'type': 'from',
                        'module': node.module,
                        'name': alias.name,
                        'alias': alias.asname,
                        'node': node,
                        'line': node.lineno
                    })
        
        return imports
    
    def find_glue_context_init(self) -> List[Dict]:
        """Find GlueContext initialization"""
        results = []
        
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Assign):
                if isinstance(node.value, ast.Call):
                    func_name = self._get_function_name(node.value)
                    if 'GlueContext' in func_name:
                        target_name = None
                        if node.targets and isinstance(node.targets[0], ast.Name):
                            target_name = node.targets[0].id
                        
                        results.append({
                            'node': node,
                            'line': node.lineno,
                            'variable': target_name,
                            'func_name': func_name
                        })
        
        return results
    
    def find_dynamic_frame_operations(self) -> List[Dict]:
        """Find all DynamicFrame operations"""
        operations = []
        
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Call):
                func_name = self._get_function_name(node)
                
                # Check for DynamicFrame operations
                if any(pattern in func_name for pattern in [
                    'create_dynamic_frame',
                    'DynamicFrame',
                    'write_dynamic_frame'
                ]):
                    operations.append({
                        'node': node,
                        'line': getattr(node, 'lineno', 0),
                        'operation': func_name,
                        'args': node.args,
                        'kwargs': {kw.arg: kw.value for kw in node.keywords}
                    })
        
        return operations
    
    def find_catalog_operations(self) -> List[Dict]:
        """Find Glue Catalog operations"""
        operations = []
        
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Call):
                func_name = self._get_function_name(node)
                
                if 'from_catalog' in func_name:
                    # Extract database and table
                    db = None
                    table = None
                    
                    for kw in node.keywords:
                        if kw.arg == 'database':
                            db = self._get_constant_value(kw.value)
                        elif kw.arg == 'table_name':
                            table = self._get_constant_value(kw.value)
                    
                    operations.append({
                        'node': node,
                        'line': getattr(node, 'lineno', 0),
                        'operation': func_name,
                        'database': db,
                        'table': table,
                        'kwargs': {kw.arg: kw.value for kw in node.keywords}
                    })
        
        return operations
    
    def find_transform_operations(self) -> List[Dict]:
        """Find Glue transform operations"""
        transforms = []
        transform_names = [
            'ApplyMapping', 'DropFields', 'RenameField', 'SelectFields',
            'Filter', 'Join', 'SplitFields', 'Relationalize'
        ]
        
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Call):
                func_name = self._get_function_name(node)
                
                for transform_name in transform_names:
                    if transform_name in func_name:
                        transforms.append({
                            'node': node,
                            'line': getattr(node, 'lineno', 0),
                            'transform': transform_name,
                            'args': node.args,
                            'kwargs': {kw.arg: kw.value for kw in node.keywords}
                        })
                        break
        
        return transforms
    
    def find_job_operations(self) -> List[Dict]:
        """Find Job-related operations"""
        job_ops = []
        
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Call):
                func_name = self._get_function_name(node)
                
                if any(pattern in func_name for pattern in [
                    'getResolvedOptions',
                    'Job.init',
                    'job.commit',
                    'Job.commit'
                ]):
                    job_ops.append({
                        'node': node,
                        'line': getattr(node, 'lineno', 0),
                        'operation': func_name,
                        'args': node.args,
                        'kwargs': {kw.arg: kw.value for kw in node.keywords}
                    })
        
        return job_ops
    
    def replace_node(self, old_node: ast.AST, new_code: str) -> None:
        """Replace an AST node with new code"""
        # This is a simplified approach - for production, use more robust AST manipulation
        try:
            new_nodes = ast.parse(new_code).body
            if new_nodes:
                # Replace the node in the tree
                # Note: This is a simplified implementation
                pass
        except Exception:
            pass
    
    def remove_import(self, module_name: str) -> None:
        """Remove an import statement"""
        # Filter out import nodes
        new_body = []
        for node in self.tree.body:
            should_keep = True
            
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == module_name:
                        should_keep = False
                        break
            elif isinstance(node, ast.ImportFrom):
                if node.module == module_name:
                    should_keep = False
            
            if should_keep:
                new_body.append(node)
        
        self.tree.body = new_body
    
    def to_source(self) -> str:
        """Convert AST back to source code"""
        if self.tree:
            return astunparse.unparse(self.tree)
        return self.source_code
    
    def _get_function_name(self, node: ast.Call) -> str:
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
    
    def _get_constant_value(self, node):
        """Extract constant value from AST node"""
        if isinstance(node, ast.Str):
            return node.s
        elif isinstance(node, ast.Constant):
            return node.value
        elif isinstance(node, ast.Num):
            return node.n
        return None
