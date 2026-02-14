"""
AST-Based Code Transformer

Handles complex transformations that regex can't do properly:
- Class method transformations
- Proper variable scoping
- Complex nested patterns
- Type annotation updates
"""

import ast
import copy
from typing import Dict, List, Set, Optional, Any


class GlueClassTransformer(ast.NodeTransformer):
    """
    AST transformer that properly converts Glue helper classes.
    
    Handles:
    - GlueContextManager → DatabricksJobRunner
    - Method body transformations
    - Attribute access updates
    - Return type changes
    """
    
    def __init__(self, target_catalog: str = 'main'):
        self.target_catalog = target_catalog
        self.transformations = []
        self.current_class = None
        self.glue_vars = set()
    
    def visit_ClassDef(self, node: ast.ClassDef) -> ast.ClassDef:
        """Transform Glue-related classes."""
        self.current_class = node.name
        
        # Rename Glue-related classes
        class_renames = {
            'GlueContextManager': 'DatabricksJobRunner',
            'GlueJobHelper': 'DatabricksJobHelper',
            'GlueDataFrameHelper': 'SparkDataFrameHelper',
        }
        
        if node.name in class_renames:
            new_name = class_renames[node.name]
            self.transformations.append(f"Renamed class {node.name} → {new_name}")
            node.name = new_name
        
        # Process all methods
        self.generic_visit(node)
        self.current_class = None
        
        return node
    
    def visit_FunctionDef(self, node: ast.FunctionDef) -> ast.FunctionDef:
        """Transform function definitions."""
        
        # Transform return type annotations
        if node.returns:
            node.returns = self._transform_annotation(node.returns)
        
        # Transform parameter annotations
        for arg in node.args.args:
            if arg.annotation:
                arg.annotation = self._transform_annotation(arg.annotation)
        
        # Continue with body transformations
        self.generic_visit(node)
        return node
    
    def visit_Assign(self, node: ast.Assign) -> Optional[ast.AST]:
        """Transform assignments."""
        
        # Check for self.glueContext = None pattern
        if len(node.targets) == 1:
            target = node.targets[0]
            if isinstance(target, ast.Attribute):
                if target.attr in ('glueContext', 'glue_ctx'):
                    # Change to self.spark = None
                    target.attr = 'spark'
                    self.transformations.append(f"Changed self.glueContext → self.spark")
        
        # Check for GlueContext() or Job() initialization
        if isinstance(node.value, ast.Call):
            func = node.value.func
            
            # GlueContext(...) → SparkSession.builder.getOrCreate()
            if isinstance(func, ast.Name) and func.id == 'GlueContext':
                if len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
                    var_name = node.targets[0].id
                    self.glue_vars.add(var_name)
                
                node.value = ast.Call(
                    func=ast.Attribute(
                        value=ast.Attribute(
                            value=ast.Name(id='SparkSession', ctx=ast.Load()),
                            attr='builder',
                            ctx=ast.Load()
                        ),
                        attr='getOrCreate',
                        ctx=ast.Load()
                    ),
                    args=[],
                    keywords=[]
                )
                # Change target variable name to 'spark'
                if len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
                    node.targets[0].id = 'spark'
                self.transformations.append("Replaced GlueContext → SparkSession")
                return node
            
            # SparkContext() → Remove (not needed)
            if isinstance(func, ast.Name) and func.id == 'SparkContext':
                self.transformations.append("Removed SparkContext() initialization")
                return None  # Remove this assignment
            
            # Job(...) → Remove (not needed)
            if isinstance(func, ast.Name) and func.id == 'Job':
                self.transformations.append("Removed Job() initialization")
                return None
        
        self.generic_visit(node)
        return node
    
    def visit_Expr(self, node: ast.Expr) -> Optional[ast.AST]:
        """Transform expression statements."""
        
        # Check for method calls like job.commit(), job.init()
        if isinstance(node.value, ast.Call):
            call = node.value
            if isinstance(call.func, ast.Attribute):
                attr = call.func.attr
                
                # Remove job.commit() and job.init()
                if attr in ('commit', 'init'):
                    if isinstance(call.func.value, ast.Name):
                        if call.func.value.id in ('job', 'Job'):
                            self.transformations.append(f"Removed {call.func.value.id}.{attr}()")
                            return None
        
        self.generic_visit(node)
        return node
    
    def visit_Call(self, node: ast.Call) -> ast.Call:
        """Transform function calls."""
        
        # Transform getResolvedOptions
        if isinstance(node.func, ast.Name) and node.func.id == 'getResolvedOptions':
            # This is handled at a higher level, but mark it
            self.transformations.append("Found getResolvedOptions - needs manual conversion")
        
        # Transform create_dynamic_frame.from_catalog
        if isinstance(node.func, ast.Attribute):
            attr_chain = self._get_attr_chain(node.func)
            
            if 'create_dynamic_frame' in attr_chain and 'from_catalog' in attr_chain:
                # Convert to spark.table()
                database = self._get_keyword_value(node, 'database')
                table_name = self._get_keyword_value(node, 'table_name')
                
                if database and table_name:
                    unity_ref = f"{self.target_catalog}.{database}.{table_name}"
                    
                    new_call = ast.Call(
                        func=ast.Attribute(
                            value=ast.Name(id='spark', ctx=ast.Load()),
                            attr='table',
                            ctx=ast.Load()
                        ),
                        args=[ast.Constant(value=unity_ref)],
                        keywords=[]
                    )
                    self.transformations.append(f"Catalog: {database}.{table_name} → {unity_ref}")
                    return new_call
        
        self.generic_visit(node)
        return node
    
    def visit_Attribute(self, node: ast.Attribute) -> ast.Attribute:
        """Transform attribute access."""
        
        # Replace glueContext.xxx with spark.xxx
        if isinstance(node.value, ast.Name):
            if node.value.id in self.glue_vars or node.value.id == 'glueContext':
                node.value.id = 'spark'
        
        # self.glueContext → self.spark
        if isinstance(node.value, ast.Attribute):
            if node.value.attr in ('glueContext', 'glue_ctx'):
                node.value.attr = 'spark'
        
        self.generic_visit(node)
        return node
    
    def visit_Name(self, node: ast.Name) -> ast.Name:
        """Transform variable names."""
        
        # DynamicFrame → DataFrame
        if node.id == 'DynamicFrame':
            node.id = 'DataFrame'
            self.transformations.append("Changed DynamicFrame → DataFrame")
        
        return node
    
    def _transform_annotation(self, ann: ast.AST) -> ast.AST:
        """Transform type annotations."""
        if isinstance(ann, ast.Name):
            if ann.id == 'DynamicFrame':
                ann.id = 'DataFrame'
        elif isinstance(ann, ast.Subscript):
            # Handle Optional[DynamicFrame], List[DynamicFrame], etc.
            if isinstance(ann.slice, ast.Name) and ann.slice.id == 'DynamicFrame':
                ann.slice.id = 'DataFrame'
        return ann
    
    def _get_attr_chain(self, node: ast.AST) -> List[str]:
        """Get the chain of attribute names."""
        chain = []
        current = node
        while isinstance(current, ast.Attribute):
            chain.append(current.attr)
            current = current.value
        if isinstance(current, ast.Name):
            chain.append(current.id)
        return chain
    
    def _get_keyword_value(self, call: ast.Call, keyword_name: str) -> Optional[str]:
        """Get the value of a keyword argument."""
        for kw in call.keywords:
            if kw.arg == keyword_name:
                if isinstance(kw.value, ast.Constant):
                    return str(kw.value.value)
        return None


class ASTCodeTransformer:
    """
    High-level AST-based code transformer.
    
    Uses AST parsing for accurate transformations of complex patterns.
    """
    
    def __init__(self, target_catalog: str = 'main'):
        self.target_catalog = target_catalog
        self.transformations = []
        self.errors = []
    
    def transform(self, source_code: str) -> str:
        """
        Transform source code using AST.
        
        Falls back to returning original code if AST parsing fails.
        """
        try:
            tree = ast.parse(source_code)
        except SyntaxError as e:
            self.errors.append(f"Parse error: {e}")
            return source_code  # Return original if can't parse
        
        # Apply transformations
        transformer = GlueClassTransformer(self.target_catalog)
        new_tree = transformer.visit(tree)
        
        # Fix missing locations
        ast.fix_missing_locations(new_tree)
        
        # Collect transformations
        self.transformations.extend(transformer.transformations)
        
        # Convert back to source code
        try:
            import astunparse
            return astunparse.unparse(new_tree)
        except ImportError:
            # Python 3.9+ has ast.unparse
            try:
                return ast.unparse(new_tree)
            except AttributeError:
                self.errors.append("Cannot unparse AST - astunparse not installed")
                return source_code
    
    def get_transformations(self) -> List[str]:
        return self.transformations
    
    def get_errors(self) -> List[str]:
        return self.errors


def transform_with_ast(source_code: str, target_catalog: str = 'main') -> str:
    """
    Convenience function to transform code using AST.
    
    Args:
        source_code: Python source code to transform
        target_catalog: Unity Catalog name
    
    Returns:
        Transformed source code
    """
    transformer = ASTCodeTransformer(target_catalog)
    return transformer.transform(source_code)
