"""
Git Extractor
Clone repositories and extract Glue entities

Author: Analytics360
Version: 2.0.0
"""

import os
import ast
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


@dataclass
class ExtractedEntity:
    """Represents an extracted code entity."""
    entity_type: str  # 'module', 'class', 'function', 'glue_job'
    name: str
    file_path: str
    line_start: int
    line_end: int
    code: str
    signature: str
    docstring: Optional[str]
    imports: List[str]
    dependencies: List[str]
    is_glue_specific: bool
    glue_patterns: List[str]  # DynamicFrame, GlueContext, etc.


@dataclass  
class ExtractionResult:
    """Result of repository extraction."""
    success: bool
    repo_path: str
    branch: str
    commit_hash: str
    entities: List[ExtractedEntity]
    stats: Dict[str, int]
    errors: List[str]
    extraction_time: float


class GitExtractor:
    """
    Extract code entities from Git repositories.
    
    Capabilities:
    - Clone repositories (HTTPS, SSH)
    - Parse Python files
    - Identify Glue-specific patterns
    - Extract entities (modules, classes, functions)
    - Track dependencies
    
    Example:
        ```python
        extractor = GitExtractor()
        result = extractor.extract_from_repo(
            repo_url="https://github.com/company/glue-etl.git",
            branch="main"
        )
        
        for entity in result.entities:
            print(f"{entity.entity_type}: {entity.name}")
        ```
    """
    
    GLUE_PATTERNS = [
        'GlueContext', 'DynamicFrame', 'glueContext',
        'create_dynamic_frame', 'write_dynamic_frame',
        'from_catalog', 'from_options', 'apply_mapping',
        'awsglue', 'Job.init', 'Job.commit',
        'getResolvedOptions', 'GlueArgumentError'
    ]
    
    def __init__(self, work_dir: Optional[str] = None):
        """
        Initialize extractor.
        
        Args:
            work_dir: Working directory for cloned repos
        """
        self.work_dir = work_dir or tempfile.mkdtemp(prefix="glue2lakehouse_")
        self.current_repo_path = None
    
    def extract_from_repo(
        self,
        repo_url: str,
        branch: str = "main",
        subdirectory: Optional[str] = None,
        cleanup: bool = True
    ) -> ExtractionResult:
        """
        Extract entities from a Git repository.
        
        Args:
            repo_url: Git repository URL
            branch: Branch to clone
            subdirectory: Subdirectory to scan (optional)
            cleanup: Remove cloned repo after extraction
        
        Returns:
            ExtractionResult with all entities
        """
        start_time = datetime.now()
        errors = []
        entities = []
        
        try:
            # Clone repository
            repo_path = self._clone_repo(repo_url, branch)
            self.current_repo_path = repo_path
            
            # Get commit hash
            commit_hash = self._get_commit_hash(repo_path)
            
            # Determine scan path
            scan_path = Path(repo_path)
            if subdirectory:
                scan_path = scan_path / subdirectory
            
            if not scan_path.exists():
                raise FileNotFoundError(f"Subdirectory not found: {subdirectory}")
            
            # Extract entities from Python files
            python_files = list(scan_path.rglob("*.py"))
            
            for py_file in python_files:
                try:
                    file_entities = self._extract_from_file(py_file, repo_path)
                    entities.extend(file_entities)
                except Exception as e:
                    errors.append(f"Error parsing {py_file}: {e}")
            
            # Calculate stats
            stats = self._calculate_stats(entities)
            
            # Cleanup if requested
            if cleanup:
                shutil.rmtree(repo_path, ignore_errors=True)
            
            return ExtractionResult(
                success=True,
                repo_path=str(repo_path),
                branch=branch,
                commit_hash=commit_hash,
                entities=entities,
                stats=stats,
                errors=errors,
                extraction_time=(datetime.now() - start_time).total_seconds()
            )
            
        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            return ExtractionResult(
                success=False,
                repo_path="",
                branch=branch,
                commit_hash="",
                entities=[],
                stats={},
                errors=[str(e)],
                extraction_time=(datetime.now() - start_time).total_seconds()
            )
    
    def extract_from_local(self, path: str) -> ExtractionResult:
        """Extract entities from local directory."""
        start_time = datetime.now()
        errors = []
        entities = []
        
        scan_path = Path(path)
        if not scan_path.exists():
            return ExtractionResult(
                success=False, repo_path=path, branch="local",
                commit_hash="", entities=[], stats={},
                errors=[f"Path not found: {path}"],
                extraction_time=0
            )
        
        python_files = list(scan_path.rglob("*.py"))
        
        for py_file in python_files:
            try:
                file_entities = self._extract_from_file(py_file, scan_path)
                entities.extend(file_entities)
            except Exception as e:
                errors.append(f"Error parsing {py_file}: {e}")
        
        return ExtractionResult(
            success=True,
            repo_path=str(path),
            branch="local",
            commit_hash="local",
            entities=entities,
            stats=self._calculate_stats(entities),
            errors=errors,
            extraction_time=(datetime.now() - start_time).total_seconds()
        )
    
    def _clone_repo(self, repo_url: str, branch: str) -> str:
        """Clone a Git repository."""
        repo_name = repo_url.split("/")[-1].replace(".git", "")
        repo_path = os.path.join(self.work_dir, repo_name)
        
        # Remove if exists
        if os.path.exists(repo_path):
            shutil.rmtree(repo_path)
        
        # Clone
        cmd = ["git", "clone", "--branch", branch, "--depth", "1", repo_url, repo_path]
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            raise RuntimeError(f"Git clone failed: {result.stderr}")
        
        logger.info(f"Cloned {repo_url} to {repo_path}")
        return repo_path
    
    def _get_commit_hash(self, repo_path: str) -> str:
        """Get current commit hash."""
        cmd = ["git", "-C", repo_path, "rev-parse", "HEAD"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        return result.stdout.strip()[:8] if result.returncode == 0 else "unknown"
    
    def _extract_from_file(self, file_path: Path, base_path: Path) -> List[ExtractedEntity]:
        """Extract entities from a Python file."""
        entities = []
        
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        try:
            tree = ast.parse(content)
        except SyntaxError as e:
            logger.warning(f"Syntax error in {file_path}: {e}")
            return entities
        
        # Get relative path
        rel_path = str(file_path.relative_to(base_path))
        
        # Extract imports
        imports = self._extract_imports(tree)
        
        # Check for Glue patterns in file
        glue_patterns = [p for p in self.GLUE_PATTERNS if p in content]
        is_glue_file = len(glue_patterns) > 0
        
        # Extract module-level entity
        module_name = file_path.stem
        entities.append(ExtractedEntity(
            entity_type='module',
            name=module_name,
            file_path=rel_path,
            line_start=1,
            line_end=len(content.splitlines()),
            code=content,
            signature=f"# Module: {module_name}",
            docstring=ast.get_docstring(tree),
            imports=imports,
            dependencies=[],
            is_glue_specific=is_glue_file,
            glue_patterns=glue_patterns
        ))
        
        # Extract classes and functions
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                entities.append(self._extract_class(node, rel_path, content, glue_patterns))
            elif isinstance(node, ast.FunctionDef) and not self._is_method(node, tree):
                entities.append(self._extract_function(node, rel_path, content, glue_patterns))
        
        return entities
    
    def _extract_class(self, node: ast.ClassDef, file_path: str, content: str, glue_patterns: List[str]) -> ExtractedEntity:
        """Extract class entity."""
        lines = content.splitlines()
        code = '\n'.join(lines[node.lineno-1:node.end_lineno])
        
        # Get methods
        methods = [n.name for n in node.body if isinstance(n, ast.FunctionDef)]
        
        return ExtractedEntity(
            entity_type='class',
            name=node.name,
            file_path=file_path,
            line_start=node.lineno,
            line_end=node.end_lineno or node.lineno,
            code=code,
            signature=f"class {node.name}",
            docstring=ast.get_docstring(node),
            imports=[],
            dependencies=methods,
            is_glue_specific=any(p in code for p in self.GLUE_PATTERNS),
            glue_patterns=[p for p in glue_patterns if p in code]
        )
    
    def _extract_function(self, node: ast.FunctionDef, file_path: str, content: str, glue_patterns: List[str]) -> ExtractedEntity:
        """Extract function entity."""
        lines = content.splitlines()
        code = '\n'.join(lines[node.lineno-1:node.end_lineno])
        
        # Get arguments
        args = [arg.arg for arg in node.args.args]
        signature = f"def {node.name}({', '.join(args)})"
        
        # Find function calls
        calls = self._extract_calls(node)
        
        return ExtractedEntity(
            entity_type='function',
            name=node.name,
            file_path=file_path,
            line_start=node.lineno,
            line_end=node.end_lineno or node.lineno,
            code=code,
            signature=signature,
            docstring=ast.get_docstring(node),
            imports=[],
            dependencies=calls,
            is_glue_specific=any(p in code for p in self.GLUE_PATTERNS),
            glue_patterns=[p for p in glue_patterns if p in code]
        )
    
    def _extract_imports(self, tree: ast.Module) -> List[str]:
        """Extract import statements."""
        imports = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.append(alias.name)
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                for alias in node.names:
                    imports.append(f"{module}.{alias.name}")
        return imports
    
    def _extract_calls(self, node: ast.FunctionDef) -> List[str]:
        """Extract function calls within a function."""
        calls = []
        for child in ast.walk(node):
            if isinstance(child, ast.Call):
                if isinstance(child.func, ast.Name):
                    calls.append(child.func.id)
                elif isinstance(child.func, ast.Attribute):
                    calls.append(child.func.attr)
        return list(set(calls))
    
    def _is_method(self, node: ast.FunctionDef, tree: ast.Module) -> bool:
        """Check if function is a class method."""
        for parent in ast.walk(tree):
            if isinstance(parent, ast.ClassDef):
                for child in parent.body:
                    if child is node:
                        return True
        return False
    
    def _calculate_stats(self, entities: List[ExtractedEntity]) -> Dict[str, int]:
        """Calculate extraction statistics."""
        stats = {
            'total_entities': len(entities),
            'modules': sum(1 for e in entities if e.entity_type == 'module'),
            'classes': sum(1 for e in entities if e.entity_type == 'class'),
            'functions': sum(1 for e in entities if e.entity_type == 'function'),
            'glue_specific': sum(1 for e in entities if e.is_glue_specific),
            'total_lines': sum(e.line_end - e.line_start + 1 for e in entities if e.entity_type == 'module')
        }
        return stats
    
    def cleanup(self):
        """Clean up temporary files."""
        if os.path.exists(self.work_dir):
            shutil.rmtree(self.work_dir, ignore_errors=True)
