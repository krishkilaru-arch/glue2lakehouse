"""
Dependency Analyzer
Detects circular dependencies and builds dependency graphs

Critical for migration ordering and refactoring recommendations.

Author: Analytics360
Version: 2.0.0
"""

from typing import Dict, List, Set, Tuple, Optional, Any
from dataclasses import dataclass
import ast
import os
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


@dataclass
class DependencyNode:
    """Represents a module/file in dependency graph."""
    module_name: str
    file_path: str
    imports: List[str]
    imported_by: List[str]
    is_external: bool


@dataclass
class CircularDependency:
    """Represents a circular dependency."""
    cycle: List[str]
    severity: str  # 'critical', 'warning'
    recommendation: str


@dataclass
class DependencyAnalysisResult:
    """Result of dependency analysis."""
    total_modules: int
    total_dependencies: int
    circular_dependencies: List[CircularDependency]
    dependency_graph: Dict[str, DependencyNode]
    migration_order: List[str]
    external_dependencies: List[str]
    isolated_modules: List[str]


class DependencyAnalyzer:
    """
    Analyzes Python module dependencies for migration planning.
    
    Capabilities:
    - Detect circular dependencies
    - Build dependency graph
    - Determine migration order (topological sort)
    - Identify external dependencies
    - Find isolated modules
    
    Example:
        ```python
        analyzer = DependencyAnalyzer()
        
        result = analyzer.analyze_repository("/path/to/glue/repo")
        
        if result.circular_dependencies:
            print("⚠️ Circular dependencies detected:")
            for circ in result.circular_dependencies:
                print(f"  {' → '.join(circ.cycle)}")
                print(f"  Recommendation: {circ.recommendation}")
        
        # Get migration order
        for module in result.migration_order:
            print(f"Migrate: {module}")
        ```
    """
    
    def __init__(self):
        """Initialize dependency analyzer."""
        logger.info("DependencyAnalyzer initialized")
    
    def analyze_repository(self, repo_path: str) -> DependencyAnalysisResult:
        """
        Analyze entire repository for dependencies.
        
        Args:
            repo_path: Path to repository root
        
        Returns:
            DependencyAnalysisResult
        """
        logger.info(f"Analyzing repository: {repo_path}")
        
        # Build dependency graph
        graph = self._build_dependency_graph(repo_path)
        
        # Detect circular dependencies
        circular = self._detect_circular_dependencies(graph)
        
        # Calculate migration order
        migration_order = self._calculate_migration_order(graph)
        
        # Find external dependencies
        external = self._find_external_dependencies(graph)
        
        # Find isolated modules
        isolated = self._find_isolated_modules(graph)
        
        # Count dependencies
        total_deps = sum(len(node.imports) for node in graph.values())
        
        logger.info(
            f"Analysis complete: {len(graph)} modules, "
            f"{len(circular)} circular dependencies"
        )
        
        return DependencyAnalysisResult(
            total_modules=len(graph),
            total_dependencies=total_deps,
            circular_dependencies=circular,
            dependency_graph=graph,
            migration_order=migration_order,
            external_dependencies=external,
            isolated_modules=isolated
        )
    
    def _build_dependency_graph(self, repo_path: str) -> Dict[str, DependencyNode]:
        """Build dependency graph from repository."""
        graph = {}
        repo = Path(repo_path)
        
        # Find all Python files
        python_files = list(repo.rglob("*.py"))
        
        for py_file in python_files:
            if '__pycache__' in str(py_file):
                continue
            
            # Get module name
            rel_path = py_file.relative_to(repo)
            module_name = str(rel_path.with_suffix('')).replace(os.sep, '.')
            
            # Parse imports
            imports = self._extract_imports(py_file)
            
            # Create node
            node = DependencyNode(
                module_name=module_name,
                file_path=str(py_file),
                imports=imports,
                imported_by=[],
                is_external=False
            )
            
            graph[module_name] = node
        
        # Build reverse dependencies
        for module_name, node in graph.items():
            for imported in node.imports:
                if imported in graph:
                    graph[imported].imported_by.append(module_name)
        
        return graph
    
    def _extract_imports(self, file_path: Path) -> List[str]:
        """Extract imports from Python file."""
        imports = []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                tree = ast.parse(f.read())
            
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        imports.append(alias.name)
                
                elif isinstance(node, ast.ImportFrom):
                    if node.module:
                        imports.append(node.module)
        
        except Exception as e:
            logger.warning(f"Failed to parse {file_path}: {e}")
        
        return imports
    
    def _detect_circular_dependencies(
        self,
        graph: Dict[str, DependencyNode]
    ) -> List[CircularDependency]:
        """Detect circular dependencies using DFS."""
        visited = set()
        rec_stack = set()
        circular_deps = []
        
        def dfs(module: str, path: List[str]) -> bool:
            visited.add(module)
            rec_stack.add(module)
            path.append(module)
            
            for imported in graph.get(module, DependencyNode('', '', [], [], False)).imports:
                if imported not in graph:
                    continue  # External dependency
                
                if imported not in visited:
                    if dfs(imported, path[:]):
                        return True
                elif imported in rec_stack:
                    # Found cycle
                    cycle_start = path.index(imported)
                    cycle = path[cycle_start:] + [imported]
                    
                    # Determine severity
                    severity = 'critical' if len(cycle) <= 3 else 'warning'
                    
                    # Generate recommendation
                    recommendation = self._generate_refactoring_recommendation(cycle)
                    
                    circular_deps.append(CircularDependency(
                        cycle=cycle,
                        severity=severity,
                        recommendation=recommendation
                    ))
                    return True
            
            rec_stack.remove(module)
            return False
        
        for module in graph:
            if module not in visited:
                dfs(module, [])
        
        return circular_deps
    
    def _generate_refactoring_recommendation(self, cycle: List[str]) -> str:
        """Generate refactoring recommendation for circular dependency."""
        if len(cycle) == 2:
            return (
                f"Merge {cycle[0]} and {cycle[1]} into a single module, "
                f"or extract shared code into a third module"
            )
        elif len(cycle) == 3:
            return (
                f"Extract common interfaces/base classes from cycle into "
                f"a separate module that all three can import"
            )
        else:
            return (
                f"Complex circular dependency. Consider architectural refactoring: "
                f"split modules by layer (data, business logic, presentation)"
            )
    
    def _calculate_migration_order(
        self,
        graph: Dict[str, DependencyNode]
    ) -> List[str]:
        """
        Calculate migration order using topological sort.
        
        Modules with no dependencies first, then modules that depend on them.
        """
        # Kahn's algorithm
        in_degree = {module: 0 for module in graph}
        
        for module in graph:
            for imported in graph[module].imports:
                if imported in graph:
                    in_degree[imported] = in_degree.get(imported, 0) + 1
        
        queue = [module for module in graph if in_degree[module] == 0]
        order = []
        
        while queue:
            # Sort for deterministic output
            queue.sort()
            module = queue.pop(0)
            order.append(module)
            
            for dependent in graph[module].imported_by:
                if dependent in in_degree:
                    in_degree[dependent] -= 1
                    if in_degree[dependent] == 0:
                        queue.append(dependent)
        
        # If we couldn't order all modules, there are circular dependencies
        if len(order) < len(graph):
            # Add remaining modules (part of cycles) at the end
            remaining = [m for m in graph if m not in order]
            logger.warning(
                f"Could not determine order for {len(remaining)} modules "
                f"due to circular dependencies"
            )
            order.extend(sorted(remaining))
        
        return order
    
    def _find_external_dependencies(
        self,
        graph: Dict[str, DependencyNode]
    ) -> List[str]:
        """Find external dependencies (imported but not in repo)."""
        external = set()
        
        for node in graph.values():
            for imported in node.imports:
                if imported not in graph:
                    # Extract top-level package
                    top_level = imported.split('.')[0]
                    external.add(top_level)
        
        return sorted(list(external))
    
    def _find_isolated_modules(
        self,
        graph: Dict[str, DependencyNode]
    ) -> List[str]:
        """Find modules with no imports or imported_by."""
        isolated = []
        
        for module, node in graph.items():
            internal_imports = [imp for imp in node.imports if imp in graph]
            
            if not internal_imports and not node.imported_by:
                isolated.append(module)
        
        return isolated
    
    def visualize_dependencies(
        self,
        graph: Dict[str, DependencyNode],
        output_format: str = 'mermaid'
    ) -> str:
        """
        Generate visualization of dependency graph.
        
        Args:
            graph: Dependency graph
            output_format: 'mermaid' or 'dot'
        
        Returns:
            Graph definition string
        """
        if output_format == 'mermaid':
            return self._generate_mermaid(graph)
        elif output_format == 'dot':
            return self._generate_dot(graph)
        else:
            raise ValueError(f"Unsupported format: {output_format}")
    
    def _generate_mermaid(self, graph: Dict[str, DependencyNode]) -> str:
        """Generate Mermaid diagram."""
        lines = ["```mermaid", "graph TD"]
        
        # Limit to first 20 modules for readability
        modules = list(graph.keys())[:20]
        
        for module in modules:
            node = graph[module]
            safe_module = module.replace('.', '_')
            
            for imported in node.imports:
                if imported in modules:
                    safe_imported = imported.replace('.', '_')
                    lines.append(f"    {safe_module} --> {safe_imported}")
        
        lines.append("```")
        return '\n'.join(lines)
    
    def _generate_dot(self, graph: Dict[str, DependencyNode]) -> str:
        """Generate GraphViz DOT format."""
        lines = ["digraph dependencies {"]
        
        for module, node in graph.items():
            for imported in node.imports:
                if imported in graph:
                    lines.append(f'    "{module}" -> "{imported}";')
        
        lines.append("}")
        return '\n'.join(lines)


class DependencyRefactorer:
    """
    Suggests refactorings to break circular dependencies.
    """
    
    def suggest_refactorings(
        self,
        circular: CircularDependency,
        graph: Dict[str, DependencyNode]
    ) -> List[Dict[str, Any]]:
        """
        Suggest refactorings for circular dependency.
        
        Returns:
            List of refactoring suggestions
        """
        suggestions = []
        cycle = circular.cycle
        
        # Strategy 1: Dependency Inversion
        suggestions.append({
            'strategy': 'Dependency Inversion',
            'description': 'Create abstract interfaces that both modules depend on',
            'steps': [
                f"1. Extract common interfaces from {cycle[0]}",
                f"2. Create new module 'interfaces' with abstractions",
                f"3. Have {' and '.join(cycle)} depend on interfaces instead",
                "4. Break the cycle by removing direct dependencies"
            ],
            'difficulty': 'medium'
        })
        
        # Strategy 2: Module Merge
        if len(cycle) == 2:
            suggestions.append({
                'strategy': 'Module Merge',
                'description': f'Merge {cycle[0]} and {cycle[1]} into single module',
                'steps': [
                    f"1. Create new module combining {cycle[0]} and {cycle[1]}",
                    "2. Move all code into new module",
                    "3. Update all imports to reference new module",
                    "4. Delete old modules"
                ],
                'difficulty': 'easy'
            })
        
        # Strategy 3: Extract Common Code
        suggestions.append({
            'strategy': 'Extract Common Code',
            'description': 'Move shared code to a new utils module',
            'steps': [
                "1. Identify code that creates the circular dependency",
                "2. Create new 'common' or 'utils' module",
                "3. Move shared code to new module",
                "4. Update imports in original modules"
            ],
            'difficulty': 'easy'
        })
        
        return suggestions
