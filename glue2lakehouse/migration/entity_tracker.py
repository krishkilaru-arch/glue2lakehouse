"""
Entity Tracking System for Migration Management

Tracks all entities (modules, classes, functions) from both Glue and Databricks
codebases in a SQLite database for visualization and management.

Author: Analytics360
Version: 1.0.0
Date: 2026-02-13
"""

import sqlite3
import ast
import os
from pathlib import Path
from typing import List, Dict, Any, Optional, Set
from datetime import datetime
from dataclasses import dataclass, asdict
import hashlib
import logging

logger = logging.getLogger(__name__)


@dataclass
class Entity:
    """Represents a code entity (module, class, function, etc.)"""
    
    # Identity
    entity_id: str  # Unique hash
    entity_type: str  # module, class, function, method, variable
    name: str
    full_path: str  # e.g., "src.utils.s3.write_to_table"
    
    # Location
    source_type: str  # "glue" or "databricks"
    file_path: str
    module_name: str
    line_start: int
    line_end: int
    
    # Code info
    definition: str  # First line or signature
    docstring: Optional[str]
    complexity: int  # Cyclomatic complexity
    lines_of_code: int
    
    # Git info
    git_commit: Optional[str]
    git_author: Optional[str]
    created_date: str
    updated_date: str
    
    # Migration info
    migration_status: str  # "pending", "migrated", "skipped", "databricks_native"
    needs_sync: bool  # Does this need to be synced from Glue?
    is_databricks_native: bool  # Is this Databricks-specific?
    has_conflicts: bool
    
    # Dependencies
    imports: List[str]  # List of imports used
    calls_functions: List[str]  # Functions this entity calls
    
    # Metadata
    notes: Optional[str]
    tags: List[str]


class EntityTracker:
    """
    Tracks all code entities from source and target repositories.
    
    Provides:
    - Entity scanning and discovery
    - SQLite database storage
    - Migration status tracking
    - Query and reporting capabilities
    - Streamlit dashboard data
    
    Example:
        ```python
        tracker = EntityTracker("migration_entities.db")
        
        # Scan source (Glue)
        tracker.scan_repository("/path/to/glue", source_type="glue")
        
        # Scan target (Databricks)
        tracker.scan_repository("/path/to/databricks", source_type="databricks")
        
        # Query entities
        entities = tracker.get_entities(source_type="glue", entity_type="function")
        
        # Get migration stats
        stats = tracker.get_migration_stats()
        ```
    """
    
    def __init__(self, db_path: str = "migration_entities.db"):
        """
        Initialize entity tracker.
        
        Args:
            db_path: Path to SQLite database
        """
        self.db_path = db_path
        self.conn = None
        self._init_database()
        logger.info(f"EntityTracker initialized with database: {db_path}")
    
    def _init_database(self):
        """Initialize SQLite database with schema."""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        
        cursor = self.conn.cursor()
        
        # Entities table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS entities (
                entity_id TEXT PRIMARY KEY,
                entity_type TEXT NOT NULL,
                name TEXT NOT NULL,
                full_path TEXT NOT NULL,
                
                source_type TEXT NOT NULL,
                file_path TEXT NOT NULL,
                module_name TEXT NOT NULL,
                line_start INTEGER,
                line_end INTEGER,
                
                definition TEXT,
                docstring TEXT,
                complexity INTEGER,
                lines_of_code INTEGER,
                
                git_commit TEXT,
                git_author TEXT,
                created_date TEXT NOT NULL,
                updated_date TEXT NOT NULL,
                
                migration_status TEXT DEFAULT 'pending',
                needs_sync INTEGER DEFAULT 1,
                is_databricks_native INTEGER DEFAULT 0,
                has_conflicts INTEGER DEFAULT 0,
                
                imports TEXT,
                calls_functions TEXT,
                
                notes TEXT,
                tags TEXT
            )
        """)
        
        # Migration history table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS migration_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_id TEXT,
                action TEXT,
                timestamp TEXT,
                details TEXT,
                FOREIGN KEY (entity_id) REFERENCES entities(entity_id)
            )
        """)
        
        # Create indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_source_type ON entities(source_type)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_entity_type ON entities(entity_type)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_migration_status ON entities(migration_status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_full_path ON entities(full_path)")
        
        self.conn.commit()
        logger.info("Database schema initialized")
    
    def _compute_entity_id(self, entity: Entity) -> str:
        """Compute unique entity ID based on full path and source."""
        content = f"{entity.source_type}:{entity.full_path}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]
    
    def _parse_file(self, file_path: Path, module_base: str) -> List[Entity]:
        """
        Parse a Python file and extract entities.
        
        Args:
            file_path: Path to Python file
            module_base: Base module name (e.g., "src")
        
        Returns:
            List of Entity objects
        """
        entities = []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            tree = ast.parse(content)
            
            # Get module name
            rel_path = file_path.relative_to(Path(module_base).parent)
            module_name = str(rel_path.with_suffix('')).replace(os.sep, '.')
            
            # Extract entities
            for node in ast.walk(tree):
                entity = None
                
                # Module-level
                if isinstance(node, ast.Module):
                    docstring = ast.get_docstring(node)
                    entity = Entity(
                        entity_id="",
                        entity_type="module",
                        name=file_path.stem,
                        full_path=module_name,
                        source_type="",
                        file_path=str(file_path),
                        module_name=module_name,
                        line_start=1,
                        line_end=len(content.split('\n')),
                        definition=f"module {module_name}",
                        docstring=docstring,
                        complexity=0,
                        lines_of_code=len([l for l in content.split('\n') if l.strip()]),
                        git_commit=None,
                        git_author=None,
                        created_date=datetime.now().isoformat(),
                        updated_date=datetime.now().isoformat(),
                        migration_status="pending",
                        needs_sync=True,
                        is_databricks_native=False,
                        has_conflicts=False,
                        imports=self._extract_imports(tree),
                        calls_functions=[],
                        notes=None,
                        tags=[]
                    )
                
                # Class
                elif isinstance(node, ast.ClassDef):
                    docstring = ast.get_docstring(node)
                    full_path = f"{module_name}.{node.name}"
                    
                    entity = Entity(
                        entity_id="",
                        entity_type="class",
                        name=node.name,
                        full_path=full_path,
                        source_type="",
                        file_path=str(file_path),
                        module_name=module_name,
                        line_start=node.lineno,
                        line_end=node.end_lineno or node.lineno,
                        definition=f"class {node.name}",
                        docstring=docstring,
                        complexity=self._calculate_complexity(node),
                        lines_of_code=node.end_lineno - node.lineno if node.end_lineno else 1,
                        git_commit=None,
                        git_author=None,
                        created_date=datetime.now().isoformat(),
                        updated_date=datetime.now().isoformat(),
                        migration_status="pending",
                        needs_sync=True,
                        is_databricks_native=False,
                        has_conflicts=False,
                        imports=[],
                        calls_functions=[],
                        notes=None,
                        tags=[]
                    )
                
                # Function/Method
                elif isinstance(node, ast.FunctionDef):
                    docstring = ast.get_docstring(node)
                    full_path = f"{module_name}.{node.name}"
                    
                    # Get function signature
                    args = [arg.arg for arg in node.args.args]
                    definition = f"def {node.name}({', '.join(args)})"
                    
                    entity = Entity(
                        entity_id="",
                        entity_type="function",
                        name=node.name,
                        full_path=full_path,
                        source_type="",
                        file_path=str(file_path),
                        module_name=module_name,
                        line_start=node.lineno,
                        line_end=node.end_lineno or node.lineno,
                        definition=definition,
                        docstring=docstring,
                        complexity=self._calculate_complexity(node),
                        lines_of_code=node.end_lineno - node.lineno if node.end_lineno else 1,
                        git_commit=None,
                        git_author=None,
                        created_date=datetime.now().isoformat(),
                        updated_date=datetime.now().isoformat(),
                        migration_status="pending",
                        needs_sync=True,
                        is_databricks_native=False,
                        has_conflicts=False,
                        imports=[],
                        calls_functions=self._extract_function_calls(node),
                        notes=None,
                        tags=[]
                    )
                
                if entity:
                    entities.append(entity)
        
        except Exception as e:
            logger.error(f"Failed to parse {file_path}: {e}")
        
        return entities
    
    def _extract_imports(self, tree: ast.Module) -> List[str]:
        """Extract import statements."""
        imports = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.append(alias.name)
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    for alias in node.names:
                        imports.append(f"{node.module}.{alias.name}")
        return imports
    
    def _extract_function_calls(self, node: ast.FunctionDef) -> List[str]:
        """Extract function calls from a function."""
        calls = []
        for child in ast.walk(node):
            if isinstance(child, ast.Call):
                if isinstance(child.func, ast.Name):
                    calls.append(child.func.id)
                elif isinstance(child.func, ast.Attribute):
                    calls.append(child.func.attr)
        return list(set(calls))
    
    def _calculate_complexity(self, node) -> int:
        """Calculate cyclomatic complexity (simplified)."""
        complexity = 1
        for child in ast.walk(node):
            if isinstance(child, (ast.If, ast.While, ast.For, ast.ExceptHandler)):
                complexity += 1
        return complexity
    
    def scan_repository(
        self,
        repo_path: str,
        source_type: str,
        scan_paths: Optional[List[str]] = None
    ):
        """
        Scan a repository and extract all entities.
        
        Args:
            repo_path: Path to repository
            source_type: "glue" or "databricks"
            scan_paths: Specific paths to scan (relative to repo_path)
        """
        logger.info(f"Scanning {source_type} repository: {repo_path}")
        
        repo = Path(repo_path)
        scan_dirs = [repo / p for p in (scan_paths or ['.'])]
        
        total_files = 0
        total_entities = 0
        
        for scan_dir in scan_dirs:
            if not scan_dir.exists():
                logger.warning(f"Scan directory not found: {scan_dir}")
                continue
            
            # Find all Python files
            for py_file in scan_dir.rglob("*.py"):
                if any(pattern in str(py_file) for pattern in ['__pycache__', '.git', 'venv']):
                    continue
                
                total_files += 1
                
                # Parse file and extract entities
                entities = self._parse_file(py_file, str(repo))
                
                # Save entities
                for entity in entities:
                    entity.source_type = source_type
                    entity.entity_id = self._compute_entity_id(entity)
                    self.save_entity(entity)
                    total_entities += 1
        
        logger.info(
            f"Scan complete: {total_files} files, {total_entities} entities found"
        )
    
    def save_entity(self, entity: Entity):
        """Save or update entity in database."""
        cursor = self.conn.cursor()
        
        # Convert lists to JSON strings
        entity_dict = asdict(entity)
        entity_dict['imports'] = ','.join(entity.imports) if entity.imports else ''
        entity_dict['calls_functions'] = ','.join(entity.calls_functions) if entity.calls_functions else ''
        entity_dict['tags'] = ','.join(entity.tags) if entity.tags else ''
        entity_dict['needs_sync'] = 1 if entity.needs_sync else 0
        entity_dict['is_databricks_native'] = 1 if entity.is_databricks_native else 0
        entity_dict['has_conflicts'] = 1 if entity.has_conflicts else 0
        
        # Upsert
        cursor.execute("""
            INSERT OR REPLACE INTO entities VALUES (
                :entity_id, :entity_type, :name, :full_path,
                :source_type, :file_path, :module_name, :line_start, :line_end,
                :definition, :docstring, :complexity, :lines_of_code,
                :git_commit, :git_author, :created_date, :updated_date,
                :migration_status, :needs_sync, :is_databricks_native, :has_conflicts,
                :imports, :calls_functions, :notes, :tags
            )
        """, entity_dict)
        
        self.conn.commit()
    
    def get_entities(
        self,
        source_type: Optional[str] = None,
        entity_type: Optional[str] = None,
        migration_status: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Query entities with filters.
        
        Args:
            source_type: Filter by source ("glue" or "databricks")
            entity_type: Filter by entity type
            migration_status: Filter by migration status
            limit: Maximum number of results
        
        Returns:
            List of entity dictionaries
        """
        cursor = self.conn.cursor()
        
        query = "SELECT * FROM entities WHERE 1=1"
        params = []
        
        if source_type:
            query += " AND source_type = ?"
            params.append(source_type)
        
        if entity_type:
            query += " AND entity_type = ?"
            params.append(entity_type)
        
        if migration_status:
            query += " AND migration_status = ?"
            params.append(migration_status)
        
        if limit:
            query += " LIMIT ?"
            params.append(limit)
        
        cursor.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]
    
    def get_migration_stats(self) -> Dict[str, Any]:
        """Get migration statistics."""
        cursor = self.conn.cursor()
        
        stats = {
            'source': {},
            'target': {},
            'overall': {}
        }
        
        # Source (Glue) stats
        cursor.execute("""
            SELECT 
                entity_type,
                COUNT(*) as count,
                SUM(CASE WHEN migration_status = 'migrated' THEN 1 ELSE 0 END) as migrated,
                SUM(CASE WHEN migration_status = 'pending' THEN 1 ELSE 0 END) as pending
            FROM entities
            WHERE source_type = 'glue'
            GROUP BY entity_type
        """)
        stats['source'] = {row['entity_type']: dict(row) for row in cursor.fetchall()}
        
        # Target (Databricks) stats
        cursor.execute("""
            SELECT 
                entity_type,
                COUNT(*) as count,
                SUM(CASE WHEN is_databricks_native = 1 THEN 1 ELSE 0 END) as databricks_native
            FROM entities
            WHERE source_type = 'databricks'
            GROUP BY entity_type
        """)
        stats['target'] = {row['entity_type']: dict(row) for row in cursor.fetchall()}
        
        # Overall stats
        cursor.execute("""
            SELECT 
                COUNT(*) as total_entities,
                SUM(CASE WHEN source_type = 'glue' THEN 1 ELSE 0 END) as source_entities,
                SUM(CASE WHEN source_type = 'databricks' THEN 1 ELSE 0 END) as target_entities,
                SUM(CASE WHEN migration_status = 'migrated' THEN 1 ELSE 0 END) as migrated,
                SUM(CASE WHEN migration_status = 'pending' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN is_databricks_native = 1 THEN 1 ELSE 0 END) as databricks_native,
                SUM(CASE WHEN has_conflicts = 1 THEN 1 ELSE 0 END) as conflicts
            FROM entities
        """)
        stats['overall'] = dict(cursor.fetchone())
        
        return stats
    
    def mark_as_migrated(self, entity_id: str):
        """Mark entity as migrated."""
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE entities 
            SET migration_status = 'migrated', updated_date = ?
            WHERE entity_id = ?
        """, (datetime.now().isoformat(), entity_id))
        
        # Log to history
        cursor.execute("""
            INSERT INTO migration_history (entity_id, action, timestamp, details)
            VALUES (?, 'migrated', ?, 'Entity migrated to Databricks')
        """, (entity_id, datetime.now().isoformat()))
        
        self.conn.commit()
    
    def mark_as_databricks_native(self, entity_id: str):
        """Mark entity as Databricks-native."""
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE entities 
            SET is_databricks_native = 1, needs_sync = 0, updated_date = ?
            WHERE entity_id = ?
        """, (datetime.now().isoformat(), entity_id))
        self.conn.commit()
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
