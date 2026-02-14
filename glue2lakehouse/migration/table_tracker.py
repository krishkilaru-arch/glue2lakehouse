"""
Table Metadata Tracker
Tracks tables, schemas, columns from both Glue Catalog and Unity Catalog

Ensures source and destination tables are in sync.
"""

import sqlite3
from typing import List, Dict, Any, Optional, Set
from datetime import datetime
from dataclasses import dataclass, asdict
import logging

logger = logging.getLogger(__name__)


@dataclass
class Column:
    """Represents a table column."""
    name: str
    data_type: str
    is_nullable: bool
    comment: Optional[str] = None
    is_partition: bool = False


@dataclass
class Table:
    """Represents a database table."""
    
    # Identity
    table_id: str  # Unique hash
    catalog_type: str  # "glue" or "unity"
    
    # Location
    catalog_name: str
    database_name: str
    table_name: str
    full_name: str  # e.g., "glue://raw.customers"
    
    # Schema
    columns: List[Column]
    partition_keys: List[str]
    
    # Metadata
    table_type: str  # "EXTERNAL", "MANAGED", "VIEW"
    location: Optional[str]
    format: str  # "parquet", "delta", "csv", etc.
    
    # Migration info
    migration_status: str  # "pending", "migrated", "synced"
    needs_sync: bool
    has_schema_drift: bool
    
    # Timestamps
    created_date: str
    updated_date: str
    last_synced: Optional[str] = None
    
    # Usage info
    row_count: Optional[int] = None
    size_bytes: Optional[int] = None
    
    # Notes
    notes: Optional[str] = None
    tags: List[str] = None


class TableTracker:
    """
    Tracks table metadata from Glue Catalog and Unity Catalog.
    
    Ensures source and destination tables are in sync:
    - Schema validation
    - Column comparison
    - Type mapping
    - Drift detection
    
    Example:
        ```python
        tracker = TableTracker("migration_tables.db")
        
        # Scan Glue Catalog
        tracker.scan_glue_catalog(glue_client, databases=["raw", "curated"])
        
        # Scan Unity Catalog
        tracker.scan_unity_catalog(spark, catalog="production")
        
        # Compare schemas
        drifts = tracker.detect_schema_drift()
        
        # Get migration status
        status = tracker.get_migration_status()
        ```
    """
    
    def __init__(self, db_path: str = "migration_tables.db"):
        """Initialize table tracker."""
        self.db_path = db_path
        self.conn = None
        self._init_database()
        logger.info(f"TableTracker initialized with database: {db_path}")
    
    def _init_database(self):
        """Initialize SQLite database with schema."""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        
        cursor = self.conn.cursor()
        
        # Tables table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tables (
                table_id TEXT PRIMARY KEY,
                catalog_type TEXT NOT NULL,
                catalog_name TEXT NOT NULL,
                database_name TEXT NOT NULL,
                table_name TEXT NOT NULL,
                full_name TEXT NOT NULL,
                
                partition_keys TEXT,
                table_type TEXT,
                location TEXT,
                format TEXT,
                
                migration_status TEXT DEFAULT 'pending',
                needs_sync INTEGER DEFAULT 1,
                has_schema_drift INTEGER DEFAULT 0,
                
                created_date TEXT NOT NULL,
                updated_date TEXT NOT NULL,
                last_synced TEXT,
                
                row_count INTEGER,
                size_bytes INTEGER,
                
                notes TEXT,
                tags TEXT
            )
        """)
        
        # Columns table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS columns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_id TEXT NOT NULL,
                name TEXT NOT NULL,
                data_type TEXT NOT NULL,
                is_nullable INTEGER,
                comment TEXT,
                is_partition INTEGER DEFAULT 0,
                position INTEGER,
                FOREIGN KEY (table_id) REFERENCES tables(table_id)
            )
        """)
        
        # Schema drift history
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS schema_drift (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_id TEXT NOT NULL,
                drift_type TEXT NOT NULL,
                details TEXT,
                detected_date TEXT NOT NULL,
                resolved INTEGER DEFAULT 0,
                FOREIGN KEY (table_id) REFERENCES tables(table_id)
            )
        """)
        
        # Create indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_catalog_type ON tables(catalog_type)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_full_name ON tables(full_name)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_migration_status ON tables(migration_status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_table_columns ON columns(table_id)")
        
        self.conn.commit()
        logger.info("Table tracking database schema initialized")
    
    def save_table(self, table: Table):
        """Save or update table in database."""
        cursor = self.conn.cursor()
        
        # Convert table to dict
        table_dict = {
            'table_id': table.table_id,
            'catalog_type': table.catalog_type,
            'catalog_name': table.catalog_name,
            'database_name': table.database_name,
            'table_name': table.table_name,
            'full_name': table.full_name,
            'partition_keys': ','.join(table.partition_keys) if table.partition_keys else '',
            'table_type': table.table_type,
            'location': table.location,
            'format': table.format,
            'migration_status': table.migration_status,
            'needs_sync': 1 if table.needs_sync else 0,
            'has_schema_drift': 1 if table.has_schema_drift else 0,
            'created_date': table.created_date,
            'updated_date': table.updated_date,
            'last_synced': table.last_synced,
            'row_count': table.row_count,
            'size_bytes': table.size_bytes,
            'notes': table.notes,
            'tags': ','.join(table.tags) if table.tags else ''
        }
        
        # Upsert table
        cursor.execute("""
            INSERT OR REPLACE INTO tables VALUES (
                :table_id, :catalog_type, :catalog_name, :database_name, :table_name, :full_name,
                :partition_keys, :table_type, :location, :format,
                :migration_status, :needs_sync, :has_schema_drift,
                :created_date, :updated_date, :last_synced,
                :row_count, :size_bytes, :notes, :tags
            )
        """, table_dict)
        
        # Delete old columns
        cursor.execute("DELETE FROM columns WHERE table_id = ?", (table.table_id,))
        
        # Insert columns
        for i, col in enumerate(table.columns):
            cursor.execute("""
                INSERT INTO columns (table_id, name, data_type, is_nullable, comment, is_partition, position)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                table.table_id,
                col.name,
                col.data_type,
                1 if col.is_nullable else 0,
                col.comment,
                1 if col.is_partition else 0,
                i
            ))
        
        self.conn.commit()
    
    def get_tables(
        self,
        catalog_type: Optional[str] = None,
        database_name: Optional[str] = None,
        migration_status: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Query tables with filters."""
        cursor = self.conn.cursor()
        
        query = "SELECT * FROM tables WHERE 1=1"
        params = []
        
        if catalog_type:
            query += " AND catalog_type = ?"
            params.append(catalog_type)
        
        if database_name:
            query += " AND database_name = ?"
            params.append(database_name)
        
        if migration_status:
            query += " AND migration_status = ?"
            params.append(migration_status)
        
        cursor.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]
    
    def get_table_columns(self, table_id: str) -> List[Dict[str, Any]]:
        """Get columns for a table."""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT * FROM columns 
            WHERE table_id = ? 
            ORDER BY position
        """, (table_id,))
        return [dict(row) for row in cursor.fetchall()]
    
    def detect_schema_drift(self) -> List[Dict[str, Any]]:
        """
        Detect schema drift between Glue and Unity Catalog tables.
        
        Returns:
            List of drift records
        """
        drifts = []
        cursor = self.conn.cursor()
        
        # Get all Glue tables
        glue_tables = self.get_tables(catalog_type="glue")
        
        for glue_table in glue_tables:
            # Find corresponding Unity table
            unity_tables = self.get_tables(
                catalog_type="unity",
                database_name=glue_table['database_name']
            )
            unity_table = next(
                (t for t in unity_tables if t['table_name'] == glue_table['table_name']),
                None
            )
            
            if not unity_table:
                # Table doesn't exist in Unity
                drift = {
                    'table_id': glue_table['table_id'],
                    'table_name': glue_table['full_name'],
                    'drift_type': 'MISSING_IN_UNITY',
                    'details': f"Table exists in Glue but not in Unity Catalog"
                }
                drifts.append(drift)
                self._save_drift(drift)
                continue
            
            # Compare columns
            glue_cols = self.get_table_columns(glue_table['table_id'])
            unity_cols = self.get_table_columns(unity_table['table_id'])
            
            glue_col_dict = {c['name']: c for c in glue_cols}
            unity_col_dict = {c['name']: c for c in unity_cols}
            
            # Missing columns
            for col_name in glue_col_dict:
                if col_name not in unity_col_dict:
                    drift = {
                        'table_id': glue_table['table_id'],
                        'table_name': glue_table['full_name'],
                        'drift_type': 'MISSING_COLUMN',
                        'details': f"Column '{col_name}' exists in Glue but not in Unity"
                    }
                    drifts.append(drift)
                    self._save_drift(drift)
            
            # Type mismatches
            for col_name in glue_col_dict:
                if col_name in unity_col_dict:
                    glue_type = glue_col_dict[col_name]['data_type']
                    unity_type = unity_col_dict[col_name]['data_type']
                    
                    if glue_type != unity_type:
                        drift = {
                            'table_id': glue_table['table_id'],
                            'table_name': glue_table['full_name'],
                            'drift_type': 'TYPE_MISMATCH',
                            'details': f"Column '{col_name}': Glue={glue_type}, Unity={unity_type}"
                        }
                        drifts.append(drift)
                        self._save_drift(drift)
        
        return drifts
    
    def _save_drift(self, drift: Dict[str, Any]):
        """Save schema drift record."""
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO schema_drift (table_id, drift_type, details, detected_date)
            VALUES (?, ?, ?, ?)
        """, (
            drift['table_id'],
            drift['drift_type'],
            drift['details'],
            datetime.now().isoformat()
        ))
        
        # Mark table as having drift
        cursor.execute("""
            UPDATE tables SET has_schema_drift = 1 WHERE table_id = ?
        """, (drift['table_id'],))
        
        self.conn.commit()
    
    def get_migration_stats(self) -> Dict[str, Any]:
        """Get table migration statistics."""
        cursor = self.conn.cursor()
        
        stats = {}
        
        # Overall stats
        cursor.execute("""
            SELECT 
                COUNT(*) as total_tables,
                SUM(CASE WHEN catalog_type = 'glue' THEN 1 ELSE 0 END) as glue_tables,
                SUM(CASE WHEN catalog_type = 'unity' THEN 1 ELSE 0 END) as unity_tables,
                SUM(CASE WHEN migration_status = 'migrated' THEN 1 ELSE 0 END) as migrated,
                SUM(CASE WHEN migration_status = 'pending' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN has_schema_drift = 1 THEN 1 ELSE 0 END) as drifts
            FROM tables
        """)
        stats['overall'] = dict(cursor.fetchone())
        
        # By database
        cursor.execute("""
            SELECT 
                database_name,
                COUNT(*) as table_count,
                SUM(CASE WHEN migration_status = 'migrated' THEN 1 ELSE 0 END) as migrated
            FROM tables
            WHERE catalog_type = 'glue'
            GROUP BY database_name
        """)
        stats['by_database'] = [dict(row) for row in cursor.fetchall()]
        
        return stats
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
