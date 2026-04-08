#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQLite Local Database Connection Module

Provides the same interface as Supabase_connection.py but using SQLite for local storage.
Maintains identical function signatures for easy switching between remote (Supabase) 
and local (SQLite) databases.

Created: 2026-02-12
@author: aleja
"""

import os
import sqlite3
import json
import sys
from typing import List, Dict, Tuple, Optional, Any
from datetime import datetime
import warnings


# ============================================================================
# DATABASE CONFIGURATION
# ============================================================================

# Module-level variables for database paths
_project_path = None
_DB_DIRECTORY = None
_DB_PATH = None
_database_initialized = False


def set_project_path(path: str) -> None:
    """
    Set the project path for database operations.
    Must be called before any database operations.
    
    Args:
        path: Path to project data directory
    """
    global _project_path, _DB_DIRECTORY, _DB_PATH, _database_initialized

    if not isinstance(path, str) or not path.strip():
        raise ValueError("Project path must be a non-empty string")

    normalized_path = os.path.abspath(path)

    # Skip reconfiguration when nothing changed and DB is already ready
    if _project_path == normalized_path and _database_initialized:
        return

    _project_path = normalized_path
    _DB_DIRECTORY = os.path.join(normalized_path, "DB")
    _DB_PATH = os.path.join(_DB_DIRECTORY, "volcanic_data.db")
    
    # Ensure DB directory exists
    os.makedirs(_DB_DIRECTORY, exist_ok=True)

    # Initialize DB schema once project path is known
    _initialize_database()
    _database_initialized = True


def get_db_path() -> str:
    """Get the current database path."""
    if _DB_PATH is None:
        raise RuntimeError("Project path not set. Call set_project_path() first.")
    return _DB_PATH


def _ensure_database_ready() -> None:
    """Ensure project path is configured and schema exists before DB access."""
    global _database_initialized

    if _DB_PATH is None:
        raise RuntimeError("Project path not set. Call set_project_path() first.")

    if not _database_initialized:
        _initialize_database()
        _database_initialized = True
        raise RuntimeError("Database initialized. Please retry the operation.")


# ============================================================================
# DATABASE INITIALIZATION & SCHEMA
# ============================================================================

def _initialize_database() -> None:
    """
    Initialize SQLite database with required schema.
    Creates tables if they don't exist.
    """
    try:
        conn = sqlite3.connect(get_db_path())
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Enable foreign keys
        cursor.execute("PRAGMA foreign_keys = ON")
        
        # Volcanoes table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS volcanoes (
                volcano_id TEXT PRIMARY KEY,
                name TEXT,
                country TEXT,
                region TEXT,
                latitude REAL,
                longitude REAL,
                elevation REAL,
                general_type TEXT,
                tectonic_setting TEXT,
                rock_type TEXT,
                rock_type2 TEXT,
                rock_type3 TEXT,
                rock_type4 TEXT,
                rock_type5 TEXT,
                minor_rock_type TEXT,
                minor_rock_type2 TEXT,
                minor_rock_type3 TEXT,
                minor_rock_type4 TEXT,
                minor_rock_type5 TEXT,
                temperature REAL,
                density REAL,
                specific_heat REAL,
                last_eruption_year INTEGER,
                holocene_eruptions INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Eruptions table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS eruptions (
                _id TEXT PRIMARY KEY,
                volcano TEXT NOT NULL,
                year INTEGER,
                month INTEGER,
                day INTEGER,
                VEI INTEGER,
                eruption_type TEXT,
                certainty TEXT,
                volume BLOB,
                column_height REAL,
                mer REAL,
                temperature REAL,
                density REAL,
                specific_heat REAL,
                e_tp REAL,
                e_tl REAL,
                e_tvc BLOB,
                energy REAL,
                biblio TEXT,
                error INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (volcano) REFERENCES volcanoes(volcano_id)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS epdfs (
                _id TEXT PRIMARY KEY,
                volcano TEXT NOT NULL,
                type TEXT,
                parameters BLOB,
                distribution TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (volcano) REFERENCES volcanoes(volcano_id)
            )
        """)

        
        # Create indexes for common queries
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_eruptions_volcano 
            ON eruptions(volcano)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_eruptions_year 
            ON eruptions(year)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_eruptions_year_month 
            ON eruptions(year, month)
        """)
        
        conn.commit()
        conn.close()
        
        print(f"[INFO] Database initialized at {get_db_path()}")
        
    except Exception as e:
        print(f"[ERROR] Failed to initialize database: {e}")
        raise


def _dict_to_blob(data: Any) -> bytes:
    """Convert Python object to JSON blob for storage."""
    if data is None:
        return None
    try:
        return json.dumps(data).encode('utf-8')
    except Exception as e:
        warnings.warn(f"Could not serialize data: {e}")
        return None


def _blob_to_dict(blob: bytes) -> Any:
    """Convert JSON blob back to Python object."""
    if blob is None:
        return None
    try:
        return json.loads(blob.decode('utf-8'))
    except Exception as e:
        warnings.warn(f"Could not deserialize data: {e}")
        return None


# ============================================================================
# CONNECTION MANAGEMENT
# ============================================================================

def _get_connection() -> sqlite3.Connection:
    """
    Get database connection with row factory enabled.
    
    Returns:
        SQLite connection object
    """
    _ensure_database_ready()
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


# ============================================================================
# VOLCANO OPERATIONS
# ============================================================================

def add_volcanoSmith(volcano: Dict, eruptions: List[Dict]) -> None:
    """
    Add volcano and eruptions to database if not already present.
    
    Args:
        volcano: Volcano dictionary with volcano_id and properties
        eruptions: List of eruption dictionaries
        
    Raises:
        Exception: If database operation fails
    """
    try:
        conn = _get_connection()
        cursor = conn.cursor()
        
        volcano_id = str(volcano.get("volcano_id"))
        
        # Check if volcano already exists
        cursor.execute("SELECT volcano_id FROM volcanoes WHERE volcano_id = ?", 
                      (volcano_id,))
        existing = cursor.fetchone()
        
        if not existing:
            # Insert volcano
            cursor.execute("""
                INSERT INTO volcanoes (
                    volcano_id, name, country, region, latitude, longitude, 
                    elevation, general_type, tectonic_setting, rock_type
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                volcano_id,
                volcano.get("name"),
                volcano.get("country"),
                volcano.get("region"),
                volcano.get("latitude"),
                volcano.get("longitude"),
                volcano.get("elevation"),
                volcano.get("general_type"),
                volcano.get("tectonic_setting"),
                volcano.get("rock_type")
            ))
            print(f"[INFO] Volcano {volcano_id} added to database")
        else:
            print(f"[INFO] Volcano {volcano_id} already exists in database")
        
        # Add eruptions
        for eruption in eruptions:
            eruption_id = str(eruption.get("_id"))
            
            # Check if eruption already exists
            cursor.execute("SELECT _id FROM eruptions WHERE _id = ?", 
                          (eruption_id,))
            if not cursor.fetchone():
                cursor.execute("""
                    INSERT INTO eruptions (
                        _id, volcano, year, month, day, VEI, eruption_type, 
                        certainty, volume, column_height
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    eruption_id,
                    volcano_id,
                    eruption.get("year"),
                    eruption.get("month"),
                    eruption.get("day"),
                    eruption.get("VEI"),
                    eruption.get("eruption_type"),
                    eruption.get("certainty"),
                    _dict_to_blob(eruption.get("volume")),
                    eruption.get("column_height")
                ))
                print(f"[INFO] Eruption {eruption_id} added to database")
        
        conn.commit()
        conn.close()
        
    except Exception as e:
        print(f"[ERROR] Failed to add volcano/eruptions: {e}")
        raise


def volcano_data(volcano_id: str) -> "Result":
    """
    Fetch all volcano data.
    
    Args:
        volcano_id: Volcano identifier
        
    Returns:
        Result object with volcano data
    """
    try:
        conn = _get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT * FROM volcanoes WHERE volcano_id = ?
        """, (volcano_id,))
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            volcano_dict = dict(row)
            return Result([volcano_dict])
        else:
            return Result([])
        
    except Exception as e:
        print(f"[ERROR] Failed to fetch volcano data: {e}")
        raise


def volcano_data_completeness(volcano_id: str) -> "Result":
    """
    Fetch eruption years for completeness analysis.
    
    Args:
        volcano_id: Volcano identifier
        
    Returns:
        Result object with eruption year data
    """
    try:
        conn = _get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT year FROM eruptions WHERE volcano = ? ORDER BY year ASC
        """, (volcano_id,))
        
        rows = cursor.fetchall()
        conn.close()
        
        eruptions = [{"year": row[0]} for row in rows if row[0] is not None]
        
        return Result(eruptions)
        
    except Exception as e:
        print(f"[ERROR] Failed to fetch completeness data: {e}")
        raise


# ============================================================================
# ERUPTION OPERATIONS
# ============================================================================

def eruptions_energy(volcano_id: str) -> "Result":
    """
    Fetch all eruptions with energy data for a volcano.
    
    Args:
        volcano_id: Volcano identifier
        
    Returns:
        Result object with eruption energy data
    """
    try:
        conn = _get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT * FROM eruptions 
            WHERE volcano = ? AND energy IS NOT NULL AND energy != ''
            ORDER BY year ASC
        """, (volcano_id,))
        
        rows = cursor.fetchall()
        conn.close()
        
        eruptions = []
        for row in rows:
            eruption_dict = dict(row)
            # Deserialize BLOB fields
            if eruption_dict.get('volume'):
                eruption_dict['volume'] = _blob_to_dict(eruption_dict['volume'])
            if eruption_dict.get('e_tvc'):
                eruption_dict['e_tvc'] = _blob_to_dict(eruption_dict['e_tvc'])
            eruptions.append(eruption_dict)
        
        return Result(eruptions)
        
    except Exception as e:
        print(f"[ERROR] Failed to fetch eruption energy data: {e}")
        raise
def get_eruptions_volcano(volcano_id: str) -> "Result":
    """
    Fetch all eruptions with energy data for a volcano.
    
    Args:
        volcano_id: Volcano identifier
        
    Returns:
        Result object with eruption energy data
    """
    try:
        conn = _get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT * FROM eruptions 
            WHERE volcano = ? AND year IS NOT NULL
            ORDER BY year ASC
        """, (volcano_id,))
        
        rows = cursor.fetchall()
        conn.close()
        
        eruptions = []
        for row in rows:
            eruption_dict = dict(row)
            # Deserialize BLOB fields
            if eruption_dict.get('year'):
                eruption_dict['year'] = eruption_dict['year']
            if eruption_dict.get('month'):
                eruption_dict['month'] = eruption_dict['month']
            if eruption_dict.get('day'):
                eruption_dict['day'] = eruption_dict['day']
            eruptions.append(eruption_dict)
        
        return Result(eruptions)
        
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(f"[ERROR] Failed to fetch eruption data: {e}")
        print(f"Error type: {exc_type.__name__}")
        print(f"Error message: {e}")
        print(f"Line number: {exc_tb.tb_lineno}")
        raise


def eruptions_energy_all(volcano_id: str) -> None:
    """
    Calculate and update energy values for eruptions of a specific volcano.
    
    Args:
        volcano_id: Volcano identifier
        
    Computes thermal energy from eruption volumes and properties.
    """
    try:
        conn = _get_connection()
        cursor = conn.cursor()
        
        variables = {
            "pyroclastic_temperature": 775,
            "lava_temperature": 1175,
            "pyroclastic_specific_heat": 949.7,
            "lava_specific_heat": 1046.7,
            "lava_latent_heat": 209340,
            "pyroclastic_density": 1000,
            "lava_density": 2500,
            "tmin": 15 * 60,
            "tmax": 24 * 3600
        }
        
        # Get all eruptions with volume data for the specified volcano
        cursor.execute("""
            SELECT * FROM eruptions 
            WHERE volcano = ? AND (volume IS NOT NULL OR VEI IS NOT NULL)
        """, (volcano_id,))
        eruptions = cursor.fetchall()
        print(len(eruptions))
        for eruption in eruptions:
            eruption_dict = dict(eruption)
            print(f"Processing eruption {eruption_dict['_id']} for energy calculation") 
            
            # Get volcano data
            cursor.execute("""
                SELECT * FROM volcanoes WHERE volcano_id = ?
            """, (eruption_dict['volcano'],))
            
            volcano_row = cursor.fetchone()
            if not volcano_row:
                continue
            volcano = dict(volcano_row)
            
            volume = _blob_to_dict(eruption_dict['volume'])
            # Determine temperature
            if eruption_dict.get('temperature'):
                lt = float(eruption_dict['temperature'])
            elif volcano.get('temperature'):
                lt = float(volcano['temperature'])
            else:
                lt = variables["lava_temperature"]
            
            # Determine density
            if eruption_dict.get('density'):
                rho = float(eruption_dict['density'])
            elif volcano.get('density'):
                rho = float(volcano['density'])
            else:
                rho = variables["lava_density"]
            
            # Determine specific heat
            if eruption_dict.get('specific_heat'):
                cp = float(eruption_dict['specific_heat'])
            elif volcano.get('specific_heat'):
                cp = float(volcano['specific_heat'])
            else:
                cp = variables["lava_specific_heat"]
            
            if isinstance(volume, list) and (volume[0] != 0 or volume[1] != 0):
               
                # Calculate energies
                e_tp = (volume[0] * 1e9 * variables["pyroclastic_density"]) * \
                       variables["pyroclastic_temperature"] * \
                       variables["pyroclastic_specific_heat"]
                
                e_tl = (volume[1] * 1e9 * rho) * \
                       ((lt * cp) + variables["lava_latent_heat"])
                
                # Update eruption with calculated energies
                update_eruption(eruption_dict['_id'], "e_tp", e_tp)
                update_eruption(eruption_dict['_id'], "e_tl", e_tl)
                update_eruption(eruption_dict['_id'], "energy", e_tp + e_tl)
                
                # Handle column height if present
                if eruption_dict.get('column_height'):
                    h = float(eruption_dict['column_height'])
                    e_vc = [
                        (h ** 4) * variables["tmin"] * 1000 / (8.2 ** 4),
                        (h ** 4) * variables["tmax"] * 1000 / (8.2 ** 4)
                    ]
                    update_eruption(eruption_dict['_id'], "e_tvc", e_vc)
            else:
                eruption_id = eruption_dict['_id']
                # Calculate energy from VEI if volume data is invalid
                if eruption_dict.get('VEI') is not None:
                    vei = eruption_dict['VEI']
                    volume_from_vei = 10 **( vei + 4) # Convert from km³ to m³
                    e_total = (volume_from_vei * 1e9 * variables["pyroclastic_density"]) * \
                       variables["pyroclastic_temperature"] * \
                       variables["pyroclastic_specific_heat"]
                    update_eruption(eruption_id, "energy", e_total)
                    warnings.warn(f"Eruption {eruption_id} has invalid volume data: {volume}. Energy estimated from VEI.")
        conn.commit()
        conn.close()
        print("[INFO] Energy calculation complete")
        
    except Exception as e:
        print(f"[ERROR] Failed to calculate eruption energies: {e}")
        raise


def query_eruption_ym(volcano_id: str, year: int, month: int) -> Optional[str]:
    """
    Query eruption by volcano, year, and month.
    
    Args:
        volcano_id: Volcano identifier
        year: Eruption year
        month: Eruption month
        
    Returns:
        Eruption ID if found, None otherwise
    """
    try:
        conn = _get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT _id FROM eruptions 
            WHERE volcano = ? AND year = ? AND month = ?
        """, (volcano_id, int(year), int(month)))
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return row[0]
        else:
            return None
            
    except Exception as e:
        print(f"[ERROR] Failed to query eruption: {e}")
        raise


def query_eruption_ym_biblio(eruption_id: str) -> Optional[str]:
    """
    Query bibliography for an eruption.
    
    Args:
        eruption_id: Eruption identifier
        
    Returns:
        Bibliography reference if found, None otherwise
    """
    try:
        conn = _get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT biblio FROM eruptions WHERE _id = ?
        """, (eruption_id,))
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return row[0]
        else:
            return None
            
    except Exception as e:
        print(f"[ERROR] Failed to query eruption bibliography: {e}")
        raise


def add_eruption(eruption: Dict) -> None:
    """
    Add a single eruption to database.
    
    Args:
        eruption: Eruption dictionary
    """
    try:
        conn = _get_connection()
        cursor = conn.cursor()
        
        # Helper function to convert numpy types to native Python types
        def to_native(val):
            if val is None:
                return None
            # Convert numpy types to native Python types
            if hasattr(val, 'item'):  # numpy types
                return val.item()
            return val
        
        # Convert biblio list to JSON string
        biblio = eruption.get("biblio")
        if isinstance(biblio, list):
            biblio = json.dumps(biblio)
        
        cursor.execute("""
            INSERT INTO eruptions (
                _id, volcano, year, month, day, VEI, eruption_type,
                certainty, volume, column_height, mer, biblio
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?)
        """, (
            str(eruption.get("_id")),
            str(eruption.get("volcano")),
            to_native(eruption.get("year")),
            to_native(eruption.get("month")),
            to_native(eruption.get("day")),
            to_native(eruption.get("VEI")),
            eruption.get("eruption_type"),
            eruption.get("certainty"),
            _dict_to_blob(eruption.get("volume")),
            to_native(eruption.get("column_height")),
            to_native(eruption.get("mer")),
            biblio
        ))
        
        conn.commit()
        conn.close()
        print(f"[INFO] Eruption {eruption.get('_id')} added")
        
    except Exception as e:
        print(f"[ERROR] Failed to add eruption:{eruption} {e}")
        raise


def update_eruption(eruption_id: str, tag: str, value: Any) -> None:
    """
    Update a single field in an eruption record.
    
    Args:
        eruption_id: Eruption identifier
        tag: Field name to update
        value: New value
    """
    try:
        conn = _get_connection()
        cursor = conn.cursor()
        
        # Helper function to convert numpy types to native Python types
        def to_native(val):
            if val is None:
                return None
            # Convert numpy types to native Python types
            if hasattr(val, 'item'):  # numpy types
                return val.item()
            return val
        
        # Handle BLOB fields
        if tag in ['volume', 'e_tvc']:
            value = _dict_to_blob(value)
        # Handle biblio list field
        elif tag == 'biblio' and isinstance(value, list):
            value = json.dumps(value)
        else:
            # Convert numpy types to native Python types
            value = to_native(value)
        
        # Build dynamic update query
        cursor.execute(f"""
            UPDATE eruptions 
            SET {tag} = ?, updated_at = CURRENT_TIMESTAMP
            WHERE _id = ?
        """, (value, eruption_id))
        
        conn.commit()
        conn.close()
        print(f"[INFO] Eruption {eruption_id} updated: {tag}={value}")
        
    except Exception as e:
        print(f"[ERROR] Failed to update eruption: {eruption_id} {tag}={value} {e}")
        raise


def update_year(eruption_id: str, new_year: int, error: int) -> None:
    """
    Update eruption year and error fields.
    
    Args:
        eruption_id: Eruption identifier
        new_year: New year value
        error: Error margin
    """
    try:
        conn = _get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE eruptions 
            SET year = ?, error = ?, updated_at = CURRENT_TIMESTAMP
            WHERE _id = ?
        """, (int(new_year), int(error), eruption_id))
        
        conn.commit()
        conn.close()
        print(f"[INFO] Eruption {eruption_id} year updated to {new_year}")
        
    except Exception as e:
        print(f"[ERROR] Failed to update eruption year: {e}")
        raise


def update_rock_type(row: Dict, volcano_id: str) -> None:
    """
    Update volcano rock type information.
    
    Args:
        row: Dictionary with rock type data
        volcano_id: Volcano identifier
    """
    try:
        # Clean row - replace special characters with None
        cleaned_row = {}
        for key, value in row.items():
            if value == '\xa0' or value == '':
                cleaned_row[key] = None
            else:
                cleaned_row[key] = value
        
        conn = _get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE volcanoes 
            SET rock_type2 = ?, rock_type3 = ?, rock_type4 = ?, rock_type5 = ?,
                minor_rock_type = ?, minor_rock_type2 = ?, minor_rock_type3 = ?,
                minor_rock_type4 = ?, minor_rock_type5 = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE volcano_id = ?
        """, (
            cleaned_row.get("Major_Rock_2"),
            cleaned_row.get("Major_Rock_3"),
            cleaned_row.get("Major_Rock_4"),
            cleaned_row.get("Major_Rock_5"),
            cleaned_row.get("Minor_Rock_1"),
            cleaned_row.get("Minor_Rock_2"),
            cleaned_row.get("Minor_Rock_3"),
            cleaned_row.get("Minor_Rock_4"),
            cleaned_row.get("Minor_Rock_5"),
            volcano_id
        ))
        
        conn.commit()
        conn.close()
        print(f"[INFO] Rock types updated for volcano {volcano_id}")
        
    except Exception as e:
        print(f"[ERROR] Failed to update rock types: {e}")
        raise


def eruptions_count() -> Dict:
    """
    Count eruptions by volcano and VEI.
    
    Returns:
        Dictionary with eruption counts aggregated by volcano and VEI
    """
    try:
        conn = _get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT volcano, VEI, COUNT(*) as count
            FROM eruptions
            GROUP BY volcano, VEI
            ORDER BY volcano, VEI
        """)
        
        results = {}
        for row in cursor.fetchall():
            volcano = row[0]
            vei = row[1]
            count = row[2]
            
            if volcano not in results:
                results[volcano] = {
                    'tipo': [],
                    'total_ev': 0
                }
            
            results[volcano]['tipo'].append({
                'vei': vei,
                'count': count
            })
            results[volcano]['total_ev'] += count
        
        conn.close()
        
        return results
        
    except Exception as e:
        print(f"[ERROR] Failed to count eruptions: {e}")
        raise


def volcanoes_data_cluster() -> Dict:
    """
    Retrieve all volcanoes with eruption counts and properties, grouped by VEI.
    
    Combines volcano properties with eruption statistics for clustering/grouping analysis.
    
    Returns:
        Dictionary with volcano_id as key, containing:
        - ID: Volcano identifier
        - Name: Volcano name
        - General_type: Volcano classification
        - Tectonic_setting: Tectonic context
        - Rock_type: Primary rock type
        - Total_events: Total number of eruptions
        - VEI_None, VEI_0, ..., VEI_6: Count by VEI level
    """
    try:
        conn = _get_connection()
        cursor = conn.cursor()
        
        # Get eruption counts by volcano and VEI
        eruption_counts = eruptions_count()
        
        result_dict = {}
        
        # Fetch all volcanoes
        cursor.execute("""
            SELECT volcano_id, name, general_type, tectonic_setting, rock_type
            FROM volcanoes
            ORDER BY volcano_id
        """)
        
        for row in cursor.fetchall():
            volcano_id = row[0]
            name = row[1]
            general_type = row[2]
            tectonic_setting = row[3]
            rock_type = row[4]
            
            # Get eruption statistics for this volcano
            eruption_info = eruption_counts.get(volcano_id, {'tipo': [], 'total_ev': 0})
            eruptions_by_vei = eruption_info['tipo']
            total_eruptions = eruption_info['total_ev']
            
            # Helper to find count for specific VEI
            def find_vei_count(vei_list, vei_target):
                for item in vei_list:
                    if str(item.get('vei')) == str(vei_target):
                        return item.get('count', 0)
                return 0
            
            # Build result dictionary
            result_dict[str(volcano_id)] = {
                'ID': str(volcano_id),
                'Name': name or 'Unknown',
                'General_type': general_type or 'Unknown',
                'Tectonic_setting': tectonic_setting or 'Unknown',
                'Rock_type': rock_type or 'Unknown',
                'Total_events': total_eruptions,
                'VEI_None': find_vei_count(eruptions_by_vei, 'None'),
                'VEI_0': find_vei_count(eruptions_by_vei, '0'),
                'VEI_1': find_vei_count(eruptions_by_vei, '1'),
                'VEI_2': find_vei_count(eruptions_by_vei, '2'),
                'VEI_3': find_vei_count(eruptions_by_vei, '3'),
                'VEI_4': find_vei_count(eruptions_by_vei, '4'),
                'VEI_5': find_vei_count(eruptions_by_vei, '5'),
                'VEI_6': find_vei_count(eruptions_by_vei, '6'),
            }
        
        conn.close()
        
        return result_dict
        
    except Exception as e:
        print(f"[ERROR] Failed to retrieve volcano cluster data: {e}")
        raise


def clean(result: "Result") -> "Result":
    """
    Filter eruptions with valid energy data.
    
    Args:
        result: Result object with eruptions
        
    Returns:
        Filtered Result object
    """
    filtered_data = [
        eruption for eruption in result.data 
        if eruption.get("energy") is not None and eruption.get("energy") != ""
    ]
    return Result(filtered_data)


# ============================================================================
# ROCK TYPE UTILITIES
# ============================================================================

def calculate_mean(values: str) -> float:
    """
    Calculate mean from range string "min - max".
    
    Args:
        values: String in format "min - max"
        
    Returns:
        Mean value
    """
    try:
        start, end = map(float, values.split(" - "))
        return (start + end) / 2
    except Exception as e:
        print(f"[ERROR] Could not calculate mean from '{values}': {e}")
        return 0.0


def get_rock_properties(rock_type: str, 
                       file_path: str = "resources/rock.json") -> Dict:
    """
    Get rock properties from JSON file.
    
    Args:
        rock_type: Rock type name
        file_path: Path to rock properties JSON file
        
    Returns:
        Dictionary with rock properties (temperature, density, specific_heat)
    """
    try:
        if not os.path.exists(file_path):
            print(f"[WARNING] File {file_path} does not exist")
            return None
        
        with open(file_path, 'r') as f:
            rocks = json.load(f).get("rocks", [])
        
        for rock in rocks:
            if rock.get("rock_type") == rock_type:
                return {
                    "rock_type": rock_type,
                    "mean_specific_heat_J_kg_C": calculate_mean(rock["specific_heat_J_kg_C"]),
                    "mean_density_kg_m3": calculate_mean(rock["density_kg_m3"]),
                    "mean_temperature_C": calculate_mean(rock["temperature_C"])
                }
        
        return {"error": f"Rock type '{rock_type}' not found"}
        
    except FileNotFoundError:
        return {"error": f"File '{file_path}' not found"}
    except KeyError as e:
        return {"error": f"Missing key in dataset: {e}"}
    except Exception as e:
        return {"error": f"An error occurred: {e}"}


# ============================================================================
# EPDF OPERATIONS (Energy PDF Data)
# ============================================================================

def upsert_epdf(volcano_id: str, epdf_type: str, parameters: Dict, 
                distribution: str) -> None:
    """
    Insert or update an energy PDF record in the epdfs table.
    
    Args:
        volcano_id: Volcano identifier
        epdf_type: Type of PDF ('E_total', 'phi', 'temporal', 'marginal')
        parameters: Dictionary with PDF parameters
        distribution: Name of the fitted distribution
        
    Raises:
        ValueError: If epdf_type is not valid
        Exception: If database operation fails
    """
    
    valid_types = ['E_total', 'phi', 'temporal', 'marginal','absolute_total','absolute_tephra','absolute_lava']
    
    if epdf_type not in valid_types:
        raise ValueError(
            f"Invalid epdf_type '{epdf_type}'. "
            f"Must be one of: {', '.join(valid_types)}"
        )
    
    try:
        conn = _get_connection()
        cursor = conn.cursor()
        
        # Create unique ID for this PDF record
        record_id = f"{volcano_id}_{epdf_type}_{datetime.now().timestamp()}"
        
        # Check if record already exists for this volcano and type
        cursor.execute(
            "SELECT _id FROM epdfs WHERE volcano = ? AND type = ?",
            (volcano_id, epdf_type)
        )
        existing = cursor.fetchone()
        
        if existing:
            # Update existing record
            cursor.execute("""
                UPDATE epdfs 
                SET parameters = ?, distribution = ?, updated_at = CURRENT_TIMESTAMP
                WHERE volcano = ? AND type = ?
            """, (
                _dict_to_blob(parameters),
                distribution,
                volcano_id,
                epdf_type
            ))
            print(f"[INFO] EPDF record updated: {volcano_id} {epdf_type}")
        else:
            # Insert new record
            cursor.execute("""
                INSERT INTO epdfs (
                    _id, volcano, type, parameters, distribution
                ) VALUES (?, ?, ?, ?, ?)
            """, (
                record_id,
                volcano_id,
                epdf_type,
                _dict_to_blob(parameters),
                distribution
            ))
            print(f"[INFO] EPDF record inserted: {volcano_id} {epdf_type}")
        
        conn.commit()
        conn.close()
        
    except ValueError as e:
        print(f"[ERROR] Invalid parameter: {e}")
        raise
    except Exception as e:
        print(f"[ERROR] Failed to upsert EPDF record: {e}")
        raise


def get_epdf(volcano_id: str, epdf_type: str) -> Optional[Dict]:
    """
    Retrieve an energy PDF record from the epdfs table.
    
    Args:
        volcano_id: Volcano identifier
        epdf_type: Type of PDF ('E_total', 'phi', 'temporal', 'marginal')
        
    Returns:
        Dictionary with PDF data if found, None otherwise
    """
    
    try:
        conn = _get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT _id, volcano, type, parameters, distribution, created_at, updated_at "
            "FROM epdfs WHERE volcano = ? AND type = ?",
            (volcano_id, epdf_type)
        )
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            epdf_dict = dict(row)
            # Deserialize parameters
            if epdf_dict.get('parameters'):
                epdf_dict['parameters'] = _blob_to_dict(epdf_dict['parameters'])
            return epdf_dict
        else:
            return None
            
    except Exception as e:
        print(f"[ERROR] Failed to retrieve EPDF record: {e}")
        raise


def get_all_epdfs(volcano_id: str) -> "Result":
    """
    Retrieve all energy PDF records for a volcano.
    
    Args:
        volcano_id: Volcano identifier
        
    Returns:
        Result object with all EPDF records for the volcano
    """
    
    try:
        conn = _get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT _id, volcano, type, parameters, distribution, created_at, updated_at "
            "FROM epdfs WHERE volcano = ? ORDER BY type, updated_at DESC",
            (volcano_id,)
        )
        
        rows = cursor.fetchall()
        conn.close()
        
        epdfs = []
        for row in rows:
            epdf_dict = dict(row)
            # Deserialize parameters
            if epdf_dict.get('parameters'):
                epdf_dict['parameters'] = _blob_to_dict(epdf_dict['parameters'])
            epdfs.append(epdf_dict)
        
        return Result(epdfs)
        
    except Exception as e:
        print(f"[ERROR] Failed to retrieve EPDF records: {e}")
        raise


def delete_epdf(volcano_id: str, epdf_type: str) -> None:
    """
    Delete an energy PDF record from the epdfs table.
    
    Args:
        volcano_id: Volcano identifier
        epdf_type: Type of PDF to delete
    """
    
    try:
        conn = _get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            "DELETE FROM epdfs WHERE volcano = ? AND type = ?",
            (volcano_id, epdf_type)
        )
        
        conn.commit()
        conn.close()
        
        print(f"[INFO] EPDF record deleted: {volcano_id} {epdf_type}")
        
    except Exception as e:
        print(f"[ERROR] Failed to delete EPDF record: {e}")
        raise


# ============================================================================
# RESULT WRAPPER CLASS
# ============================================================================

class Result:
    """
    Wrapper class to match Supabase result format.
    
    Provides compatibility with existing code that expects
    Supabase API response objects.
    """
    
    def __init__(self, data: List[Dict]):
        """
        Initialize Result object.
        
        Args:
            data: List of data dictionaries
        """
        self.data = data if isinstance(data, list) else [data]
    
    def __len__(self) -> int:
        """Return number of records."""
        return len(self.data)
    
    def __repr__(self) -> str:
        """String representation."""
        return f"Result(data={self.data})"


# Database initialization is intentionally deferred until set_project_path()
