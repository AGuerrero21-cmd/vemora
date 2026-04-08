# SQLite Database Implementation Guide

## Overview

A complete SQLite implementation has been created that mirrors the Supabase database interface. This allows seamless switching between remote (Supabase) and local (SQLite) databases without modifying the analysis code.

## Files Created

### 1. **SQLite_connection.py**
Local SQLite database module with identical function signatures to `Supabase_connection.py`.

**Location:** `{repository}/SQLite_connection.py`

**Database Location:** `{project_path}/DB/volcanic_data.db`

**Key Features:**
- Automatic schema creation on first use
- Identical function signatures for drop-in replacement
- Proper foreign key relationships
- BLOB storage for complex data types (lists, volumes)
- Automatic timestamps for created_at and updated_at
- Indexed queries for performance

### 2. **db_config.py**
Configuration module that handles switching between database backends.

**Location:** `/Users/aleja/Documents/PhD/Code/phd/db_config.py`

**Usage:** Import from db_config instead of directly importing Supabase_connection or SQLite_connection

## Database Schema

### Volcanoes Table

```
volcano_id (PK)          TEXT - Unique volcano identifier
name                     TEXT - Volcano name
country                  TEXT - Country location
region                   TEXT - Geographic region
latitude                 REAL - Latitude coordinate
longitude                REAL - Longitude coordinate
elevation                REAL - Elevation in meters
general_type             TEXT - Volcano type classification
tectonic_setting         TEXT - Tectonic setting
rock_type                TEXT - Primary rock type
rock_type2-5            TEXT - Secondary rock types
minor_rock_type         TEXT - Minor rock types
temperature             REAL - Temperature properties
density                 REAL - Density value
specific_heat           REAL - Specific heat capacity
last_eruption_year      INT  - Last eruption year
holocene_eruptions      INT  - Number of Holocene eruptions
created_at              TIMESTAMP - Record creation time
updated_at              TIMESTAMP - Record update time
```

### Eruptions Table

```
_id (PK)                TEXT  - Unique eruption identifier
volcano (FK)            TEXT  - Reference to volcano_id
year                    INT   - Eruption year
month                   INT   - Eruption month (1-12)
day                     INT   - Eruption day (1-31)
VEI                     INT   - Volcanic Explosivity Index
eruption_type           TEXT  - Type of eruption
certainty               TEXT  - Certainty level of record
volume                  BLOB  - JSON array [pyroclastic_vol, lava_vol]
column_height           REAL  - Eruption column height (km)
temperature             REAL  - Eruption temperature
density                 REAL  - Material density
specific_heat           REAL  - Specific heat capacity
e_tp                    REAL  - Pyroclastic thermal energy (J)
e_tl                    REAL  - Lava thermal energy (J)
e_tvc                   BLOB  - JSON array [min_col_energy, max_col_energy]
energy                  REAL  - Total energy (J)
biblio                  TEXT  - Bibliography reference
error                   INT   - Error margin
created_at              TIMESTAMP - Record creation time
updated_at              TIMESTAMP - Record update time
```

## How to Use

### Option 1: Using db_config (RECOMMENDED)

Replace imports in your code:

```python
# Old way:
# import Supabase_connection as mdb

# New way - same functions, configurable backend:
import db_config as mdb

# Use exactly the same as before:
eruptions = mdb.eruptions_energy(volcano_id)
volcano = mdb.volcano_data(volcano_id)
```

### Option 2: Direct SQLite Import

```python
import SQLite_connection as mdb

# All functions work identically
eruptions = mdb.eruptions_energy(volcano_id)
```

### Option 3: Environment Variable Configuration

```bash
# Use SQLite
export DATABASE_BACKEND=sqlite
python main_analysis.py

# Use Supabase
export DATABASE_BACKEND=supabase
python main_analysis.py
```

## Function Mapping

All functions from `Supabase_connection.py` are available with identical signatures:

| Function | SQLite Status | Description |
|----------|--------------|-------------|
| `add_volcanoSmith()` | ✅ | Add volcano and eruptions |
| `volcano_data()` | ✅ | Fetch volcano data |
| `volcano_data_completeness()` | ✅ | Get eruption years for analysis |
| `eruptions_energy()` | ✅ | Get eruptions with energy data |
| `eruptions_energy_all()` | ✅ | Calculate energy for all eruptions |
| `query_eruption_ym()` | ✅ | Query by year/month |
| `query_eruption_ym_biblio()` | ✅ | Get bibliography |
| `add_eruption()` | ✅ | Add single eruption |
| `update_eruption()` | ✅ | Update eruption field |
| `update_year()` | ✅ | Update year and error |
| `update_rock_type()` | ✅ | Update rock classification |
| `eruptions_count()` | ✅ | Count eruptions by VEI |
| `clean()` | ✅ | Filter valid energy data |
| `get_rock_properties()` | ✅ | Get rock type properties |
| `calculate_mean()` | ✅ | Calculate mean from range |
| `volcanoes_data_cluster()` | ⚠️ | Not yet implemented for SQLite |

## Key Features

### 1. **Automatic Schema Creation**
Database and tables are created automatically on first import. No manual setup required.

### 2. **Data Type Compatibility**
- Complex types (lists, arrays) are stored as JSON in BLOB fields
- Automatic serialization/deserialization via `_dict_to_blob()` and `_blob_to_dict()`

### 3. **Result Wrapper Class**
The `Result` class matches Supabase's response format:

```python
result = mdb.eruptions_energy(volcano_id)
# result.data contains the list of records
for eruption in result.data:
    print(eruption['energy'])
```

### 4. **Automatic Timestamps**
All records automatically track creation and update times:

```python
eruption['created_at']  # When record was created
eruption['updated_at']  # When record was last modified
```

### 5. **Foreign Key Relationships**
Database enforces referential integrity:
- All eruptions must reference an existing volcano
- Deleting a volcano cascades to eruptions (if configured)

### 6. **Indexed Queries**
Performance optimization through indexes on:
- `eruptions.volcano` (for filtering by volcano)
- `eruptions.year` (for temporal queries)
- `eruptions.year + month` (for combined queries)

## Migration Guide

### From Supabase to SQLite

1. **Export your Supabase data:**
   ```python
   import Supabase_connection as mdb
   
   # Fetch all data
   volcanoes = mdb.volcano_data(volcano_id)
   eruptions = mdb.eruptions_energy(volcano_id)
   ```

2. **Import into SQLite:**
   ```python
   import SQLite_connection as sqlite_db
   
   sqlite_db.add_volcanoSmith(volcano_data, eruptions)
   ```

3. **Switch configuration:**
   ```bash
   export DATABASE_BACKEND=sqlite
   ```

### From SQLite to Supabase

Simply change the environment variable:
```bash
export DATABASE_BACKEND=supabase
```

All existing code will continue working without modification.

## Testing the Installation

```python
import SQLite_connection as db
from main_analysis import project_path
import os

# Verify database was created
db_path = os.path.join(project_path, "DB", "volcanic_data.db")
print(f"Database exists: {os.path.exists(db_path)}")

# Test adding data
test_volcano = {
    "volcano_id": "TEST001",
    "name": "Test Volcano",
    "country": "Test Country",
    "latitude": 0.0,
    "longitude": 0.0
}

test_eruption = {
    "_id": "TEST_ERU001",
    "volcano": "TEST001",
    "year": 2020,
    "month": 1,
    "day": 1
}

db.add_volcanoSmith(test_volcano, [test_eruption])

# Test retrieval
result = db.volcano_data("TEST001")
print(f"Found {len(result.data)} volcano records")

# Test cleaning up
import sqlite3
conn = sqlite3.connect(db_path)
conn.execute("DELETE FROM eruptions WHERE volcano = ?", ("TEST001",))
conn.execute("DELETE FROM volcanoes WHERE volcano_id = ?", ("TEST001",))
conn.commit()
conn.close()

print("Test completed successfully!")
```

## Performance Considerations

### SQLite Advantages
- **Zero setup** - No server configuration needed
- **Zero network latency** - All queries are instant
- **Portability** - Single file can be copied/backed up
- **Development speed** - Quick iteration without internet dependency

### SQLite Limitations
- **Single writer** - Only one process can write at a time (fine for single analysis script)
- **File size** - Better for < 1GB data (plenty for volcanic records)
- **No concurrent access** - Not suitable for simultaneous writes from multiple processes

### Optimization Tips

1. **Batch Operations:**
   ```python
   # Instead of multiple updates
   for eruption_id in eruption_ids:
       db.update_eruption(eruption_id, "VEI", 3)
   
   # Create a batch update function if many updates needed
   ```

2. **Query Optimization:**
   - Use `eruptions_energy()` to get pre-filtered data
   - Filter in Python if the query is complex

## Database Maintenance

### Backup
```bash
cp {project_path}/DB/volcanic_data.db {project_path}/DB/volcanic_data_backup.db
```

### Reset Database
```python
import os
from main_analysis import project_path

db_path = os.path.join(project_path, "DB", "volcanic_data.db")
os.remove(db_path)
# Database will be recreated on next import
```

### View Database Content
```bash
sqlite3 {project_path}/DB/volcanic_data.db

# Inside sqlite3 prompt:
.tables                    # List all tables
.schema eruptions          # Show eruptions table structure
SELECT COUNT(*) FROM eruptions;  # Count records
.quit                      # Exit
```

## Troubleshooting

### "Database is locked" error
- SQLite is very strict about simultaneous access
- Solution: Ensure only one Python process is using the database
- Close any database viewers/inspectors while code is running

### Missing imports
- Ensure `db_config.py` or SQLite files are in the same directory as your analysis scripts
- Or add the path: `sys.path.insert(0, '/path/to/phd')`



## Support & Questions

For issues or questions about the SQLite implementation, check:
1. Database file permissions
2. Project path configuration in `main_analysis.py`
3. Environment variable setup
4. SQLite version compatibility (Python 3.6+ includes sqlite3)
