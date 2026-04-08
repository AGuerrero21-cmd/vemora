#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Database Connection Configuration Module

Allows switching between Supabase (remote) and SQLite (local) databases
without changing the rest of the codebase.

Configuration can be set via environment variable or by modifying this file.

Usage:
    import db_config as mdb
    
    # This will use the configured database backend automatically
    eruptions = mdb.eruptions_energy(volcano_id)

Created: 2026-02-12
@author: aleja
"""

import os

# ============================================================================
# DATABASE BACKEND SELECTION
# ============================================================================

# Set database backend
# Options: "supabase" or "sqlite"
# Can be overridden with environment variable: DATABASE_BACKEND=sqlite
DATABASE_BACKEND = os.getenv("DATABASE_BACKEND", "sqlite").lower()

# ============================================================================
# DYNAMIC IMPORT
# ============================================================================

if DATABASE_BACKEND == "supabase":
    print("[INFO] Using Supabase database backend")
    from Supabase_connection import (
        database_connection,
        add_volcanoSmith,
        volcano_data,
        volcano_data_completeness,
        eruptions_energy,
        eruptions_energy_all,
        query_eruption_ym,
        query_eruption_ym_biblio,
        add_eruption,
        update_eruption,
        update_year,
        update_rock_type,
        eruptions_count,
        clean,
        get_rock_properties,
        calculate_mean,
        volcanoes_data_cluster
    )

elif DATABASE_BACKEND == "sqlite":
    print("[INFO] Using SQLite database backend")
    from SQLite_connection import (
        add_volcanoSmith,
        volcano_data,
        volcano_data_completeness,
        eruptions_energy,
        eruptions_energy_all,
        query_eruption_ym,
        query_eruption_ym_biblio,
        add_eruption,
        update_eruption,
        update_year,
        update_rock_type,
        eruptions_count,
        clean,
        get_rock_properties,
        calculate_mean,
        volcanoes_data_cluster
    )

else:
    raise ValueError(f"Invalid DATABASE_BACKEND: {DATABASE_BACKEND}. "
                    "Must be 'supabase' or 'sqlite'")

# ============================================================================
# DOCUMENTATION
# ============================================================================

__doc__ += f"""

Current Backend: {DATABASE_BACKEND.upper()}

Available Functions:
    - add_volcanoSmith(volcano, eruptions)
    - volcano_data(volcano_id)
    - volcano_data_completeness(volcano_id)
    - eruptions_energy(volcano_id)
    - eruptions_energy_all()
    - query_eruption_ym(volcano_id, year, month)
    - query_eruption_ym_biblio(eruption_id)
    - add_eruption(eruption)
    - update_eruption(eruption_id, tag, value)
    - update_year(eruption_id, new_year, error)
    - update_rock_type(row, volcano_id)
    - eruptions_count()
    - clean(result)
    - get_rock_properties(rock_type, file_path)
    - calculate_mean(values)
    - volcanoes_data_cluster()

To switch backends, set environment variable before importing:
    export DATABASE_BACKEND=sqlite
    # or
    export DATABASE_BACKEND=supabase
"""
