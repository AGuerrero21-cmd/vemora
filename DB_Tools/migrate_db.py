#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Database Migration Utility

Migrate data between Supabase and SQLite databases.
Supports one-way migration with validation and error handling.

Usage:
    python migrate_db.py --from supabase --to sqlite --volcano-id 211060
    python migrate_db.py --from sqlite --to supabase --all
    python migrate_db.py --backup sqlite

Created: 2026-02-12
@author: aleja
"""

import argparse
import sys
import os
from typing import List, Dict, Optional
from datetime import datetime

try:
    import Supabase_connection as supabase_db
    import SQLite_connection as sqlite_db
except ImportError as e:
    print(f"[ERROR] Failed to import database modules: {e}")
    sys.exit(1)

from vemora import project_path


# ============================================================================
# MIGRATION FUNCTIONS
# ============================================================================

def migrate_volcano_data(volcano_id: str, source: str, target: str) -> bool:
    """
    Migrate single volcano and its eruptions.
    
    Args:
        volcano_id: Volcano identifier
        source: Source database ('supabase' or 'sqlite')
        target: Target database ('supabase' or 'sqlite')
        
    Returns:
        True if successful, False otherwise
    """
    try:
        source_db = supabase_db if source == "supabase" else sqlite_db
        target_db = sqlite_db if target == "sqlite" else supabase_db
        
        print(f"[INFO] Fetching volcano {volcano_id} from {source}...")
        
        # Get volcano data
        volcano_result = source_db.volcano_data(volcano_id)
        if not volcano_result.data:
            print(f"[ERROR] Volcano {volcano_id} not found in {source}")
            return False
        
        volcano_data = volcano_result.data[0]
        print(f"[INFO] Found volcano: {volcano_data.get('name', 'Unknown')}")
        
        # Get eruptions
        eruptions_result = source_db.eruptions_energy(volcano_id)
        eruptions_data = eruptions_result.data
        print(f"[INFO] Found {len(eruptions_data)} eruptions")
        
        # Migrate to target
        print(f"[INFO] Migrating to {target}...")
        target_db.add_volcanoSmith(volcano_data, eruptions_data)
        
        print(f"[SUCCESS] Volcano {volcano_id} migrated successfully")
        return True
        
    except Exception as e:
        print(f"[ERROR] Migration failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def migrate_all_volcanoes(source: str, target: str) -> bool:
    """
    Migrate all volcanoes and eruptions.
    
    Args:
        source: Source database
        target: Target database
        
    Returns:
        True if successful
    """
    try:
        source_db = supabase_db if source == "supabase" else sqlite_db
        
        print(f"[INFO] Fetching all volcanoes from {source}...")
        
        # Get eruption counts to identify all volcanoes
        counts = source_db.eruptions_count()
        volcano_ids = list(counts.keys()) if isinstance(counts, dict) else []
        
        if not volcano_ids:
            print("[WARNING] No volcanoes found to migrate")
            return False
        
        print(f"[INFO] Found {len(volcano_ids)} volcanoes")
        
        success_count = 0
        failed_count = 0
        
        for i, volcano_id in enumerate(volcano_ids, 1):
            print(f"\n[{i}/{len(volcano_ids)}] Migrating volcano {volcano_id}...")
            if migrate_volcano_data(volcano_id, source, target):
                success_count += 1
            else:
                failed_count += 1
        
        print(f"\n[SUMMARY] Migration complete:")
        print(f"  Successful: {success_count}")
        print(f"  Failed: {failed_count}")
        
        return failed_count == 0
        
    except Exception as e:
        print(f"[ERROR] Batch migration failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def backup_database(database: str) -> bool:
    """
    Create backup of database.
    
    Args:
        database: Database to backup ('supabase' or 'sqlite')
        
    Returns:
        True if successful
    """
    try:
        if database == "supabase":
            print("[INFO] Backing up Supabase database...")
            print("[WARNING] Supabase backups must be created via their web interface")
            return False
        
        elif database == "sqlite":
            import shutil
            
            db_path = os.path.join(project_path, "DB", "volcanic_data.db")
            
            if not os.path.exists(db_path):
                print(f"[ERROR] Database file not found: {db_path}")
                return False
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = os.path.join(
                project_path, "DB", f"volcanic_data_backup_{timestamp}.db"
            )
            
            shutil.copy2(db_path, backup_path)
            print(f"[SUCCESS] Database backed up to: {backup_path}")
            return True
        
        else:
            print(f"[ERROR] Unknown database: {database}")
            return False
            
    except Exception as e:
        print(f"[ERROR] Backup failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def validate_migration(volcano_id: str, source: str, target: str) -> bool:
    """
    Validate that migration was successful.
    
    Args:
        volcano_id: Volcano identifier
        source: Source database
        target: Target database
        
    Returns:
        True if data matches
    """
    try:
        source_db = supabase_db if source == "supabase" else sqlite_db
        target_db = sqlite_db if target == "sqlite" else supabase_db
        
        print(f"[INFO] Validating migration of {volcano_id}...")
        
        # Get source data
        source_eruptions = source_db.eruptions_energy(volcano_id)
        source_count = len(source_eruptions.data)
        
        # Get target data
        target_eruptions = target_db.eruptions_energy(volcano_id)
        target_count = len(target_eruptions.data)
        
        if source_count != target_count:
            print(f"[ERROR] Eruption count mismatch: {source_count} vs {target_count}")
            return False
        
        print(f"[SUCCESS] Migration validated: {target_count} eruptions match")
        return True
        
    except Exception as e:
        print(f"[ERROR] Validation failed: {e}")
        return False


def show_statistics(database: str) -> bool:
    """
    Show database statistics.
    
    Args:
        database: Database to analyze
        
    Returns:
        True if successful
    """
    try:
        db = supabase_db if database == "supabase" else sqlite_db
        
        print(f"\n[INFO] Statistics for {database.upper()} database:")
        print("=" * 60)
        
        # Get eruption counts
        counts = db.eruptions_count()
        
        total_eruptions = 0
        total_volcanoes = len(counts) if isinstance(counts, dict) else 0
        
        if isinstance(counts, dict):
            for volcano_id, data in counts.items():
                total_eruptions += data.get('total_ev', 0)
        
        print(f"Total volcanoes: {total_volcanoes}")
        print(f"Total eruptions: {total_eruptions}")
        
        if isinstance(counts, dict) and total_volcanoes > 0:
            avg_eruptions = total_eruptions / total_volcanoes
            print(f"Average eruptions per volcano: {avg_eruptions:.1f}")
        
        print("=" * 60)
        return True
        
    except Exception as e:
        print(f"[ERROR] Failed to get statistics: {e}")
        return False


# ============================================================================
# COMMAND LINE INTERFACE
# ============================================================================

def main():
    """Parse arguments and execute migration."""
    parser = argparse.ArgumentParser(
        description="Migrate volcanic data between Supabase and SQLite databases",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Migrate single volcano from Supabase to SQLite
  python migrate_db.py --from supabase --to sqlite --volcano-id 211060
  
  # Migrate all volcanoes from SQLite to Supabase
  python migrate_db.py --from sqlite --to supabase --all
  
  # Backup SQLite database
  python migrate_db.py --backup sqlite
  
  # Show database statistics
  python migrate_db.py --stats sqlite
  
  # Validate migration
  python migrate_db.py --validate 211060 --from supabase --to sqlite
        """
    )
    
    # Migration commands
    migrate_group = parser.add_argument_group('migration commands')
    migrate_group.add_argument(
        '--from',
        choices=['supabase', 'sqlite'],
        help='Source database'
    )
    migrate_group.add_argument(
        '--to',
        choices=['supabase', 'sqlite'],
        help='Target database'
    )
    migrate_group.add_argument(
        '--volcano-id',
        help='Migrate single volcano by ID'
    )
    migrate_group.add_argument(
        '--all',
        action='store_true',
        help='Migrate all volcanoes'
    )
    
    # Utility commands
    util_group = parser.add_argument_group('utility commands')
    util_group.add_argument(
        '--backup',
        choices=['supabase', 'sqlite'],
        help='Backup database'
    )
    util_group.add_argument(
        '--stats',
        choices=['supabase', 'sqlite'],
        help='Show database statistics'
    )
    util_group.add_argument(
        '--validate',
        help='Validate migration by volcano ID'
    )
    
    args = parser.parse_args()
    
    # Check that at least one command is provided
    if not any([args.backup, args.stats, args.validate]) and \
       not (args.from_ and args.to and (args.volcano_id or args.all)):
        if not args.validate or not (args.from_ and args.to):
            parser.print_help()
            return 1
    
    # Execute commands
    success = True
    
    if args.backup:
        success = backup_database(args.backup)
    
    elif args.stats:
        success = show_statistics(args.stats)
    
    elif args.validate and args.from_ and args.to:
        success = validate_migration(args.validate, args.from_, args.to)
    
    elif args.from_ and args.to:
        if args.volcano_id:
            success = migrate_volcano_data(args.volcano_id, args.from_, args.to)
        elif args.all:
            success = migrate_all_volcanoes(args.from_, args.to)
        else:
            parser.error("Must specify --volcano-id or --all")
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
