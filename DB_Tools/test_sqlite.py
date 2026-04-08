#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQLite Database Test and Verification Script

Tests all SQLite database functions to ensure proper installation and operation.

Usage:
    python test_sqlite.py              # Run all tests
    python test_sqlite.py --quick      # Run basic tests only
    python test_sqlite.py --verbose    # Show detailed output

Created: 2026-02-12
@author: aleja
"""

import sys
import os
import argparse
from typing import Dict, List, Tuple
import traceback

try:
    import SQLite_connection as db
    from vemora import project_path
except ImportError as e:
    print(f"[ERROR] Failed to import required modules: {e}")
    print("Make sure SQLite_connection.py is in the same directory as this script")
    sys.exit(1)


# ============================================================================
# TEST UTILITIES
# ============================================================================

class TestResult:
    """Container for test results."""
    
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.errors = []
    
    def add_pass(self, test_name: str) -> None:
        """Record passed test."""
        self.passed += 1
        print(f"  ✓ {test_name}")
    
    def add_fail(self, test_name: str, error: str) -> None:
        """Record failed test."""
        self.failed += 1
        print(f"  ✗ {test_name}: {error}")
        self.errors.append((test_name, error))
    
    def summary(self) -> str:
        """Get test summary."""
        total = self.passed + self.failed
        return f"\n{'='*60}\nTest Results: {self.passed}/{total} passed\n{'='*60}"
    
    def success(self) -> bool:
        """Check if all tests passed."""
        return self.failed == 0


# ============================================================================
# TEST FUNCTIONS
# ============================================================================

def test_database_initialization(results: TestResult, verbose: bool = False) -> None:
    """Test that database is properly initialized."""
    try:
        db_path = os.path.join(project_path, "DB", "volcanic_data.db")
        
        if os.path.exists(db_path):
            results.add_pass("Database file exists")
            if verbose:
                size_mb = os.path.getsize(db_path) / (1024 * 1024)
                print(f"    Database size: {size_mb:.2f} MB")
        else:
            results.add_fail("Database file exists", f"Not found at {db_path}")
        
        # Test database connection
        import sqlite3
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check tables exist
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name IN ('volcanoes', 'eruptions')
        """)
        tables = [row[0] for row in cursor.fetchall()]
        
        if 'volcanoes' in tables and 'eruptions' in tables:
            results.add_pass("Database tables created")
        else:
            results.add_fail("Database tables created", f"Found: {tables}")
        
        conn.close()
        
    except Exception as e:
        results.add_fail("Database initialization", str(e))
        if verbose:
            traceback.print_exc()


def test_volcano_operations(results: TestResult, verbose: bool = False) -> None:
    """Test volcano add/retrieve operations."""
    try:
        # Create test volcano
        test_volcano = {
            "volcano_id": "TEST_001",
            "name": "Test Volcano",
            "country": "Test Country",
            "latitude": 10.5,
            "longitude": -20.3,
            "elevation": 3000.0,
            "general_type": "Stratovolcano",
            "rock_type": "Basalt"
        }
        
        # Add volcano
        try:
            db.add_volcanoSmith(test_volcano, [])
            results.add_pass("Add volcano")
        except Exception as e:
            results.add_fail("Add volcano", str(e))
            return
        
        # Retrieve volcano
        try:
            result = db.volcano_data("TEST_001")
            if result.data and result.data[0]['name'] == "Test Volcano":
                results.add_pass("Retrieve volcano data")
                if verbose:
                    print(f"    Retrieved: {result.data[0]['name']}")
            else:
                results.add_fail("Retrieve volcano data", "Data mismatch")
        except Exception as e:
            results.add_fail("Retrieve volcano data", str(e))
        
        # Cleanup
        import sqlite3
        db_path = os.path.join(project_path, "DB", "volcanic_data.db")
        conn = sqlite3.connect(db_path)
        conn.execute("DELETE FROM volcanoes WHERE volcano_id = ?", ("TEST_001",))
        conn.commit()
        conn.close()
        
    except Exception as e:
        results.add_fail("Volcano operations", str(e))
        if verbose:
            traceback.print_exc()


def test_eruption_operations(results: TestResult, verbose: bool = False) -> None:
    """Test eruption add/update/retrieve operations."""
    try:
        # Setup test volcano first
        test_volcano = {
            "volcano_id": "TEST_ERU_001",
            "name": "Test Eruption Volcano"
        }
        test_eruption = {
            "_id": "TEST_ERP_001",
            "volcano": "TEST_ERU_001",
            "year": 2020,
            "month": 5,
            "day": 15,
            "VEI": 3,
            "energy": 1e18
        }
        
        # Add volcano and eruption
        try:
            db.add_volcanoSmith(test_volcano, [test_eruption])
            results.add_pass("Add eruption")
        except Exception as e:
            results.add_fail("Add eruption", str(e))
            return
        
        # Query eruption
        try:
            eruption_id = db.query_eruption_ym("TEST_ERU_001", 2020, 5)
            if eruption_id:
                results.add_pass("Query eruption by Y/M")
                if verbose:
                    print(f"    Found eruption: {eruption_id}")
            else:
                results.add_fail("Query eruption by Y/M", "Not found")
        except Exception as e:
            results.add_fail("Query eruption by Y/M", str(e))
        
        # Update eruption
        try:
            db.update_eruption("TEST_ERP_001", "VEI", 4)
            
            # Verify update
            result = db.eruptions_energy("TEST_ERU_001")
            if result.data and result.data[0].get('VEI') == 4:
                results.add_pass("Update eruption")
            else:
                results.add_fail("Update eruption", "Update not reflected")
        except Exception as e:
            results.add_fail("Update eruption", str(e))
        
        # Cleanup
        import sqlite3
        db_path = os.path.join(project_path, "DB", "volcanic_data.db")
        conn = sqlite3.connect(db_path)
        conn.execute("DELETE FROM eruptions WHERE volcano = ?", ("TEST_ERU_001",))
        conn.execute("DELETE FROM volcanoes WHERE volcano_id = ?", ("TEST_ERU_001",))
        conn.commit()
        conn.close()
        
    except Exception as e:
        results.add_fail("Eruption operations", str(e))
        if verbose:
            traceback.print_exc()


def test_energy_calculation(results: TestResult, verbose: bool = False) -> None:
    """Test energy calculation functionality."""
    try:
        # Setup test data
        test_volcano = {
            "volcano_id": "TEST_ENE_001",
            "name": "Test Energy Volcano",
            "temperature": 1200.0,
            "density": 2500.0,
            "specific_heat": 1046.7
        }
        test_eruption = {
            "_id": "TEST_ENP_001",
            "volcano": "TEST_ENE_001",
            "year": 2020,
            "month": 5,
            "volume": [0.5, 0.3],  # [pyroclastic, lava] in km³
            "column_height": 20.0
        }
        
        try:
            db.add_volcanoSmith(test_volcano, [test_eruption])
            results.add_pass("Add eruption with volume data")
        except Exception as e:
            results.add_fail("Add eruption with volume data", str(e))
            return
        
        # Calculate energies
        try:
            db.eruptions_energy_all()
            
            # Verify energies were calculated
            result = db.eruptions_energy("TEST_ENE_001")
            if result.data and result.data[0].get('energy'):
                energy = result.data[0]['energy']
                results.add_pass("Calculate eruption energy")
                if verbose:
                    print(f"    Calculated energy: {energy:.2e} J")
            else:
                results.add_fail("Calculate eruption energy", "Energy not calculated")
        except Exception as e:
            results.add_fail("Calculate eruption energy", str(e))
        
        # Cleanup
        import sqlite3
        db_path = os.path.join(project_path, "DB", "volcanic_data.db")
        conn = sqlite3.connect(db_path)
        conn.execute("DELETE FROM eruptions WHERE volcano = ?", ("TEST_ENE_001",))
        conn.execute("DELETE FROM volcanoes WHERE volcano_id = ?", ("TEST_ENE_001",))
        conn.commit()
        conn.close()
        
    except Exception as e:
        results.add_fail("Energy calculation", str(e))
        if verbose:
            traceback.print_exc()


def test_rock_properties(results: TestResult, verbose: bool = False) -> None:
    """Test rock property retrieval."""
    try:
        # Test with common rock types
        rock_types = ["Basalt", "Andesite", "Dacite"]
        found = 0
        
        for rock_type in rock_types:
            try:
                props = db.get_rock_properties(rock_type)
                if "error" not in props and props.get("mean_temperature_C"):
                    found += 1
                    if verbose:
                        print(f"    Found {rock_type}: {props['mean_temperature_C']:.0f}°C")
            except Exception:
                pass
        
        if found > 0:
            results.add_pass(f"Rock properties ({found} types found)")
        else:
            results.add_fail("Rock properties", "No rock types found")
        
    except Exception as e:
        results.add_fail("Rock properties", str(e))
        if verbose:
            traceback.print_exc()


def test_result_wrapper(results: TestResult, verbose: bool = False) -> None:
    """Test Result wrapper class."""
    try:
        # Create result object
        test_data = [
            {"id": 1, "name": "test1"},
            {"id": 2, "name": "test2"}
        ]
        result = db.Result(test_data)
        
        # Test length
        if len(result) == 2:
            results.add_pass("Result wrapper length")
        else:
            results.add_fail("Result wrapper length", f"Got {len(result)}, expected 2")
        
        # Test data access
        if result.data[0]['name'] == "test1":
            results.add_pass("Result wrapper data access")
        else:
            results.add_fail("Result wrapper data access", "Data mismatch")
        
    except Exception as e:
        results.add_fail("Result wrapper", str(e))
        if verbose:
            traceback.print_exc()


def test_database_permissions(results: TestResult, verbose: bool = False) -> None:
    """Test database directory and file permissions."""
    try:
        db_dir = os.path.join(project_path, "DB")
        db_path = os.path.join(db_dir, "volcanic_data.db")
        
        # Check directory
        if os.path.isdir(db_dir):
            results.add_pass("Database directory exists")
            if verbose:
                print(f"    Path: {db_dir}")
        else:
            results.add_fail("Database directory exists", f"Not found: {db_dir}")
            return
        
        # Check directory is writable
        if os.access(db_dir, os.W_OK):
            results.add_pass("Database directory is writable")
        else:
            results.add_fail("Database directory is writable", "No write permission")
        
        # Check file is readable/writable
        if os.path.exists(db_path):
            if os.access(db_path, os.R_OK) and os.access(db_path, os.W_OK):
                results.add_pass("Database file is readable/writable")
            else:
                results.add_fail("Database file permissions", "Insufficient permissions")
        
    except Exception as e:
        results.add_fail("Database permissions", str(e))
        if verbose:
            traceback.print_exc()


# ============================================================================
# MAIN TEST EXECUTION
# ============================================================================

def run_all_tests(verbose: bool = False) -> bool:
    """Run all tests."""
    print("\n" + "="*60)
    print("SQLite Database Tests")
    print("="*60 + "\n")
    
    results = TestResult()
    
    print("Testing database initialization...")
    test_database_initialization(results, verbose)
    
    print("\nTesting database permissions...")
    test_database_permissions(results, verbose)
    
    print("\nTesting Result wrapper...")
    test_result_wrapper(results, verbose)
    
    print("\nTesting volcano operations...")
    test_volcano_operations(results, verbose)
    
    print("\nTesting eruption operations...")
    test_eruption_operations(results, verbose)
    
    print("\nTesting rock properties...")
    test_rock_properties(results, verbose)
    
    print("\nTesting energy calculations...")
    test_energy_calculation(results, verbose)
    
    print(results.summary())
    
    if results.errors:
        print("\nFailed Tests:")
        for test_name, error in results.errors:
            print(f"  - {test_name}: {error}")
    
    return results.success()


def run_quick_tests(verbose: bool = False) -> bool:
    """Run quick tests only."""
    print("\n" + "="*60)
    print("SQLite Database Quick Tests")
    print("="*60 + "\n")
    
    results = TestResult()
    
    print("Testing database initialization...")
    test_database_initialization(results, verbose)
    
    print("\nTesting database permissions...")
    test_database_permissions(results, verbose)
    
    print(results.summary())
    
    return results.success()


def main():
    """Parse arguments and run tests."""
    parser = argparse.ArgumentParser(
        description="Test SQLite database installation and functionality"
    )
    parser.add_argument(
        '--quick',
        action='store_true',
        help='Run only basic tests'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Show detailed output'
    )
    
    args = parser.parse_args()
    
    if args.quick:
        success = run_quick_tests(args.verbose)
    else:
        success = run_all_tests(args.verbose)
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
