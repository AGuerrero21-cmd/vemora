#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 17 13:26:08 2022

@author: aleja
"""
import os
import argparse
import SmithsonianDB as sm
import Data_Analysis_VEMORA as dt
import SQLite_connection as sqlite
import Read_json as rj
import DB_enrichment as rc
from DB_Tools.pdf_normalization_check import (
    check_pdf_normalization_from_db,
    print_pdf_normalization_report,
)
import sys
#TENERIFE Example
#volcanoid='383010' #La Palma
#volcanoid='383030' #Tenerife
#volcanoid='211060' #Etna
#list_coords= [28.60,27.95,-16.321,-16.998] #NS EW WGS84

def get_volcano_data(volcanoid, project_path):
    """Connect to GVP and fetch volcano and eruption info."""
    try:
        eruptions, volcano = sm.connect_wfs(volcanoid)
        if volcano is None:
            print(f"[ERROR] Volcano with ID {volcanoid} not found in GVP database.")
            return None, None
        else:
            print(f"[INFO] Volcano {volcanoid} found. Adding to DB if not exists...")
            sqlite.add_volcanoSmith(volcano, eruptions)
            print(f"[INFO] Volcano {volcanoid} and its eruptions {len(eruptions)} added to the database.")
            return volcano, eruptions
    except Exception as e:
        print(f"[ERROR] Failed to fetch volcano data: {e}")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(f"Error type: {exc_type.__name__}")
        print(f"Error message: {e}")
        print(f"Line number: {exc_tb.tb_lineno}")
        return None, None


def apply_corrections(volcanoid, csv_path, project_path):
    """Read corrections from CSV files."""
    try:
        rc.read_events(csv_path, str(volcanoid))
        rc.read_rock_type(volcanoid)
        print("[INFO] Corrections applied successfully.")
    except Exception as e:
        print(f"[ERROR] Failed to apply corrections: {e}")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(f"Error type: {exc_type.__name__}")
        print(f"Error message: {e}")
        print(f"Line number: {exc_tb.tb_lineno}")


def compute_energy(volcanoid, project_path):
    """Compute energy for all eruptions in the database."""
    try:
        sqlite.eruptions_energy_all(volcanoid)
        print("[INFO] Energy calculations completed.")
    except Exception as e:
        print(f"[ERROR] Failed to compute energy: {e}")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(f"Error type: {exc_type.__name__}")
        print(f"Error message: {e}")
        print(f"Line number: {exc_tb.tb_lineno}")


def analyze_completeness(volcanoid, project_path):
    """Perform completeness analysis."""
    try:
        complete, years = dt.completeness(volcanoid, project_path)
        print(f"[INFO] Completeness analysis done for {volcanoid}.")
        return complete, years
    except Exception as e:
        print(f"[ERROR] Completeness analysis failed: {e}")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(f"Error type: {exc_type.__name__}")
        print(f"Error message: {e}")
        print(f"Line number: {exc_tb.tb_lineno}")
        return None, None
def compute_temporal(volcanoid, period, complete, project_path):
    """Perform temporal analysis."""
    try:
        temporal=dt.temporal_analysis(volcanoid, period, complete, project_path)
        print(f"[INFO] Temporal analysis done for {volcanoid}.")
        return temporal, period
    except Exception as e:
        print(f"[ERROR] Temporal analysis failed: {e}")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(f"Error type: {exc_type.__name__}")
        print(f"Error message: {e}")
        print(f"Line number: {exc_tb.tb_lineno}")
        return None, None

def compute_pdfs(volcanoid, phi, project_path,temporal):
    """Compute PDFs for volcano eruptions."""
    try:
        dt.pdfs_volcano(volcanoid, phi, project_path,temporal)
        print("[INFO] PDFs computation completed.")
    except Exception as e:
        print(f"[ERROR] PDFs computation failed: {e}")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(f"Error type: {exc_type.__name__}")
        print(f"Error message: {e}")
        print(f"Line number: {exc_tb.tb_lineno}")

def compute_clustering(volcanoid, project_path):
    """Compute clustering for volcano eruptions."""
    try:
        dt.clustering(volcanoid, project_path)
        print("[INFO] Clustering computation completed.")
    except Exception as e:
        print(f"[ERROR] Clustering computation failed: {e}")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(f"Error type: {exc_type.__name__}")
        print(f"Error message: {e}")
        print(f"Line number: {exc_tb.tb_lineno}")

def main():
    parser = argparse.ArgumentParser(
        description="VEMORA: Volcano Eruption Modal and Risk Analysis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic analysis with volcano ID
  python vemora.py --volcano 383030
  
  # Complete analysis with all options
  python vemora.py --volcano 383030 --biblio /path/to/bib.csv --completeness --period 1000 --clustering --phi 4.5
  
  # Interactive mode (no volcano provided)
  python vemora.py
        """
    )
    
    parser.add_argument(
        '--volcano',
        type=str,
        default=None,
        help='Volcano ID to analyze'
    )
    parser.add_argument(
        '--biblio',
        type=str,
        default=None,
        nargs='?',
        const=False,
        help='Path to bibliography/corrections CSV file (optional, defaults to False if not provided)'
    )
    parser.add_argument(
        '--completeness',
        action='store_true',
        help='Perform completeness analysis'
    )
    parser.add_argument(
        '--period',
        type=float,
        default=None,
        help='Time period (in years) for temporal analysis'
    )
    parser.add_argument(
        '--clustering',
        action='store_true',
        help='Compute clustering analysis'
    )
    parser.add_argument(
        '--phi',
        type=float,
        default=None,
        help='Override phi distribution with this value'
    )
    parser.add_argument(
        '--project-path',
        type=str,
        default="/Users/aleja/Documents/PhD/Data/Data_Analysis",
        help='Path to project data directory (default: /Users/aleja/Documents/PhD/Data/Data_Analysis)'
    )
    
    args = parser.parse_args()
    
    # Exit if help was requested (argparse handles this automatically)
    project_path = args.project_path
    
    # Set project path in SQLite module before any database operations
    sqlite.set_project_path(project_path)
    
    # Get volcano ID from args or prompt user
    if args.volcano:
        volcanoid = args.volcano
    else:
    
        raise ValueError("[ERROR] Volcano ID is required.")
            
    if os.path.exists(os.path.join(project_path, volcanoid, "Logs", "analysis.log")):
        os.remove(os.path.join(project_path, volcanoid, "Logs", "analysis.log"))
    
    # Fetch volcano and eruptions
    volcano, eruptions = get_volcano_data(volcanoid, project_path)
    if volcano is None:
        return
    
    # Handle biblio/corrections
    if args.biblio and args.biblio is not False:
        csv_path = args.biblio
        apply_corrections(volcanoid, csv_path, project_path)
    elif not args.volcano:  # Interactive mode only
        apply_corr = input("Apply CSV corrections? (y/n): ").strip().lower()
        if apply_corr == 'y':
            csv_path = input("Enter CSV path: ").strip()
            apply_corrections(volcanoid, csv_path, project_path)
    
    # Handle energy computation (interactive mode only if not via CLI)
    compute_energy(volcanoid, project_path)
            
    temporal=None
    # Completeness analysis
    if args.completeness:
        complete, years = analyze_completeness(volcanoid, project_path)
        if args.period:
            if complete is not None:
                print(f"[INFO] Period analysis: {args.period} years")

                temporal=compute_temporal(volcanoid, args.period, complete, project_path)
    else:
        print(f"[WARNING] Period analysis requested without completeness analysis. Period = {args.period}.")

        temporal=compute_temporal(volcanoid, args.period, None, project_path)
    # PDFs computation
    if args.phi is not None:
        print(f"[INFO] Phi value overridden to {args.phi}")
    print(f"Temporality for PDFs: {temporal}")
    compute_pdfs(volcanoid, phi=args.phi, project_path=project_path, temporal=temporal)

    try:
        normalization_ok, report = check_pdf_normalization_from_db(volcanoid)
        print_pdf_normalization_report(report)
        if not normalization_ok:
            raise RuntimeError(
                "PDF normalization or absolute PDF DB persistence checks failed. "
                "See report above for details."
            )
    except Exception as e:
        print(f"[ERROR] Failed to run PDF normalization DB check: {e}")
        raise
    
    # Clustering analysis
    if args.clustering:
        compute_clustering(volcanoid, project_path)
    elif not args.volcano:
        compute_cl = input("Compute Clustering? (y/n): ").strip().lower()
        if compute_cl == 'y':
            compute_clustering(volcanoid, project_path)
    
    # Handle phi override
    if args.phi is not None:
        dt.log_print(f"Phi value overridden to {args.phi}", volcanoid, "phi_override", project_path) 
    
    print("[INFO] Process completed successfully.")


if __name__ == "__main__":
    main()