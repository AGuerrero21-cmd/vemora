#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generate a complete volcano report from SQLite database.

Usage:
  python DB_Tools/generate_volcano_report.py --volcano 383030
  python DB_Tools/generate_volcano_report.py --volcano 383030 --project-path /path/to/data
  python DB_Tools/generate_volcano_report.py --volcano 383030 --output-dir /tmp/reports
"""

import os
import sys
import json
import argparse
import sqlite3
from datetime import datetime
from typing import Any, Dict, List

# Ensure project root is importable when running from DB_Tools/
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import SQLite_connection as db


def _decode_blob(value: Any) -> Any:
    """Decode JSON blobs when possible; otherwise return the original value."""
    if value is None:
        return None

    if isinstance(value, (bytes, bytearray)):
        try:
            return json.loads(value.decode("utf-8"))
        except Exception:
            return value.decode("utf-8", errors="replace")

    return value


def _clean_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Convert sqlite row dict into JSON-serializable dict."""
    cleaned: Dict[str, Any] = {}
    for key, value in record.items():
        cleaned[key] = _decode_blob(value)
    return cleaned


def _fetch_all_eruptions(volcano_id: str) -> List[Dict[str, Any]]:
    """Fetch all eruption records for a volcano, including nullable fields."""
    conn = sqlite3.connect(db.get_db_path())
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT *
        FROM eruptions
        WHERE volcano = ?
        ORDER BY year ASC, month ASC, day ASC, _id ASC
        """,
        (volcano_id,)
    )

    rows = cursor.fetchall()
    conn.close()

    return [_clean_record(dict(row)) for row in rows]


def build_volcano_report(volcano_id: str) -> Dict[str, Any]:
    """Build full volcano report from DB."""
    volcano_result = db.volcano_data(volcano_id)
    if not volcano_result.data:
        raise ValueError(f"Volcano '{volcano_id}' not found in database")

    volcano_data = volcano_result.data[0]
    eruptions = _fetch_all_eruptions(volcano_id)
    epdfs = db.get_all_epdfs(volcano_id).data

    # Basic summary stats
    years = [e.get("year") for e in eruptions if isinstance(e.get("year"), int)]
    energies = [e.get("energy") for e in eruptions if isinstance(e.get("energy"), (int, float))]

    summary = {
        "total_eruptions": len(eruptions),
        "years_min": min(years) if years else None,
        "years_max": max(years) if years else None,
        "with_energy": len(energies),
        "epdf_records": len(epdfs),
        "epdf_types": sorted(list({str(item.get('type')) for item in epdfs if item.get('type') is not None}))
    }

    report = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "volcano_id": volcano_id,
        "summary": summary,
        "volcano": volcano_data,
        "eruptions": eruptions,
        "epdfs": epdfs
    }

    return report


def save_report(report: Dict[str, Any], output_dir: str) -> Dict[str, str]:
    """Save report to JSON and TXT files."""
    volcano_id = report["volcano_id"]
    os.makedirs(output_dir, exist_ok=True)

    json_path = os.path.join(output_dir, f"volcano_report_{volcano_id}.json")
    txt_path = os.path.join(output_dir, f"volcano_report_{volcano_id}.txt")

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("=" * 70 + "\n")
        f.write(f"VOLCANO DATABASE REPORT - {volcano_id}\n")
        f.write("=" * 70 + "\n\n")

        volcano = report.get("volcano", {})
        summary = report.get("summary", {})

        f.write("[VOLCANO]\n")
        for key, value in volcano.items():
            f.write(f"- {key}: {value}\n")

        f.write("\n[SUMMARY]\n")
        for key, value in summary.items():
            f.write(f"- {key}: {value}\n")

        f.write("\n[ERUPTIONS]\n")
        f.write(f"Total records: {len(report.get('eruptions', []))}\n")

        f.write("\n[EPDFS]\n")
        epdfs = report.get("epdfs", [])
        f.write(f"Total records: {len(epdfs)}\n")
        for item in epdfs:
            f.write(
                f"- type={item.get('type')} distribution={item.get('distribution')} "
                f"updated_at={item.get('updated_at')}\n"
            )

    return {"json": json_path, "txt": txt_path}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate complete volcano report from SQLite DB"
    )
    parser.add_argument("--volcano", required=True, help="Volcano ID")
    parser.add_argument(
        "--project-path",
        default="/Users/aleja/Documents/PhD/Data/Data_Analysis",
        help="Path containing DB/volcanic_data.db"
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Output directory for report files (default: <project_path>/<volcano_id>/Reports)"
    )

    args = parser.parse_args()

    try:
        db.set_project_path(args.project_path)

        report = build_volcano_report(args.volcano)

        output_dir = args.output_dir
        if not output_dir:
            output_dir = os.path.join(args.project_path, args.volcano, "Reports")

        paths = save_report(report, output_dir)

        print(f"[INFO] Volcano report generated for {args.volcano}")
        print(f"[INFO] JSON: {paths['json']}")
        print(f"[INFO] TXT : {paths['txt']}")
        return 0

    except ValueError as e:
        print(f"[WARNING] {e}")
        return 1
    except Exception as e:
        exc_type, _, exc_tb = sys.exc_info()
        print(f"[ERROR] Failed to generate report: {e}")
        print(f"Error type: {exc_type.__name__}")
        print(f"Line number: {exc_tb.tb_lineno}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
