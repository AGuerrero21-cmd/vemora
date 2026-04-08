#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PDF normalization checks for EPDF records stored in SQLite.

This module validates that areas under stored PDFs are approximately 1.
It is intended to be called from `vemora.py` after PDF computation.
"""

from typing import Dict, Optional, Tuple

import numpy as np

import SQLite_connection as sqlite


DEFAULT_TOLERANCE = 1e-2


def _to_float_array(values) -> Optional[np.ndarray]:
    """Convert iterable values to 1D float array, returning None if invalid."""
    try:
        arr = np.array(values, dtype=float).ravel()
        if arr.size == 0 or not np.all(np.isfinite(arr)):
            return None
        return arr
    except Exception:
        return None


def _compute_area(x_values, y_values) -> Optional[float]:
    """Compute numerical integral using trapezoidal rule."""
    x = _to_float_array(x_values)
    y = _to_float_array(y_values)

    if x is None or y is None:
        return None
    if len(x) != len(y) or len(x) < 2:
        return None

    order = np.argsort(x)
    x_sorted = x[order]
    y_sorted = y[order]

    return float(np.trapz(y_sorted, x_sorted))


def check_pdf_normalization_from_db(volcano_id: str,
                                    tolerance: float = DEFAULT_TOLERANCE) -> Tuple[bool, Dict]:
    """
    Validate normalization of total, lava and tephra PDFs stored in SQLite.

    Args:
        volcano_id: Volcano identifier
        tolerance: Allowed absolute error against area=1

    Returns:
        Tuple (is_ok, report_dict)
    """
    report = {
        'volcano_id': volcano_id,
        'tolerance': float(tolerance),
        'checks': {},
        'absolute_pdf_records_present': {}
    }

    total_record = sqlite.get_epdf(volcano_id, 'E_total')
    marginal_record = sqlite.get_epdf(volcano_id, 'marginal')

    if total_record is None:
        report['checks']['total_pdf'] = {
            'ok': False,
            'error': "Missing EPDF record 'E_total'"
        }
    else:
        params = total_record.get('parameters') or {}
        x = params.get('bin_centers')
        # Prefer fitted total PDF when present, else empirical
        y = params.get('pdf_fitted')
        if y is None:
            y = params.get('pdf_empirical')

        area = _compute_area(x, y)
        if area is None:
            report['checks']['total_pdf'] = {
                'ok': False,
                'error': 'Could not compute area (invalid or mismatched arrays)'
            }
        else:
            ok = abs(area - 1.0) <= tolerance
            report['checks']['total_pdf'] = {
                'ok': ok,
                'area': area,
                'target': 1.0,
                'abs_error': abs(area - 1.0)
            }

    if marginal_record is None:
        report['checks']['lava_pdf'] = {
            'ok': False,
            'error': "Missing EPDF record 'marginal'"
        }
        report['checks']['tephra_pdf'] = {
            'ok': False,
            'error': "Missing EPDF record 'marginal'"
        }
    else:
        params = marginal_record.get('parameters') or {}
        x = params.get('bin_centers')

        lava_area = _compute_area(x, params.get('pdf_lava'))
        if lava_area is None:
            report['checks']['lava_pdf'] = {
                'ok': False,
                'error': 'Could not compute area (invalid or mismatched arrays)'
            }
        else:
            lava_ok = abs(lava_area - 1.0) <= tolerance
            report['checks']['lava_pdf'] = {
                'ok': lava_ok,
                'area': lava_area,
                'target': 1.0,
                'abs_error': abs(lava_area - 1.0)
            }

        tephra_area = _compute_area(x, params.get('pdf_tephra'))
        if tephra_area is None:
            report['checks']['tephra_pdf'] = {
                'ok': False,
                'error': 'Could not compute area (invalid or mismatched arrays)'
            }
        else:
            tephra_ok = abs(tephra_area - 1.0) <= tolerance
            report['checks']['tephra_pdf'] = {
                'ok': tephra_ok,
                'area': tephra_area,
                'target': 1.0,
                'abs_error': abs(tephra_area - 1.0)
            }

    # Ensure absolute PDFs stay persisted in DB
    for epdf_type in ('absolute_total', 'absolute_lava', 'absolute_tephra'):
        report['absolute_pdf_records_present'][epdf_type] = sqlite.get_epdf(volcano_id, epdf_type) is not None

    checks_ok = all(item.get('ok', False) for item in report['checks'].values()) if report['checks'] else False
    absolute_ok = all(report['absolute_pdf_records_present'].values())

    return checks_ok and absolute_ok, report


def print_pdf_normalization_report(report: Dict) -> None:
    """Pretty-print normalization report."""
    volcano_id = report.get('volcano_id', 'unknown')
    tol = report.get('tolerance', DEFAULT_TOLERANCE)

    print(f"[INFO] PDF normalization check for volcano {volcano_id} (tolerance={tol})")

    checks = report.get('checks', {})
    for check_name in ('total_pdf', 'lava_pdf', 'tephra_pdf'):
        info = checks.get(check_name, {})
        if info.get('ok'):
            print(
                f"  [PASS] {check_name}: area={info.get('area'):.6f}, "
                f"|error|={info.get('abs_error'):.6e}"
            )
        else:
            error_text = info.get('error', 'Area outside tolerance')
            if 'area' in info:
                print(
                    f"  [FAIL] {check_name}: area={info.get('area'):.6f}, "
                    f"|error|={info.get('abs_error'):.6e}"
                )
            else:
                print(f"  [FAIL] {check_name}: {error_text}")


