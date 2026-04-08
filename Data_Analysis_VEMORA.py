#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Volcanic Eruption Data Analysis Module

This module provides comprehensive analysis of volcanic eruption data including:
- Completeness and trend analysis
- Probability distribution fitting
- Energy distribution analysis
- Temporal clustering
- Weibull parameter estimation

Created on Thu Mar 17 17:06:04 2022
@author: Alejandra Guerrero López
"""

import sys
import os
from datetime import datetime
from typing import List, Tuple, Dict, Optional, Any
import warnings

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import math

from scipy import stats
from scipy.optimize import curve_fit
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, silhouette_score
from sklearn.cluster import AgglomerativeClustering, KMeans
from kneed import KneeLocator
from tslearn.clustering import TimeSeriesKMeans
import ruptures as rpt

# Custom imports
import Read_json as rj
import SQLite_connection as mdb


# ============================================================================
# CONFIGURATION & CONSTANTS
# ============================================================================

ENERGY_BIN_LIMITS = [1e5, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 
                     5e15, 5e15, 1e16, 5e16, 1e17, 5e17, 1e18, 5e18, 1e19,1e20, 5e20, 1e21, 5e21, 1e22, 5e22, 1e23, 5e23, 1e24, 5e24, 1e25, 5e25, 1e26, 5e26, 1e27, 5e27, 1e28, 5e28, 1e29, 5e29, 1e30, 5e30, 1e31, 5e31, 1e32]

MIN_ERUPTIONS_COMPLETENESS = 10
MIN_ERUPTIONS_PDF = 20
MIN_PHI_DATA_POINTS = 10

BOOTSTRAP_ITERATIONS = 1000
EXPONENTIAL_FIT_THRESHOLD = 0.7
PVALUE_THRESHOLD = 0.05
NEWTON_EPSILON = 0.00001
NEWTON_MAX_ITERATIONS = 100000000

# Distribution names for PDF fitting
DISTRIBUTION_NAMES = [
    'exponweib', 'chi', 'beta', 'norm', 'lognorm', 'gumbel_r',
    'logistic', 'gamma', 'weibull_min', 'weibull_max', 'expon', 'rayleigh',
    'pareto', 'lomax', 'fisk', 'cauchy', 't', 'laplace', 'uniform',
    'genextreme', 'genpareto', 'invgauss', 'halfnorm', 'loggamma',
    'fatiguelife', 'triang', 'truncnorm', 'arcsine', 'powerlaw',
    'gumbel_l', 'invweibull', 'gengamma'
]

DISTRIBUTION_LABELS = [
    'Exponentiated Weibull', 'Chi', 'Beta', 'Normal', 'LogNormal', 'Gumbel Right',
    'Logistic', 'Gamma', 'Weibull Min', 'Weibull Max', 'Exponential', 'Rayleigh',
    'Pareto', 'Lomax (Pareto II)', 'Log-Logistic', 'Cauchy', "Student's t",
    'Laplace', 'Uniform', 'Generalized Extreme Value', 'Generalized Pareto',
    'Inverse Gaussian', 'Half-Normal', 'Log-Gamma', 'Birnbaum–Saunders',
    'Triangular', 'Truncated Normal', 'Arcsine', 'Power Law', 'Gumbel Left',
    'Inverse Weibull', 'Generalized Gamma'
]

# Distributions suitable for bounded data [0, 1] like phi = E_lava / E_total
PHI_DISTRIBUTION_NAMES = ['beta', 'truncnorm', 'arcsine', 'uniform']
PHI_DISTRIBUTION_LABELS = ['Beta', 'Truncated Normal', 'Arcsine', 'Uniform']


# ============================================================================
# VALIDATION & ERROR HANDLING UTILITIES
# ============================================================================

class ValidationError(Exception):
    """Custom exception for validation errors."""
    pass


class DataError(Exception):
    """Custom exception for data-related errors."""
    pass


def validate_volcano_id(volcano_id: str) -> None:
    """
    Validate volcano ID format.
    
    Args:
        volcano_id: Volcano identifier string
        
    Raises:
        ValidationError: If volcano_id is invalid
    """
    if not isinstance(volcano_id, str):
        raise ValidationError(f"volcano_id must be a string, got {type(volcano_id)}")
    if len(volcano_id) == 0:
        raise ValidationError("volcano_id cannot be empty")


def validate_array_input(data: Any, name: str, min_length: int = 0) -> np.ndarray:
    """
    Validate and convert input to numpy array.
    
    Args:
        data: Input data to validate
        name: Name of the variable (for error messages)
        min_length: Minimum required array length
        
    Returns:
        Validated numpy array
        
    Raises:
        ValidationError: If data is invalid
    """
    try:
        arr = np.array(data, dtype=float)
    except (TypeError, ValueError) as e:
        raise ValidationError(f"Cannot convert {name} to float array: {e}")
    
    if len(arr) < min_length:
        raise ValidationError(
            f"{name} has length {len(arr)}, minimum required: {min_length}"
        )
    
    return arr


def validate_positive_values(data: np.ndarray, name: str, allow_zero: bool = False) -> None:
    """
    Validate that array contains only positive values.
    
    Args:
        data: Array to validate
        name: Name of the variable (for error messages)
        allow_zero: Whether to allow zero values
        
    Raises:
        ValidationError: If array contains invalid values
    """
    if allow_zero:
        if np.any(data < 0):
            raise ValidationError(f"{name} contains negative values")
    else:
        if np.any(data <= 0):
            raise ValidationError(f"{name} contains non-positive values")


def safe_log10(data: np.ndarray, default_min: float = 1e-5) -> np.ndarray:
    """
    Safely compute log10, replacing invalid values.
    
    Args:
        data: Input array
        default_min: Default value for zero/negative entries in log space
        
    Returns:
        Array with log10 values, invalid entries replaced
    """
    result = np.full_like(data, default_min)
    mask = data > 0
    result[mask] = np.log10(data[mask])
    return result


def to_serializable(value: Any) -> Any:
    """
    Convert numpy/scipy-compatible values into JSON-serializable Python types.

    Args:
        value: Any Python object

    Returns:
        JSON-serializable representation of value
    """
    if isinstance(value, np.ndarray):
        return [to_serializable(v) for v in value.tolist()]
    if isinstance(value, (np.floating, np.integer)):
        return value.item()
    if isinstance(value, (list, tuple)):
        return [to_serializable(v) for v in value]
    if isinstance(value, dict):
        return {k: to_serializable(v) for k, v in value.items()}
    return value


# ============================================================================
# LOGGING & FILE UTILITIES
# ============================================================================

def log_print(message: str, volcano_id: str, section: str, 
              project_path: str, filename: str = "analysis.log") -> None:
    """
    Print message to console and log file with timestamp.
    
    Args:
        message: Message to log
        volcano_id: Volcano identifier
        section: Analysis section name
        project_path: Path to project data directory
        filename: Log filename
        
    Raises:
        IOError: If log file cannot be written
    """
    if not isinstance(message, str):
        message = str(message)
    
    validate_volcano_id(volcano_id)
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_msg = f"[{timestamp}] [{section}] {message}"
    
    print(log_msg)
    
    try:
        directory = os.path.join(project_path, volcano_id, "Logs")
        os.makedirs(directory, exist_ok=True)
        
        filepath = os.path.join(directory, filename)
        with open(filepath, "a+") as f:
            f.write(log_msg + "\n")
    except IOError as e:
        print(f"WARNING: Could not write to log file: {e}")


def create_output_directory(volcano_id: str, subdirectory: str, project_path: str) -> str:
    """
    Create output directory structure.
    
    Args:
        volcano_id: Volcano identifier
        subdirectory: Subdirectory name (e.g., 'PDFs', 'Clustering')
        project_path: Path to project data directory
        
    Returns:
        Full path to created directory
        
    Raises:
        ValidationError: If inputs are invalid
        IOError: If directory cannot be created
    """
    validate_volcano_id(volcano_id)
    
    if not isinstance(subdirectory, str) or len(subdirectory) == 0:
        raise ValidationError("subdirectory must be non-empty string")
    
    try:
        directory = os.path.join(project_path, volcano_id, subdirectory)
        os.makedirs(directory, exist_ok=True)
        return directory
    except OSError as e:
        raise IOError(f"Cannot create directory {directory}: {e}")


# ============================================================================
# PLOTTING UTILITIES
# ============================================================================

def random_color() -> Tuple[float, float, float]:
    """
    Generate random RGB color tuple.
    
    Returns:
        Tuple of (R, G, B) values between 0 and 1
    """
    return (np.random.random(), np.random.random(), np.random.random())


def xplot(x: np.ndarray, xlabel: str, ylabel: str) -> None:
    """
    Plot single array as scatter plot.
    
    Args:
        x: Data array to plot
        xlabel: X-axis label
        ylabel: Y-axis label
    """
    try:
        x = validate_array_input(x, "x")
        plt.figure()
        plt.plot(x, 'ro')
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.show()
    except (ValidationError, Exception) as e:
        warnings.warn(f"Error in xplot: {e}")


def pplot(x: np.ndarray, y: np.ndarray, xlabel: str, ylabel: str, 
          volcano_id: str, title: str, save: bool, name: str, project_path: str) -> None:
    """
    Plot x vs y data with optional save.
    
    Args:
        x: X-axis data
        y: Y-axis data
        xlabel: X-axis label
        ylabel: Y-axis label
        volcano_id: Volcano identifier
        title: Plot title
        save: Whether to save figure
        name: Filename (without extension)
        project_path: Path to project data directory
        
    Raises:
        ValidationError: If inputs are invalid
    """
    try:
        x = validate_array_input(x, "x")
        y = validate_array_input(y, "y")
        validate_volcano_id(volcano_id)
        
        if len(x) != len(y):
            raise ValidationError(f"x and y have different lengths: {len(x)} vs {len(y)}")
        
        plt.figure()
        plt.plot(x, y, 'rx')
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.title(title)
        
        if save:
            directory = create_output_directory(volcano_id, "PDFs", project_path)
            filepath = os.path.join(directory, f"{volcano_id}_{name}.png")
            plt.savefig(filepath, dpi=300, bbox_inches='tight')
            log_print(f"Figure saved: {filepath}", volcano_id, "Plotting", project_path)
        
        plt.show()
    except (ValidationError, IOError) as e:
        log_print(f"Error in pplot: {e}", volcano_id, "Plotting", project_path)
        raise


def barplot(x: np.ndarray, y: np.ndarray, xlabel: str, ylabel: str) -> None:
    """
    Create bar plot.
    
    Args:
        x: X-axis data
        y: Y-axis data
        xlabel: X-axis label
        ylabel: Y-axis label
    """
    try:
        x = validate_array_input(x, "x")
        y = validate_array_input(y, "y")
        
        plt.figure()
        plt.bar(x, y, color="grey", width=1, alpha=1)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
    except ValidationError as e:
        warnings.warn(f"Error in barplot: {e}")


def plt_bar_dis(freq: np.ndarray, lim: np.ndarray, title: str, 
                results: List[np.ndarray], labels: List[str], 
                limi: np.ndarray, volcano_id: str, save: bool, name: str, project_path: str) -> None:
    """
    Plot bar distribution with fitted curves.
    
    Args:
        freq: Frequency data
        lim: Bin limits
        title: Plot title
        results: List of fitted distribution results
        labels: Labels for distributions
        limi: Interpolation points
        volcano_id: Volcano identifier
        save: Whether to save figure
        name: Filename (without extension)
        project_path: Path to project data directory
        
    Raises:
        ValidationError: If inputs are invalid
    """
    try:
        freq = validate_array_input(freq, "freq")
        lim = validate_array_input(lim, "lim")
        limi = validate_array_input(limi, "limi")
        validate_volcano_id(volcano_id)
        
        if len(results) != len(labels):
            raise ValidationError("results and labels have different lengths")
        
        barplot(lim, np.multiply(freq, 100), "Energy (log10)", "Frequency (%)")
        plt.title(title)
        
        for distribution, label in zip(results, labels):
            distribution = validate_array_input(distribution, "distribution")
            plt.plot(limi, np.multiply(distribution, 100), c=random_color(), label=label)
        
        plt.legend(loc='best', frameon=False)
        
        if save:
            directory = create_output_directory(volcano_id, "PDFs", project_path)
            filepath = os.path.join(directory, f"{volcano_id}_{name}.png")
            plt.savefig(filepath, dpi=300, bbox_inches='tight')
            log_print(f"Figure saved: {filepath}", volcano_id, "Plotting", project_path)
        
        plt.show()
        plt.close()
    except (ValidationError, IOError) as e:
        log_print(f"Error in plt_bar_dis: {e}", volcano_id, "Plotting", project_path)
        raise


# ============================================================================
# DATA UTILITIES
# ============================================================================

def cumulativesum(x: List[Any]) -> np.ndarray:
    """
    Sort array in ascending order.
    
    Args:
        x: Input list or array
        
    Returns:
        Sorted numpy array
    """
    return np.array(sorted(x))


def search_values_change(index: np.ndarray, sort_year: np.ndarray) -> List[Any]:
    """
    Extract year values at specified change point indices.
    
    Args:
        index: Array of indices for change points
        sort_year: Sorted years array
        
    Returns:
        Years corresponding to change point indices
    """
    try:
        index = validate_array_input(index, "index", min_length=1)
        sort_year = validate_array_input(sort_year, "sort_year", min_length=1)
        
        return [sort_year[int(i) - 1] for i in index[:-1] if 0 < int(i) <= len(sort_year)]
    except (ValidationError, IndexError) as e:
        raise DataError(f"Error extracting change point values: {e}")


# ============================================================================
# TIME SERIES ANALYSIS - COMPLETENESS
# ============================================================================

def linear_analysis(list_eruptions: List[Dict], cumsum: List[int], 
                   volcano_id: str, project_path: str) -> Tuple[float, float, float, float]:
    """
    Perform linear and exponential regression on eruption time series.
    
    Fits both linear and exponential models to eruption data and computes
    Mean Squared Error and R² values for model comparison.
    
    Args:
        list_eruptions: List of eruption dictionaries with 'year' key
        cumsum: Cumulative sum array
        volcano_id: Volcano identifier for logging and output
        project_path: Path to project data directory
        
    Returns:
        Tuple of (mse_linear, mse_exponential, r2_linear, r2_exponential)
        
    Raises:
        ValidationError: If inputs are invalid
        DataError: If data processing fails
    """
    section = "completeness_tendency_behavior"
    
    try:
        if not isinstance(list_eruptions, list) or len(list_eruptions) == 0:
            raise ValidationError("list_eruptions must be non-empty list")
        validate_volcano_id(volcano_id)
        
        cumsum = validate_array_input(cumsum, "cumsum", min_length=2)
        
        # Extract years and sort
        years_data = []
        for e in list_eruptions:
            if 'year' not in e:
                raise ValidationError("Eruption entry missing 'year' field")
            try:
                y_int = int(e.get('year'))
            except Exception:
                raise ValidationError(f"Year value is not an integer: {e.get('year')}")
            # Allow negative years (BCE) and positive years. Do not reject based on sign.
            years_data.append(y_int)

        years = cumulativesum(years_data)
        years = years.reshape(-1, 1)
        cumsum_array = np.array(cumsum, dtype=float)
        
        # Linear regression
        model_linear = LinearRegression()
        model_linear.fit(years, cumsum_array)
        y_pred_linear = model_linear.predict(years)
        r2_linear = model_linear.score(years, cumsum_array)
        
        log_print(f"R² linear: {r2_linear:.4f}", volcano_id, section, project_path)
        
        # Bootstrap confidence intervals - Linear
        ci_lower_linear, ci_upper_linear = _bootstrap_linear_ci(
            years, cumsum_array, BOOTSTRAP_ITERATIONS
        )
        
        # Plot linear fit
        _plot_linear_fit(years.flatten(), cumsum_array, y_pred_linear, 
                        ci_lower_linear, ci_upper_linear, volcano_id, project_path)
        
        # Exponential regression
        def exp_func(x, a, b):
            return a * np.exp(b * x)
        
        try:
            popt, _ = curve_fit(exp_func, years.flatten(), cumsum_array, 
                              p0=(1, 0.01), maxfev=10000)
            y_pred_exponential = exp_func(years.flatten(), *popt)
        except RuntimeError as e:
            log_print(f"Warning: Exponential fit failed: {e}", volcano_id, section, project_path)
            # Fallback to linear prediction
            y_pred_exponential = y_pred_linear
            popt = (1, 0)
        
        # Bootstrap confidence intervals - Exponential
        ci_lower_exp, ci_upper_exp = _bootstrap_exponential_ci(
            years.flatten(), cumsum_array, BOOTSTRAP_ITERATIONS
        )
        
        # Calculate R² exponential
        residuals_exp = cumsum_array - y_pred_exponential
        ss_res = np.sum(residuals_exp ** 2)
        ss_tot = np.sum((cumsum_array - np.mean(cumsum_array)) ** 2)
        r2_exponential = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0
        
        log_print(f"R² exponential: {r2_exponential:.4f}", volcano_id, section, project_path)
        
        # Plot exponential fit
        _plot_exponential_fit(years.flatten(), cumsum_array, y_pred_exponential, 
                             ci_lower_exp, ci_upper_exp, volcano_id, project_path)
        
        # Calculate MSE
        mse_linear = mean_squared_error(cumsum_array, y_pred_linear)
        mse_exponential = mean_squared_error(cumsum_array, y_pred_exponential)
        
        # Save results
        output_file = os.path.join(project_path, volcano_id, 
                                  f"Change_Point/regression_{volcano_id}.txt")
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        with open(output_file, 'w') as f:
            f.write(f"MSE linear: {mse_linear:.6f}\n")
            f.write(f"R² linear: {r2_linear:.6f}\n")
            f.write(f"MSE exponential: {mse_exponential:.6f}\n")
            f.write(f"R² exponential: {r2_exponential:.6f}\n")
            if mse_exponential < mse_linear and r2_exponential > EXPONENTIAL_FIT_THRESHOLD:
                f.write("Result: Data shows exponential behavior. Completeness analysis required.\n")
            else:
                f.write("Result: Data does not show significant exponential growth. "
                       "All time series taken into account.\n")
        
        log_print(f"MSE Linear: {mse_linear:.6f}", volcano_id, section, project_path)
        log_print(f"MSE Exponential: {mse_exponential:.6f}", volcano_id, section, project_path)
        
        return mse_linear, mse_exponential, r2_linear, r2_exponential
        
    except (ValidationError, DataError, Exception) as e:
        log_print(f"Error in linear_analysis: {e}", volcano_id, section, project_path)
        raise


def _bootstrap_linear_ci(years: np.ndarray, cumsum: np.ndarray, 
                        n_bootstrap: int) -> Tuple[np.ndarray, np.ndarray]:
    """Bootstrap confidence intervals for linear regression."""
    bootstrap_preds = np.zeros((n_bootstrap, len(years)))
    np.random.seed(42)
    
    for i in range(n_bootstrap):
        indices = np.random.choice(len(years), len(years), replace=True)
        years_bs = years[indices]
        cumsum_bs = cumsum[indices]
        model_bs = LinearRegression()
        model_bs.fit(years_bs, cumsum_bs)
        bootstrap_preds[i] = model_bs.predict(years)
    
    ci_lower = np.nanpercentile(bootstrap_preds, 2.5, axis=0)
    ci_upper = np.nanpercentile(bootstrap_preds, 97.5, axis=0)
    return ci_lower, ci_upper


def _bootstrap_exponential_ci(years: np.ndarray, cumsum: np.ndarray, 
                             n_bootstrap: int) -> Tuple[np.ndarray, np.ndarray]:
    """Bootstrap confidence intervals for exponential regression."""
    def exp_func(x, a, b):
        return a * np.exp(b * x)
    
    bootstrap_preds = np.zeros((n_bootstrap, len(years)))
    np.random.seed(42)
    
    for i in range(n_bootstrap):
        indices = np.random.choice(len(years), len(years), replace=True)
        years_bs = years[indices]
        cumsum_bs = cumsum[indices]
        
        try:
            popt, _ = curve_fit(exp_func, years_bs, cumsum_bs, p0=(1, 0.01), maxfev=10000)
            bootstrap_preds[i] = exp_func(years, *popt)
        except RuntimeError:
            bootstrap_preds[i] = np.nan
    
    ci_lower = np.nanpercentile(bootstrap_preds, 2.5, axis=0)
    ci_upper = np.nanpercentile(bootstrap_preds, 97.5, axis=0)
    return ci_lower, ci_upper


def _plot_linear_fit(years: np.ndarray, data: np.ndarray, pred: np.ndarray,
                    ci_lower: np.ndarray, ci_upper: np.ndarray, 
                    volcano_id: str, project_path: str) -> None:
    """Plot linear regression fit with confidence interval."""
    try:
        plt.figure(figsize=(10, 6))
        plt.scatter(years, data, label='Data', alpha=0.6)
        plt.plot(years, pred, color='red', label='Linear Fit', linewidth=2)
        plt.fill_between(years, ci_lower, ci_upper, color='red', alpha=0.2, 
                        label='95% Confidence Interval')
        plt.legend()
        plt.title('Linear Fit with Bootstrapped 95% CI')
        plt.xlabel('Years')
        plt.ylabel('Cumulative number of events')
        plt.grid(True, alpha=0.3)
        
        filepath = os.path.join(project_path, volcano_id, 
                               f"Change_Point/linear_regression_{volcano_id}.png")
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        plt.savefig(filepath, dpi=300, bbox_inches='tight')
        plt.show()
    except Exception as e:
        log_print(f"Warning: Could not save linear fit plot: {e}", volcano_id, "Plotting", project_path)


def _plot_exponential_fit(years: np.ndarray, data: np.ndarray, pred: np.ndarray,
                         ci_lower: np.ndarray, ci_upper: np.ndarray, 
                         volcano_id: str, project_path: str) -> None:
    """Plot exponential regression fit with confidence interval."""
    try:
        plt.figure(figsize=(10, 6))
        plt.scatter(years, data, label='Data', alpha=0.6)
        plt.plot(years, pred, color='green', label='Exponential Fit', linewidth=2)
        plt.fill_between(years, ci_lower, ci_upper, color='green', alpha=0.2,
                        label='95% Confidence Interval')
        plt.legend()
        plt.title('Exponential Fit with Bootstrapped 95% CI')
        plt.xlabel('Years')
        plt.ylabel('Cumulative number of eruptions')
        plt.grid(True, alpha=0.3)
        
        filepath = os.path.join(project_path, volcano_id,
                               f"Change_Point/exponential_regression_{volcano_id}.png")
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        plt.savefig(filepath, dpi=300, bbox_inches='tight')
        plt.show()
    except Exception as e:
        log_print(f"Warning: Could not save exponential fit plot: {e}", volcano_id, "Plotting", project_path)


def completeness(volcano_id: str, project_path: str) -> Optional[Tuple[Any, Any]]:
    """
    Analyze completeness of eruption recording over time.
    
    Detects change points in the cumulative eruption time series to identify
    when recording practices changed or became more complete.
    
    Args:
        volcano_id: Volcano identifier
        project_path: Path to project data directory
        
    Returns:
        Tuple of (change_points, eruptions) if successful, None otherwise
        
    Raises:
        ValidationError: If volcano_id is invalid
    """
    section = "completeness"
    
    try:
        validate_volcano_id(volcano_id)
        
        # Fetch data
        eruptions = mdb.volcano_data_completeness(volcano_id)
        data_vol = mdb.volcano_data(volcano_id)
        
        if not eruptions or not eruptions.data:
            log_print("No eruption data available", volcano_id, section, project_path)
            return None
        
        # Create output directory
        create_output_directory(volcano_id, "Change_Point", project_path)
        
        n_eruptions = len(eruptions.data)
        if n_eruptions <= MIN_ERUPTIONS_COMPLETENESS:
            log_print(
                f"WARNING: Requires {MIN_ERUPTIONS_COMPLETENESS}+ events. "
                f"Provided: {n_eruptions}. All events taken for analysis.",
                volcano_id, section, project_path
            )
            return None
        
        # Extract years and create cumulative sum
        years = [int(e['year']) for e in eruptions.data]
        cumsum = list(range(1, n_eruptions + 1))
        
        # Perform regression analysis
        mse_linear, mse_exponential, r2_linear, r2_exponential = linear_analysis(
            eruptions.data, cumsum, volcano_id, project_path
        )
        
        # Check if exponential fit is appropriate
        if mse_exponential < mse_linear and r2_exponential > EXPONENTIAL_FIT_THRESHOLD:
            years_sorted = cumulativesum(years)
            data = years_sorted.reshape(-1, 1)
            
            # Detect change point
            algo = rpt.Binseg(model="normal").fit(data)
            result = algo.predict(n_bkps=1)
            
            # Calculate and save segment statistics
            segments = np.split(data, result[:-1])
            segment_stats = [
                {
                    'mean': float(np.mean(seg)),
                    'variance': float(np.var(seg)),
                    'size': len(seg)
                }
                for seg in segments
            ]
            
            _save_segment_stats(volcano_id, segment_stats, project_path)
            _plot_change_points(years_sorted, cumsum, result, volcano_id, 
                               eruptions, data_vol, project_path)
            
            change_points = [years_sorted[idx - 1] for idx in result]
            log_print(f"Change points at years: {change_points}", volcano_id, section, project_path)
            
            return change_points, eruptions
        else:
            log_print(
                "Time series does not show significant exponential increase. "
                "All events taken into account.",
                volcano_id, section, project_path
            )
            return None
            
    except (ValidationError, DataError, Exception) as e:
        log_print(f"Error in completeness: {e}", volcano_id, section, project_path)
        raise


def _save_segment_stats(volcano_id: str, stats: List[Dict], project_path: str) -> None:
    """Save segment statistics to file."""
    try:
        output_file = os.path.join(project_path, volcano_id,
                                  f"Change_Point/change_point_stats_{volcano_id}.txt")
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        with open(output_file, 'w') as f:
            f.write("Segment Statistics\n\n")
            for i, stat in enumerate(stats):
                f.write(f"Segment {i + 1}:\n")
                f.write(f"  Mean: {stat['mean']:.2f}\n")
                f.write(f"  Variance: {stat['variance']:.2f}\n")
                f.write(f"  Number of points: {stat['size']}\n")
                f.write("\n")
    except IOError as e:
        log_print(f"Warning: Could not save segment stats: {e}", volcano_id, "Completeness", project_path)


def _plot_change_points(years: np.ndarray, cumsum: List[int], 
                       changepoints: np.ndarray, volcano_id: str,
                       eruptions: Any, data_vol: Any, project_path: str) -> None:
    """Plot change point detection results."""
    try:
        volcano_name = data_vol.data[0].get("name", "Unknown")
        volcano_country = data_vol.data[0].get("country", "Unknown")
        
        plt.figure(figsize=(12, 6))
        plt.plot(years, cumsum, label='Cumulative Sum', marker='o', markersize=4)
        
        for i, cp in enumerate(changepoints[:-1]):
            label = 'Change Point' if i == 0 else ""
            plt.axvline(x=years[cp - 1], color='r', linestyle='--', label=label)
        
        plt.title("Change Point Detection")
        plt.suptitle(f"Volcano: {volcano_id} {volcano_name}, {volcano_country}")
        plt.xlabel("Years")
        plt.ylabel("Cumulative Sum")
        plt.grid(True, alpha=0.3)
        plt.legend()
        
        filepath = os.path.join(project_path, volcano_id,
                               f"Change_Point/change_point_detection_{volcano_id}.png")
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        plt.savefig(filepath, dpi=300, bbox_inches='tight')
        plt.show()
    except Exception as e:
        log_print(f"Warning: Could not plot change points: {e}", volcano_id, "Completeness", project_path)


# ============================================================================
# PDF DATA PREPARATION
# ============================================================================

def frequency_cal(list_energies: List[float], 
                 limits: np.ndarray) -> Tuple[Optional[np.ndarray], 
                                             Optional[np.ndarray], 
                                             Optional[np.ndarray]]:
    """
    Calculate empirical PDF and CDF using predefined energy bins.
    
    Args:
        list_energies: List of energy values
        limits: Array of bin edge limits
        
    Returns:
        Tuple of (bin_centers_log10, pdf, cdf) or (None, None, None) if invalid
        
    Raises:
        ValidationError: If inputs are invalid
    """
    try:
        energies = validate_array_input(list_energies, "list_energies")
        limits = validate_array_input(limits, "limits", min_length=2)
        
        # Clean data - keep only positive values
        energies = energies[energies > 0]
        
        if len(energies) == 0:
            return None, None, None
        
        # Compute histogram
        counts, bin_edges = np.histogram(energies, bins=limits)

        # PDF (normalized frequency)
        total_count = counts.sum()
        if total_count == 0:
            return None, None, None
        
        pdf = counts / total_count
       
        # CDF
        cdf = np.cumsum(pdf)
        
        # Bin centers in log10 scale
        bin_centers = 0.5 * (bin_edges[:-1] + bin_edges[1:])
        bin_centers_log10 = np.log10(bin_centers)
        
        return bin_centers_log10, pdf, cdf
        
    except ValidationError as e:
        warnings.warn(f"Error in frequency_cal: {e}")
        return None, None, None


def cumulative_energy(eruptions: List[Dict], tag: str) -> Tuple[List[int], List[float]]:
    """
    Calculate cumulative energy over time, handling duplicate years.
    
    Args:
        eruptions: List of eruption dictionaries
        tag: Dictionary key for energy value (e.g., 'energy', 'e_tl')
        
    Returns:
        Tuple of (years, cumulative_energies)
        
    Raises:
        ValidationError: If inputs are invalid
        DataError: If data processing fails
    """
    try:
        if not isinstance(eruptions, list) or len(eruptions) == 0:
            raise ValidationError("eruptions must be non-empty list")
        
        if not isinstance(tag, str):
            raise ValidationError("tag must be string")
        
        # Sort by year
        sorted_eruptions = sorted(eruptions, key=lambda d: int(d.get('year', 0)))
        
        years = []
        energies = []
        
        for eruption in sorted_eruptions:
            year = int(eruption.get('year', 0))
            energy = float(eruption.get(tag, 0))
            
            if year <= 0:
                continue
            
            years.append(year)
            energies.append(energy)
        
        if len(years) == 0:
            raise DataError("No valid eruption data extracted")
        
        # Aggregate by year
        new_years = []
        new_energies = []
        cumulative = 0.0
        
        for year, energy in zip(years, energies):
            year_count = years.count(year)
            
            if year_count == 1:
                cumulative += energy
                new_years.append(year)
                new_energies.append(cumulative)
            elif year not in new_years:
                year_indices = [i for i, y in enumerate(years) if y == year]
                year_total = sum(energies[i] for i in year_indices)
                cumulative += year_total
                new_years.append(year)
                new_energies.append(cumulative)
        
        return new_years, new_energies
        
    except (ValidationError, DataError, ValueError) as e:
        raise DataError(f"Error in cumulative_energy: {e}")


def eruptions_divide(eruptions_energy: List[Dict]) -> Tuple[List[float], List[float], 
                                                            List[float], List[int], 
                                                            int, List[int]]:
    """
    Separate eruption energies by type (pyroclast, lava, total).
    
    Args:
        eruptions_energy: List of eruption dictionaries with energy data.
                         Supports scalar energy or list formats:
                         [total] or [total, lava, pyroclast]
        
    Returns:
        Tuple of (e_pyroclast, e_lava, e_total, cumulative_count, total_count, years)
        
    Raises:
        ValidationError: If inputs are invalid
    """
    try:
        if not isinstance(eruptions_energy, list):
            raise ValidationError("eruptions_energy must be list")
        
        e_p = []  # pyroclast energy
        e_l = []  # lava energy
        e_t = []  # total energy
        cne = []  # cumulative number
        years = []
        count = 1
        
        for eruption in eruptions_energy:
            if not isinstance(eruption, dict):
                continue

            try:
                energy_raw = eruption.get("energy", 0)
                energy_total = np.nan
                e_tl_val = np.nan
                e_tp_val = np.nan

                # New format support: energy can be [total] or [total, lava, pyroclast]
                if isinstance(energy_raw, (list, tuple, np.ndarray)):
                    parsed_energy = []
                    for value in energy_raw:
                        try:
                            parsed_energy.append(float(value))
                        except (TypeError, ValueError):
                            parsed_energy.append(np.nan)

                    if len(parsed_energy) >= 1:
                        energy_total = parsed_energy[0]
                    if len(parsed_energy) >= 3:
                        e_tl_val = parsed_energy[1]
                        e_tp_val = parsed_energy[2]
                    elif len(parsed_energy) == 2:
                        e_tl_val = parsed_energy[1]
                else:
                    # Backward compatibility: scalar total energy + optional e_tl/e_tp fields
                    energy_total = float(energy_raw)

                    e_tl_raw = eruption.get("e_tl")
                    e_tp_raw = eruption.get("e_tp")

                    if e_tl_raw is not None:
                        e_tl_val = float(e_tl_raw)
                    if e_tp_raw is not None:
                        e_tp_val = float(e_tp_raw)

                if not np.isfinite(energy_total) or energy_total == 0:
                    continue

                e_t.append(int(energy_total))
                e_l.append(e_tl_val if np.isfinite(e_tl_val) else np.nan)
                e_p.append(e_tp_val if np.isfinite(e_tp_val) else np.nan)
                years.append(int(eruption.get("year", 0)))
                cne.append(count)
                count += 1
            except (ValueError, TypeError):
                continue
        
        return e_p, e_l, e_t, cne, count, years
        
    except ValidationError as e:
        raise DataError(f"Error in eruptions_divide: {e}")


# ============================================================================
# STATISTICAL DISTRIBUTION FUNCTIONS
# ============================================================================

def pdf_computation(dist: Any, params: Tuple, lim: np.ndarray) -> np.ndarray:
    """
    Compute PDF values for a distribution.
    
    Args:
        dist: scipy.stats distribution object
        params: Distribution parameters
        lim: Points at which to evaluate PDF
        
    Returns:
        PDF values at specified points
    """
    try:
        lim = validate_array_input(lim, "lim")
        
        arg = params[:-2]
        loc = params[-2]
        scale = params[-1]
        
        if arg:
            pdf = (dist.cdf(lim, *arg, loc=loc, scale=scale) -
                   dist.cdf(lim - 1, *arg, loc=loc, scale=scale))
        else:
            pdf = (dist.cdf(lim, loc=loc, scale=scale) -
                   dist.cdf(lim - 1, loc=loc, scale=scale))
        
        return pdf
    except Exception as e:
        warnings.warn(f"Error computing PDF: {e}")
        return np.zeros_like(lim)


def cdf_computation(dist: Any, params: Tuple, lim: np.ndarray) -> np.ndarray:
    """
    Compute CDF values for a distribution.
    
    Args:
        dist: scipy.stats distribution object
        params: Distribution parameters
        lim: Points at which to evaluate CDF
        
    Returns:
        CDF values at specified points
    """
    try:
        lim = validate_array_input(lim, "lim")
        
        arg = params[:-2]
        loc = params[-2]
        scale = params[-1]
        
        if arg:
            cdf = dist.cdf(lim, *arg, loc=loc, scale=scale)
        else:
            cdf = dist.cdf(lim, loc=loc, scale=scale)
        
        return cdf
    except Exception as e:
        warnings.warn(f"Error computing CDF: {e}")
        return np.zeros_like(lim)


def pdf_function(list_energies: List[float], 
                limits_interp: np.ndarray) -> Tuple[List[np.ndarray], List[str], 
                                                    List[str], List[np.ndarray]]:
    """
    Fit multiple distributions to energy data.
    
    Args:
        list_energies: List of energy values
        limits_interp: Interpolation points for PDF evaluation
        
    Returns:
        Tuple of (pdf_results, labels, dist_names, cdf_results)
        
    Raises:
        ValidationError: If inputs are invalid
    """
    try:
        energies = validate_array_input(list_energies, "list_energies", min_length=1)
        energies = energies[energies > 0]
        
        if len(energies) == 0:
            raise ValidationError("No positive energy values")
        
        y = safe_log10(energies)
        
        pdf_results = []
        cdf_results = []
        
        for dist_name in DISTRIBUTION_NAMES:
            try:
                dist = getattr(stats, dist_name)
                params = dist.fit(y)
                
                pdf_fitted = pdf_computation(dist, params, limits_interp)
                cdf_fitted = cdf_computation(dist, params, limits_interp)
                
                pdf_results.append(pdf_fitted)
                cdf_results.append(cdf_fitted)
            except Exception as e:
                warnings.warn(f"Failed to fit {dist_name}: {e}")
                pdf_results.append(np.zeros_like(limits_interp))
                cdf_results.append(np.zeros_like(limits_interp))
        
        return pdf_results, DISTRIBUTION_LABELS, DISTRIBUTION_NAMES, cdf_results
        
    except ValidationError as e:
        raise DataError(f"Error in pdf_function: {e}")


def best_fit(cdf: np.ndarray, data: List[float], dist_names: List[str],
            lim: np.ndarray) -> Optional[Tuple]:
    """
    Select best-fitting distribution using Kolmogorov-Smirnov test.
    
    Args:
        cdf: Empirical CDF values
        data: Original energy data
        dist_names: List of distribution names to test
        lim: Evaluation points
        
    Returns:
        Tuple of (statistic, p_value, dist_name, params, p_value) if valid fit found
        
    Raises:
        ValidationError: If inputs are invalid
    """
    try:
        cdf = validate_array_input(cdf, "cdf")
        data = validate_array_input(data, "data", min_length=1)
        
        x = safe_log10(data)
        
        best_statistic = 1.0
        best_pvalue = 0.0
        best_dist = None
        best_params = None
        
        for dist_name in dist_names:
            try:
                dist = getattr(stats, dist_name)
                params = dist.fit(x)
                
                cdf_fitted = cdf_computation(dist, params, lim)
                statistic, pvalue = stats.ks_2samp(cdf, cdf_fitted)
                
                if pvalue > best_pvalue:
                    best_pvalue = pvalue
                    best_statistic = statistic
                    best_dist = dist_name
                    best_params = params
            except Exception:
                continue
        
        if best_pvalue > PVALUE_THRESHOLD:
            return (best_statistic, best_pvalue, best_dist, best_params, best_pvalue)
        else:
            return None
            
    except ValidationError as e:
        raise DataError(f"Error in best_fit: {e}")


# ============================================================================
# MARGINAL PDF COMPUTATION
# ============================================================================

def bi_weibull_pdf(x: float, p: float, k1: float, l1: float, 
                  k2: float, l2: float) -> float:
    """
    Bi-Weibull mixture PDF (not currently used but kept for reference).
    
    Returns mixture of two Weibull distributions.
    """
    return (p * stats.weibull_min.pdf(x, k1, scale=l1) +
            (1 - p) * stats.weibull_min.pdf(x, k2, scale=l2))


def bi_lognorm_pdf(x: float, p: float, s1: float, scale1: float,
                  s2: float, scale2: float) -> float:
    """
    Bi-LogNormal mixture PDF (not currently used but kept for reference).
    
    Returns mixture of two LogNormal distributions.
    """
    return (p * stats.lognorm.pdf(x, s1, scale=scale1) +
            (1 - p) * stats.lognorm.pdf(x, s2, scale=scale2))




def extend_pdf_range(bin_centers: np.ndarray, pdf_total: np.ndarray, 
                     extension_factor: float = 5.0) -> Tuple[np.ndarray, np.ndarray]:
    """
    Extend PDF range to cover wider energy scale for scaled computations.
    
    This is necessary when computing marginal PDFs like f_E_lava(e) = (1/phi)*f_E0(e/phi),
    which requires f_E0 at energies scaled by 1/phi. For phi=0.6, this requires
    f_E0 at energies up to 1/0.4 = 2.5 times the maximum.
    
    Args:
        bin_centers: Original bin centers (log10 scale)
        pdf_total: Original PDF values
        extension_factor: Maximum scaling factor to support (default 5.0)
        
    Returns:
        Tuple of (extended_bin_centers, extended_pdf)
    """
    try:
        bin_centers = validate_array_input(bin_centers, "bin_centers")
        pdf_total = validate_array_input(pdf_total, "pdf_total")
        
        if len(bin_centers) == 0 or len(pdf_total) == 0:
            return bin_centers, pdf_total
        
        # Original range
        e_min = bin_centers[0]
        e_max = bin_centers[-1]
        e_range = e_max - e_min
        
        # Extended range: stretch upper end
        e_max_extended = e_max + (extension_factor - 1.0) * e_range
        
        # Create extended bin centers with same spacing as original
        delta_e = e_range / (len(bin_centers) - 1) if len(bin_centers) > 1 else 1.0
        n_extended = int(np.ceil((e_max_extended - e_min) / delta_e)) + 1
        extended_bin_centers = np.linspace(e_min, e_max_extended, n_extended)
        
        # Extrapolate PDF in log-log space (power-law tail)
        # Fit a line in log-log space for the last few points
        n_fit = min(5, len(bin_centers) // 2)  # Use last ~5 points for fitting
        if n_fit >= 2 and np.all(pdf_total[-n_fit:] > 0) and np.all(bin_centers[-n_fit:] > 0):
            # Log-log fit: log(pdf) = a + b*log(e)
            log_e = np.log10(bin_centers[-n_fit:])
            log_pdf = np.log10(pdf_total[-n_fit:])
            
            # Linear regression in log-log space
            coeffs = np.polyfit(log_e, log_pdf, 1)  # b, a
            b, a = coeffs[0], coeffs[1]  # slope and intercept
            
            # Extrapolate: pdf(e) = 10^(a + b*log(e)) = 10^a * e^b
            extended_pdf = np.zeros_like(extended_bin_centers)
            
            # Copy original values
            extended_pdf[:len(bin_centers)] = pdf_total
            
            # Extrapolate beyond original range
            for i in range(len(bin_centers), len(extended_bin_centers)):
                log_e_ext = np.log10(extended_bin_centers[i])
                extended_pdf[i] = 10.0 ** (a + b * log_e_ext)
        else:
            # Fallback: exponential decay with fixed rate
            extended_pdf = np.zeros_like(extended_bin_centers)
            extended_pdf[:len(bin_centers)] = pdf_total
            
            if len(pdf_total) > 1 and pdf_total[-1] > 0 and pdf_total[-2] > 0:
                # Exponential extrapolation
                decay_rate = np.log(pdf_total[-2] / pdf_total[-1])
                for i in range(len(bin_centers), len(extended_bin_centers)):
                    steps = i - (len(bin_centers) - 1)
                    extended_pdf[i] = pdf_total[-1] * np.exp(-decay_rate * steps)
        
        # Normalize extended PDF
        integral = np.trapz(extended_pdf, extended_bin_centers)
        if integral > 1e-10:
            extended_pdf /= integral
        
        return extended_bin_centers, extended_pdf
        
    except Exception as e:
        warnings.warn(f"Error in extend_pdf_range: {e}")
        return bin_centers, pdf_total


def marginal_pdf_energy(total_energy: List[float], phi_fit_result: Dict,
                       bin_centers: np.ndarray, pdf_total: np.ndarray,
                       volcano_id: str, best_fit_dist: Optional[Tuple] = None, 
                       section: str = None, project_path: str = None) -> Tuple[np.ndarray, np.ndarray]:
    """
    Compute marginal PDFs for lava and tephra from fitted phi distribution.
    
    Args:
        total_energy: Array of total energies (used for range)
        phi_fit_result: Dict with 'name' and 'params' from phi fitting
                       If name is 'point_estimate', params is a single phi value
        bin_centers: Energy bin centers (original range)
        pdf_total: Total energy PDF on original range
        volcano_id: Volcano identifier
        best_fit_dist: Optional tuple (KS_stat, pvalue, dist_name, dist_params) for best-fit distribution
        section: Analysis section name
        project_path: Path to project data directory
        
    Returns:
        Tuple of (f_lava, f_tephra) marginal PDFs on original bin_centers range
        
    Raises:
        ValidationError: If inputs are invalid
    """
    try:
        if not isinstance(phi_fit_result, dict):
            raise ValidationError("phi_fit_result must be dictionary")
        if 'name' not in phi_fit_result or 'params' not in phi_fit_result:
            raise ValidationError("phi_fit_result missing 'name' or 'params'")
        
        bin_centers = validate_array_input(bin_centers, "bin_centers")
        pdf_total = validate_array_input(pdf_total, "pdf_total")
        
        phi_dist_name = phi_fit_result['name']
        phi_params = phi_fit_result['params']
        
        # First, extend the PDF range to support wider scaling
        # For point estimate or distribution, we need f_total(e/phi) and f_total(e/(1-phi))
        # Maximum scaling is 1/phi_min and 1/(1-phi_max)
        phi_min = 0.01  # Conservative lower bound
        phi_max = 0.99  # Conservative upper bound
        max_scale_lava = 1.0 / phi_min  # ~100
        max_scale_tephra = 1.0 / (1.0 - phi_max)  # ~100
        max_scale = max(max_scale_lava, max_scale_tephra)
        
        # Extend the f_total PDF to support the scaled evaluations
        bin_centers_ext, pdf_total_ext = extend_pdf_range(bin_centers, pdf_total, 
                                                          extension_factor=max_scale)
        
        f_lava = np.zeros_like(bin_centers)
        f_tephra = np.zeros_like(bin_centers)
        
        # Special case: point estimate (single phi value)
        if phi_dist_name == 'point_estimate':
            phi0 = float(phi_params)
            print("Point estimate phi:", phi0, "Tephra fraction:", 1-phi0)
            if not (0.0 < phi0 < 1.0):
                raise ValidationError("point_estimate phi must be in (0, 1)")
            
            # Follow the exact equations:
            # f_E_lava(e) = (1/phi0) * f_E0(e/phi0)
            # f_E_tep(e) = (1/(1-phi0)) * f_E0(e/(1-phi0))
            
            # For each bin_center e, compute f_total at e/phi0 and e/(1-phi0)
            e_total_lava = bin_centers 
            bin_centers_lava = bin_centers * phi0
            e_total_tephra = bin_centers 
            bin_centers_tephra = bin_centers * (1.0 - phi0)
            
            pdf_total_lava = pdf_total / phi0
            pdf_total_tephra = pdf_total / (1.0 - phi0)
            
            # Normalize the scaled PDFs to integrate to 1
            integral_lava = np.trapz(pdf_total_lava, bin_centers_lava)
            integral_tephra = np.trapz(pdf_total_tephra, bin_centers_tephra)
            
            pdf_total_lava_normalized = pdf_total_lava / integral_lava if integral_lava > 1e-10 else pdf_total_lava
            pdf_total_tephra_normalized = pdf_total_tephra / integral_tephra if integral_tephra > 1e-10 else pdf_total_tephra
            
            # Verify normalization (should be ~1.0)
            integral_lava_check = np.trapz(pdf_total_lava_normalized, bin_centers_lava)
            integral_tephra_check = np.trapz(pdf_total_tephra_normalized, bin_centers_tephra)
            print(f"Lava PDF integral after normalization: {integral_lava_check:.6f}")
            print(f"Tephra PDF integral after normalization: {integral_tephra_check:.6f}")
            
            f_lava = np.interp(bin_centers, bin_centers_lava, pdf_total_lava_normalized, left=0.0, right=0.0)
            f_tephra = np.interp(bin_centers, bin_centers_tephra, pdf_total_tephra_normalized, left=0.0, right=0.0)
            
            # Plot and save lava energy distribution
            plt.figure(figsize=(10, 6))
            plt.plot(bin_centers_lava, pdf_total_lava_normalized, 'o-', color='orange', linewidth=2, markersize=4)
            plt.xlabel('Energy (log10 J)', fontsize=12)
            plt.ylabel('PDF', fontsize=12)
            plt.title('Lava Energy PDF normalized', fontsize=12)
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            
            lava_dir = create_output_directory(volcano_id, "PDFs", project_path)
            lava_plot_file = os.path.join(lava_dir, f"marginal_pdf_lava_{volcano_id}_{section}.png")
            plt.savefig(lava_plot_file, dpi=300, bbox_inches='tight')
            plt.close()
            
            # Save lava arrays to file
            lava_data_file = os.path.join(lava_dir, f"marginal_pdf_lava_data_{volcano_id}_{section}.txt")
            np.savetxt(lava_data_file, np.column_stack((bin_centers_lava, pdf_total_lava_normalized)),
                      header='bin_centers_lava\tpdf_total_lava', comments='')
            
            # Plot and save tephra energy distribution
            plt.figure(figsize=(10, 6))
            plt.plot(bin_centers_tephra, pdf_total_tephra_normalized, 'o-', color='brown', linewidth=2, markersize=4)
            plt.xlabel('Energy (log10 J)', fontsize=12)
            plt.ylabel('PDF', fontsize=12)
            plt.title('Tephra Energy PDF normalized', fontsize=12)
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            
            tephra_plot_file = os.path.join(lava_dir, f"marginal_pdf_tephra_{volcano_id}_{section}.png")
            plt.savefig(tephra_plot_file, dpi=300, bbox_inches='tight')
            plt.close()
            
            # Save tephra arrays to file
            tephra_data_file = os.path.join(lava_dir, f"marginal_pdf_tephra_data_{volcano_id}_{section}.txt")
            np.savetxt(tephra_data_file, np.column_stack((bin_centers_tephra, pdf_total_tephra_normalized)),
                      header='bin_centers_tephra\tpdf_total_tephra', comments='')
            
            return f_lava, f_tephra
        
        # Distribution-based case: integrate over phi
        phi_grid = np.linspace(0.01, 0.99, 200)
        phi_dist = getattr(stats, phi_dist_name)
        
        # Compute phi PDF
        if phi_dist_name == 'truncnorm':
            a, b, mu, sigma = phi_params
            f_phi = stats.truncnorm.pdf(phi_grid, a, b, mu, sigma)
        else:
            f_phi = phi_dist.pdf(phi_grid, *phi_params)
        
        # Normalize
        integral = np.trapz(f_phi, phi_grid)
        if integral > 0:
            f_phi /= integral
        
        # For each output bin center, integrate over phi distribution
        for i, e in enumerate(bin_centers):
            # For lava: E_total = e / phi
            # f_lava(e) = integral of f_total(e/phi) * (1/phi) * f_phi(phi) d(phi)
            e_total_lava = e / phi_grid
            pdf_total_lava = np.interp(e_total_lava, bin_centers_ext, pdf_total_ext,
                                       left=0.0, right=0.0)
            integrand_lava = pdf_total_lava * f_phi / phi_grid
            f_lava[i] = np.trapz(integrand_lava, phi_grid)
            
            # For tephra: E_total = e / (1-phi)
            # f_tephra(e) = integral of f_total(e/(1-phi)) * (1/(1-phi)) * f_phi(phi) d(phi)
            e_total_tephra = e / (1.0 - phi_grid)
            pdf_total_tephra = np.interp(e_total_tephra, bin_centers_ext, pdf_total_ext,
                                        left=0.0, right=0.0)
            integrand_tephra = pdf_total_tephra * f_phi / (1.0 - phi_grid)
            f_tephra[i] = np.trapz(integrand_tephra, phi_grid)
        
        # Normalize marginal PDFs
        integral_lava = np.trapz(f_lava, bin_centers)
        integral_tephra = np.trapz(f_tephra, bin_centers)
        
        if integral_lava > 1e-10:
            f_lava /= integral_lava
        if integral_tephra > 1e-10:
            f_tephra /= integral_tephra
        
        return f_lava, f_tephra
        
    except (ValidationError, Exception) as e:
        raise DataError(f"Error in marginal_pdf_energy: {e}")


def best_fit_phi_and_plot(phi_list: List[float], volcano_id: str,
                         nbins: int = 20, project_path: str = None) -> Optional[Tuple]:
    """
    Find best-fitting distribution for lava energy fraction φ = E_lava / E_total.
    
    Args:
        phi_list: List of phi values (between 0 and 1)
        volcano_id: Volcano identifier
        nbins: Number of histogram bins
        project_path: Path to project data directory
        
    Returns:
        Tuple of (best_model_dict, best_aic, best_ks_pvalue, x_eval)
        
    Raises:
        ValidationError: If inputs are invalid
    """
    section = "phi_fitting"
    
    try:
        validate_volcano_id(volcano_id)
        
        # Clean and validate data
        phi = np.array(phi_list, dtype=float)
        phi = phi[(phi > 0) & (phi < 1)]
        
        if len(phi) < MIN_PHI_DATA_POINTS:
            log_print(f"Insufficient phi data: {len(phi)} < {MIN_PHI_DATA_POINTS}",
                     volcano_id, section, project_path)
            return None
        
        x_eval = np.linspace(0.001, 0.999, 500)
        best_model = None
        best_aic = np.inf
        
        # Try only distributions suitable for bounded [0,1] data
        for dist_name in PHI_DISTRIBUTION_NAMES:
            try:
                dist = getattr(stats, dist_name)
                
                # Special handling for bounded distributions
                if dist_name == "beta":
                    # Beta is naturally bounded [0, 1]
                    params = dist.fit(phi, floc=0, fscale=1)
                    pdf_vals = dist.pdf(phi, *params)
                    pdf_eval = dist.pdf(x_eval, *params)
                    k = 2
                    
                elif dist_name == "arcsine":
                    # Arcsine is a special case, bounded [0, 1]
                    params = dist.fit(phi, floc=0, fscale=1)
                    pdf_vals = dist.pdf(phi, *params)
                    pdf_eval = dist.pdf(x_eval, *params)
                    k = 1
                    
                elif dist_name == "uniform":
                    # Uniform bounded [0, 1]
                    params = dist.fit(phi, floc=0, fscale=1)
                    pdf_vals = dist.pdf(phi, *params)
                    pdf_eval = dist.pdf(x_eval, *params)
                    k = 0
                    
                elif dist_name == "truncnorm":
                    # Truncated normal to [0, 1]
                    mu, sigma = stats.norm.fit(phi)
                    a = (0 - mu) / sigma
                    b = (1 - mu) / sigma
                    params = (a, b, mu, sigma)
                    pdf_vals = stats.truncnorm.pdf(phi, a, b, mu, sigma)
                    pdf_eval = stats.truncnorm.pdf(x_eval, a, b, mu, sigma)
                    k = 2
                
                # Calculate AIC
                pdf_vals_positive = pdf_vals[pdf_vals > 0]
                if len(pdf_vals_positive) == 0:
                    continue
                
                ll = np.sum(np.log(pdf_vals_positive))
                aic = 2 * k - 2 * ll
                
                # Check goodness of fit using KS test
                if aic < best_aic:
                    try:
                        if dist_name == "truncnorm":
                            a, b, mu, sigma = params
                            cdf_func = lambda x: stats.truncnorm.cdf(x, a, b, mu, sigma)
                        elif dist_name == "uniform":
                            cdf_func = lambda x: dist.cdf(x, *params)
                        else:
                            cdf_func = lambda x: dist.cdf(x, *params)
                        
                        D, pval = stats.kstest(phi, cdf_func)
                        best_aic = aic
                        best_model = {
                            "name": dist_name,
                            "params": params,
                            "AIC": aic,
                            "KS_p": pval,
                            "pdf_eval": pdf_eval
                        }
                    except Exception:
                        continue
            except Exception as e:
                continue
        
        if best_model is None:
            log_print("No suitable distribution found for phi", volcano_id, section, project_path)
            return None
        
        # Save results
        directory = create_output_directory(volcano_id, "PDFs", project_path)
        output_file = os.path.join(directory, f"PHI_parameters_{volcano_id}.txt")
        
        with open(output_file, 'w') as f:
            f.write("Best-fit distribution for φ = E_lava / E_total\n")
            f.write("=" * 50 + "\n")
            f.write(f"Model: {best_model['name']}\n")
            f.write(f"Parameters: {best_model['params']}\n")
            f.write(f"AIC: {best_model['AIC']:.2f}\n")
            f.write(f"KS p-value: {best_model['KS_p']:.3f}\n")
        
        log_print(f"Best fit: {best_model['name']} (AIC={best_model['AIC']:.2f})",
                 volcano_id, section, project_path)
        
        # Plot results
        _plot_phi_fit(phi, best_model, x_eval, volcano_id, project_path)
        
        return best_model, best_aic, best_model['KS_p'], x_eval
        
    except (ValidationError, Exception) as e:
        log_print(f"Error in best_fit_phi_and_plot: {e}", volcano_id, section, project_path)
        return None


def _plot_phi_fit(phi: np.ndarray, best_model: Dict, x_eval: np.ndarray,
                 volcano_id: str, project_path: str) -> None:
    """Plot phi distribution fit with all tested distributions for comparison."""
    try:
        directory = create_output_directory(volcano_id, "PDFs", project_path)
        
        plt.figure(figsize=(12, 7))
        
        # Histogram
        plt.hist(phi, bins=20, density=True, alpha=0.4, label="Empirical PDF", color="gray")
        
        # Best fit using pre-evaluated PDF
        y = best_model.get("pdf_eval")
        if y is None:
            # Fallback: recalculate if not pre-computed
            best_dist = getattr(stats, best_model["name"])
            if best_model["name"] == "truncnorm":
                a, b, mu, sigma = best_model["params"]
                y = stats.truncnorm.pdf(x_eval, a, b, mu, sigma)
            else:
                y = best_dist.pdf(x_eval, *best_model["params"])
        
        plt.plot(x_eval, y, 'r-', lw=3, label=f"Best fit: {best_model['name']} "
                 f"(AIC={best_model['AIC']:.1f}, KS p={best_model['KS_p']:.3f})")
        
        # KDE reference
        try:
            kde = stats.gaussian_kde(phi)
            plt.plot(x_eval, kde(x_eval), 'g--', lw=2, label="Kernel Density Estimate")
        except Exception:
            pass
        
        plt.xlabel("Lava energy fraction φ", fontsize=12)
        plt.ylabel("PDF", fontsize=12)
        plt.title(f"Phi Distribution Fit - Volcano {volcano_id}\n"
                 f"(Only bounded [0,1] distributions tested)", fontsize=12)
        plt.legend(fontsize=10)
        plt.grid(True, alpha=0.3)
        plt.xlim([0, 1])
        plt.tight_layout()
        
        filepath = os.path.join(directory, f"Phi_PDF_{volcano_id}.png")
        plt.savefig(filepath, dpi=300, bbox_inches='tight')
        plt.show()
    except Exception as e:
        log_print(f"Warning: Could not plot phi fit: {e}", volcano_id, "Plotting", project_path)


# ============================================================================
# TEMPORAL ANALYSIS
# ============================================================================

def time_slots(volcano_id, eruptions: List[Dict], project_path: str) -> Tuple[List[float], List[float], List[float]]:
    """
    Calculate time intervals between consecutive eruptions in months and years.
    
    Handles month values including month 0 (treated as month 1/January).
    
    Args:
        volcano_id: Volcano identifier
        eruptions: List of eruption dictionaries with 'year' and optional 'month' keys
        project_path: Path to project data directory
        
    Returns:
        Tuple of (time_intervals_months, time_intervals_years, eruption_dates)
        where eruption_dates are in decimal years (year + month/12)
        
    Raises:
        ValidationError: If inputs are invalid
    """
    try:
        if  len(eruptions) < 2:
            raise ValidationError("eruptions must be list with at least 2 events")
        
        # Extract year and month, handling edge cases
        eruption_dates = []
        
        for eruption in eruptions.data:
            try:
                year = int(eruption.get('year', 0))
                month = int(eruption.get('month', 1))
                
                
                
                # Handle month 0 (treat as month 1 / January)
                if month <= 0:
                    month = 1
                # Cap month at 12
                elif month > 12:
                    month = 12
                
                # Convert to decimal year
                decimal_year = year + (month - 1) / 12.0
                eruption_dates.append(decimal_year)
            except (ValueError, TypeError):
                continue
        
        # Sort by date
        eruption_dates.sort()
        
        if len(eruption_dates) < 2:
            raise ValidationError("Insufficient valid eruption dates with year and month")
        
        # Calculate intervals in months and years
        time_intervals_months = []
        time_intervals_years = []
        
        for i in range(1, len(eruption_dates)):
            # Interval in decimal years
            interval_years = eruption_dates[i] - eruption_dates[i-1]
            # Convert to months (multiply by 12)
            interval_months = interval_years * 12.0
            
            # Ensure minimum interval of 1 month
            interval_months = max(interval_months, 1.0)
            interval_years = max(interval_years, 1.0 / 12.0)
            
            time_intervals_months.append(interval_months)
            time_intervals_years.append(interval_years)
        
        return time_intervals_months, time_intervals_years, eruption_dates
        
    except (ValidationError, ValueError) as e:
        
        exc_type, exc_obj, exc_tb = sys.exc_info()
        log_print(f"Error in time_slots : {e}", volcano_id, "temporal_analysis", project_path)
        
        print(f"Error type: {exc_type.__name__}")
        print(f"Error message: {e}")
        print(f"Line number: {exc_tb.tb_lineno}")
        raise DataError(f"Error in time_slots: {e}")

def temporal_analysis(volcano_id: str = None, 
                     period: Optional[float] = None,
                     complete: Optional[int] = None,
                     project_path: str = None
                        ) -> Optional[Dict]:
    """
    Analyze temporal distribution of eruptions.
    
    Fits an exponential Weibull distribution to time intervals between eruptions.
    Can accept either raw time interval data or pre-computed mean/variance parameters.
    
    Args:
        volcano_id: Volcano identifier for saving results (optional)
        period: Time period for which to calculate probability (optional)
        complete: Year from which eruption recording is considered complete (optional)
        project_path: Path to project data directory
        years: List of eruption years (required if complete is provided)
        
    Returns:
        Dictionary with temporal analysis results (floc, beta, floc1, alpha) 
        if successful, None otherwise. If period is provided, includes 'probability' key.
        
    Raises:
        ValidationError: If inputs are invalid
    """
    try:

        time_slots_data = mdb.get_eruptions_volcano(volcano_id)
     
        print(f"Retrieved {len(time_slots_data)} eruptions for volcano {volcano_id}")

        time_slots_data=time_slots(volcano_id,time_slots_data, project_path) # Get intervals in months
        
        print(f"Calculated {len(time_slots_data[0])} time intervals between eruptions")
        
        
        # Filter data by complete year if provided
        if complete is not None:
            if complete is None:
                raise ValidationError("years list required when complete parameter is provided")
            if not isinstance(complete, list) or len(complete) == 0:
                raise ValidationError("complete must be non-empty list")
            
            
            # Filter to eruptions from complete year onwards
            
            filtered_intervals = []
           
            for i in range(len(time_slots_data[0])):
                # complete[i] is the start year of this interval, complete[i+1] is the end year
                if complete[0] <= time_slots_data[2][i]:
                    filtered_intervals.append(time_slots_data[0][i])


            if len(filtered_intervals) < 1:
                log_print(  
                    f"No eruptions after completeness year {complete}",
                    volcano_id if volcano_id else "", "temporal_analysis", project_path
                )
                return None
            
            time_slots_data = np.array(filtered_intervals)
            print(f"Filtered to {len(time_slots_data)} intervals after completeness year {complete}")
            log_print(
                f"Filtering to eruptions from year {complete} onwards: "
                f"{len(filtered_intervals)} intervals",
                volcano_id if volcano_id else "", "temporal_analysis", project_path
            )
        else:
            time_slots_data = np.array(time_slots_data[0])  # Use all intervals if no completeness filter
        # Fit exponential Weibull

        try:
            fit_candidates = []

            try:
                # scipy.stats.exponweib parameter order: a, c, loc, scale
                a_shape, c_shape, loc_param, scale_param = stats.exponweib.fit(time_slots_data, floc=0)
                d_exp, p_exp = stats.kstest(
                    time_slots_data,
                    lambda values: stats.exponweib.cdf(values, a_shape, c_shape,
                                                       loc=loc_param, scale=scale_param)
                )
                fit_candidates.append({
                    'distribution': 'exponweib',
                    'params': (float(a_shape), float(c_shape), float(loc_param), float(scale_param)),
                    'ks_stat': float(d_exp),
                    'ks_p': float(p_exp)
                })
            except Exception as e:
                log_print(f"Warning: ExponWeib fit failed: {e}", volcano_id if volcano_id else "",
                         "temporal_analysis", project_path)

            try:
                shape_w, loc_w, scale_w = stats.weibull_min.fit(time_slots_data, floc=0)
                d_w, p_w = stats.kstest(
                    time_slots_data,
                    lambda values: stats.weibull_min.cdf(values, shape_w, loc=loc_w, scale=scale_w)
                )
                fit_candidates.append({
                    'distribution': 'weibull_min',
                    'params': (float(shape_w), float(loc_w), float(scale_w)),
                    'ks_stat': float(d_w),
                    'ks_p': float(p_w)
                })
            except Exception as e:
                log_print(f"Warning: Weibull-min fit failed: {e}", volcano_id if volcano_id else "",
                         "temporal_analysis", project_path)

            if len(fit_candidates) == 0:
                raise DataError("Could not fit any temporal Weibull model")

            best_candidate = min(fit_candidates, key=lambda item: (item['ks_stat'], -item['ks_p']))
            temporal_result = {
                'distribution': best_candidate['distribution'],
                'ks_statistic': best_candidate['ks_stat'],
                'ks_pvalue': best_candidate['ks_p']
            }

            if best_candidate['distribution'] == 'exponweib':
                a_shape, c_shape, loc_param, scale_param = best_candidate['params']
                temporal_result.update({
                    'a_shape': float(a_shape),
                    'c_shape': float(c_shape),
                    'loc': float(loc_param),
                    'scale': float(scale_param),
                    # Legacy keys kept for backwards compatibility in saved outputs
                    'floc': float(loc_param),
                    'beta': float(c_shape),
                    'floc1': float(a_shape),
                    'alpha': float(scale_param)
                })
            else:
                shape_w, loc_w, scale_w = best_candidate['params']
                temporal_result.update({
                    'shape': float(shape_w),
                    'loc': float(loc_w),
                    'scale': float(scale_w),
                    # Legacy keys kept for backwards compatibility in saved outputs
                    'floc': float(loc_w),
                    'beta': float(shape_w),
                    'floc1': 1.0,
                    'alpha': float(scale_w)
                })
            
            # Calculate mean and variance from time_slots_data
            mean_param = float(np.mean(time_slots_data))
            variance_param = float(np.var(time_slots_data))

            print(f"Calculated mean: {mean_param:.4f}, variance: {variance_param:.4f} from time_slots_data. Miu {mean_param/variance_param:.4f}")    
            
            # Add mean and variance if provided
            if mean_param is not None:
                temporal_result['mean_param'] = float(mean_param)
            if variance_param is not None:
                temporal_result['variance_param'] = float(variance_param)
            
            # Calculate empirical statistics
            empirical_mean = float(np.mean(time_slots_data))
            empirical_std = float(np.std(time_slots_data))
            temporal_result['empirical_mean'] = empirical_mean
            temporal_result['empirical_std'] = empirical_std
            
            
            
            # Calculate probability for given period if provided
            if period is not None:
                if not isinstance(period, (int, float)) or period <= 0:
                    raise ValidationError("period must be a positive number")
                
                # Calculate CDF at the given period: P(T <= period)
                period=period*12 # Convert years to months for CDF calculation
                if temporal_result['distribution'] == 'exponweib':
                    prob = stats.exponweib.cdf(
                        period,
                        temporal_result['a_shape'],
                        temporal_result['c_shape'],
                        loc=temporal_result['loc'],
                        scale=temporal_result['scale']
                    )
                else:
                    prob = stats.weibull_min.cdf(
                        period,
                        temporal_result['shape'],
                        loc=temporal_result['loc'],
                        scale=temporal_result['scale']
                    )
                temporal_result['period'] = float(period)
                temporal_result['probability'] = float(prob)
                
                print(f"Probability of eruption within {period/12} years: {prob:.4f} ({prob*100:.2f}%)")

            if volcano_id:
                x_years, temporal_pdf, temporal_cdf = temporal_distribution_curves(time_slots_data, temporal_result)
                sorted_data_years = np.sort(np.array(time_slots_data, dtype=float) / 12.0)
                empirical_cdf = np.arange(1, len(sorted_data_years) + 1) / len(sorted_data_years)

                temporal_db_payload = {
                    'fit': temporal_result,
                    'grid_years': x_years,
                    'pdf_fitted': temporal_pdf,
                    'cdf_fitted': temporal_cdf,
                    'intervals_months': np.array(time_slots_data, dtype=float),
                    'empirical_cdf_years': sorted_data_years,
                    'empirical_cdf_values': empirical_cdf
                }

                try:
                    distribution_name = str(temporal_result.get('distribution') or 'unknown')
                    mdb.upsert_epdf(
                        volcano_id,
                        'temporal',
                        to_serializable(temporal_db_payload),
                        distribution_name
                    )
                    log_print(f"Temporal EPDF stored: {distribution_name}",
                              volcano_id, "temporal_analysis", project_path)
                except Exception as e:
                    log_print(f"Warning: Could not store temporal EPDF: {e}",
                              volcano_id, "temporal_analysis", project_path)
            
            # Plot temporal distribution
            _plot_temporal_distribution(time_slots_data, temporal_result, volcano_id, project_path)
            
            # Save results to file
            if volcano_id:
                _save_temporal_results(temporal_result, time_slots_data, volcano_id, project_path)
            print(f"Temporal analysis completed for volcano {volcano_id}, {temporal_result}")
            return prob if period is not None else None
        except Exception as e:
            print(f"Warning: Exponential Weibull fit failed: {e}")
            return None
            
    except (ValidationError, Exception) as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        log_print(f"Error in pdfs_volcano: {e}", volcano_id, "temporal_analysis", project_path)
        
        print(f"Error type: {exc_type.__name__}")
        print(f"Error message: {e}")
        print(f"Line number: {exc_tb.tb_lineno}")


def temporal_distribution_curves(time_slots_data: np.ndarray,
                                 temporal_result: Dict,
                                 n_points: int = 200) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Compute fitted temporal PDF and CDF curves in years.

    Args:
        time_slots_data: Time intervals in months
        temporal_result: Dictionary with fitted model parameters
        n_points: Number of points for output grid

    Returns:
        Tuple of (x_years, pdf_values, cdf_values)
    """
    time_slots_data = validate_array_input(time_slots_data, "time_slots_data", min_length=1)

    max_years = float(np.max(time_slots_data) / 12.0)
    max_years = max(max_years, 1e-6)

    x_years = np.linspace(0, max_years * 1.05, n_points)
    x_months = x_years * 12.0

    distribution_name = temporal_result.get('distribution', 'exponweib')
    loc_param = temporal_result.get('loc', temporal_result.get('floc', 0.0))
    scale_param = temporal_result.get('scale', temporal_result.get('alpha', 1.0))

    if distribution_name == 'weibull_min':
        shape_w = temporal_result.get('shape', temporal_result.get('beta', 1.0))
        pdf = stats.weibull_min.pdf(x_months, shape_w, loc=loc_param, scale=scale_param)
        fitted_cdf = stats.weibull_min.cdf(x_months, shape_w, loc=loc_param, scale=scale_param)
    else:
        a_shape = temporal_result.get('a_shape', temporal_result.get('floc1', 1.0))
        c_shape = temporal_result.get('c_shape', temporal_result.get('beta', 1.0))
        pdf = stats.exponweib.pdf(x_months, a_shape, c_shape, loc=loc_param, scale=scale_param)
        fitted_cdf = stats.exponweib.cdf(x_months, a_shape, c_shape, loc=loc_param, scale=scale_param)

    return x_years, pdf, fitted_cdf


def _plot_temporal_distribution(time_slots_data: np.ndarray, 
                               temporal_result: Dict, 
                               volcano_id: Optional[str] = None,
                               project_path: Optional[str] = None) -> None:
    """
    Plot cumulative temporal distribution (CDF) with fitted Weibull model.
    
    Args:
        time_slots_data: Time intervals data
        temporal_result: Dictionary with Weibull parameters
        volcano_id: Volcano identifier for file saving
        project_path: Path to project data directory
    """
    try:
        fig, axes = plt.subplots(1, 2, figsize=(14, 5))

        # Convert time intervals from months to years for plotting
        time_slots_years = np.array(time_slots_data) / 12.0

        # Histogram of time intervals (years)
        axes[0].hist(time_slots_years, bins=20, density=True, alpha=0.6,
                    color='skyblue', edgecolor='black', label='Empirical')

        distribution_name = temporal_result.get('distribution', 'exponweib')
        x, pdf, fitted_cdf = temporal_distribution_curves(time_slots_data, temporal_result)
        fit_label = 'Weibull-Min Fit' if distribution_name == 'weibull_min' else 'Exponential Weibull Fit'
        cdf_label = 'Weibull-Min CDF' if distribution_name == 'weibull_min' else 'Exponential Weibull CDF'

        axes[0].plot(x, pdf, 'r-', linewidth=2, label=fit_label)
        axes[0].set_xlabel('Time Interval (years)', fontsize=12)
        axes[0].set_ylabel('PDF', fontsize=12)
        axes[0].set_title('Temporal Distribution of Eruptions', fontsize=12)
        axes[0].legend()
        axes[0].grid(True, alpha=0.3)

        # CDF comparison
        sorted_data_years = np.sort(time_slots_years)
        empirical_cdf = np.arange(1, len(sorted_data_years) + 1) / len(sorted_data_years)
        axes[1].plot(sorted_data_years, empirical_cdf, 'bo', alpha=0.5, label='Empirical CDF')

        axes[1].plot(x, fitted_cdf, 'r-', linewidth=2, label=cdf_label)
        axes[1].set_xlabel('Time Interval (years)', fontsize=12)
        axes[1].set_ylabel('CDF', fontsize=12)
        axes[1].set_title('Cumulative Distribution Function', fontsize=12)
        axes[1].legend()
        axes[1].grid(True, alpha=0.3)

        plt.tight_layout()

        # Save figure if volcano_id provided
        if volcano_id:
            print(f"Saving temporal distribution plot for volcano {volcano_id}")
            directory = create_output_directory(volcano_id, "PDFs", project_path)
            filepath = os.path.join(directory, f"Temporal_distribution_{volcano_id}.png")
            plt.savefig(filepath, dpi=300, bbox_inches='tight')
            log_print(f"Temporal distribution plot saved: {filepath}", volcano_id, "Temporal", project_path)

        plt.show()
    except Exception as e:
        warnings.warn(f"Could not plot temporal distribution: {e}")


def _save_temporal_results(temporal_result: Dict, time_slots_data: np.ndarray,
                          volcano_id: str, project_path: str) -> None:
    """
    Save temporal analysis results to text file.
    
    Args:
        temporal_result: Dictionary with fitted parameters
        time_slots_data: Original time interval data
        volcano_id: Volcano identifier
        project_path: Path to project data directory
    """
    try:
        directory = create_output_directory(volcano_id, "PDFs", project_path)
        filepath = os.path.join(directory, f"Temporal_parameters_{volcano_id}.txt")
        
        with open(filepath, 'w') as f:
            f.write("=" * 60 + "\n")
            f.write("TEMPORAL ANALYSIS - ERUPTION TIME INTERVALS\n")
            f.write("=" * 60 + "\n\n")

            distribution_name = temporal_result.get('distribution', 'exponweib')

            f.write(f"FITTED DISTRIBUTION: {distribution_name}\n")
            f.write("-" * 60 + "\n")
            if distribution_name == 'weibull_min':
                f.write(f"Shape (k):              {temporal_result.get('shape', float('nan')):.6f}\n")
                f.write(f"Location (loc):         {temporal_result.get('loc', float('nan')):.6f}\n")
                f.write(f"Scale:                  {temporal_result.get('scale', float('nan')):.6f}\n")
            else:
                f.write(f"Shape a:                {temporal_result.get('a_shape', float('nan')):.6f}\n")
                f.write(f"Shape c:                {temporal_result.get('c_shape', float('nan')):.6f}\n")
                f.write(f"Location (loc):         {temporal_result.get('loc', float('nan')):.6f}\n")
                f.write(f"Scale:                  {temporal_result.get('scale', float('nan')):.6f}\n")
            f.write(f"KS statistic:           {temporal_result.get('ks_statistic', float('nan')):.6f}\n")
            f.write(f"KS p-value:             {temporal_result.get('ks_pvalue', float('nan')):.6f}\n\n")
            
            if 'mean_param' in temporal_result:
                f.write(f"Provided mean parameter: {temporal_result['mean_param']:.6f}\n")
            if 'variance_param' in temporal_result:
                f.write(f"Provided variance parameter: {temporal_result['variance_param']:.6f}\n\n")
            
            f.write("EMPIRICAL STATISTICS FROM DATA:\n")
            f.write("-" * 60 + "\n")
            f.write(f"Number of intervals:    {len(time_slots_data)}\n")
            f.write(f"Empirical mean:         {temporal_result.get('empirical_mean', 'N/A'):.6f} months\n")
            f.write(f"Empirical std dev:      {temporal_result.get('empirical_std', 'N/A'):.6f} months\n")
            f.write(f"Empirical variance:     {np.var(time_slots_data):.6f}\n")
            f.write(f"Minimum interval:       {np.min(time_slots_data):.4f} months\n")
            f.write(f"Maximum interval:       {np.max(time_slots_data):.4f} months\n")
            f.write(f"Median interval:        {np.median(time_slots_data):.4f} months\n\n")
            
         
        
        log_print(f"Temporal parameters saved to {filepath}", volcano_id, "Temporal", project_path)
    except IOError as e:
        log_print(f"Warning: Could not save temporal results: {e}", volcano_id, "Temporal", project_path)


# ============================================================================
# WEIBULL ANALYSIS
# ============================================================================

def beta_function(array: np.ndarray, beta_param: float) -> float:
    """
    Calculate Weibull beta function for Newton-Raphson iteration.
    
    Args:
        array: Time intervals array
        beta_param: Current beta parameter estimate
        
    Returns:
        Function value for Newton-Raphson method
    """
    try:
        array = validate_array_input(array, "array", min_length=1)
        validate_positive_values(array, "array", allow_zero=False)
        
        if beta_param <= 0:
            raise ValidationError("beta_param must be positive")
        
        n = len(array)
        first_sum = np.sum((array ** beta_param) * np.log(array))
        second_sum = np.sum(array ** beta_param)
        third_sum = np.sum(np.log(array))
        
        if second_sum <= 0:
            raise ValidationError("Sum of weighted array is non-positive")
        
        fb = (first_sum / second_sum) - (1 / beta_param) - ((1 / n) * third_sum)
        return fb
        
    except ValidationError:
        return -10000


def beta_d_function(array: np.ndarray, beta_param: float) -> float:
    """
    Calculate derivative of Weibull beta function.
    
    Args:
        array: Time intervals array
        beta_param: Current beta parameter estimate
        
    Returns:
        Derivative value for Newton-Raphson method
    """
    try:
        array = validate_array_input(array, "array", min_length=1)
        validate_positive_values(array, "array", allow_zero=False)
        
        if beta_param <= 0:
            raise ValidationError("beta_param must be positive")
        
        n = len(array)
        log_array = np.log(array)
        
        first_sum = np.sum((array ** beta_param) * (log_array ** 2))
        second_sum = np.sum((array ** beta_param) * 
                           (beta_param * log_array - 1))
        third_sum = np.sum(log_array)
        fourth_sum = np.sum(log_array * (array ** beta_param))
        
        fbd = (first_sum - ((1 / (beta_param ** 2)) * second_sum) -
               ((1 / n) * third_sum * fourth_sum))
        
        return fbd
        
    except ValidationError:
        return -10000


def newton_raphson(array: np.ndarray, x0: float, epsilon: float = NEWTON_EPSILON,
                  max_iter: int = NEWTON_MAX_ITERATIONS) -> float:
    """
    Newton-Raphson method to find Weibull shape parameter.
    
    Args:
        array: Time intervals array
        x0: Initial guess for beta
        epsilon: Convergence threshold
        max_iter: Maximum iterations
        
    Returns:
        Estimated beta parameter
        
    Raises:
        ValidationError: If inputs are invalid
    """
    try:
        array = validate_array_input(array, "array", min_length=1)
        
        if not isinstance(x0, (int, float)) or x0 <= 0:
            raise ValidationError("x0 must be positive number")
        if not isinstance(epsilon, (int, float)) or epsilon <= 0:
            raise ValidationError("epsilon must be positive")
        if not isinstance(max_iter, int) or max_iter <= 0:
            raise ValidationError("max_iter must be positive integer")
        
        xn = float(x0)
        
        for iteration in range(max_iter):
            fxn = beta_function(array, xn)
            
            if abs(fxn) < epsilon:
                log_print(f"Convergence in {iteration} iterations", "", "Newton-Raphson")
                return xn
            
            dfxn = beta_d_function(array, xn)
            
            if fxn == -10000 or dfxn == -10000:
                return x0
            if abs(dfxn) < 1e-10:
                return x0
            
            xn = xn - (fxn / dfxn)
        
        return xn
        
    except ValidationError as e:
        warnings.warn(f"Error in newton_raphson: {e}")
        return x0


def alpha_beta_wb(array_intervals: List[float], time_window: float,
                 beta_initial: float) -> Optional[Tuple[float, float]]:
    """
    Estimate Weibull alpha and beta parameters.
    
    Args:
        array_intervals: Time intervals between events
        time_window: Time window for analysis
        beta_initial: Initial beta estimate
        
    Returns:
        Tuple of (alpha, beta) or None if invalid
        
    Raises:
        ValidationError: If inputs are invalid
    """
    try:
        array_intervals = validate_array_input(array_intervals, "array_intervals",
                                              min_length=1)
        
        if not isinstance(time_window, (int, float)) or time_window <= 1:
            raise ValidationError("time_window must be greater than 1")
        
        # Filter data to time window
        beta_array = []
        for ti in array_intervals:
            if ti <= 0:
                ti = 1
            if ti <= time_window:
                beta_array.append(ti)
        
        if len(beta_array) == 0:
            log_print("No data in time window", "", "Weibull")
            return None
        
        # Estimate beta using Newton-Raphson
        beta = newton_raphson(np.array(beta_array), beta_initial)
        
        if beta <= 0:
            return None
        
        # Calculate alpha
        sum_alpha = np.sum(np.array(beta_array) ** beta)
        alpha = (sum_alpha / len(beta_array)) ** (1 / beta)
        
        return alpha, beta
        
    except (ValidationError, Exception) as e:
        warnings.warn(f"Error in alpha_beta_wb: {e}")
        return None


def weibull2d(time_window: float, alpha: float, beta: float) -> float:
    """
    Compute Weibull cumulative distribution value.
    
    Args:
        time_window: Time value for CDF evaluation
        alpha: Weibull scale parameter
        beta: Weibull shape parameter
        
    Returns:
        CDF value (probability)
        
    Raises:
        ValidationError: If inputs are invalid
    """
    try:
        if not all(isinstance(x, (int, float)) for x in [time_window, alpha, beta]):
            raise ValidationError("All parameters must be numbers")
        if not all(x > 0 for x in [time_window, alpha, beta]):
            raise ValidationError("All parameters must be positive")
        
        vt = 1 - math.exp(-(time_window / alpha) ** beta)
        return vt
        
    except ValidationError as e:
        warnings.warn(f"Error in weibull2d: {e}")
        return 0.0


# ============================================================================
# MAIN PDF ANALYSIS
# ============================================================================

def pdfs_volcano(volcano_id: str, phi,project_path: str, temporal ) -> None:
    """
    Comprehensive PDF analysis for volcanic eruption energy.
    
    Computes energy distributions, fits statistical models, analyzes
    lava/tephra fractions, and generates visualizations.
    
    Args:
        volcano_id: Volcano identifier
        phi: Phi parameter value or None
        project_path: Path to project data directory
        
    Raises:
        ValidationError: If volcano_id is invalid
    """
    section = "ePDFs"
    
    try:
        validate_volcano_id(volcano_id)
        
        # Fetch data
        eruptions_energy = mdb.eruptions_energy(volcano_id)
        data_vol = mdb.volcano_data(volcano_id)
        
        if not eruptions_energy or not eruptions_energy.data:
            raise DataError("No eruption data available")
        
        volcano_name = data_vol.data[0].get("name", "Unknown")
        
        # Separate energies
        e_p, e_l, e_t, cne, count, years = eruptions_divide(eruptions_energy.data)
       
        if len(e_t) <= MIN_ERUPTIONS_PDF:
            log_print(
                f"Insufficient eruptions: {len(e_t) - 1} < {MIN_ERUPTIONS_PDF}",
                volcano_id, section, project_path
            )
            return
        
        # Create output directory
        create_output_directory(volcano_id, "PDFs", project_path)
        
        # Analyze lava fraction
        phi_list = []
        for t, l in zip(e_t, e_l):
            if t == 0 or not np.isfinite(l):
                continue
            phi_list.append(l / t)
        
        phi_fit_result = None
        if phi is not None:
            log_print(f"Using provided phi value {phi} for analysis", volcano_id, section, project_path)
        else:
            if len(phi_list) >= MIN_PHI_DATA_POINTS:
                result = best_fit_phi_and_plot(phi_list, volcano_id, project_path=project_path)
                if result:
                    phi_fit_result, _, _, _ = result
                    
                    # Store phi results in database
                    phi_params = {
                        'name': phi_fit_result.get('name'),
                        'params': phi_fit_result.get('params')
                    }
                    try:
                        phi_distribution = str(phi_fit_result.get('name') or 'unknown')
                        mdb.upsert_epdf(volcano_id, 'phi', to_serializable(phi_params), phi_distribution)
                        log_print(f"Phi EPDF stored: {phi_distribution}", volcano_id, section, project_path)
                    except Exception as e:
                        log_print(f"Warning: Could not store phi EPDF: {e}", volcano_id, section, project_path)
            else:
                log_print(f"Insufficient phi data: {len(phi_list)} < {MIN_PHI_DATA_POINTS}",
                        volcano_id, section, project_path)
        # Total energy PDF
        c_y, c_e = cumulative_energy(eruptions_energy.data, "energy")
        bin_centers, pdf_total, cdf_total = frequency_cal(e_t, ENERGY_BIN_LIMITS)
        
        if pdf_total is None:
            log_print("Could not calculate Total Energy PDF", volcano_id, section, project_path)
            return
        
        # Fit distributions
        limi = np.arange(min(bin_centers), max(bin_centers) + 10, 0.1)
        results, labels, dist_names, results_cdf = pdf_function(e_t, limi)
        best_f = best_fit(cdf_total, e_t, dist_names, bin_centers)
         
        # Generate distribution values using best_f and bin_centers
        best_fit_pdf = None
        if best_f is not None:
            try:
                dist_name = best_f[2]
                dist_params = tuple(float(p) for p in best_f[3])
                dist = getattr(stats, dist_name)
                best_fit_pdf = pdf_computation(dist, dist_params, bin_centers)
                
                log_print(f"Generated PDF values for {dist_name} distribution", 
                         volcano_id, section, project_path)
            except Exception as e:
                log_print(f"Warning: Could not generate distribution values: {e}",
                         volcano_id, section, project_path)
                best_fit_pdf = None 

        # Fallback to empirical PDF if fitted PDF is unavailable
        if best_fit_pdf is None:
            best_fit_pdf = np.array(pdf_total, dtype=float)
        
        # Marginal PDFs: always use extended grid for better extrapolation
        bin_centers_ext, pdf_total_ext = extend_pdf_range(bin_centers, best_fit_pdf, extension_factor=10)

        f_lava = np.zeros_like(bin_centers, dtype=float)
        f_tephra = np.zeros_like(bin_centers, dtype=float)
        marginal_available = True

        if phi_fit_result is not None:
            # Fitted distribution from data: use best_fit_dist for extrapolation
            f_lava_ext, f_tephra_ext = marginal_pdf_energy(e_t, phi_fit_result, 
                                                           bin_centers_ext, pdf_total_ext,
                                                           best_fit_dist=best_f, volcano_id=volcano_id,
                                                           section=section, project_path=project_path)
            # Interpolate back to original bin_centers
            f_lava = np.interp(bin_centers, bin_centers_ext, f_lava_ext, left=0.0, right=0.0)
            f_tephra = np.interp(bin_centers, bin_centers_ext, f_tephra_ext, left=0.0, right=0.0)
        elif phi is not None:
            # Point estimate (phi provided directly): use interpolation on extended grid
            f_lava, f_tephra = marginal_pdf_energy(e_t, {'name': 'point_estimate', 
                                                               'params': phi},
                                                           bin_centers, best_fit_pdf,
                                                           best_fit_dist=best_f, volcano_id=volcano_id,
                                                           section=section, project_path=project_path)
        else:
            marginal_available = False
            log_print(
                "Skipping marginal PDFs: no valid phi estimate or override available",
                volcano_id, section, project_path
            )

        # Multiply PDFs by temporal PDF if available
        absolute_pdf_total = None
        absolute_pdf_lava = None
        absolute_pdf_tephra = None
        temporal_pdf = None
        temporal_grid = None
        print(f"Temporal PDF available: {temporal is not None}, marginal PDFs available: {marginal_available}")
        if temporal is not None:
            # Retrieve temporal PDF and grid
            try:
                temporal_scale = float(np.ravel(np.array(temporal, dtype=float))[0])
                absolute_pdf_total = np.ravel(np.array(best_fit_pdf, dtype=float)) * temporal_scale
                # Marginal PDFs
                if marginal_available:
                    absolute_pdf_lava = np.ravel(np.array(f_lava, dtype=float)) * temporal_scale
                    absolute_pdf_tephra = np.ravel(np.array(f_tephra, dtype=float)) * temporal_scale
                
            except Exception as e:
                log_print(f"Error multiplying PDFs by temporal PDF: {e}", volcano_id, section, project_path)

        # Plot results
        title = f"Volcano: {volcano_name} Code: {volcano_id}\nTotal Energy"
        pplot(np.array(years), np.array(e_t), "Years", "Log10 Energy J",
               volcano_id, title, True, "Total_energy_per_event", project_path)
       
        
        # Plot best fit
        if best_f is not None:
            directory = create_output_directory(volcano_id, "PDFs", project_path)
            filepath = os.path.join(directory, f"PDF_energy_{volcano_id}.txt")
            params_clean = tuple(float(p) for p in best_f[3])
            
            with open(filepath, 'w') as f:
                f.write("Best-fit distribution for Energy\n")
                f.write("=" * 50 + "\n")
                f.write(f"Distribution: {best_f[2]}\n")
                f.write(f"Parameters: {params_clean}\n")
                f.write(f"KS statistic: {best_f[0]:.6f}\n")
                f.write(f"P-value: {best_f[1]:.6f}\n")
            
            # Plot best fit PDF
            title = f"Volcano: {volcano_name} Code: {volcano_id}\nBest Fit PDF (Total E)"
            plt_bar_dis(pdf_total, bin_centers, title, 
                       [pdf_computation(getattr(stats, best_f[2]), params_clean, limi)],
                       [best_f[2]], limi, volcano_id, True, "PDF_Best_Fit_Total_Energy", project_path)
            
            log_print(f"Best fit: {best_f[2]}", volcano_id, section, project_path)

        e_total_distribution = best_f[2] if best_f is not None else "empirical"
        e_total_params = {
            'ks_statistic': float(best_f[0]) if best_f is not None else None,
            'p_value': float(best_f[1]) if best_f is not None else None,
            'parameters': tuple(float(p) for p in best_f[3]) if best_f is not None else None,
            'bin_centers': bin_centers,
            'pdf_empirical': pdf_total,
            'cdf_empirical': cdf_total,
            'pdf_fitted': best_fit_pdf
        }
        try:
            mdb.upsert_epdf(
                volcano_id,
                'E_total',
                to_serializable(e_total_params),
                e_total_distribution
            )
            log_print(f"E_total EPDF stored: {e_total_distribution}", volcano_id, section, project_path)
        except Exception as e:
            log_print(f"Warning: Could not store E_total EPDF: {e}", volcano_id, section, project_path)
        
        # Plot marginal PDFs only when available
        if marginal_available:
            plt.figure(figsize=(10, 6))
            plt.plot(bin_centers, f_lava, label="Lava Energy PDF", color="orange", lw=2)
            plt.plot(bin_centers, f_tephra, label="Tephra Energy PDF", color="brown", lw=2)
            plt.xlabel("Energy (J)")
            plt.ylabel("PDF")
            plt.title(f"{volcano_name} - Marginal PDFs")
            plt.legend()
            plt.grid(True, alpha=0.3)
            plt.tight_layout()

            directory = create_output_directory(volcano_id, "PDFs", project_path)
            filepath = os.path.join(directory, f"Partitioned_pdf_{volcano_id}.png")
            plt.savefig(filepath, dpi=300, bbox_inches='tight')
            plt.show()

            marginal_distribution = 'point_estimate'
            if phi_fit_result is not None:
                marginal_distribution = str(phi_fit_result.get('name') or 'unknown')

            marginal_params = {
                'bin_centers': bin_centers,
                'pdf_lava': f_lava,
                'pdf_tephra': f_tephra,
                'phi_value': float(phi) if phi is not None else None,
                'phi_distribution': phi_fit_result.get('name') if phi_fit_result is not None else None,
                'phi_parameters': phi_fit_result.get('params') if phi_fit_result is not None else None
            }

            try:
                mdb.upsert_epdf(
                    volcano_id,
                    'marginal',
                    to_serializable(marginal_params),
                    marginal_distribution
                )
                log_print(f"Marginal EPDF stored: {marginal_distribution}", volcano_id, section, project_path)
            except Exception as e:
                log_print(f"Warning: Could not store marginal EPDF: {e}", volcano_id, section, project_path)

        # Plot absolute PDFs if computed
        if absolute_pdf_total is not None:
            plt.figure(figsize=(10, 6))
            plt.plot(bin_centers, absolute_pdf_total, label="Absolute Total PDF", color="blue", lw=2)
            plt.xlabel("Year")
            plt.ylabel("Absolute PDF")
            plt.title(f"{volcano_name} - Absolute Total PDF")
            plt.legend()
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            directory = create_output_directory(volcano_id, "PDFs", project_path)
            filepath = os.path.join(directory, f"Absolute_pdf_total_{volcano_id}.png")
            plt.savefig(filepath, dpi=300, bbox_inches='tight')
            plt.show()
            # Store in DB
            try:
                mdb.upsert_epdf(
                    volcano_id,
                    'absolute_total',
                    to_serializable({
                        'bin_centers': bin_centers,
                        'years': years,
                        'absolute_pdf_total': absolute_pdf_total
                    }),
                    'absolute_total'
                )
                log_print("Absolute total PDF stored in DB", volcano_id, section, project_path)
            except Exception as e:
                log_print(f"Warning: Could not store absolute total PDF: {e}", volcano_id, section, project_path)

        if absolute_pdf_lava is not None:
            plt.figure(figsize=(10, 6))
            plt.plot(bin_centers, absolute_pdf_lava, label="Absolute Lava PDF", color="orange", lw=2)
            plt.xlabel("Year")
            plt.ylabel("Absolute PDF")
            plt.title(f"{volcano_name} - Absolute Lava PDF")
            plt.legend()
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            directory = create_output_directory(volcano_id, "PDFs", project_path)
            filepath = os.path.join(directory, f"Absolute_pdf_lava_{volcano_id}.png")
            plt.savefig(filepath, dpi=300, bbox_inches='tight')
            plt.show()
            # Store in DB
            try:
                mdb.upsert_epdf(
                    volcano_id,
                    'absolute_lava',
                    to_serializable({
                        'bin_centers': bin_centers,
                        'years': years,
                        'absolute_pdf_lava': absolute_pdf_lava
                    }),
                    'absolute_lava'
                )
                log_print("Absolute lava PDF stored in DB", volcano_id, section, project_path)
            except Exception as e:
                log_print(f"Warning: Could not store absolute lava PDF: {e}", volcano_id, section, project_path)

        if absolute_pdf_tephra is not None:
            plt.figure(figsize=(10, 6))
            plt.plot(bin_centers, absolute_pdf_tephra, label="Absolute Tephra PDF", color="brown", lw=2)
            plt.xlabel("Year")
            plt.ylabel("Absolute PDF")
            plt.title(f"{volcano_name} - Absolute Tephra PDF")
            plt.legend()
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            directory = create_output_directory(volcano_id, "PDFs", project_path)
            filepath = os.path.join(directory, f"Absolute_pdf_tephra_{volcano_id}.png")
            plt.savefig(filepath, dpi=300, bbox_inches='tight')
            plt.show()
            # Store in DB
            try:
                mdb.upsert_epdf(
                    volcano_id,
                    'absolute_tephra',
                    to_serializable({
                        'bin_centers': bin_centers,
                        'years': years,
                        'absolute_pdf_tephra': absolute_pdf_tephra
                    }),
                    'absolute_tephra'
                )
                log_print("Absolute tephra PDF stored in DB", volcano_id, section, project_path)
            except Exception as e:
                log_print(f"Warning: Could not store absolute tephra PDF: {e}", volcano_id, section, project_path)

        log_print("PDF analysis complete", volcano_id, section, project_path)
        
    except (ValidationError, DataError, Exception) as e:
        log_print(f"Error in pdfs_volcano: {e}", volcano_id, section, project_path)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(f"Error type: {exc_type.__name__}")
        print(f"Error message: {e}")
        print(f"Line number: {exc_tb.tb_lineno}")
        raise


# ============================================================================
# CLUSTERING ANALYSIS
# ============================================================================

def clustering(volcano_id: str, project_path: str) -> None:
    """
    Perform clustering analysis on cumulative eruption energy.
    
    Uses hierarchical clustering and K-means with silhouette analysis
    to identify distinct patterns in eruption behavior.
    
    Args:
        volcano_id: Volcano identifier
        
    Raises:
        ValidationError: If volcano_id is invalid
    """
    section = "clustering"
    
    try:
        validate_volcano_id(volcano_id)
        
        # Fetch data
        eruptions_energy = mdb.eruptions_energy(volcano_id)
        if not eruptions_energy or not eruptions_energy.data:
            raise DataError("No eruption data available")
        
        c_y, c_e = cumulative_energy(eruptions_energy.data, "energy")
        data = np.column_stack((np.array(c_y), np.array(c_e)))
        
        if len(data) < 4:
            log_print(f"Insufficient data for clustering: {len(data)} points",
                     volcano_id, section, project_path)
            return
        
        # Create output directory
        directory = create_output_directory(volcano_id, "Clustering", project_path)
        
        # Elbow and silhouette analysis
        max_clusters = max(3, min(int(len(c_y) / 2), 10))
        wcss = []
        silhouette_scores = []
        cluster_range = list(range(2, max_clusters + 1))
        
        for n_clusters in cluster_range:
            kmeans = KMeans(n_clusters=n_clusters, init='k-means++', 
                          random_state=0, n_init=10)
            kmeans.fit(data)
            wcss.append(kmeans.inertia_)
            
            score = silhouette_score(data, kmeans.labels_)
            silhouette_scores.append(score)
        
        # Find optimal clusters
        try:
            kl = KneeLocator(cluster_range, wcss, curve="convex", 
                           direction="decreasing")
            elbow_clusters = kl.elbow if kl.elbow else cluster_range[0]
        except Exception:
            elbow_clusters = cluster_range[0]
        
        silhouette_clusters = cluster_range[np.argmax(silhouette_scores)]
        
        log_print(f"Elbow: {elbow_clusters}", volcano_id, section, project_path)
        log_print(f"Silhouette optimal: {silhouette_clusters}", volcano_id, section, project_path)
        
        # Plot elbow
        plt.figure(figsize=(10, 6))
        plt.plot(cluster_range, wcss, 'bo-')
        if elbow_clusters:
            plt.axvline(x=elbow_clusters, color='r', linestyle='--', label='Elbow')
        plt.xlabel('Number of clusters')
        plt.ylabel('Inertia')
        plt.title('Elbow Test')
        plt.legend()
        plt.grid(True, alpha=0.3)
        filepath = os.path.join(directory, f"{volcano_id}_elbow_test.png")
        plt.savefig(filepath, dpi=300, bbox_inches='tight')
        plt.show()
        
        # Plot silhouette
        plt.figure(figsize=(10, 6))
        plt.plot(cluster_range, silhouette_scores, 'go-')
        plt.axvline(x=silhouette_clusters, color='r', linestyle='--', label='Max')
        plt.xlabel('Number of clusters')
        plt.ylabel('Silhouette Coefficient')
        plt.title('Silhouette Analysis')
        plt.legend()
        plt.grid(True, alpha=0.3)
        filepath = os.path.join(directory, f"{volcano_id}_silhouette_analysis.png")
        plt.savefig(filepath, dpi=300, bbox_inches='tight')
        plt.show()
        
        # Perform clustering with optimal parameters
        for n_clusters in [elbow_clusters, silhouette_clusters]:
            if not n_clusters or n_clusters < 2:
                continue
            
            n_clusters = int(n_clusters)
            
            # Hierarchical
            hierarchical = AgglomerativeClustering(n_clusters=n_clusters,
                                                  linkage='ward')
            labels_h = hierarchical.fit_predict(data)
            
            # K-means
            kmeans = TimeSeriesKMeans(n_clusters=n_clusters, metric="dtw")
            labels_k = kmeans.fit_predict(data)
            
            # Plot results
            _plot_clustering_results(c_y, c_e, labels_h, labels_k, n_clusters,
                                    directory, volcano_id)
            
            # Calculate statistics
            _save_clustering_stats(data, labels_h, labels_k, n_clusters,
                                  directory, volcano_id)
        
        log_print("Clustering analysis complete", volcano_id, section, project_path)
        
    except (ValidationError, DataError, Exception) as e:
        log_print(f"Error in clustering: {e}", volcano_id, section, project_path)
        raise


def _plot_clustering_results(c_y: List[int], c_e: List[float], 
                             labels_h: np.ndarray, labels_k: np.ndarray,
                             n_clusters: int, directory: str,
                             volcano_id: str) -> None:
    """Plot clustering results."""
    try:
        # Hierarchical
        plt.figure(figsize=(10, 6))
        plt.scatter(c_y, c_e, c=labels_h, cmap='viridis', s=50)
        plt.xlabel('Year')
        plt.ylabel('Cumulative Energy (Log10)')
        plt.title(f'Hierarchical Clustering (k={n_clusters})')
        plt.colorbar(label='Cluster')
        plt.grid(True, alpha=0.3)
        filepath = os.path.join(directory, f"{volcano_id}_hierarchical_k{n_clusters}.png")
        plt.savefig(filepath, dpi=300, bbox_inches='tight')
        plt.show()
        
        # K-means
        plt.figure(figsize=(10, 6))
        plt.scatter(c_y, c_e, c=labels_k, cmap='viridis', s=50)
        plt.xlabel('Year')
        plt.ylabel('Cumulative Energy (Log10)')
        plt.title(f'K-means DTW Clustering (k={n_clusters})')
        plt.colorbar(label='Cluster')
        plt.grid(True, alpha=0.3)
        filepath = os.path.join(directory, f"{volcano_id}_kmeans_k{n_clusters}.png")
        plt.savefig(filepath, dpi=300, bbox_inches='tight')
        plt.show()
    except Exception as e:
        warnings.warn(f"Could not plot clustering results: {e}")


def _save_clustering_stats(data: np.ndarray, labels_h: np.ndarray,
                          labels_k: np.ndarray, n_clusters: int,
                          directory: str, volcano_id: str) -> None:
    """Save clustering statistics."""
    try:
        df = pd.DataFrame(data, columns=['Year', 'Cumulative Energy'])
        
        # Hierarchical stats
        df['Cluster_H'] = labels_h
        stats_h = df.groupby('Cluster_H').agg({
            'Year': ['mean', 'std', 'count'],
            'Cumulative Energy': ['mean', 'std']
        })
        
        # K-means stats
        df['Cluster_K'] = labels_k
        stats_k = df.groupby('Cluster_K').agg({
            'Year': ['mean', 'std', 'count'],
            'Cumulative Energy': ['mean', 'std']
        })
        
        # Save to file
        filepath = os.path.join(directory, f"{volcano_id}_clustering_stats.txt")
        with open(filepath, 'w') as f:
            f.write(f"Clustering Statistics (k={n_clusters})\n")
            f.write("=" * 50 + "\n\n")
            f.write("Hierarchical Clustering\n")
            f.write(str(stats_h))
            f.write("\n\nK-means Clustering\n")
            f.write(str(stats_k))
    except Exception as e:
        warnings.warn(f"Could not save clustering stats: {e}")
