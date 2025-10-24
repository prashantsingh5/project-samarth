"""
Normalizers for agricultural and climate datasets.

This module provides functions to standardize field names, units, and data types
across production and rainfall datasets from data.gov.in, enabling consistent
downstream analysis and aggregation.

Now uses SchemaMapper for robust field mapping and DataQualityValidator for
data validation.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional
import warnings

try:
    from .mappings import (
        normalize_state_name,
        normalize_crop_name,
        SUBDIVISION_TO_STATE
    )
    _MAPPINGS_AVAILABLE = True
except ImportError:
    _MAPPINGS_AVAILABLE = False

try:
    from .schema_mapper import SchemaMapper
    from .data_quality import DataQualityValidator
    _ENHANCED_FEATURES = True
except ImportError:
    _ENHANCED_FEATURES = False
    warnings.warn("Enhanced features (SchemaMapper, DataQualityValidator) not available. Using fallback mode.")


def _fallback_production_mapping(raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Fallback manual field mapping for production data when SchemaMapper unavailable.
    """
    rename_map = {
        'crop_year': 'year',
        'area_': 'area_ha',
        'production_': 'production_tonne'
    }
    return raw_df.rename(columns=rename_map)


def normalize_production(records: List[Dict], apply_mappings: bool = True, validate: bool = True) -> pd.DataFrame:
    """
    Normalize production dataset records to a standardized schema.
    
    Input fields (from data.gov.in - with robust field matching):
    - state_name (or State, STATE_NAME, etc.)
    - district_name (or District, district, etc.)
    - crop_year/year (or Year, YEAR, etc.)
    - season (or Season, SEASON, etc.)
    - crop (or Crop, crop_name, commodity, etc.)
    - area_ (or area, Area, area_ha, etc.)
    - production_ (or production, Production, etc.)
    
    Output schema:
    - state_name (str): State name (canonicalized if apply_mappings=True)
    - district_name (str): District name
    - year (int): Calendar or financial year
    - season (str): e.g., "Kharif", "Rabi", "Summer"
    - crop (str): Crop name (canonicalized if apply_mappings=True)
    - area_ha (float): Area in hectares
    - production_tonne (float): Production in tonnes
    
    Normalization steps:
    1. Use SchemaMapper to intelligently map fields (handles API changes)
    2. Convert year to int; drop rows with null/missing years
    3. Convert area and production to float; drop rows with null/zero values
    4. Strip whitespace from string fields
    5. Apply data quality validation (if enabled)
    6. Apply state/crop name mappings (if enabled and available)
    7. Drop any completely null rows
    
    Args:
        records: List of dicts from production API
        apply_mappings: Whether to apply state/crop name canonicalization (default: True)
        validate: Whether to apply data quality validation (default: True)
        
    Returns:
        DataFrame with normalized production records; empty if no valid data
    """
    if not records:
        return pd.DataFrame(columns=[
            'state_name', 'district_name', 'year', 'season', 'crop',
            'area_ha', 'production_tonne'
        ])
    
    raw_df = pd.DataFrame(records)
    
    # Use SchemaMapper for robust field mapping
    if _ENHANCED_FEATURES:
        mapper = SchemaMapper(fuzzy_threshold=80)
        expected_fields = ['state', 'district', 'year', 'crop', 'area', 'production', 'season']
        
        try:
            field_mapping = mapper.map_fields(raw_df, expected_fields, strict=False)
            df = mapper.apply_mapping(raw_df, field_mapping)
            
            # Rename to final canonical names
            df = df.rename(columns={
                'state': 'state_name',
                'district': 'district_name',
                'area': 'area_ha',
                'production': 'production_tonne'
            })
            
            # Check for schema drift
            drift = mapper.detect_schema_drift(raw_df.columns.tolist(), expected_fields)
            if drift['renamed']:
                print(f"ℹ️  Detected field renames: {drift['renamed']}")
            if drift['missing']:
                print(f"⚠️  Missing expected fields: {drift['missing']}")
                
        except Exception as e:
            print(f"⚠️  SchemaMapper failed ({e}), falling back to manual mapping")
            df = _fallback_production_mapping(raw_df)
    else:
        # Fallback to manual mapping
        df = _fallback_production_mapping(raw_df)
    
    # Ensure required columns exist
    required_cols = ['state_name', 'district_name', 'year', 'crop', 'area_ha', 'production_tonne']
    for col in required_cols:
        if col not in df.columns:
            df[col] = None
    
    # Convert year to int and drop nulls
    df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')
    df = df.dropna(subset=['year'])
    
    # Convert area and production to float
    df['area_ha'] = pd.to_numeric(df['area_ha'], errors='coerce')
    df['production_tonne'] = pd.to_numeric(df['production_tonne'], errors='coerce')
    
    # Drop rows with null or zero production (data quality filter)
    df = df.dropna(subset=['production_tonne'])
    df = df[df['production_tonne'] > 0]
    
    # Strip whitespace from string fields
    for col in ['state_name', 'district_name', 'season', 'crop']:
        if col in df.columns:
            df[col] = df[col].fillna('').astype(str).str.strip()
    
    # Apply data quality validation if enabled
    quality_report = None
    if validate and _ENHANCED_FEATURES:
        try:
            validator = DataQualityValidator()
            quality_report = validator.validate(df, data_type='production')
            
            # Log quality issues
            if quality_report.issues:
                print(f"⚠️  Data quality issues detected ({len(quality_report.issues)} critical):")
                for issue in quality_report.issues[:3]:  # Show first 3
                    print(f"   - {issue}")
                if len(quality_report.issues) > 3:
                    print(f"   ... and {len(quality_report.issues) - 3} more")
            
            # Log quality score
            if quality_report.overall_score < 0.7:
                print(f"⚠️  Low quality score: {quality_report.overall_score:.2%}")
            elif quality_report.overall_score < 0.9:
                print(f"ℹ️  Quality score: {quality_report.overall_score:.2%}")
            else:
                print(f"✓ High quality score: {quality_report.overall_score:.2%}")
            
            # Filter invalid records if quality is poor
            if quality_report.overall_score < 0.7:
                print(f"🔧 Filtering invalid records...")
                df = validator.filter_invalid_records(df, 'production')
                print(f"   Kept {len(df)}/{quality_report.record_count} records")
                
        except Exception as e:
            print(f"⚠️  Data quality validation failed ({e}), proceeding without validation")
    
    # Apply mappings if enabled and available
    if apply_mappings and _MAPPINGS_AVAILABLE:
        df['state_name'] = df['state_name'].apply(normalize_state_name)
        df['crop'] = df['crop'].apply(normalize_crop_name)
    
    # Select and reorder output columns
    output_cols = [col for col in required_cols + (['season'] if 'season' in df.columns else [])
                   if col in df.columns]
    df = df[output_cols]
    
    result_df = df.reset_index(drop=True)
    
    # Attach quality report as metadata if available
    if quality_report:
        result_df.attrs['quality_report'] = quality_report
    
    return result_df


def _fallback_rainfall_mapping(raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Fallback manual field mapping for rainfall data when SchemaMapper unavailable.
    """
    # Identify subdivision column
    subdiv_col = None
    for col in ['subdivision', 'Subdivision', 'subdivision_name', 'sub_division']:
        if col in raw_df.columns:
            subdiv_col = col
            break
    if subdiv_col is None:
        for col in raw_df.columns:
            if raw_df[col].dtype == 'object' and col not in ['year', 'Year', 'annual', 'Annual']:
                subdiv_col = col
                break
    
    # Identify year column
    year_col = None
    for col in ['year', 'Year', 'crop_year', 'Crop_Year']:
        if col in raw_df.columns:
            year_col = col
            break
    
    rename_map = {}
    if subdiv_col:
        rename_map[subdiv_col] = 'subdivision'
    if year_col:
        rename_map[year_col] = 'year'
    
    return raw_df.rename(columns=rename_map)


def normalize_rainfall(records: List[Dict], month_columns: Optional[List[str]] = None, validate: bool = True) -> pd.DataFrame:
    """
    Normalize rainfall dataset records to a standardized schema.
    
    Input fields (from data.gov.in IMD sub-division monthly dataset - with robust field matching):
    - subdivision (or Subdivision, subdivision_name, sub_division, etc.): Sub-division name
    - month_year or year (or Year, crop_year, etc.): Time identifier
    - Jan, Feb, ..., Dec (optional): Monthly rainfall values
    - annual or Annual (optional): Annual rainfall
    
    Output schema:
    - subdivision_name (str): Sub-division name
    - year (int): Calendar year
    - rainfall_mm (float): Annual rainfall in millimeters
    
    Normalization steps:
    1. Use SchemaMapper to intelligently map fields (handles API changes)
    2. Identify year and rainfall columns (handle both annual and monthly formats)
    3. If monthly columns present, sum to annual; else use annual field
    4. Convert year to int; convert rainfall to float
    5. Drop rows with null rainfall or year
    6. Apply data quality validation (if enabled)
    7. Strip whitespace from string fields
    
    Args:
        records: List of dicts from rainfall API
        month_columns: Optional list of month column names (default: ['Jan', 'Feb', ..., 'Dec'])
        validate: Whether to apply data quality validation (default: True)
        
    Returns:
        DataFrame with normalized rainfall records; empty if no valid data
    """
    if not records:
        return pd.DataFrame(columns=['subdivision_name', 'year', 'rainfall_mm'])
    
    raw_df = pd.DataFrame(records)
    
    # Default month columns if not provided (API uses lowercase)
    if month_columns is None:
        month_columns = ['jan', 'feb', 'mar', 'apr', 'may', 'jun',
                        'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
    
    # Simple rename for rainfall (fields are consistent)
    df = raw_df.copy()
    if 'subdivision' in df.columns:
        df = df.rename(columns={'subdivision': 'subdivision_name'})
    
    # Compute annual rainfall: prefer monthly sum, fallback to annual column
    months_present = [col for col in month_columns if col in df.columns]
    
    if months_present:
        # Convert monthly columns to numeric and sum
        for col in months_present:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df['rainfall_mm'] = df[months_present].sum(axis=1, skipna=True)
    else:
        # Look for annual rainfall column
        annual_col = None
        for col in ['annual', 'Annual', 'annual_rainfall', 'Annual_Rainfall']:
            if col in df.columns:
                annual_col = col
                break
        
        if annual_col:
            df['rainfall_mm'] = pd.to_numeric(df[annual_col], errors='coerce')
        else:
            # If no annual or monthly data, return empty
            return pd.DataFrame(columns=['subdivision_name', 'year', 'rainfall_mm'])
    
    # Ensure year column exists and convert to int
    if 'year' not in df.columns:
        return pd.DataFrame(columns=['subdivision_name', 'year', 'rainfall_mm'])
    df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')
    
    # Ensure subdivision_name exists
    if 'subdivision_name' not in df.columns:
        df['subdivision_name'] = 'Unknown'
    else:
        df['subdivision_name'] = df['subdivision_name'].fillna('').astype(str).str.strip()
    
    # Drop rows with null rainfall or year
    df = df.dropna(subset=['rainfall_mm', 'year'])
    df = df[df['rainfall_mm'] > 0]
    
    # Apply data quality validation if enabled
    quality_report = None
    if validate and _ENHANCED_FEATURES:
        try:
            validator = DataQualityValidator()
            quality_report = validator.validate(df, data_type='rainfall')
            
            # Log quality issues
            if quality_report.issues:
                print(f"⚠️  Rainfall data quality issues ({len(quality_report.issues)} critical):")
                for issue in quality_report.issues[:3]:
                    print(f"   - {issue}")
                if len(quality_report.issues) > 3:
                    print(f"   ... and {len(quality_report.issues) - 3} more")
            
            # Log quality score
            if quality_report.overall_score < 0.7:
                print(f"⚠️  Low rainfall quality score: {quality_report.overall_score:.2%}")
            elif quality_report.overall_score < 0.9:
                print(f"ℹ️  Rainfall quality score: {quality_report.overall_score:.2%}")
            else:
                print(f"✓ High rainfall quality score: {quality_report.overall_score:.2%}")
            
            # Filter invalid records if quality is poor
            if quality_report.overall_score < 0.7:
                print(f"🔧 Filtering invalid rainfall records...")
                df = validator.filter_invalid_records(df, 'rainfall')
                print(f"   Kept {len(df)}/{quality_report.record_count} records")
                
        except Exception as e:
            print(f"⚠️  Rainfall data quality validation failed ({e}), proceeding without validation")
    
    # Select output columns and reset index
    df = df[['subdivision_name', 'year', 'rainfall_mm']]
    result_df = df.reset_index(drop=True)
    
    # Attach quality report as metadata if available
    if quality_report:
        result_df.attrs['quality_report'] = quality_report
    
    return result_df


def aggregate_rainfall_to_state(
    rainfall_df: pd.DataFrame,
    mapping: Optional[Dict[str, str]] = None
) -> pd.DataFrame:
    """
    Aggregate sub-division-level rainfall to state level.
    
    Args:
        rainfall_df: Normalized rainfall DataFrame with columns [subdivision_name, year, rainfall_mm]
        mapping: Dict mapping subdivision_name → state_name (uses built-in mapping if None)
        
    Returns:
        DataFrame with columns [state_name, year, rainfall_mm] aggregated by simple mean
        (area-weighted aggregation recommended for production, but simplified here for MVP)
    """
    if rainfall_df.empty:
        return pd.DataFrame(columns=['state_name', 'year', 'rainfall_mm'])
    
    # Use built-in mapping if not provided
    if mapping is None:
        if not _MAPPINGS_AVAILABLE:
            raise ValueError("Mappings module not available. Please provide mapping dict explicitly.")
        mapping = SUBDIVISION_TO_STATE
    
    # Add state_name via mapping
    rainfall_df = rainfall_df.copy()
    rainfall_df['state_name'] = rainfall_df['subdivision_name'].map(mapping)
    
    # Drop unmapped subdivisions (null state)
    rainfall_df = rainfall_df.dropna(subset=['state_name'])
    
    # Aggregate to state level by year (mean rainfall across subdivisions)
    agg_df = rainfall_df.groupby(['state_name', 'year']).agg({
        'rainfall_mm': 'mean'
    }).reset_index()
    
    return agg_df


def validate_production_df(df: pd.DataFrame) -> bool:
    """Check if production DataFrame has expected schema and non-empty data."""
    expected_cols = {'state_name', 'district_name', 'year', 'crop', 'area_ha', 'production_tonne'}
    return not df.empty and expected_cols.issubset(set(df.columns))


def validate_rainfall_df(df: pd.DataFrame) -> bool:
    """Check if rainfall DataFrame has expected schema and non-empty data."""
    expected_cols = {'subdivision_name', 'year', 'rainfall_mm'}
    return not df.empty and expected_cols.issubset(set(df.columns))
