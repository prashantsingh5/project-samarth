"""
Data Quality Validator: Assess and validate data quality before use.

This module checks data completeness, consistency, accuracy, and provides
quality scores to determine answer confidence.
"""

from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
import pandas as pd
import numpy as np
from datetime import datetime


@dataclass
class QualityReport:
    """Data quality assessment results."""
    overall_score: float  # 0-1
    completeness_score: float  # 0-1
    consistency_score: float  # 0-1
    accuracy_score: float  # 0-1
    issues: List[str]  # Critical issues found
    warnings: List[str]  # Non-blocking warnings
    record_count: int
    valid_record_count: int
    
    def is_acceptable(self, threshold: float = 0.7) -> bool:
        """Check if quality meets minimum threshold."""
        return self.overall_score >= threshold


class DataQualityValidator:
    """Validate data quality across multiple dimensions."""
    
    def __init__(self):
        """Initialize validator."""
        self.domain_rules = self._init_domain_rules()
    
    def _init_domain_rules(self) -> Dict:
        """Initialize domain-specific validation rules."""
        return {
            'production': {
                'min': 0,
                'max': 100_000_000,  # 100M tonnes (upper bound for any crop)
                'required_fields': ['state', 'crop', 'year']
            },
            'area': {
                'min': 0,
                'max': 10_000_000,  # 10M hectares
                'required_fields': ['state', 'crop', 'year']
            },
            'rainfall': {
                'min': 0,
                'max': 12_000,  # 12,000mm (Mawsynram gets ~11,000mm)
                'required_fields': ['state', 'year']
            },
            'year': {
                'min': 1901,  # Earliest rainfall data
                'max': datetime.now().year + 1
            }
        }
    
    def validate(self, df: pd.DataFrame, data_type: str = 'general') -> QualityReport:
        """
        Comprehensive data quality validation.
        
        Args:
            df: DataFrame to validate
            data_type: Type of data ('production', 'rainfall', 'general')
            
        Returns:
            QualityReport with scores and issues
        """
        issues = []
        warnings = []
        
        if df.empty:
            return QualityReport(
                overall_score=0.0,
                completeness_score=0.0,
                consistency_score=0.0,
                accuracy_score=0.0,
                issues=["No data returned"],
                warnings=[],
                record_count=0,
                valid_record_count=0
            )
        
        original_count = len(df)
        
        # 1. Completeness check
        completeness_score, completeness_issues = self._check_completeness(df, data_type)
        issues.extend(completeness_issues)
        
        # 2. Consistency check
        consistency_score, consistency_issues = self._check_consistency(df, data_type)
        issues.extend(consistency_issues)
        
        # 3. Accuracy check (outliers, invalid ranges)
        accuracy_score, accuracy_issues, cleaned_df = self._check_accuracy(df, data_type)
        warnings.extend(accuracy_issues)
        
        # 4. Calculate overall score (weighted average)
        overall_score = (
            0.35 * completeness_score +
            0.35 * consistency_score +
            0.30 * accuracy_score
        )
        
        return QualityReport(
            overall_score=overall_score,
            completeness_score=completeness_score,
            consistency_score=consistency_score,
            accuracy_score=accuracy_score,
            issues=issues,
            warnings=warnings,
            record_count=original_count,
            valid_record_count=len(cleaned_df)
        )
    
    def _check_completeness(self, df: pd.DataFrame, data_type: str) -> Tuple[float, List[str]]:
        """
        Check data completeness.
        
        Returns:
            (score, issues_list)
        """
        issues = []
        
        # Get required fields for this data type
        required_fields = self.domain_rules.get(data_type, {}).get('required_fields', [])
        
        # Check if required fields exist
        missing_required = [f for f in required_fields if f not in df.columns]
        if missing_required:
            issues.append(f"Missing required fields: {missing_required}")
            return 0.0, issues
        
        # Calculate null ratio for required fields
        total_cells = 0
        null_cells = 0
        
        for field in required_fields:
            if field in df.columns:
                total_cells += len(df)
                null_cells += df[field].isnull().sum()
        
        if total_cells == 0:
            return 1.0, issues
        
        completeness = 1.0 - (null_cells / total_cells)
        
        if completeness < 0.8:
            issues.append(f"Low completeness: {completeness:.1%} of required fields are non-null")
        
        return completeness, issues
    
    def _check_consistency(self, df: pd.DataFrame, data_type: str) -> Tuple[float, List[str]]:
        """
        Check logical consistency.
        
        Returns:
            (score, issues_list)
        """
        issues = []
        inconsistencies = 0
        total_checks = 0
        
        # Check 1: Production can't exceed area * reasonable yield
        if 'production' in df.columns and 'area' in df.columns:
            total_checks += 1
            # Max yield ~20 tonnes/hectare for sugarcane (highest yielding crop)
            df_clean = df.dropna(subset=['production', 'area'])
            impossible = df_clean[df_clean['production'] > df_clean['area'] * 20]
            if len(impossible) > 0:
                inconsistencies += 1
                issues.append(f"Found {len(impossible)} records where production exceeds reasonable yield")
        
        # Check 2: Year must be within valid range
        if 'year' in df.columns:
            total_checks += 1
            year_range = self.domain_rules.get('year', {})
            df_clean = df.dropna(subset=['year'])
            out_of_range = df_clean[
                (df_clean['year'] < year_range.get('min', 0)) | 
                (df_clean['year'] > year_range.get('max', 9999))
            ]
            if len(out_of_range) > 0:
                inconsistencies += 1
                issues.append(f"Found {len(out_of_range)} records with invalid years")
        
        # Check 3: Negative values where they shouldn't exist
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            if col in ['production', 'area', 'rainfall']:
                total_checks += 1
                negative = df[df[col] < 0]
                if len(negative) > 0:
                    inconsistencies += 1
                    issues.append(f"Found {len(negative)} negative values in {col}")
        
        if total_checks == 0:
            return 1.0, issues
        
        consistency_score = 1.0 - (inconsistencies / total_checks)
        return consistency_score, issues
    
    def _check_accuracy(self, df: pd.DataFrame, data_type: str) -> Tuple[float, List[str], pd.DataFrame]:
        """
        Check accuracy using outlier detection and range validation.
        
        Returns:
            (score, warnings_list, cleaned_dataframe)
        """
        warnings = []
        cleaned_df = df.copy()
        total_outliers = 0
        total_checked = 0
        
        # Get domain-specific ranges
        rules = self.domain_rules.get(data_type, {})
        
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols:
            if len(df[col].dropna()) < 4:  # Need at least 4 values for IQR
                continue
            
            total_checked += 1
            
            # Range-based validation
            col_rules = self.domain_rules.get(col, rules)
            if 'min' in col_rules or 'max' in col_rules:
                min_val = col_rules.get('min', -np.inf)
                max_val = col_rules.get('max', np.inf)
                
                out_of_range = cleaned_df[
                    (cleaned_df[col] < min_val) | 
                    (cleaned_df[col] > max_val)
                ]
                
                if len(out_of_range) > 0:
                    total_outliers += len(out_of_range)
                    warnings.append(
                        f"Removed {len(out_of_range)} out-of-range values from {col} "
                        f"(valid range: {min_val}-{max_val})"
                    )
                    cleaned_df = cleaned_df[
                        (cleaned_df[col] >= min_val) & 
                        (cleaned_df[col] <= max_val)
                    ]
            
            # IQR-based outlier detection
            Q1 = cleaned_df[col].quantile(0.25)
            Q3 = cleaned_df[col].quantile(0.75)
            IQR = Q3 - Q1
            
            if IQR > 0:  # Avoid division by zero
                # Use 3*IQR for more lenient outlier detection
                lower_bound = Q1 - 3 * IQR
                upper_bound = Q3 + 3 * IQR
                
                outliers = cleaned_df[
                    (cleaned_df[col] < lower_bound) | 
                    (cleaned_df[col] > upper_bound)
                ]
                
                if len(outliers) > 0:
                    outlier_pct = len(outliers) / len(cleaned_df) * 100
                    if outlier_pct > 5:  # Only flag if >5% are outliers
                        warnings.append(
                            f"Found {len(outliers)} statistical outliers in {col} "
                            f"({outlier_pct:.1f}%)"
                        )
        
        # Calculate accuracy score
        if total_checked == 0:
            accuracy_score = 1.0
        else:
            accuracy_score = 1.0 - min(total_outliers / len(df), 0.5)  # Cap penalty at 50%
        
        return accuracy_score, warnings, cleaned_df
    
    def filter_invalid_records(self, df: pd.DataFrame, data_type: str) -> pd.DataFrame:
        """
        Remove invalid records based on domain rules.
        
        Args:
            df: Input DataFrame
            data_type: Type of data for validation rules
            
        Returns:
            Cleaned DataFrame
        """
        if df.empty:
            return df
        
        cleaned = df.copy()
        
        # Remove nulls from required fields
        required_fields = self.domain_rules.get(data_type, {}).get('required_fields', [])
        for field in required_fields:
            if field in cleaned.columns:
                cleaned = cleaned[cleaned[field].notna()]
        
        # Apply range filters
        rules = self.domain_rules.get(data_type, {})
        if 'min' in rules and 'max' in rules:
            # Find the main metric column (production, rainfall, etc.)
            metric_col = None
            for col in cleaned.columns:
                if data_type in col.lower() or col.lower() == data_type:
                    metric_col = col
                    break
            
            if metric_col and metric_col in cleaned.columns:
                cleaned = cleaned[
                    (cleaned[metric_col] >= rules['min']) & 
                    (cleaned[metric_col] <= rules['max'])
                ]
        
        # Remove negative values from metrics
        for col in ['production', 'area', 'rainfall']:
            if col in cleaned.columns:
                cleaned = cleaned[cleaned[col] >= 0]
        
        return cleaned
