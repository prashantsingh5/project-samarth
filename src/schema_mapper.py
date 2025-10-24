"""
Schema Mapper: Intelligent field name mapping for robust API integration.

This module handles API schema variations, field name changes, and typos
using fuzzy matching and known alias patterns.
"""

from typing import List, Dict, Optional, Set
import pandas as pd
from fuzzywuzzy import fuzz


class SchemaMapper:
    """Intelligent field name mapping with fuzzy matching."""
    
    # Known field aliases across different APIs
    FIELD_ALIASES = {
        'state': ['state_name', 'state', 'State', 'STATE_NAME', 'statename', 'State Name'],
        'district': ['district_name', 'district', 'District', 'DISTRICT_NAME', 'districtname'],
        'year': ['year', 'crop_year', 'Year', 'YEAR', 'cropYear', 'Crop_Year'],
        'crop': ['crop', 'Crop', 'CROP', 'crop_name', 'commodity', 'Commodity'],
        'area': ['area_', 'area', 'Area', 'area_hectare', 'area_ha', 'Area_'],
        'production': ['production_', 'production', 'Production', 'PRODUCTION', 'Production_'],
        'season': ['season', 'Season', 'SEASON'],
        'rainfall': ['rainfall_mm', 'rainfall', 'Rainfall', 'RAINFALL', 'precipitation'],
        'subdivision': ['subdivision', 'Subdivision', 'SUBDIVISION', 'sub_division'],
    }
    
    def __init__(self, fuzzy_threshold: int = 80):
        """
        Initialize mapper.
        
        Args:
            fuzzy_threshold: Minimum similarity score (0-100) for fuzzy matches
        """
        self.fuzzy_threshold = fuzzy_threshold
        self._cache = {}  # Cache successful mappings
        
    def map_fields(self, 
                   raw_df: pd.DataFrame, 
                   expected_fields: List[str],
                   strict: bool = False) -> Dict[str, str]:
        """
        Map raw API fields to canonical field names.
        
        Args:
            raw_df: DataFrame from API
            expected_fields: List of canonical field names we want
            strict: If True, raise error if any field not found
            
        Returns:
            Dict mapping {canonical_name: actual_column_name}
        """
        if raw_df.empty:
            return {}
        
        raw_columns = raw_df.columns.tolist()
        mapping = {}
        
        for canonical in expected_fields:
            # Check cache first
            cache_key = (canonical, tuple(sorted(raw_columns)))
            if cache_key in self._cache:
                actual_col = self._cache[cache_key]
                if actual_col in raw_columns:
                    mapping[canonical] = actual_col
                    continue
            
            # Try exact match
            if canonical in raw_columns:
                mapping[canonical] = canonical
                self._cache[cache_key] = canonical
                continue
            
            # Try known aliases
            if canonical in self.FIELD_ALIASES:
                for alias in self.FIELD_ALIASES[canonical]:
                    if alias in raw_columns:
                        mapping[canonical] = alias
                        self._cache[cache_key] = alias
                        break
                        
            # Try fuzzy match if still not found
            if canonical not in mapping:
                fuzzy_match = self._fuzzy_match(canonical, raw_columns)
                if fuzzy_match:
                    mapping[canonical] = fuzzy_match
                    self._cache[cache_key] = fuzzy_match
                elif strict:
                    raise ValueError(f"Required field '{canonical}' not found in API response. Available: {raw_columns}")
        
        return mapping
    
    def _fuzzy_match(self, target: str, candidates: List[str]) -> Optional[str]:
        """
        Find best fuzzy match for field name.
        
        Args:
            target: Field name we're looking for
            candidates: Available column names
            
        Returns:
            Best matching candidate or None
        """
        best_score = 0
        best_match = None
        
        for candidate in candidates:
            # Try different matching strategies
            score = max(
                fuzz.ratio(target.lower(), candidate.lower()),
                fuzz.partial_ratio(target.lower(), candidate.lower()),
                fuzz.token_sort_ratio(target.lower(), candidate.lower())
            )
            
            if score > best_score and score >= self.fuzzy_threshold:
                best_score = score
                best_match = candidate
        
        return best_match
    
    def apply_mapping(self, df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
        """
        Apply field mapping to DataFrame.
        
        Args:
            df: DataFrame with raw column names
            mapping: Dict {canonical_name: actual_column_name}
            
        Returns:
            DataFrame with canonical column names
        """
        # Create new DataFrame with canonical names
        mapped_df = pd.DataFrame()
        
        for canonical, actual in mapping.items():
            if actual in df.columns:
                mapped_df[canonical] = df[actual]
        
        return mapped_df
    
    def detect_schema_drift(self, 
                           current_columns: List[str], 
                           expected_columns: List[str]) -> Dict[str, List[str]]:
        """
        Detect schema changes between expected and actual columns.
        
        Args:
            current_columns: Columns in current API response
            expected_columns: Columns we expected to see
            
        Returns:
            Dict with 'missing', 'extra', 'renamed' lists
        """
        current_set = set(current_columns)
        expected_set = set(expected_columns)
        
        missing = list(expected_set - current_set)
        extra = list(current_set - expected_set)
        
        # Try to identify renamed fields
        renamed = []
        for missing_field in missing[:]:
            for extra_field in extra[:]:
                score = fuzz.ratio(missing_field.lower(), extra_field.lower())
                if score >= self.fuzzy_threshold:
                    renamed.append((missing_field, extra_field))
                    missing.remove(missing_field)
                    extra.remove(extra_field)
                    break
        
        return {
            'missing': missing,
            'extra': extra,
            'renamed': renamed
        }
