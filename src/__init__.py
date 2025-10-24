"""
Project Samarth - Data.gov.in Q&A System

A production-ready Q&A system that fetches live agricultural and climate datasets
from data.gov.in, performs cross-domain reasoning, and returns cited answers.
"""

__version__ = "1.0.0"

from .query_planner import QueryPlanner, QueryPlan
from .query_executor import QueryExecutor, QueryResult
from .data_connector import DataGovInConnector
from .normalizers import normalize_production, normalize_rainfall
from .mappings import SUBDIVISION_TO_STATE, CROP_ALIASES, normalize_crop_name
from .cache import DataCache
from .answer_generator import AnswerGenerator

__all__ = [
    'QueryPlanner',
    'QueryPlan',
    'QueryExecutor',
    'QueryResult',
    'DataGovInConnector',
    'normalize_production',
    'normalize_rainfall',
    'SUBDIVISION_TO_STATE',
    'CROP_ALIASES',
    'normalize_crop_name',
    'DataCache',
    'AnswerGenerator'
]
