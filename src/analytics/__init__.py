"""
Analytics Package
Unified analytics components for all entity types
"""

from .data_consolidation import DataConsolidator
from .analytics_etl import AnalyticsETL
from .scd_processor import SCDType2Processor
from .fixtures import FixturesProcessor  # ADD THIS LINE

__all__ = ['DataConsolidator', 'AnalyticsETL', 'SCDType2Processor', 'FixturesProcessor']  # ADD FixturesProcessor