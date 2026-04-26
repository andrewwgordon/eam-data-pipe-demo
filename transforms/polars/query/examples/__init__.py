"""Example queries for the Iceberg Query Module.

Contains example query scripts demonstrating common analytical patterns:
- Asset availability analysis
- Maintenance history tracking  
- S5000F compliance checking
- Cross-layer comparison

Each example can be run independently to demonstrate query capabilities.
"""

from transforms.polars.query.examples.asset_availability_query import run_asset_availability_queries
from transforms.polars.query.examples.maintenance_history_query import run_maintenance_history_queries
from transforms.polars.query.examples.s5000f_compliance_query import run_s5000f_compliance_queries
from transforms.polars.query.examples.cross_layer_comparison import run_cross_layer_comparison_queries

__all__ = [
    "run_asset_availability_queries",
    "run_maintenance_history_queries", 
    "run_s5000f_compliance_queries",
    "run_cross_layer_comparison_queries",
]