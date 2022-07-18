"""
Implements the interface to the yr.no forecasting service
"""
from typing import Dict, Any

import data_crawler.sources.abc.abstract_source as abstract_source


class LocationForecast(abstract_source.AbstractSourceAPI):
    """Queries the location forecast of yr.no"""

    def __init__(self, source_parameters, **kwargs):
        pass  # TODO: Implement 

    def fetch_data(self) -> Dict[str, Any]:
        return {}  # TODO: Implement the query function
