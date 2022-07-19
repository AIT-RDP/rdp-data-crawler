"""
Implements caching mechanisms to save bandwith and API calls for REST APIs
"""

import abc

import cachecontrol
import cachecontrol.caches.file_cache as file_cache
import cachecontrol.heuristics
import pandas as pd
import requests

import data_crawler.sources.abc.abstract_source as abstract_source


class GenericHTTPSourceAPI(abstract_source.AbstractSourceAPI, abc.ABC):
    """
    Provides an HTTP session member that caches local calls according to the caching configuration
    """

    def __init__(self, source_parameters: dict, **kwargs):
        """
        Initializes the cache

        :param source_parameters: The source configuration including a cache section
        :param kwargs: Any additional parameters to maintain compatibility
        """

        super(GenericHTTPSourceAPI, self).__init__(source_parameters=source_parameters, **kwargs)

        config = source_parameters.get("cache", {})

        req_session = requests.Session()
        cache = file_cache.FileCache(config.get("directory", ".cache"))

        if "expire" in config:
            expiration_time = pd.Timedelta(config["expire"])
            heuristic = cachecontrol.heuristics.ExpiresAfter(seconds=expiration_time.total_seconds())
            # Rewrite the expiration date:
            #req_session.mount('http://', cachecontrol.CacheControlAdapter(heuristic=heuristic))
            #req_session.mount('https://', cachecontrol.CacheControlAdapter(heuristic=heuristic))
        else:
            heuristic = None

        self.session = cachecontrol.CacheControl(req_session, cache, heuristic=heuristic)