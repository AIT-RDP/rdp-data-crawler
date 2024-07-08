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


class SyncHTTPMixin(abc.ABC):
    """
    Generic mixin class that can be used to include HTTP caching functionality

    The class is intended to provide caching to arbitrary synchronous sources. Just use the class as the first parent
    and make sure that the source_parameters keyword argument is given. It will relay all other arguments automatically
    to the other parents.
    """

    def __init__(self, *args, **kwargs):
        """
        Initializes the cache

        :param args: Positional arguments to be relayed to the other parent classes
        :param kwargs: Keyword arguments to be relayed to the other parent classes. The keyword arguments must include
            the source_parameters keyword.
        """
        super().__init__(*args, **kwargs)
        assert "source_parameters" in kwargs, "Expect the 'source_parameters' keyword"

        source_parameters = kwargs["source_parameters"]
        config = source_parameters.get("cache", {})

        req_session = requests.Session()
        cache = file_cache.FileCache(config.get("directory", ".cache"))

        if "expire" in config:
            expiration_time = pd.Timedelta(config["expire"])
            heuristic = cachecontrol.heuristics.ExpiresAfter(seconds=expiration_time.total_seconds())
        else:
            heuristic = None

        self.session = cachecontrol.CacheControl(req_session, cache, heuristic=heuristic)


class GenericHTTPSourceAPI(SyncHTTPMixin, abstract_source.AbstractSourceAPI, abc.ABC):
    """
    Provides an HTTP session member that caches local calls according to the caching configuration

    The class is provided for compatibility reasons and can be seen as deprecated. Please use the appropriate
    SyncHTTPMixin mixin instead.
    """

    def __init__(self, source_parameters: dict, **kwargs):
        """
        Initializes the cache

        :param source_parameters: The source configuration including a cache section
        :param kwargs: Any additional parameters to maintain compatibility
        """

        super(GenericHTTPSourceAPI, self).__init__(source_parameters=source_parameters, **kwargs)
