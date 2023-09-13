"""
A Redis-based persistent storage implementation to manage persistent values
"""
import json

import redis


class PersistentAPIStorage:
    """
    Provides some persistent storage space for highly stateful APIs.

    Note that the value to store needs to be JSON-serializable in order to avoid version upgrade conflicts. Items can be
    accessed using the standard item access protocol.
    """

    def __init__(self, redis_pool: redis.ConnectionPool, executor_name: str):
        """
        :param redis_pool: The connection pool to access the Redis-based storage
        :param executor_name: The unique name to prefix the store
        """

        self._prefix = f"_data_crawler.store.{executor_name}"
        self._client = redis.Redis(connection_pool=redis_pool)

    def __getitem__(self, item):
        """
        Synchronously queries the storage and returns the result

        :param item: The name of the item to query. Please note that the name must only contain valid Redis identifiers
        :return: The stored and deserialized value
        """

        key = f"{self._prefix}.{item}"
        ret = self._client.get(key)
        if ret is None:
            raise KeyError(F"The key '{item}' does not exist within the store prefixed '{self._prefix}'")

        ret = json.loads(ret)
        return ret

    def __contains__(self, item):
        """
         Checks whether the given key is contained in the store

        :param item: The key to query
        :return: True iff the key already exists
        """

        key = f"{self._prefix}.{item}"
        return self._client.exists(key)

    def __setitem__(self, item, value):
        """
        Sets the item in the persistent storage

        :param item: Tke storage key to persist
        :param value: The value to persist. The value must be JSON serializable
        :return: The original value
        """

        key = f"{self._prefix}.{item}"
        encoded_value = json.dumps(value, allow_nan=True)
        self._client.set(key, encoded_value)
        return value

    def __delitem__(self, item):
        """
        Deletes the given item from the store

        :param key: The key to delete
        """

        key = f"{self._prefix}.{item}"
        self._client.delete(key)
