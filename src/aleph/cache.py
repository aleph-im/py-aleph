from collections import defaultdict


class Cache:
    def __init__(self):
        self._cache = defaultdict(dict)

    def get(self, key, namespace):
        return self._cache[namespace].get(key)

    def set(self, key, value, namespace):
        self._cache[namespace][key] = value

    def exists(self, key, namespace):
        return key in self._cache[namespace]

    def delete_namespace(self, namespace):
        if namespace in self._cache:
            self._cache[namespace] = {}

    def delete(self, key, namespace):
        if self.exists(key, namespace):
            del self._cache[namespace]


# simple in memory cache
# we can't use aiocache here because most of ORM methods are not async compatible
cache = Cache()
