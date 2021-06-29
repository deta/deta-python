import os
import json
import typing
from urllib.parse import quote

from .service import _Service, JSON_MIME


class FetchResponse:
    def __init__(self, count=0, last=None, items=[]):
        self._count = count
        self._last = last
        self._items = items

    @property
    def count(self):
        return self._count

    @property
    def last(self):
        return self._last

    @property
    def items(self):
        return self._items

    def __eq__(self, other):
        return (
            self.count == other.count
            and self.last == other.last
            and self.items == other.items
        )


class Util:
    class Trim:
        pass

    class Increment:
        def __init__(self, value=None):
            self.val = value
            if not value:
                self.val = 1

    class Append:
        def __init__(self, value):
            self.val = value
            if not isinstance(value, list):
                self.val = [value]

    class Prepend:
        def __init__(self, value):
            self.val = value
            if not isinstance(value, list):
                self.val = [value]

    def trim(self):
        return self.Trim()

    def increment(self, value: typing.Union[int, float] = None):
        return self.Increment(value)

    def append(self, value: typing.Union[dict, list, str, int, float, bool]):
        return self.Append(value)

    def prepend(self, value: typing.Union[dict, list, str, int, float, bool]):
        return self.Prepend(value)


class _Base(_Service):
    def __init__(self, name: str, project_key: str, project_id: str, host: str = None):
        assert name, "No Base name provided"

        host = host or os.getenv("DETA_BASE_HOST") or "database.deta.sh"
        super().__init__(
            project_key=project_key,
            project_id=project_id,
            host=host,
            name=name,
            timeout=3,
        )
        self.util = Util()

    def get(self, key: str):
        if key == "":
            raise ValueError("Key is empty")

        # encode key
        key = quote(key, safe="")
        _, res = self._request("/items/{}".format(key), "GET")
        return res or None

    def delete(self, key: str):
        """Delete an item from the database
        key: the key of item to be deleted
        """
        if key == "":
            raise ValueError("Key is empty")

        # encode key
        key = quote(key, safe="")
        self._request("/items/{}".format(key), "DELETE")
        return None

    def insert(self, data: typing.Union[dict, list, str, int, bool], key: str = None):
        if not isinstance(data, dict):
            data = {"value": data}
        else:
            data = data.copy()

        if key:
            data["key"] = key

        code, res = self._request(
            "/items", "POST", {"item": data}, content_type=JSON_MIME
        )
        if code == 201:
            return res
        elif code == 409:
            raise Exception("Item with key '{4}' already exists".format(key))

    def put(self, data: typing.Union[dict, list, str, int, bool], key: str = None):
        """store (put) an item in the database. Overrides an item if key already exists.
        `key` could be provided as function argument or a field in the data dict.
        If `key` is not provided, the server will generate a random 12 chars key.
        """

        if not isinstance(data, dict):
            data = {"value": data}
        else:
            data = data.copy()

        if key:
            data["key"] = key

        code, res = self._request(
            "/items", "PUT", {"items": [data]}, content_type=JSON_MIME
        )
        return res["processed"]["items"][0] if res and code == 207 else None

    def put_many(self, items: typing.List[typing.Union[dict, list, str, int, bool]]):
        assert len(items) <= 25, "We can't put more than 25 items at a time."
        _items = []
        for i in items:
            if not isinstance(i, dict):
                _items.append({"value": i})
            else:
                _items.append(i)

        _, res = self._request(
            "/items", "PUT", {"items": _items}, content_type=JSON_MIME
        )
        return res

    def _fetch(
        self,
        query: typing.Union[dict, list] = None,
        buffer: int = None,
        last: str = None,
    ) -> typing.Optional[typing.Tuple[int, list]]:
        """This is where actual fetch happens."""
        payload = {
            "limit": buffer,
            "last": last if not isinstance(last, bool) else None,
        }

        if query:
            payload["query"] = query if isinstance(query, list) else [query]

        code, res = self._request("/query", "POST", payload, content_type=JSON_MIME)
        return code, res

    def fetch(
        self,
        query: typing.Union[dict, list] = None,
        *,
        limit: int = 1000,
        last: str = None,
    ):
        """
        fetch items from the database.
            `query` is an optional filter or list of filters. Without filter, it will return the whole db.
        """
        _, res = self._fetch(query, limit, last)

        paging = res.get("paging")

        return FetchResponse(paging.get("size"), paging.get("last"), res.get("items"))

    def update(self, updates: dict, key: str):
        """
        update an item in the database
        `updates` specifies the attribute names and values to update,add or remove
        `key` is the kye of the item to be updated
        """

        if key == "":
            raise ValueError("Key is empty")

        payload = {
            "set": {},
            "increment": {},
            "append": {},
            "prepend": {},
            "delete": [],
        }
        for attr, value in updates.items():
            if isinstance(value, Util.Trim):
                payload["delete"].append(attr)
            elif isinstance(value, Util.Increment):
                payload["increment"][attr] = value.val
            elif isinstance(value, Util.Append):
                payload["append"][attr] = value.val
            elif isinstance(value, Util.Prepend):
                payload["prepend"][attr] = value.val
            else:
                payload["set"][attr] = value

        encoded_key = quote(key, safe="")
        code, _ = self._request(
            "/items/{}".format(encoded_key), "PATCH", payload, content_type=JSON_MIME
        )
        if code == 200:
            return None
        elif code == 404:
            raise Exception("Key '{}' not found".format(key))
