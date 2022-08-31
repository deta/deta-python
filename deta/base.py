import os
import datetime
from typing import Any, Dict, Mapping, Optional, Sequence, Union, overload
from urllib.parse import quote

from .service import _Service, JSON_MIME

# timeout for Base service in seconds
BASE_SERVICE_TIMEOUT = 300
BASE_TTL_ATTRIBUTE = "__expires"


class FetchResponse:
    def __init__(self, count: int = 0, last: Optional[str] = None, items: Optional[list] = None):
        self.count = count
        self.last = last
        self.items = items if items is not None else []

    def __eq__(self, other: "FetchResponse"):
        return self.count == other.count and self.last == other.last and self.items == other.items

    def __iter__(self):
        return iter(self.items)

    def __len__(self) -> int:
        return len(self.items)


class Util:
    class Trim:
        pass

    class Increment:
        def __init__(self, value: Union[int, float] = 1):
            self.value = value

    class Append:
        def __init__(self, value: Union[dict, list, str, int, float, bool]):
            self.value = value if isinstance(value, list) else [value]

    class Prepend:
        def __init__(self, value: Union[dict, list, str, int, float, bool]):
            self.value = value if isinstance(value, list) else [value]

    def trim(self):
        return self.Trim()

    def increment(self, value: Union[int, float] = 1):
        return self.Increment(value)

    def append(self, value: Union[dict, list, str, int, float, bool]):
        return self.Append(value)

    def prepend(self, value: Union[dict, list, str, int, float, bool]):
        return self.Prepend(value)


class _Base(_Service):
    def __init__(self, name: str, project_key: str, project_id: str, *, host: Optional[str] = None):
        if not name:
            raise ValueError("parameter 'name' must be a non-empty string")

        host = host or os.getenv("DETA_BASE_HOST") or "database.deta.sh"
        super().__init__(project_key, project_id, host, name, BASE_SERVICE_TIMEOUT)
        self._ttl_attribute = BASE_TTL_ATTRIBUTE
        self.util = Util()

    def get(self, key: str) -> Dict[str, Any]:
        if not key:
            raise ValueError("parameter 'key' must be a non-empty string")

        key = quote(key, safe="")
        _, res = self._request(f"/items/{key}", "GET")
        return res

    def delete(self, key: str):
        """Delete an item from the database

        Args:
            key: The key of item to be deleted.
        """
        if not key:
            raise ValueError("parameter 'key' must be a non-empty string")

        key = quote(key, safe="")
        self._request(f"/items/{key}", "DELETE")

    @overload
    def insert(
        self,
        data: Union[dict, list, str, int, bool],
        key: Optional[str] = None,
    ) -> dict:
        ...

    @overload
    def insert(
        self,
        data: Union[dict, list, str, int, bool],
        key: Optional[str] = None,
        *,
        expire_in: int,
    ) -> dict:
        ...

    @overload
    def insert(
        self,
        data: Union[dict, list, str, int, bool],
        key: Optional[str] = None,
        *,
        expire_at: Union[int, float, datetime.datetime],
    ) -> dict:
        ...

    def insert(self, data, key=None, *, expire_in=None, expire_at=None):
        data = data.copy() if isinstance(data, dict) else {"value": data}
        if key:
            data["key"] = key

        insert_ttl(data, self._ttl_attribute, expire_in, expire_at)
        code, res = self._request("/items", "POST", {"item": data}, content_type=JSON_MIME)

        if code == 201:
            return res
        elif code == 409:
            raise ValueError(f"item with  key '{key}' already exists")

    @overload
    def put(
        self,
        data: Union[dict, list, str, int, bool],
        key: Optional[str] = None,
    ) -> dict:
        ...

    @overload
    def put(
        self,
        data: Union[dict, list, str, int, bool],
        key: Optional[str] = None,
        *,
        expire_in: int,
    ) -> dict:
        ...

    @overload
    def put(
        self,
        data: Union[dict, list, str, int, bool],
        key: Optional[str] = None,
        *,
        expire_at: Union[int, float, datetime.datetime],
    ) -> dict:
        ...

    def put(self, data, key=None, *, expire_in=None, expire_at=None):
        """Store (put) an item in the database. Overrides an item if key already exists.
        `key` could be provided as an argument or a field in the data dict.
        If `key` is not provided, the server will generate a random 12-character key.
        """
        data = data.copy() if isinstance(data, dict) else {"value": data}
        if key:
            data["key"] = key

        insert_ttl(data, self._ttl_attribute, expire_in, expire_at)
        code, res = self._request("/items", "PUT", {"items": [data]}, content_type=JSON_MIME)
        return res["processed"]["items"][0] if res and code == 207 else None

    @overload
    def put_many(
        self,
        items: Sequence[Union[dict, list, str, int, bool]],
    ) -> dict:
        ...

    @overload
    def put_many(
        self,
        items: Sequence[Union[dict, list, str, int, bool]],
        *,
        expire_in: int,
    ) -> dict:
        ...

    @overload
    def put_many(
        self,
        items: Sequence[Union[dict, list, str, int, bool]],
        *,
        expire_at: Union[int, float, datetime.datetime],
    ) -> dict:
        ...

    def put_many(self, items, *, expire_in=None, expire_at=None):
        if len(items) > 25:
            raise ValueError("cannot put more than 25 items at a time")

        _items = []
        for item in items:
            data = item
            if not isinstance(item, dict):
                data = {"value": item}
            insert_ttl(data, self._ttl_attribute, expire_in, expire_at)
            _items.append(data)

        _, res = self._request("/items", "PUT", {"items": _items}, content_type=JSON_MIME)
        return res

    def fetch(
        self,
        query: Optional[Union[Mapping, Sequence[Mapping]]] = None,
        *,
        limit: int = 1000,
        last: Optional[str] = None,
    ):
        """Fetch items from the database. `query` is an optional filter or list of filters.
        Without a filter, it will return the whole db.
        """
        payload = {
            "limit": limit,
            "last": last if not isinstance(last, bool) else None,
        }

        if query:
            payload["query"] = query if isinstance(query, Sequence) else [query]

        _, res = self._request("/query", "POST", payload, content_type=JSON_MIME)
        paging = res.get("paging")
        return FetchResponse(paging.get("size"), paging.get("last"), res.get("items"))

    @overload
    def update(
        self,
        updates: Mapping,
        key: str,
    ):
        ...

    @overload
    def update(
        self,
        updates: Mapping,
        key: str,
        *,
        expire_in: int,
    ):
        ...

    @overload
    def update(
        self,
        updates: Mapping,
        key: str,
        *,
        expire_at: Union[int, float, datetime.datetime],
    ):
        ...

    def update(self, updates, key, *, expire_in=None, expire_at=None):
        """Update an item in the database.
        `updates` specifies the attribute names and values to update, add or remove.
        `key` is the key of the item to be updated.
        """
        if not key:
            raise ValueError("parameter 'key' must be a non-empty string")

        payload = {
            "set": {},
            "increment": {},
            "append": {},
            "prepend": {},
            "delete": [],
        }

        if updates:
            for attr, value in updates.items():
                if isinstance(value, Util.Trim):
                    payload["delete"].append(attr)
                elif isinstance(value, Util.Increment):
                    payload["increment"][attr] = value.value
                elif isinstance(value, Util.Append):
                    payload["append"][attr] = value.value
                elif isinstance(value, Util.Prepend):
                    payload["prepend"][attr] = value.value
                else:
                    payload["set"][attr] = value

        insert_ttl(payload["set"], self._ttl_attribute, expire_in, expire_at)

        encoded_key = quote(key, safe="")
        code, _ = self._request(f"/items/{encoded_key}", "PATCH", payload, content_type=JSON_MIME)
        if code == 404:
            raise ValueError(f"key '{key}' not found")


def insert_ttl(
    item,
    ttl_attribute: str,
    expire_in: Optional[Union[int, float]] = None,
    expire_at: Optional[Union[int, float, datetime.datetime]] = None,
):
    if expire_in and expire_at:
        raise ValueError("'expire_in' and 'expire_at' are mutually exclusive parameters")

    if not expire_in and not expire_at:
        return

    if expire_in:
        expire_at = datetime.datetime.now() + datetime.timedelta(seconds=expire_in)

    if isinstance(expire_at, datetime.datetime):
        expire_at = expire_at.replace(microsecond=0).timestamp()
    elif not isinstance(expire_at, (int, float)):
        raise TypeError("'expire_at' must be of type 'int', 'float' or 'datetime'")

    item[ttl_attribute] = int(expire_at)
