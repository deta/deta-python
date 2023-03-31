from __future__ import annotations

import datetime
import os
from typing import Any, Union
from urllib.parse import quote

from .service import _SERVICE_RESPONSE_TYPE, JSON_MIME, _Service

_BASE_DATA_TYPE = Union[dict[str, Any], list[Any], str, int, bool]

# timeout for Base service in seconds
BASE_SERVICE_TIMEOUT = 300
BASE_TTL_ATTTRIBUTE = "__expires"


class FetchResponse:
    def __init__(
        self,
        count: int = 0,
        last: _BASE_DATA_TYPE | None = None,
        items: list[_BASE_DATA_TYPE] = [],
    ) -> None:
        self._count = count
        self._last = last
        self._items = items

    @property
    def count(self) -> int:
        return self._count

    @property
    def last(self) -> _BASE_DATA_TYPE | None:
        return self._last

    @property
    def items(self) -> list[_BASE_DATA_TYPE]:
        return self._items

    def __eq__(
        self,
        other: FetchResponse,  # type: ignore[override]
    ) -> bool:
        return all(
            (
                self.count == other.count,
                self.last == other.last,
                self.items == other.items,
            )
        )


_UTIL_NUMERIC_VALUE_TYPE = Union[int, float]
_UTIL_ITEM_VALUE_TYPE = Union[dict[str, Any], list[Any], str, int, float, bool]


class Util:
    class Trim:
        pass

    class Increment:
        def __init__(self, value: _UTIL_NUMERIC_VALUE_TYPE | None = None) -> None:
            self.val = value
            if not value:
                self.val = 1

    class Append:
        def __init__(self, value: _UTIL_ITEM_VALUE_TYPE) -> None:
            self.val = value
            if not isinstance(value, list):
                self.val = [value]

    class Prepend:
        def __init__(self, value: _UTIL_ITEM_VALUE_TYPE) -> None:
            self.val = value
            if not isinstance(value, list):
                self.val = [value]

    def trim(self) -> Trim:
        return self.Trim()

    def increment(self, value: _UTIL_NUMERIC_VALUE_TYPE | None = None) -> Increment:
        return self.Increment(value)

    def append(self, value: _UTIL_ITEM_VALUE_TYPE) -> Append:
        return self.Append(value)

    def prepend(self, value: _UTIL_ITEM_VALUE_TYPE) -> Prepend:
        return self.Prepend(value)


class _Base(_Service):
    def __init__(
        self,
        name: str,
        project_key: str,
        project_id: str,
        host: str | None = None,
    ) -> None:
        assert name, "No Base name provided"

        host = host or os.getenv("DETA_BASE_HOST") or "database.deta.sh"
        super().__init__(
            project_key=project_key,
            project_id=project_id,
            host=host,
            name=name,
            timeout=BASE_SERVICE_TIMEOUT,
        )
        self.__ttl_attribute = "__expires"
        self.util = Util()

    def get(self, key: str) -> _SERVICE_RESPONSE_TYPE | None:
        if key == "":
            raise ValueError("Key is empty")

        # encode key
        key = quote(key, safe="")
        _, res = self._request("/items/{}".format(key), "GET")
        return res or None

    def delete(self, key: str) -> None:
        """Delete an item from the database
        key: the key of item to be deleted
        """
        if key == "":
            raise ValueError("Key is empty")

        # encode key
        key = quote(key, safe="")
        self._request("/items/{}".format(key), "DELETE")
        return None

    def insert(
        self,
        data: _BASE_DATA_TYPE,
        key: str | None = None,
        *,
        expire_in: int | None = None,
        expire_at: int | float | datetime.datetime | None = None,
    ) -> _SERVICE_RESPONSE_TYPE | None:
        if not isinstance(data, dict):
            data = {"value": data}
        else:
            data = data.copy()

        if key:
            data["key"] = key

        insert_ttl(data, self.__ttl_attribute, expire_in=expire_in, expire_at=expire_at)
        code, res = self._request("/items", "POST", {"item": data}, content_type=JSON_MIME)
        if code == 201:
            return res
        elif code == 409:
            raise Exception("Item with key '{}' already exists".format(key))
        return None

    def put(
        self,
        data: _BASE_DATA_TYPE,
        key: str | None = None,
        *,
        expire_in: int | None = None,
        expire_at: int | float | datetime.datetime | None = None,
    ) -> _BASE_DATA_TYPE | None:
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

        insert_ttl(data, self.__ttl_attribute, expire_in=expire_in, expire_at=expire_at)
        code, res = self._request("/items", "PUT", {"items": [data]}, content_type=JSON_MIME)
        if res and code == 207:
            return res["processed"]["items"][0]  # type: ignore[call-overload,index,no-any-return]
        return None

    def put_many(
        self,
        items: list[_BASE_DATA_TYPE],
        *,
        expire_in: int | None = None,
        expire_at: int | float | datetime.datetime | None = None,
    ) -> _SERVICE_RESPONSE_TYPE:
        assert len(items) <= 25, "We can't put more than 25 items at a time."
        _items = []
        for i in items:
            data = i if isinstance(i, dict) else {"value": i}
            insert_ttl(data, self.__ttl_attribute, expire_in=expire_in, expire_at=expire_at)
            _items.append(data)

        _, res = self._request("/items", "PUT", {"items": _items}, content_type=JSON_MIME)
        return res

    def _fetch(
        self,
        query: dict[str, Any] | list[Any] | None = None,
        buffer: int | None = None,
        last: str | None = None,
    ) -> tuple[int | None, Any]:
        """This is where actual fetch happens."""
        payload: dict[str, Any] = {
            "limit": buffer,
            "last": last if not isinstance(last, bool) else None,
        }

        if query:
            payload["query"] = query if isinstance(query, list) else [query]

        code, res = self._request("/query", "POST", payload, content_type=JSON_MIME)
        return code, res

    def fetch(
        self,
        query: dict[str, Any] | list[Any] | None = None,
        *,
        limit: int = 1000,
        last: str | None = None,
    ) -> FetchResponse:
        """
        fetch items from the database.
        `query` is an optional filter or list of filters.
        Without filter, it will return the whole db.
        """
        _, res = self._fetch(query, limit, last)

        paging = res.get("paging")

        return FetchResponse(paging.get("size"), paging.get("last"), res.get("items"))

    def update(
        self,
        updates: dict[str, Any],
        key: str,
        *,
        expire_in: int | None = None,
        expire_at: int | float | datetime.datetime | None = None,
    ) -> None:
        """
        update an item in the database
        `updates` specifies the attribute names and values to update,add or remove
        `key` is the key of the item to be updated
        """

        if key == "":
            raise ValueError("Key is empty")

        payload: dict[str, Any] = {
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
                    payload["increment"][attr] = value.val
                elif isinstance(value, Util.Append):
                    payload["append"][attr] = value.val
                elif isinstance(value, Util.Prepend):
                    payload["prepend"][attr] = value.val
                else:
                    payload["set"][attr] = value

        insert_ttl(
            payload["set"],
            self.__ttl_attribute,
            expire_in=expire_in,
            expire_at=expire_at,
        )

        encoded_key = quote(key, safe="")
        code, _ = self._request(
            "/items/{}".format(encoded_key),
            "PATCH",
            payload,
            content_type=JSON_MIME,
        )
        if code == 200:
            return None
        elif code == 404:
            raise Exception("Key '{}' not found".format(key))


def insert_ttl(
    item: dict[str, Any],
    ttl_attribute: str,
    expire_in: float | None = None,
    expire_at: datetime.datetime | float | None = None,
) -> None:
    if expire_in and expire_at:
        raise ValueError("both expire_in and expire_at provided")
    if expire_in is None and expire_at is None:
        return

    if expire_in is not None:
        expire_at = datetime.datetime.now() + datetime.timedelta(seconds=expire_in)

    if isinstance(expire_at, datetime.datetime):
        expire_at = expire_at.replace(microsecond=0).timestamp()

    if not isinstance(expire_at, (int, float)):
        raise TypeError("expire_at should one one of int, float or datetime")

    item[ttl_attribute] = int(expire_at)
