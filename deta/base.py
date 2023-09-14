import os
import datetime
from typing import Union, List
from urllib.parse import quote

from .service import _Service, JSON_MIME

# timeout for Base service in seconds
BASE_SERVICE_TIMEOUT = 300
BASE_TTL_ATTTRIBUTE = "__expires"


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

    def increment(self, value: Union[int, float, None] = None):
        return self.Increment(value)

    def append(self, value: Union[dict, list, str, int, float, bool]):
        return self.Append(value)

    def prepend(self, value: Union[dict, list, str, int, float, bool]):
        return self.Prepend(value)


class _Base(_Service):
    def __init__(self, name: str, project_key: str, project_id: str, host: Union[str, None] = None):
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

    def insert(
        self,
        data: Union[dict, list, str, int, bool],
        key: Union[str, None] = None,
        *,
        expire_in: Union[int, None] = None,
        expire_at: Union[int, float, datetime.datetime, None] = None,
    ):
        if not isinstance(data, dict):
            data = {"value": data}
        else:
            data = data.copy()

        if key:
            data["key"] = key

        insert_ttl(data, self.__ttl_attribute,
                   expire_in=expire_in, expire_at=expire_at)
        code, res = self._request(
            "/items", "POST", {"item": data}, content_type=JSON_MIME
        )
        if code == 201:
            return res
        elif code == 409:
            raise Exception("Item with key '{4}' already exists".format(key))

    def put(
        self,
        data: Union[dict, list, str, int, bool],
        key: Union[str, None] = None,
        *,
        expire_in: Union[int, None] = None,
        expire_at: Union[int, float, datetime.datetime, None] = None,
    ):
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

        insert_ttl(data, self.__ttl_attribute,
                   expire_in=expire_in, expire_at=expire_at)
        code, res = self._request(
            "/items", "PUT", {"items": [data]}, content_type=JSON_MIME
        )


        if code == 207 and "processed" in res:
            return res["processed"]["items"][0]
        else:
            return None

    def put_many(
        self,
        items: List[Union[dict, list, str, int, bool]],
        *,
        expire_in: Union[int, None] = None,
        expire_at: Union[int, float, datetime.datetime, None] = None,
    ):
        assert len(items) <= 25, "We can't put more than 25 items at a time."
        _items = []
        for i in items:
            data = i
            if not isinstance(i, dict):
                data = {"value": i}
            insert_ttl(
                data, self.__ttl_attribute, expire_in=expire_in, expire_at=expire_at
            )
            _items.append(data)

        _, res = self._request(
            "/items", "PUT", {"items": _items}, content_type=JSON_MIME
        )
        return res

    def _fetch(
        self,
        query: Union[dict, list, None] = None,
        buffer: Union[int, None] = None,
        last: Union[str, None] = None,
        desc: bool = False, 
    ) -> typing.Optional[typing.Tuple[int, list]]:
        """This is where actual fetch happens."""
        payload = {
            "limit": buffer,
            "last": last if not isinstance(last, bool) else None,
            "sort": "desc" if desc else "",
        }

        if query:
            payload["query"] = query if isinstance(query, list) else [query]

        _, res = self._request(
            "/query", "POST", payload, content_type=JSON_MIME)

        assert res 

        return res

    def fetch(
        self,
        query: Union[dict, list, None] = None,
        *,
        limit: int = 1000,
        last: Union[str, None] = None,
        desc: bool = False, 

    ):
        """
        fetch items from the database.
            `query` is an optional filter or list of filters. Without filter, it will return the whole db.
        """
        
        _, res = self._fetch(query, limit, last, desc)


        paging = res.get("paging")  # pyright: ignore

        return FetchResponse(paging.get("size"),
                             paging.get("last"),
                             res.get("items"))  # pyright: ignore

    def update(
        self,
        updates: dict,
        key: str,
        *,
        expire_in: Union[int, None] = None,
        expire_at: Union[int, float, datetime.datetime, None] = None,
    ):
        """
        update an item in the database
        `updates` specifies the attribute names and values to update,add or remove
        `key` is the key of the item to be updated
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
            "/items/{}".format(encoded_key), "PATCH", payload, content_type=JSON_MIME
        )
        if code == 200:
            return None
        elif code == 404:
            raise Exception("Key '{}' not found".format(key))


def insert_ttl(item, ttl_attribute, expire_in=None, expire_at=None):
    if expire_in and expire_at:
        raise ValueError("both expire_in and expire_at provided")
    if not expire_in and not expire_at:
        return

    if expire_in:
        expire_at = datetime.datetime.now() + datetime.timedelta(seconds=expire_in)

    if isinstance(expire_at, datetime.datetime):
        expire_at = expire_at.replace(microsecond=0).timestamp()

    if not isinstance(expire_at, (int, float)):
        raise TypeError("expire_at should one one of int, float or datetime")

    item[ttl_attribute] = int(expire_at)
