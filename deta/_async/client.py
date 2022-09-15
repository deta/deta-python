import os
import datetime
from typing import Mapping, Optional, Sequence, Union, overload
from urllib.parse import quote

try:
    import aiohttp
except ImportError:
    has_aiohttp = False
else:
    has_aiohttp = True

from deta.base import FetchResponse, Util, insert_ttl, BASE_TTL_ATTRIBUTE


class _AsyncBase:
    def __init__(
        self,
        name: str,
        project_key: str,
        project_id: str,
        *,
        host: Optional[str] = None,
        session: "Optional[aiohttp.ClientSession]" = None,
    ):
        if not has_aiohttp:
            raise RuntimeError("aiohttp library is required for async support")

        if not name:
            raise ValueError("parameter 'name' must be a non-empty string")

        host = host or os.getenv("DETA_BASE_HOST") or "database.deta.sh"
        self._base_url = f"https://{host}/v1/{project_id}/{name}"

        self.util = Util()
        self._ttl_attribute = BASE_TTL_ATTRIBUTE

        if session is not None:
            self._session = session
        else:
            self._session = aiohttp.ClientSession(
                headers={
                    "Content-type": "application/json",
                    "X-API-Key": project_key,
                },
                raise_for_status=True,
            )

    async def close(self):
        await self._session.close()

    async def get(self, key: str) -> Optional[dict]:
        key = quote(key, safe="")
        try:
            async with self._session.get(f"{self._base_url}/items/{key}") as resp:
                return await resp.json()
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                return
            else:
                raise e

    async def delete(self, key: str):
        key = quote(key, safe="")
        async with self._session.delete(f"{self._base_url}/items/{key}"):
            return

    @overload
    async def insert(
        self,
        data: Union[dict, list, str, int, bool],
        key: Optional[str] = None,
    ) -> dict:
        ...

    @overload
    async def insert(
        self,
        data: Union[dict, list, str, int, bool],
        key: Optional[str] = None,
        *,
        expire_in: int,
    ) -> dict:
        ...

    @overload
    async def insert(
        self,
        data: Union[dict, list, str, int, bool],
        key: Optional[str] = None,
        *,
        expire_at: Union[int, float, datetime.datetime],
    ) -> dict:
        ...

    async def insert(self, data, key=None, *, expire_in=None, expire_at=None):
        data = data.copy() if isinstance(data, dict) else {"value": data}

        if key:
            data["key"] = key

        insert_ttl(data, self._ttl_attribute, expire_in, expire_at)
        async with self._session.post(f"{self._base_url}/items", json={"item": data}) as resp:
            return await resp.json()

    @overload
    async def put(
        self,
        data: Union[dict, list, str, int, bool],
        key: Optional[str] = None,
    ) -> dict:
        ...

    @overload
    async def put(
        self,
        data: Union[dict, list, str, int, bool],
        key: Optional[str] = None,
        *,
        expire_in: int,
    ) -> dict:
        ...

    @overload
    async def put(
        self,
        data: Union[dict, list, str, int, bool],
        key: Optional[str] = None,
        *,
        expire_at: Union[int, float, datetime.datetime],
    ) -> dict:
        ...

    async def put(self, data, key=None, *, expire_in=None, expire_at=None):
        data = data.copy() if isinstance(data, dict) else {"value": data}

        if key:
            data["key"] = key

        insert_ttl(data, self._ttl_attribute, expire_in, expire_at)
        async with self._session.put(f"{self._base_url}/items", json={"items": [data]}) as resp:
            if resp.status != 207:
                return None
            resp_json = await resp.json()
            return resp_json["processed"]["items"][0]

    @overload
    async def put_many(
        self,
        items: Sequence[Union[dict, list, str, int, bool]],
    ) -> dict:
        ...

    @overload
    async def put_many(
        self,
        items: Sequence[Union[dict, list, str, int, bool]],
        *,
        expire_in: int,
    ) -> dict:
        ...

    @overload
    async def put_many(
        self,
        items: Sequence[Union[dict, list, str, int, bool]],
        *,
        expire_at: Union[int, float, datetime.datetime],
    ) -> dict:
        ...

    async def put_many(self, items, *, expire_in=None, expire_at=None):
        if len(items) > 25:
            raise ValueError("cannot put more than 25 items at a time")

        _items = []
        for item in items:
            data = item
            if not isinstance(item, dict):
                data = {"value": item}
            insert_ttl(data, self._ttl_attribute, expire_in, expire_at)
            _items.append(data)

        async with self._session.put(f"{self._base_url}/items", json={"items": _items}) as resp:
            return await resp.json()

    async def fetch(
        self,
        query: Optional[Union[Mapping, Sequence[Mapping]]] = None,
        *,
        limit: int = 1000,
        last: Optional[str] = None,
    ):
        payload = {
            "limit": limit,
            "last": last if not isinstance(last, bool) else None,
        }

        if query:
            payload["query"] = query if isinstance(query, Sequence) else [query]

        async with self._session.post(f"{self._base_url}/query", json=payload) as resp:
            resp_json = await resp.json()
            paging = resp_json.get("paging")
            return FetchResponse(paging.get("size"), paging.get("last"), resp_json.get("items"))

    @overload
    async def update(
        self,
        updates: Mapping,
        key: str,
    ):
        ...

    @overload
    async def update(
        self,
        updates: Mapping,
        key: str,
        *,
        expire_in: int,
    ):
        ...

    @overload
    async def update(
        self,
        updates: Mapping,
        key: str,
        *,
        expire_at: Union[int, float, datetime.datetime],
    ):
        ...

    async def update(self, updates, key, *, expire_in=None, expire_at=None):
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

        if not payload:
            raise ValueError("must provide at least one update action")

        insert_ttl(payload["set"], self._ttl_attribute, expire_in, expire_at)

        encoded_key = quote(key, safe="")
        await self._session.patch(f"{self._base_url}/items/{encoded_key}", json=payload)
