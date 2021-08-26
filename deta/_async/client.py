import typing

import os
import aiohttp
from urllib.parse import quote

from deta.utils import _get_project_key_id
from deta.base import FetchResponse, Util


class AsyncDeta:
    def __init__(self, project_key: str = None, *, project_id: str = None):
        project_key, project_id = _get_project_key_id(project_key, project_id)
        self.project_key = project_key
        self.project_id = project_id

    def Base(self, name: str, host: str = None) -> "_AsyncBase":
        return _AsyncBase(name, self.project_key, self.project_id, host)


def AsyncBase(name: str):
    project_key, project_id = _get_project_key_id()
    return _AsyncBase(name, project_key, project_id)


class _AsyncBase:
    def __init__(self, name: str, project_key: str, project_id: str, host: str = None):
        if not project_key:
            raise AssertionError("No Base name provided")

        host = host or os.getenv("DETA_BASE_HOST") or "database.deta.sh"
        self._base_url = f"https://{host}/v1/{project_id}/{name}"

        self.util = Util()

        self._session = aiohttp.ClientSession(
            headers={
                "Content-type": "application/json",
                "X-API-Key": project_key,
            },
            raise_for_status=True,
        )

    async def close(self) -> None:
        await self._session.close()

    async def get(self, key: str):
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

    async def insert(
        self, data: typing.Union[dict, list, str, int, bool], key: str = None
    ):
        if not isinstance(data, dict):
            data = {"value": data}
        else:
            data = data.copy()

        if key:
            data["key"] = key

        async with self._session.post(
            f"{self._base_url}/items", json={"item": data}
        ) as resp:
            return await resp.json()

    async def put(
        self, data: typing.Union[dict, list, str, int, bool], key: str = None
    ):
        if not isinstance(data, dict):
            data = {"value": data}
        else:
            data = data.copy()

        if key:
            data["key"] = key

        async with self._session.put(
            f"{self._base_url}/items", json={"items": [data]}
        ) as resp:
            if resp.status == 207:
                resp_json = await resp.json()
                return resp_json["processed"]["items"][0]
            else:
                return None

    async def put_many(
        self, items: typing.List[typing.Union[dict, list, str, int, bool]]
    ):
        if len(items) > 25:
            raise AssertionError("We can't put more than 25 items at a time.")
        _items = []
        for i in items:
            if not isinstance(i, dict):
                _items.append({"value": i})
            else:
                _items.append(i)

        async with self._session.put(
            f"{self._base_url}/items", json={"items": _items}
        ) as resp:
            return await resp.json()

    async def fetch(
        self,
        query: typing.Union[dict, list] = None,
        *,
        limit: int = 1000,
        last: str = None,
    ):
        payload = {}
        if query:
            payload["query"] = query if isinstance(query, list) else [query]
        if limit:
            payload["limit"] = limit
        if last:
            payload["last"] = last
        async with self._session.post(f"{self._base_url}/query", json=payload) as resp:
            resp_json = await resp.json()
            paging = resp_json.get("paging")
            return FetchResponse(
                paging.get("size"), paging.get("last"), resp_json.get("items")
            )

    async def update(self, updates: dict, key: str):
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

        if not payload:
            raise ValueError("Provide at least one update action.")

        key = quote(key, safe="")

        await self._session.patch(f"{self._base_url}/items/{key}", json=payload)
