import http.client
import json
import os
import typing
import urllib.error


class Base:
    def __init__(self, name: str, project_key: str, project_id: str, host: str = None):
        self.name = name
        self.project_id = project_id
        if project_key:
            self.project_key = project_key
            try:
                self.project_id = project_key.split("_")[0]
            except Exception:
                raise Exception("Invalid Project Key")

        host = host or os.getenv("DETA_BASE_HOST") or "database.deta.sh"
        self.client = http.client.HTTPSConnection(host, timeout=3)
        self.base_path = "/v1/{0}/{1}".format(self.project_id, self.name)

    def _request(self, path: str, method: str, data: dict = None):
        url = self.base_path + path
        # print(path, method, data, url)
        self.client.request(
            method,
            url,
            headers={"X-API-Key": self.project_key, "Content-Type": "application/json"},
            body=json.dumps(data),
        )
        res = self.client.getresponse()
        status = res.status
        payload = res.read()
        # print(status, res.reason, payload)

        if status in [200, 201, 207, 404]:
            return status, json.loads(payload) if status != 404 else None
        raise urllib.error.HTTPError(url, status, res.reason, res.headers, res.fp)

    def get(self, key: str) -> dict:
        _, res = self._request("/items/{}".format(key), "GET")
        return res or None

    def delete(self, key: str) -> bool:
        """Delete an item from the database
        key: the key of item to be deleted
        """
        _, _ = self._request("/items/{}".format(key), "DELETE")
        return None

    def insert(self, data: typing.Union[dict, list, str, int, bool], key: str = None):
        if not isinstance(data, dict):
            data = {"value": data}

        if key:
            data["key"] = key

        code, res = self._request("/items", "POST", {"item": data})
        if code == 201:
            return res
        elif code == 409:
            raise Exception("Item with key '{4}' already exists".format(key))

    def put(self, data: typing.Union[dict, list, str, int, bool], key: str = None):
        """ store (put) an item in the database. Overrides an item if key already exists.
            `key` could be provided as function argument or a field in the data dict.
            If `key` is not provided, the server will generate a random 12 chars key.
        """

        if not isinstance(data, dict):
            data = {"value": data}

        if key:
            data["key"] = key

        code, res = self._request("/items", "PUT", {"items": [data]})
        return res["processed"]["items"][0] if res and code == 207 else None

    def put_many(self, items: typing.List[typing.Union[dict, list, str, int, bool]]):
        assert len(items) <= 25, "We can't put more than 25 items at a time."
        _items = []
        for i in items:
            if not isinstance(i, dict):
                _items.append({"value": i})
            else:
                _items.append(i)

        _, res = self._request("/items", "PUT", {"items": _items})
        return res

    def _fetch(
        self,
        query: typing.Union[dict, list] = None,
        buffer: int = None,
        last: str = None,
    ) -> typing.Tuple[int, list]:
        """This is where actual fetch happens.
        """
        payload = {
            "limit": buffer,
            "last": last if not isinstance(last, bool) else None,
        }

        if query:
            payload["query"] = query if isinstance(query, list) else [query]

        code, res = self._request("/query", "POST", payload)
        return code, res

    def fetch(
        self,
        query: typing.Union[dict, list] = None,
        *,
        buffer: int = None,
        pages: int = 10,
    ) -> typing.Generator:
        """
        fetch items from the database.
            `query` is an optional filter or list of filters. Without filter, it will return the whole db.            
        Returns a generator with all the result, We will paginate the request based on `buffer`.
        """
        last = True
        code = 200
        counter = 0
        while code == 200 and last and pages > counter:
            code, res = self._fetch(query, buffer, last)
            items = res["items"]
            last = res["paging"].get("last")
            counter += 1
            yield items


class Deta:
    def __init__(
        self, project_key: str = None, *, project_id: str = None, host: str = None
    ):
        self.project_key = project_key
        self.project_id = project_id

    def Base(self, name: str, host: str = None):
        return Base(name, self.project_key, self.project_id, host)
