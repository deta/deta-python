import http.client
from io import BufferedIOBase
import os
import socket
import struct
import typing
import urllib.error
from urllib.parse import quote, urlencode
from urllib import request
import sys
import asyncio
from functools import partial

try:
    import orjson as json
except ImportError:
    import json


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
    
    def measureSize(self, measurable) -> int:
        if isinstance(measurable, BufferedIOBase):
            return sys.getsizeof(measurable.read())
        else:
            return sys.getsizeof(measurable)

class _Object:
    def __init__(self, project_key:str, project_id:str, host:str=None):
        assert project_key, "Please provide a project_key. Check docs.deta.sh"
        self.project_key = project_key
        host = host or os.getenv("DETA_BASE_HOST") or "database.deta.sh"
        self.base_path = "OVERRIDE ME" # TODO there must be a better way
        self.client = http.client.HTTPSConnection(host, timeout=3)
        self.util = Util()
    
    def _is_socket_closed(self):
        if not self.client.sock:
            return True
        fmt = "B" * 7 + "I" * 21
        tcp_info = struct.unpack(
            fmt, self.client.sock.getsockopt(
                socket.IPPROTO_TCP, socket.TCP_INFO, 92)
        )
        # 8 = CLOSE_WAIT
        if len(tcp_info) > 0 and tcp_info[0] == 8:
            return True
        return False

    def _request(self, path: str, method: str, data: typing.Union[str, BufferedIOBase, bytes, dict] = None, headers: dict = None, async:bool=False):
        url = self.base_path + path
        headers = headers or {"X-API-Key": self.project_key,
                              "Content-Type": "application/json"}
        # close connection if socket is closed
        if os.environ.get("DETA_RUNTIME") == "true" and self._is_socket_closed():
            self.client.close()

        if isinstance(data, dict):
            self.client.request(
                method,
                url,
                headers=headers,
                body=json.dumps(data),
            )
        else:
            self.client.request(
                method,
                url,
                headers=headers,
                body=data,
            )
        res = self.client.getresponse()
        status = res.status
        payload = res.read()

        if status in [200, 201, 207, 404]:
            return status, json.loads(payload) if status != 404 else None
        raise urllib.error.HTTPError(
            url, status, res.reason, res.headers, res.fp)

class Drive(_Object):
    def __init__(self, drive_name:str=None, project_key:str=None, project_id:str=None, host:str=None):
        assert drive_name, "Please provide a Drive name. E.g 'mydrive"
        assert project_key, "Please provide a project_key. Check docs.deta.sh"
        self.name = drive_name
        self.project_key = project_key
        self.project_id = project_id
        host = host or os.getenv("DETA_DRIVE_HOST") or "drive.deta.sh"
        self.client = http.client.HTTPSConnection(host, timeout=3)
        self.base_path = "/v1/{0}/{1}".format(self.project_id, self.name)
        self.util = Util()

    def put(self, name:str, data:typing.Union[str, BufferedIOBase, bytes]=None, *, path:str=None, content_type:str=None) -> str:
        chunk_size = 104857600 # TODO 100MB threshold needs tuning
        if (path!=None) and (data!=None):
            raise Exception("Please only provide data or a path. Not both.")
        if path != None:
            data = open(path, 'rb')
        # If it's byte or string, it's already in memory, let's just send it, no?
        if not isinstance(data, BufferedIOBase):
            _, res = self._request("/files", "POST", data)
            return res["name"]
        # Multi-part upload 
        _, res = self._request(f"/uploads?name={name}", "POST")
        uuid = res["upload_id"]
        chunk_number = 1
        retry_queue = []
        def _partial_upload():
                for chunk in iter(partial(data.read, chunk_size), b''):
                    if (chunk_number == 1) and (self.util.measureSize(chunk) < chunk_size):
                        # EOF found early, file small enough to send early
                        _, res = self._request("/files", "POST", data.read())
                    try:
                        _ , res = self._request(f"/uploads/{uuid}/parts?name={name}&part={chunk_number}", "POST", chunk)
                    except Exception as e:
                        print(f"[!] Chunk {chunk_number} of size {chunk_size} failed to upload. Added to retry queue.")
                        retry_queue.append(chunk_number)
                
        with data:
            _partial_upload()
            while len(retry_queue) > 0:
                _partial_upload()
        
        # Finish upload
        _,res = self._request(f"/uploads/{uuid}?name={name}", "PATCH")
        return res["name"]
        


            

            

class Base(_Object):
    def __init__(self, name: str, project_key: str, project_id: str, host: str = None):
        assert name, "Please provide a Base name. E.g 'mydb'"
        assert project_key, "Please provide a project_key. Check docs.deta.sh"
        self.name = name
        self.project_id = project_id
        self.project_key = project_key

        host = host or os.getenv("DETA_BASE_HOST") or "database.deta.sh"
        self.client = http.client.HTTPSConnection(host, timeout=3)
        self.base_path = "/v1/{0}/{1}".format(self.project_id, self.name)
        self.util = Util()

    def get(self, key: str) -> typing.Optional[dict]:
        if key == "":
            raise ValueError("Key is empty")

        # encode key
        key = quote(key, safe="")
        _, res = self._request("/items/{}".format(key), "GET")
        return res or None

    def delete(self, key: str) -> typing.Optional[bool]:
        """Delete an item from the database
        key: the key of item to be deleted
        """
        if key == "":
            raise ValueError("Key is empty")

        # encode key
        key = quote(key, safe="")
        _, _ = self._request("/items/{}".format(key), "DELETE")
        return None

    def insert(self, data: typing.Union[dict, list, str, int, bool], key: str = None):
        if not isinstance(data, dict):
            data = {"value": data}
        else:
            data = data.copy()

        if key:
            data["key"] = key

        code, res = self._request("/items", "POST", {"item": data})
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
    ) -> typing.Optional[typing.Tuple[int, list]]:
        """This is where actual fetch happens."""
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
        code, _ = self._request("/items/{}".format(encoded_key), "PATCH", payload)
        if code == 200:
            return None
        elif code == 404:
            raise Exception("Key '{}' not found".format(key))
