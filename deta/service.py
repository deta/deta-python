import http.client
import os
import socket
import struct
import typing
import urllib.error

try:
    import orjson as json
except ImportError:
    import json

JSON_MIME = 'application/json'

class _Service:
    def __init__(self, project_key: str, project_id: str, host: str, name: str, timeout: int):
        self.project_key = project_key
        self.base_path = "/v1/{0}/{1}".format(project_id, name)
        self.host = host
        self.client = http.client.HTTPSConnection(host, timeout=timeout)
    
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
    

    def _request(self, path: str, method: str, data:typing.Union[str, bytes, dict]= None, headers: dict = None, content_type: str = None, stream:bool = False):
        url = self.base_path + path
        headers = headers or {}
        headers["X-Api-Key"] = self.project_key
        headers["Content-Type"] = content_type or JSON_MIME

        # close connection if socket is closed
        # fix for a bug in lambda
        if os.environ.get("DETA_RUNTIME") == "true" and self._is_socket_closed():
            self.client.close()

        # send request
        body = json.dumps(data) if content_type == JSON_MIME else data 
        self.client.request(
            method,
            url,
            headers=headers,
            body=body,
        )

        # response
        res = self.client.getresponse()
        status = res.status

        if status not in [200, 201, 202, 207, 404]:
            raise urllib.error.HTTPError(
                url, status, res.reason, res.headers, res.fp
            )
        
        ## return None if not found
        if status == 404: return status, None

        ## if stream return the response without reading
        if stream: return status, res

        ## return json if application/json
        if res.getheader("content-type") == JSON_MIME: return status, json.loads(res.read())

        return status, res.read() 