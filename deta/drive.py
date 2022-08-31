import os
from io import BufferedIOBase, TextIOBase, RawIOBase, StringIO, BytesIO
from typing import Any, Dict, Iterator, Optional, Sequence, Union, overload
from urllib.parse import quote_plus

from .service import JSON_MIME, _Service

# 10 MB upload chunk size
UPLOAD_CHUNK_SIZE = 1024 * 1024 * 10

# timeout for Drive service in seconds
DRIVE_SERVICE_TIMEOUT = 300


class DriveStreamingBody:
    def __init__(self, res: BufferedIOBase):
        self._stream = res

    @property
    def closed(self):
        return self._stream.closed

    def read(self, size: Optional[int] = None) -> bytes:
        return self._stream.read(size)

    def iter_chunks(self, chunk_size: int = 1024) -> Iterator[bytes]:
        while True:
            chunk = self._stream.read(chunk_size)
            if not chunk:
                break
            yield chunk

    def iter_lines(self, chunk_size: int = 1024) -> Iterator[bytes]:
        while True:
            chunk = self._stream.readline(chunk_size)
            if not chunk:
                break
            yield chunk

    def close(self):
        try:
            self._stream.close()
        except Exception:
            pass


class _Drive(_Service):
    def __init__(self, name: str, project_key: str, project_id: str, *, host: Optional[str] = None):
        if not name:
            raise ValueError("parameter 'name' must be a non-empty string")

        host = host or os.getenv("DETA_DRIVE_HOST") or "drive.deta.sh"
        super().__init__(project_key, project_id, host, name, DRIVE_SERVICE_TIMEOUT, False)

    def _quote(self, param: str) -> str:
        return quote_plus(param)

    def get(self, name: str) -> Optional[DriveStreamingBody]:
        """Download a file from drive.
        `name` is the name of the file.
        Returns a DriveStreamingBody.
        """
        if not name:
            raise ValueError("parameter 'name' must be a non-empty string")

        _, res = self._request(f"/files/download?name={self._quote(name)}", "GET", stream=True)
        if res:
            return DriveStreamingBody(res)

    def delete(self, name: str) -> str:
        """Delete a file from drive.
        `name` is the name of the file.
        Returns the name of the file deleted.
        """
        if not name:
            raise ValueError("parameter 'name' must be a non-empty string")

        payload = self.delete_many([name])
        failed = payload.get("failed")
        if failed:
            raise Exception(f"failed to delete '{name}': {failed[name]}")

        return name

    def delete_many(self, names: Sequence[str]) -> dict:
        """Delete many files from drive in single request.
        `names` are the names of the files to be deleted.
        Returns a dict with 'deleted' and 'failed' files.
        """
        if not names:
            raise ValueError("parameter 'names' must be a non-empty list")

        if len(names) > 1000:
            raise ValueError("cannot delete more than 1000 items")

        _, res = self._request("/files", "DELETE", {"names": names}, content_type=JSON_MIME)
        return res

    @overload
    def put(
        self,
        name: str,
        data: Union[str, bytes, TextIOBase, BufferedIOBase, RawIOBase],
        *,
        content_type: str,
    ) -> str:
        ...

    @overload
    def put(
        self,
        name: str,
        *,
        path: str,
        content_type: str,
    ) -> str:
        ...

    def put(self, name, data=None, *, path=None, content_type=None):
        """Put a file in drive.
        `name` is the name of the file.
        `data` is the data to be put.
        `content_type` is the mime type of the file.
        Returns the name of the file.
        """
        if not name:
            raise ValueError("parameter 'name' must be a non-empty string")
        if not path and not data:
            raise ValueError("must provide either 'data' or 'path'")
        if path and data:
            raise ValueError("'data' and 'path' are exclusive parameters")

        # start upload
        upload_id = self._start_upload(name)
        if data:
            content_stream = self._get_content_stream(data)
        else:
            content_stream = open(path, "rb")
        part = 1

        # upload chunks
        while True:
            chunk = content_stream.read(UPLOAD_CHUNK_SIZE)
            # eof stop the loop
            if not chunk:
                self._finish_upload(name, upload_id)
                content_stream.close()
                return name

            # upload part
            try:
                self._upload_part(name, chunk, upload_id, part, content_type)
                part += 1
            # clean up on exception
            # and raise exception again
            except Exception as e:
                self._abort_upload(name, upload_id)
                content_stream.close()
                raise e

    def list(
        self,
        limit: int = 1000,
        prefix: Optional[str] = None,
        last: Optional[str] = None,
    ) -> Dict[str, Any]:
        """List file names from drive.
        `limit` is the number of file names to get, defaults to 1000.
        `prefix` is the prefix of file names.
        `last` is the last name seen in a previous paginated response.
        Returns a dict with 'paging' and 'names'.
        """
        url = f"/files?limit={limit}"
        if prefix:
            url += f"&prefix={prefix}"

        if last:
            url += f"&last={last}"

        _, res = self._request(url, "GET")
        return res

    def _start_upload(self, name: str):
        _, res = self._request(f"/uploads?name={self._quote(name)}", "POST")
        return res["upload_id"]

    def _finish_upload(self, name: str, upload_id: str):
        self._request(f"/uploads/{upload_id}?name={self._quote(name)}", "PATCH")

    def _abort_upload(self, name: str, upload_id: str):
        self._request(f"/uploads/{upload_id}?name={self._quote(name)}", "DELETE")

    def _upload_part(
        self,
        name: str,
        chunk: bytes,
        upload_id: str,
        part: int,
        content_type: Optional[str] = None,
    ):
        self._request(
            f"/uploads/{upload_id}/parts?name={self._quote(name)}&part={part}",
            "POST",
            data=chunk,
            content_type=content_type,
        )

    def _get_content_stream(
        self,
        data: Union[str, bytes, TextIOBase, BufferedIOBase, RawIOBase],
    ):
        if isinstance(data, str):
            return StringIO(data)
        elif isinstance(data, bytes):
            return BytesIO(data)
        else:
            return data
