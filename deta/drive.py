from __future__ import annotations

import os
from typing import IO, Generator, Union
from io import BufferedIOBase, BufferedReader, BytesIO, RawIOBase, StringIO, TextIOBase
from urllib.parse import quote_plus

from .service import _SERVICE_RESPONSE_TYPE, JSON_MIME, _Service

# 10 MB upload chunk size
UPLOAD_CHUNK_SIZE = 1024 * 1024 * 10

# timeout for Drive service in seconds
DRIVE_SERVICE_TIMEOUT = 300

_DRIVE_DATA_TYPE = Union[str, bytes, TextIOBase, BufferedIOBase, RawIOBase]


class DriveStreamingBody:
    def __init__(self, res: BufferedIOBase) -> None:
        self.__stream = res

    @property
    def closed(self) -> bool:
        return self.__stream.closed

    def read(self, size: int | None = None) -> bytes:
        return self.__stream.read(size)

    def iter_chunks(self, chunk_size: int = 1024) -> Generator[bytes, None, None]:
        while True:
            chunk = self.__stream.read(chunk_size)
            if not chunk:
                break
            yield chunk

    def iter_lines(self, chunk_size: int = 1024) -> Generator[bytes, None, None]:
        while True:
            chunk = self.__stream.readline(chunk_size)
            if not chunk:
                break
            yield chunk

    def close(self) -> None:
        # close stream
        try:
            self.__stream.close()
        except Exception:
            pass


class _Drive(_Service):
    def __init__(
        self,
        name: str,
        project_key: str,
        project_id: str,
        host: str | None = None,
    ) -> None:
        assert name, "No Drive name provided"
        host = host or os.getenv("DETA_DRIVE_HOST") or "drive.deta.sh"

        super().__init__(
            project_key=project_key,
            project_id=project_id,
            host=host,
            name=name,
            timeout=DRIVE_SERVICE_TIMEOUT,
            keep_alive=False,
        )

    def _quote(self, param: str) -> str:
        return quote_plus(param)

    def get(self, name: str) -> DriveStreamingBody | None:
        """Get/Download a file from drive.
        `name` is the name of the file.
        Returns a DriveStreamingBody.
        """
        assert name, "No name provided"
        _, res = self._request(f"/files/download?name={self._quote(name)}", "GET", stream=True)
        if not isinstance(res, BufferedIOBase):
            return None
        return DriveStreamingBody(res)

    def delete_many(self, names: list[str]) -> _SERVICE_RESPONSE_TYPE:
        """Delete many files from drive in single request.
        `names` are the names of the files to be deleted.
        Returns a dict with 'deleted' and 'failed' files.
        """
        assert names, "Names is empty"
        assert len(names) <= 1000, "More than 1000 names to delete"
        _, res = self._request("/files", "DELETE", {"names": names}, content_type=JSON_MIME)
        return res

    def delete(self, name: str) -> str:
        """Delete a file from drive.
        `name` is the name of the file.
        Returns the name of the file deleted.
        """
        assert name, "Name not provided or empty"
        payload = self.delete_many([name])
        failed = payload.get("failed")  # type: ignore[union-attr]
        if failed:
            raise Exception(f"Failed to delete '{name}':{failed[name]}")
        return name

    def list(
        self,
        limit: int = 1000,
        prefix: str | None = None,
        last: str | None = None,
    ) -> _SERVICE_RESPONSE_TYPE:
        """List file names from drive.
        `limit` is the limit of number of file names to get, defaults to 1000.
        `prefix` is the prefix  of file names.
        `last` is the last name seen in the a previous paginated response.
        Returns a dict with 'paging' and 'names'.
        """
        url = f"/files?limit={limit}"
        if prefix:
            url += f"&prefix={prefix}"
        if last:
            url += f"&last={last}"
        _, res = self._request(url, "GET")
        return res

    def _start_upload(self, name: str) -> str:
        _, res = self._request(f"/uploads?name={self._quote(name)}", "POST")
        return str(res["upload_id"])  # type: ignore[call-overload,index]

    def _finish_upload(self, name: str, upload_id: str) -> None:
        self._request(f"/uploads/{upload_id}?name={self._quote(name)}", "PATCH")

    def _abort_upload(self, name: str, upload_id: str) -> None:
        self._request(f"/uploads/{upload_id}?name={self._quote(name)}", "DELETE")

    def _upload_part(
        self,
        name: str,
        chunk: str | bytes,
        upload_id: str,
        part: int,
        content_type: str | None = None,
    ) -> None:
        self._request(
            f"/uploads/{upload_id}/parts?name={self._quote(name)}&part={part}",
            "POST",
            data=chunk,
            content_type=content_type,
        )

    def _get_content_stream(
        self,
        data: _DRIVE_DATA_TYPE,
    ) -> IO[str] | IO[bytes] | TextIOBase | BufferedIOBase | RawIOBase:
        if isinstance(data, str):
            return StringIO(data)
        elif isinstance(data, bytes):
            return BytesIO(data)
        return data

    def put(
        self,
        name: str,
        data: _DRIVE_DATA_TYPE | None = None,
        *,
        path: str | None = None,
        content_type: str | None = None,
    ) -> str:
        """Put a file in drive.
        `name` is the name of the file.
        `data` is the data to be put.
        `content_type` is the mime type of the file.
        Returns the name of the file.
        """
        assert name, "No name provided"
        assert path or data, "No data or path provided"
        assert not (path and data), "Both path and data provided"

        # start upload
        upload_id = self._start_upload(name)

        content_stream: BufferedReader | IO[str] | IO[bytes] | TextIOBase | BufferedIOBase | RawIOBase
        if path:
            content_stream = open(path, "rb")
        else:
            assert data, "No data provided"
            content_stream = self._get_content_stream(data)
        part = 1

        # upload chunks
        while True:
            chunk = content_stream.read(UPLOAD_CHUNK_SIZE)
            ## eof stop the loop
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
