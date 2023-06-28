import os
from typing import Union, List
from io import BufferedIOBase, TextIOBase, RawIOBase, StringIO, BytesIO
from urllib.parse import quote_plus

from .service import JSON_MIME, _Service

# 10 MB upload chunk size
UPLOAD_CHUNK_SIZE = 1024 * 1024 * 10

# timeout for Drive service in seconds
DRIVE_SERVICE_TIMEOUT = 300


class DriveStreamingBody:
    def __init__(self, res: BufferedIOBase):
        self.__stream = res

    @property
    def closed(self):
        return self.__stream.closed

    def read(self, size: Union[int, None] = None):
        return self.__stream.read(size)

    def iter_chunks(self, chunk_size: int = 1024):
        while True:
            chunk = self.__stream.read(chunk_size)
            if not chunk:
                break
            yield chunk

    def iter_lines(self, chunk_size: int = 1024):
        while True:
            chunk = self.__stream.readline(chunk_size)
            if not chunk:
                break
            yield chunk

    def close(self):
        # close stream
        try:
            self.__stream.close()
        except:
            pass


class _Drive(_Service):
    def __init__(
        self,
        name: Union[str, None] = None,
        project_key: Union[str, None] = None,
        project_id: Union[str, None] = None,
        host: Union[str, None] = None,
    ):
        assert name, "No Drive name provided"
        host = host or os.getenv("DETA_DRIVE_HOST") or "drive.deta.sh"

        assert project_key, "Project key must be provided"
        assert project_id, "Project id must be provided"

        super().__init__(
            project_key=project_key,
            project_id=project_id,
            host=host,
            name=name,
            timeout=DRIVE_SERVICE_TIMEOUT,
            keep_alive=False,
        )

    def _quote(self, param: str):
        return quote_plus(param)

    def get(self, name: str):
        """Get/Download a file from drive.
        `name` is the name of the file.
        Returns a DriveStreamingBody.
        """
        assert name, "No name provided"
        _, res = self._request(
            f"/files/download?name={self._quote(name)}", "GET", stream=True
        )
        if res:
            return DriveStreamingBody(res)  # pyright: ignore
        return None

    def delete_many(self, names: List[str]):
        """Delete many files from drive in single request.
        `names` are the names of the files to be deleted.
        Returns a dict with 'deleted' and 'failed' files.
        """
        assert names, "Names is empty"
        assert len(names) <= 1000, "More than 1000 names to delete"
        _, res = self._request(
            "/files", "DELETE", {"names": names}, content_type=JSON_MIME
        )
        return res

    def delete(self, name: str):
        """Delete a file from drive.
        `name` is the name of the file.
        Returns the name of the file deleted.
        """
        assert name, "Name not provided or empty"

        payload = self.delete_many([name])

        failed = payload.get("failed")  # pyright: ignore

        if failed:
            raise Exception(f"Failed to delete '{name}':{failed[name]}")

        return name

    def list(self, limit: int = 1000, prefix: Union[str, None] = None,
             last: Union[str, None] = None):
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

    def _start_upload(self, name: str):
        _, res = self._request(f"/uploads?name={self._quote(name)}", "POST")
        return res["upload_id"]  # pyright: ignore

    def _finish_upload(self, name: str, upload_id: str):
        self._request(f"/uploads/{upload_id}?name={self._quote(name)}", "PATCH")

    def _abort_upload(self, name: str, upload_id: str):
        self._request(
            f"/uploads/{upload_id}?name={self._quote(name)}", "DELETE")

    def _upload_part(
        self,
        name: str,
        chunk: Union[bytes, str],
        upload_id: str,
        part: int,
        content_type: Union[str, None] = None,
    ):
        self._request(
            f"/uploads/{upload_id}/parts?name={self._quote(name)}&part={part}",
            "POST",
            data=chunk,
            content_type=content_type,
        )

    def _get_content_stream(
        self, data: Union[str, bytes, TextIOBase, BufferedIOBase, RawIOBase]
    ):
        if isinstance(data, str):
            return StringIO(data)
        elif isinstance(data, bytes):
            return BytesIO(data)
        return data

    def put(
        self,
        name: str,
        data: Union[str, bytes, TextIOBase,
                    BufferedIOBase, RawIOBase, None] = None,
        *,
        path: Union[str, None] = None,
        content_type: Union[str, None] = None,
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

        if path:
            content_stream = open(path, "rb")
        else:
            assert data
            content_stream = self._get_content_stream(data)

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
