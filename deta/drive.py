import os
import typing
from io import BufferedIOBase, TextIOBase, RawIOBase, StringIO, BytesIO
from urllib.parse import quote_plus

from .service import _Service

UPLOAD_CHUNK_SIZE = 10485760


class DriveStreamingBody:
    def __init__(self, res: BufferedIOBase):
        self.stream = res

    def read(self, size: int = None):
        return self.stream.read(size)

    def iter_chunks(self, chunk_size: int = 1024):
        yield self.stream.read(chunk_size)


class Drive(_Service):
    def __init__(
        self,
        name: str = None,
        project_key: str = None,
        project_id: str = None,
        host: str = None,
    ):
        assert name, "No Drive name provided"
        host = host or os.getenv("DETA_DRIVE_HOST") or "drive.deta.sh"

        super().__init__(
            project_key=project_key,
            project_id=project_id,
            host=host,
            name=name,
            timeout=300
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
        return DriveStreamingBody(res)

    def delete_many(self, names: typing.List[str]) -> typing.Optional[dict]:
        """Delete many files from drive in single request.
        `names` are the names of the files to be deleted.
        Returns a dict with 'deleted' and 'failed' files.
        """
        assert names, "Names is empty"
        _, res = self._request("/files", "DELETE", {"names": names})
        return res

    def delete(self, name: str) -> typing.Optional[str]:
        """Delete a file from drive.
        `name` is the name of the file.
        Returns the name of the file deleted.
        """
        assert name, "Name not provided or empty"
        _, payload = self.delete_many([name])
        failed = payload.get("failed")
        if failed:
            raise Exception(f"Failed to delete '{name}':{failed[name]}")
        return name

    def list(self, limit: int = 1000, prefix: str = None, last: str = None):
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
        return res["upload_id"]

    def _finish_upload(self, name: str, upload_id: str):
        self._request(f"/uploads/{upload_id}?name={self._quote(name)}", "PATCH")

    def _abort_upload(self, name: str, upload_id: str):
        self._request(f"/uploads/{upload_id}?name={self._quote(name)}", "DELETE")

    def _upload_part(
        self, name: str, upload_id: str, part: int, content_type: str = None
    ):
        self._request(
            f"/uploads/{upload_id}/parts?name={self._quote(name)}&part={part}",
            "POST",
            content_type=content_type,
        )

    def _get_content_stream(
        self, data: typing.Union[str, bytes, TextIOBase, BufferedIOBase, RawIOBase]
    ):
        if isinstance(data, str):
            return StringIO(data)
        elif isinstance(data, bytes):
            return BytesIO(data)
        else:
            return data

    def put(
        self,
        name: str,
        data: typing.Union[str, bytes, TextIOBase, BufferedIOBase, RawIOBase] = None,
        *,
        path: str = None,
        content_type: str = None,
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

        content_stream = open(path, "rb") if path else self._get_content_stream(data)
        part = 1

        # upload chunks
        while True:
            chunk = content_stream.read(UPLOAD_CHUNK_SIZE)
            ## eof stop the loop
            if not chunk:
                self._finish_upload(name, upload_id)
                content_stream.close()
                break
            try:
                self._upload_part(name, upload_id, part, content_type)
                part += 1
            # clean up on exception
            # and raise exception again
            except Exception as e:
                self._abort_upload(name, upload_id)
                content_stream.close()
                raise e

        # finish upload
        self._finish_upload(name, upload_id)
        return name
