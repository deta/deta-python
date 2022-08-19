import os
import json
import urllib.error
import urllib.request
from typing import Optional, Sequence, Union

from .base import _Base
from ._async.client import _AsyncBase
from .drive import _Drive
from .utils import _get_project_key_id

try:
    from detalib.app import App  # type: ignore

    app = App()
except ImportError:
    pass

__version__ = "1.1.0"


def Base(name: str, host: Optional[str] = None):
    project_key, project_id = _get_project_key_id()
    return _Base(name, project_key, project_id, host)


def AsyncBase(name: str, host: Optional[str] = None):
    project_key, project_id = _get_project_key_id()
    return _AsyncBase(name, project_key, project_id, host)


def Drive(name: str, host: Optional[str] = None):
    project_key, project_id = _get_project_key_id()
    return _Drive(name, project_key, project_id, host)


class Deta:
    def __init__(self, project_key: Optional[str] = None, *, project_id: Optional[str] = None):
        self.project_key, self.project_id = _get_project_key_id(project_key, project_id)

    def Base(self, name: str, host: Optional[str] = None):
        return _Base(name, self.project_key, self.project_id, host)

    def AsyncBase(self, name: str, host: Optional[str] = None):
        return _AsyncBase(name, self.project_key, self.project_id, host)

    def Drive(self, name: str, host: Optional[str] = None):
        return _Drive(name, self.project_key, self.project_id, host)

    def send_email(
        self,
        to: Union[str, Sequence[str]],
        subject: str,
        message: str,
        charset: str = "utf-8",
    ):
        send_email(to, subject, message, charset)


def send_email(
    to: Union[str, Sequence[str]],
    subject: str,
    message: str,
    charset: str = "utf-8",
):
    # should function continue if these are not present?
    pid = os.getenv("AWS_LAMBDA_FUNCTION_NAME")
    url = os.getenv("DETA_MAILER_URL")
    api_key = os.getenv("DETA_PROJECT_KEY")
    endpoint = f"{url}/mail/{pid}"

    if isinstance(to, str):
        to = [to]
    else:
        to = list(to)

    data = {
        "to": to,
        "subject": subject,
        "message": message,
        "charset": charset,
    }
    headers = {"X-API-Key": api_key}

    req = urllib.request.Request(endpoint, json.dumps(data).encode("utf-8"), headers)

    try:
        resp = urllib.request.urlopen(req)
        if resp.getcode() != 200:
            raise Exception(resp.read().decode("utf-8"))
    except urllib.error.URLError as e:
        raise Exception(e.reason) from e
