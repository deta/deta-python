import os
import typing
import urllib.error
import urllib.request
from .base import Base

try:
    import orjson as json
except ImportError:
    import json

try:
    from detalib.app import App

    app = App()
except Exception:
    pass


class Deta:
    def __init__(
        self,
        project_key: typing.Optional[str] = None,
        *,
        project_id: typing.Optional[str] = None,
        host: typing.Optional[str] = None,
    ):
        self.project_key = project_key or os.getenv("DETA_PROJECT_KEY")
        self.project_id = project_id
        if not self.project_id:
            self.project_id = self.project_key.split("_")[0]

    def Base(self, name: str, host: typing.Optional[str] = None):
        return Base(name, self.project_key, self.project_id, host)

    def send_email(self, to, subject, message, charset="UTF-8"):
        return send_email(to, subject, message, charset)


def send_email(
    to: typing.Union[str, typing.List[str]],
    subject: str,
    message: str,
    charset: str = "UTF-8",
):
    pid = os.getenv("AWS_LAMBDA_FUNCTION_NAME")
    url = os.getenv("DETA_MAILER_URL")
    api_key = os.getenv("DETA_PROJECT_KEY")
    endpoint = f"{url}/mail/{pid}"

    to = to if type(to) == list else [to]
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
        raise Exception(e.reason)
