from typing import Any


class FakeResponse:
    def __init__(self, *, json_value: Any = None):
        self.url = "https://dataportal.material-digital.de"
        self.status_code = 200
        self.text = ""
        self.request = None
        self._json_value = json_value

    def json(self) -> Any:
        return self._json_value

    def raise_for_status(self) -> None:
        return None


class FakeSession:
    def __init__(self):
        self.headers: dict[str, str] = {}
        self.calls: list[dict[str, Any]] = []
        self.response = FakeResponse()

    def request(self, method: str, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append({"method": method, "url": url, **kwargs})
        self.response.url = url
        return self.response
