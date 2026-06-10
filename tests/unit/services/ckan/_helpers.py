from typing import Any


class FakeRequest:
    def __init__(self, method: str):
        self.method = method


class FakeResponse:
    def __init__(
        self,
        *,
        url: str = "https://ckan.test/api/3/action/package_show",
        status_code: int = 200,
        text: str = "ok",
        json_value: Any = None,
        json_exc: Exception | None = None,
        request: FakeRequest | None = None,
    ):
        self.url = url
        self.status_code = status_code
        self.text = text
        self._json_value = json_value
        self._json_exc = json_exc
        self.request = request

    def json(self) -> Any:
        if self._json_exc is not None:
            raise self._json_exc
        return self._json_value


class FakeSession:
    def __init__(self, responses: list[FakeResponse] | None = None):
        self.headers: dict[str, str] = {}
        self.calls: list[dict[str, Any]] = []
        self.responses = list(responses or [FakeResponse()])

    def request(self, method: str, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append({"method": method, "url": url, **kwargs})
        response = self.responses.pop(0)
        response.url = url
        response.request = FakeRequest(method)
        return response
