from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import time
from typing import Any
from urllib.parse import urlparse

import requests

from .config import AppConfig


@dataclass(slots=True)
class FetchResult:
    url: str
    status_code: int
    content: bytes
    content_type: str | None

    @property
    def text(self) -> str:
        return self.content.decode("utf-8", errors="replace")


class BaseFetcher:
    def __init__(self, config: AppConfig):
        self.config = config
        self._last_request_at = 0.0

    def _check_domain(self, url: str) -> None:
        host = urlparse(url).hostname or ""
        if not any(host == domain or host.endswith(f".{domain}") for domain in self.config.allowed_domains):
            raise ValueError(f"Domain not allowed: {host}")

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        delay = self.config.throttle_seconds - elapsed
        if delay > 0:
            time.sleep(delay)
        self._last_request_at = time.monotonic()


class RequestsFetcher(BaseFetcher):
    def __init__(self, config: AppConfig):
        super().__init__(config)
        self.session = requests.Session()
        self.session.headers["User-Agent"] = config.user_agent

    def fetch(self, url: str) -> FetchResult:
        return self.request("GET", url)

    def request(
        self,
        method: str,
        url: str,
        *,
        json_payload: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> FetchResult:
        self._check_domain(url)
        self._throttle()
        response = self.session.request(
            method.upper(),
            url,
            json=json_payload,
            headers=headers,
            timeout=self.config.request_timeout_seconds,
        )
        response.raise_for_status()
        return FetchResult(
            url=response.url,
            status_code=response.status_code,
            content=response.content,
            content_type=response.headers.get("Content-Type"),
        )


class PlaywrightFetcher(BaseFetcher):
    def fetch(self, url: str) -> FetchResult:
        return self.request("GET", url)

    def request(
        self,
        method: str,
        url: str,
        *,
        json_payload: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> FetchResult:
        if method.upper() != "GET":
            raise NotImplementedError("PlaywrightFetcher only supports GET requests")
        self._check_domain(url)
        self._throttle()
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as exc:
            raise RuntimeError("playwright is not installed") from exc

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            page = browser.new_page(user_agent=self.config.user_agent)
            page.goto(url, wait_until="networkidle", timeout=int(self.config.request_timeout_seconds * 1000))
            content = page.content().encode("utf-8")
            browser.close()
        return FetchResult(url=url, status_code=200, content=content, content_type="text/html")


def guess_fetcher(config: AppConfig, *, force_playwright: bool = False):
    if force_playwright or Path(".use_playwright").exists():
        return PlaywrightFetcher(config)
    return RequestsFetcher(config)
