import pytest

from ou_harvest.config import AppConfig
from ou_harvest.http import PlaywrightFetcher, RequestsFetcher


def test_playwright_fetcher_rejects_disallowed_domain():
    config = AppConfig(allowed_domains=["openu.ac.il"])
    fetcher = PlaywrightFetcher(config)

    with pytest.raises(ValueError, match="Domain not allowed"):
        fetcher._check_domain("https://evil.com/foo")


def test_requests_fetcher_rejects_disallowed_domain():
    config = AppConfig(allowed_domains=["openu.ac.il"])
    fetcher = RequestsFetcher(config)

    with pytest.raises(ValueError, match="Domain not allowed"):
        fetcher._check_domain("https://evil.com/foo")


def test_fetcher_allows_subdomain():
    config = AppConfig(allowed_domains=["openu.ac.il"])
    fetcher = PlaywrightFetcher(config)

    # Should not raise
    fetcher._check_domain("https://academic.openu.ac.il/page")
