from __future__ import annotations

from abc import ABC, abstractmethod

from ..models import (
    DiscoveryLink,
    DiscoverySnapshot,
    PersonalPageData,
    ResultPageData,
)


class UniversityAdapter(ABC):
    """Base class for university-specific scraping adapters."""

    name: str = ""
    display_name: str = ""
    default_start_url: str = ""
    default_allowed_domains: list[str] = []

    @abstractmethod
    def parse_discovery_page(self, html: str, start_url: str) -> DiscoverySnapshot:
        """Parse the landing/discovery page and extract navigation structure."""

    @abstractmethod
    def parse_results_page(self, html: str, page_url: str) -> ResultPageData:
        """Parse a staff listing page into person records."""

    @abstractmethod
    def parse_personal_page(self, html: str, page_url: str) -> PersonalPageData:
        """Parse an individual staff member's personal page."""

    @abstractmethod
    def generate_result_links(
        self, snapshot: DiscoverySnapshot, selected_filters: dict[str, list[str]]
    ) -> list[DiscoveryLink]:
        """Generate result page URLs from discovery data and connector-specific selections."""

    @abstractmethod
    def classify_link(self, url: str, label: str) -> str:
        """Classify a link found on a page (cv, personal_page, scholar, cris, publication, external)."""

    def extract_photo_url(self, html: str, page_url: str) -> str | None:
        return None

    def requires_playwright(self) -> bool:
        """Return True if this university's site requires a full browser to scrape."""
        return False
