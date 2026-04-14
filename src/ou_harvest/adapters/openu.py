from __future__ import annotations

from urllib.parse import urljoin

from bs4 import BeautifulSoup

from ..models import DiscoveryLink, DiscoverySnapshot, PersonalPageData, ResultPageData
from ..parsers import (
    classify_link as _classify_link,
    parse_discovery_page as _parse_discovery,
    parse_personal_page as _parse_personal,
    parse_results_page as _parse_results,
)
from .base import UniversityAdapter


class OpenUniversityAdapter(UniversityAdapter):
    name = "openu"
    display_name = "Open University of Israel"
    default_start_url = "https://www.openu.ac.il/staff/pages/default.aspx"
    default_allowed_domains = ["openu.ac.il", "academic.openu.ac.il"]

    def parse_discovery_page(self, html: str, start_url: str) -> DiscoverySnapshot:
        return _parse_discovery(html, start_url)

    def parse_results_page(self, html: str, page_url: str) -> ResultPageData:
        return _parse_results(html, page_url)

    def parse_personal_page(self, html: str, page_url: str) -> PersonalPageData:
        return _parse_personal(html, page_url)

    def generate_result_links(
        self, snapshot: DiscoverySnapshot, selected_filters: dict[str, list[str]]
    ) -> list[DiscoveryLink]:
        if not snapshot.available_units or not snapshot.available_staff_types:
            return []

        units = snapshot.available_units
        selected_units = selected_filters.get("unit", [])
        if selected_units:
            selected = set(selected_units)
            units = [u for u in units if u.code in selected]

        staff_types = snapshot.available_staff_types
        selected_staff_types = selected_filters.get("staff_type", [])
        if selected_staff_types:
            selected = set(selected_staff_types)
            staff_types = [s for s in staff_types if s.code in selected]

        base = self.default_start_url.rsplit("/", 1)[0] + "/results.aspx"
        links: list[DiscoveryLink] = []
        for unit in units:
            for staff in staff_types:
                url = f"{base}?unit={unit.code}&staff={staff.code}"
                links.append(DiscoveryLink(
                    url=url,
                    unit=unit.code,
                    staff=staff.code,
                    label=f"{unit.label} / {staff.label}",
                ))
        return links

    def classify_link(self, url: str, label: str) -> str:
        return _classify_link(url, label)

    def extract_photo_url(self, html: str, page_url: str) -> str | None:
        from ..parsers import _extract_figure_photo
        soup = BeautifulSoup(html, "html.parser")
        return _extract_figure_photo(soup, page_url)
