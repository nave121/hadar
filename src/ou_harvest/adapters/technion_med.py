from __future__ import annotations

import re
from urllib.parse import urljoin
from xml.etree import ElementTree

from bs4 import BeautifulSoup

from ..models import (
    ContactPoint,
    DiscoveryLink,
    DiscoverySnapshot,
    LinkRecord,
    OrgAffiliation,
    PersonalPageData,
    PersonRecord,
    ResultPageData,
    SourceEvidence,
)
from ..parsers import EMAIL_RE, normalize_space, _extract_rank, _dedupe_contacts, _dedupe_links
from .base import UniversityAdapter

SITEMAP_NS = {"s": "http://www.sitemaps.org/schemas/sitemap/0.9"}

# URL slugs that are NOT staff profiles (structural pages)
NON_PROFILE_SLUGS = {
    "education", "research", "about", "staff-page", "test", "departments",
    "contact", "news", "events", "home", "en", "he",
}


class TechnionMedAdapter(UniversityAdapter):
    """Technion Faculty of Medicine staff directory adapter.

    The site uses WordPress with dynamic content loading. Individual staff
    profile pages exist at md.technion.ac.il/{name-slug}/ and are listed in
    the page-sitemap.xml. Profile content renders server-side + JS.
    Requires Playwright for full content.
    """

    name = "technion_med"
    display_name = "Technion - Faculty of Medicine"
    default_start_url = "https://md.technion.ac.il/page-sitemap.xml"
    default_allowed_domains = ["md.technion.ac.il", "technion.ac.il"]

    def requires_playwright(self) -> bool:
        return True

    def parse_discovery_page(self, html: str, start_url: str) -> DiscoverySnapshot:
        """Parse the page-sitemap.xml to extract profile URLs.

        Despite the parameter name being 'html', this receives XML content
        when the start_url points to a sitemap.
        """
        profile_urls = self._parse_sitemap_urls(html)
        result_links = [
            DiscoveryLink(url=url, label=self._slug_to_name(url))
            for url in profile_urls
        ]

        return DiscoverySnapshot(
            connector_name=self.name,
            start_url=start_url,
            result_links=sorted(result_links, key=lambda item: item.url),
        )

    def generate_result_links(
        self, snapshot: DiscoverySnapshot, selected_filters: dict[str, list[str]]
    ) -> list[DiscoveryLink]:
        # For Technion, result_links are already the profile URLs from the sitemap.
        # No additional generation needed — discovery already populated them.
        return []

    def parse_results_page(self, html: str, page_url: str) -> ResultPageData:
        """Parse a single Technion profile page as a 'results' page with one person.

        Technion doesn't have multi-person listing pages like OU. Each crawled
        URL is an individual profile. We parse it as a single-person ResultPageData.
        """
        person = self._parse_profile(html, page_url)
        if person is None:
            return ResultPageData()
        return ResultPageData(people=[person])

    def parse_personal_page(self, html: str, page_url: str) -> PersonalPageData:
        """Parse a Technion profile page for personal data."""
        soup = BeautifulSoup(html, "html.parser")
        name = self._extract_name(soup)
        contacts = self._extract_contacts(soup)
        links = self._extract_links(soup, page_url)
        rank = _extract_rank(name or "")

        # Extract bio text for research interests (LLM enrichment will handle the rest)
        bio_text = self._extract_bio_text(soup)
        source_evidence = []
        if bio_text:
            source_evidence.append(
                SourceEvidence(
                    field_name="profile_text",
                    source_url=page_url,
                    excerpt=bio_text[:250],
                    confidence=0.85,
                )
            )

        return PersonalPageData(
            name=name,
            rank=rank,
            contacts=_dedupe_contacts(contacts),
            links=_dedupe_links(links),
            source_evidence=source_evidence,
        )

    def classify_link(self, url: str, label: str) -> str:
        lowered_url = url.lower()
        lowered_label = label.lower()
        if "cv" in lowered_label or "cv" in lowered_url.rsplit("/", 1)[-1] or "קורות חיים" in label:
            return "cv"
        if lowered_url.endswith(".pdf"):
            return "publication"
        if "scholar.google" in lowered_url:
            return "scholar"
        if "orcid.org" in lowered_url:
            return "orcid"
        if "pubmed" in lowered_url:
            return "pubmed"
        return "external"

    # -- Internal helpers --

    def _parse_sitemap_urls(self, xml_text: str) -> list[str]:
        """Extract staff profile URLs from a WordPress page-sitemap.xml."""
        try:
            root = ElementTree.fromstring(xml_text)
        except ElementTree.ParseError:
            return []

        urls: list[str] = []
        for loc in root.findall(".//s:loc", SITEMAP_NS):
            url = (loc.text or "").strip()
            if not url:
                continue
            if self._is_likely_profile_url(url):
                urls.append(url)
        return urls

    def _is_likely_profile_url(self, url: str) -> bool:
        """Heuristic: profile URLs are /name-slug/ at root level with Latin chars."""
        base = "https://md.technion.ac.il/"
        if not url.startswith(base):
            return False
        slug = url[len(base):].strip("/")
        if "/" in slug:
            return False  # Has subdirectories — not a root-level profile
        if not slug:
            return False
        if slug in NON_PROFILE_SLUGS:
            return False
        if slug.startswith("%"):
            return False  # Hebrew-encoded URL — likely a structural page
        # Profile slugs are typically firstname-lastname
        if "-" not in slug:
            return False
        return True

    def _slug_to_name(self, url: str) -> str:
        """Convert a URL slug to a readable name guess."""
        base = "https://md.technion.ac.il/"
        slug = url[len(base):].strip("/")
        return slug.replace("-", " ").title()

    def _parse_profile(self, html: str, page_url: str) -> PersonRecord | None:
        """Parse a rendered Technion profile page into a PersonRecord."""
        soup = BeautifulSoup(html, "html.parser")
        name = self._extract_name(soup)
        if not name:
            return None

        contacts = self._extract_contacts(soup)
        email = next((c.value for c in contacts if c.kind == "email"), None)
        person_id = PersonRecord.create_id(name, email)
        rank = _extract_rank(name)

        links = self._extract_links(soup, page_url)
        bio_text = self._extract_bio_text(soup)

        evidence = []
        if bio_text:
            evidence.append(
                SourceEvidence(
                    field_name="profile_text",
                    source_url=page_url,
                    excerpt=bio_text[:250],
                    confidence=0.9,
                )
            )

        affiliation = OrgAffiliation(
            organization="Technion - Israel Institute of Technology",
            department="Faculty of Medicine",
        )

        return PersonRecord(
            person_id=person_id,
            full_name=name,
            contacts=_dedupe_contacts(contacts),
            org_affiliations=[affiliation],
            current_rank=rank,
            links=_dedupe_links(links),
            source_evidence=evidence,
        )

    def _extract_name(self, soup: BeautifulSoup) -> str | None:
        """Extract person name from page title or h1."""
        # Try h1 first
        h1 = soup.find("h1")
        if h1:
            text = normalize_space(h1.get_text(" ", strip=True))
            if text:
                return text

        # Fall back to title
        title = soup.find("title")
        if title:
            text = normalize_space(title.get_text(" ", strip=True))
            # Remove site suffix
            text = re.sub(r"\s*[-–]\s*Technion Medicine\s*$", "", text).strip()
            if text:
                return text
        return None

    def _extract_contacts(self, soup: BeautifulSoup) -> list[ContactPoint]:
        """Extract emails and phones from the page."""
        contacts: list[ContactPoint] = []

        # Emails from mailto: links
        for a in soup.find_all("a", href=lambda h: h and "mailto:" in h):
            email = a["href"].replace("mailto:", "").strip()
            if email:
                contacts.append(ContactPoint(kind="email", value=email))

        # Phones from text containing Tel: or phone patterns
        text = soup.get_text("\n", strip=True)
        for line in text.split("\n"):
            if "tel:" in line.lower() or "phone:" in line.lower() or "טלפון" in line:
                phones = re.findall(r"\+?\d[\d\s\-()]{7,}", line)
                for phone in phones:
                    contacts.append(ContactPoint(kind="phone", value=normalize_space(phone)))
            if "fax:" in line.lower() or "פקס" in line:
                phones = re.findall(r"\+?\d[\d\s\-()]{7,}", line)
                for phone in phones:
                    contacts.append(ContactPoint(kind="fax", value=normalize_space(phone)))

        return contacts

    def _extract_links(self, soup: BeautifulSoup, page_url: str) -> list[LinkRecord]:
        """Extract relevant links from the profile page."""
        links: list[LinkRecord] = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith(("javascript:", "#", "mailto:", "tel:")):
                continue
            url = urljoin(page_url, href)
            label = normalize_space(a.get_text(" ", strip=True)) or None
            kind = self.classify_link(url, label or "")
            if kind in ("cv", "scholar", "orcid", "pubmed", "publication"):
                links.append(LinkRecord(kind=kind, url=url, label=label))
        return links

    def _extract_bio_text(self, soup: BeautifulSoup) -> str:
        """Extract biographical/research text from the profile."""
        # Technion profiles have content in .departmets-box or .container .row
        box = soup.find("div", class_="departmets-box")
        if box:
            return normalize_space(box.get_text("\n", strip=True))[:4000]

        # Fallback: largest text block in the page
        main = soup.find("main") or soup.find("article")
        if main:
            return normalize_space(main.get_text("\n", strip=True))[:4000]

        return ""
