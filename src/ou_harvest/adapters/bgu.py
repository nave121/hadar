from __future__ import annotations

import json
import requests
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup, Tag

from ..models import (
    ContactPoint,
    DiscoveryFilterGroup,
    DiscoveryLink,
    DiscoveryOption,
    DiscoverySnapshot,
    LinkRecord,
    OrgAffiliation,
    PersonalPageData,
    PersonRecord,
    ResultPageData,
    SourceEvidence,
)
from ..parsers import PHONE_RE, normalize_space, _extract_rank, _dedupe_contacts, _dedupe_links
from .base import UniversityAdapter

BGU_PAGE_DATA_URL = "https://www.bgu.ac.il/umbraco/api/staffMembersLobbyApi/GetPageData"
BGU_PAGE_NODE_ID = "107837"
BGU_CULTURE_CODE = "he-IL"


class BguAdapter(UniversityAdapter):
    """Ben-Gurion University staff directory adapter.

    BGU's people page (bgu.ac.il/people/) is a Vue.js SPA. Staff are listed
    as .staff-member-item cards with name, staff type, department, and email.
    Pagination goes up to ~221 pages with 30 people each.
    Requires Playwright to render.
    """

    name = "bgu"
    display_name = "Ben-Gurion University"
    default_start_url = "https://www.bgu.ac.il/people/"
    default_allowed_domains = ["bgu.ac.il", "www.bgu.ac.il", "in.bgu.ac.il", "apps4cloud.bgu.ac.il"]

    def requires_playwright(self) -> bool:
        return True

    def parse_discovery_page(self, html: str, start_url: str) -> DiscoverySnapshot:
        available_filters: list[DiscoveryFilterGroup] = []
        page_data = self._load_page_data()
        if page_data:
            available_filters = self._build_available_filters(page_data)
        return DiscoverySnapshot(
            connector_name=self.name,
            start_url=start_url,
            available_filters=available_filters,
        )

    def generate_result_links(
        self, snapshot: DiscoverySnapshot, selected_filters: dict[str, list[str]]
    ) -> list[DiscoveryLink]:
        """Generate paginated listing URLs for the BGU people directory."""
        base = self.default_start_url.rstrip("/")
        links: list[DiscoveryLink] = []
        selected_units = selected_filters.get("unit", [])
        selected_staff_types = selected_filters.get("staff_type", [])
        selected_campuses = selected_filters.get("campus", [])

        filtered_params: list[tuple[str, str]] = []
        if selected_units:
            filtered_params.append(("unit", ",".join(selected_units)))
        if selected_staff_types:
            filtered_params.append(("types", ",".join(selected_staff_types)))
        if selected_campuses:
            filtered_params.append(("campuses", ",".join(selected_campuses)))

        def build_page_url(page_num: int) -> str:
            params = list(filtered_params)
            if page_num > 1:
                params.append(("page", str(page_num)))
            if not params:
                return base + "/"
            query = "&".join(f"{key}={value}" for key, value in params)
            return f"{base}/?{query}"

        # Page 1 is the base URL (no param), then ?page=2 through ?page=221
        # Start with a conservative estimate; the crawl will follow pagination for more
        max_pages = 230
        links.append(DiscoveryLink(url=build_page_url(1), label="BGU People Page 1"))
        for page_num in range(2, max_pages + 1):
            links.append(DiscoveryLink(
                url=build_page_url(page_num),
                label=f"BGU People Page {page_num}",
            ))
        return links

    def parse_results_page(self, html: str, page_url: str) -> ResultPageData:
        """Parse BGU staff listing page with .staff-member-item cards."""
        soup = BeautifulSoup(html, "html.parser")
        people: list[PersonRecord] = []

        for card in soup.find_all("a", class_="staff-member-item"):
            record = self._parse_staff_card(card, page_url)
            if record is not None:
                people.append(record)

        # Extract pagination URLs
        pagination_urls: list[str] = []
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            if "/people/?page=" in href:
                pagination_urls.append(urljoin(page_url, href))

        return ResultPageData(people=people, pagination_urls=sorted(set(pagination_urls)))

    def parse_personal_page(self, html: str, page_url: str) -> PersonalPageData:
        """Parse a BGU individual profile page."""
        soup = BeautifulSoup(html, "html.parser")
        if self._is_cris_profile(page_url):
            return self._parse_cris_profile_page(soup, page_url)

        profile_root = self._select_profile_root(soup)
        profile_text = normalize_space(profile_root.get_text("\n", strip=True))
        name = self._extract_profile_name(profile_root, soup)
        photo_url = self.extract_photo_url(html, page_url)

        contacts = self._extract_profile_contacts(profile_root)
        links = self._extract_profile_links(profile_root, page_url)
        rank = self._extract_bgu_profile_rank(profile_root) or _extract_rank(name or "") or _extract_rank(profile_text)
        source_evidence = self._extract_profile_source_evidence(profile_root, page_url)
        return PersonalPageData(
            name=name,
            rank=rank,
            photo_url=photo_url,
            contacts=_dedupe_contacts(contacts),
            links=_dedupe_links(links),
            research_interests=[],
            source_evidence=source_evidence,
        )

    def extract_photo_url(self, html: str, page_url: str) -> str | None:
        soup = BeautifulSoup(html, "html.parser")
        image_tag = soup.select_one("figure.profile-image img")
        return self._normalize_photo_url(image_tag.get("src"), page_url) if image_tag is not None else None

    def classify_link(self, url: str, label: str) -> str:
        lowered_url = url.lower()
        lowered_label = label.lower()
        if "cv" in lowered_label or "cv" in lowered_url.rsplit("/", 1)[-1] or "קורות חיים" in label:
            return "cv"
        if lowered_url.endswith(".pdf"):
            return "publication"
        if "scholar.google" in lowered_url:
            return "scholar"
        if "scopus.com/" in lowered_url:
            return "scopus"
        if "orcid.org" in lowered_url:
            return "orcid"
        if "cris" in lowered_url or "pure" in lowered_url:
            return "cris"
        return "external"

    def _parse_staff_card(self, card, page_url: str) -> PersonRecord | None:
        """Parse a single .staff-member-item card."""
        href = card.get("href", "")
        if not href or href in ("/people/", "/people/en/", "/people/ar/"):
            return None

        name_tag = card.find(class_="member-name")
        if not name_tag:
            return None
        full_name = normalize_space(name_tag.get_text(" ", strip=True))
        if not full_name:
            return None

        # Extract staff type and department from .department div
        current_rank = None
        staff_type = None
        department = None
        dept_div = card.find(class_="department")
        if dept_div:
            labels = [
                normalize_space(span.get_text(" ", strip=True))
                for span in dept_div.find_all("span", recursive=False)
                if normalize_space(span.get_text(" ", strip=True))
            ]
            if len(labels) >= 3:
                current_rank, staff_type, department = labels[:3]
            elif len(labels) == 2:
                staff_type, department = labels
            elif len(labels) == 1:
                staff_type = labels[0]

        # Extract email from .bottom-section mailto link
        email = None
        email_link = card.find("a", href=lambda h: h and "mailto:" in h)
        if email_link:
            email = email_link.get("href", "").replace("mailto:", "").strip()

        contacts: list[ContactPoint] = []
        if email:
            contacts.append(ContactPoint(kind="email", value=email))

        profile_url = urljoin(page_url, href)
        photo_url = None
        image_tag = card.select_one("div.member-image > img")
        if image_tag is not None:
            photo_url = self._normalize_photo_url(image_tag.get("src"), page_url)
        person_id = PersonRecord.create_id(full_name, email)
        rank = current_rank or _extract_rank(full_name)

        affiliation = OrgAffiliation(
            organization="Ben-Gurion University of the Negev",
            department=department,
            faculty_or_unit=department,
            staff_type=staff_type,
        )

        links = [LinkRecord(kind="personal_page", url=profile_url, label=full_name)]
        links.extend(self._extract_staff_card_links(card, page_url))

        evidence = [
            SourceEvidence(
                field_name="directory_record",
                source_url=page_url,
                excerpt=normalize_space(card.get_text(" ", strip=True))[:250],
                confidence=0.95,
            )
        ]

        return PersonRecord(
            person_id=person_id,
            full_name=full_name,
            contacts=contacts,
            org_affiliations=[affiliation],
            current_rank=rank,
            photo_url=photo_url,
            links=_dedupe_links(links),
            source_evidence=evidence,
        )

    def _extract_staff_card_links(self, card: Tag, page_url: str) -> list[LinkRecord]:
        links: list[LinkRecord] = []
        for anchor in card.select("a[href]"):
            href = anchor.get("href", "")
            if href.startswith(("javascript:", "#", "mailto:", "tel:")):
                continue
            url = urljoin(page_url, href)
            label = normalize_space(anchor.get_text(" ", strip=True)) or None
            kind = self.classify_link(url, label or "")
            if kind not in {"orcid", "scopus", "scholar", "cris"}:
                continue
            links.append(LinkRecord(kind=kind, url=url, label=label))
        return links

    def _select_profile_root(self, soup: BeautifulSoup) -> Tag:
        return (
            soup.select_one("section.profile-data-container")
            or soup.select_one("section.staff-member-page.container")
            or soup.select_one("main.staffMember")
            or soup
        )

    def _extract_profile_name(self, profile_root: Tag, soup: BeautifulSoup) -> str | None:
        for scope in (profile_root, soup):
            for selector in ("h1", "header.top-section h1", "title", "h2"):
                tag = scope.select_one(selector) if hasattr(scope, "select_one") else None
                if tag is None:
                    continue
                text = normalize_space(tag.get_text(" ", strip=True))
                if text:
                    return text.replace(" - Ben-Gurion University", "").replace(" - BGU", "").strip()
        return None

    def _extract_profile_contacts(self, profile_root: Tag) -> list[ContactPoint]:
        contacts: list[ContactPoint] = []
        contact_scope = profile_root.select_one("section.member-contacts") or profile_root

        for anchor in contact_scope.select("a[href^='mailto:']"):
            email = normalize_space(anchor.get("href", "").removeprefix("mailto:"))
            if email:
                contacts.append(ContactPoint(kind="email", value=email))

        for anchor in contact_scope.select("a[href^='tel:']"):
            phone = normalize_space(anchor.get("href", "").removeprefix("tel:"))
            if phone:
                contacts.append(ContactPoint(kind="phone", value=phone))

        for line in self._text_lines(contact_scope):
            if not any(marker in line.lower() for marker in ("טלפון", "phone", "פקס", "fax")):
                continue
            match = PHONE_RE.search(line)
            if not match:
                continue
            kind = "fax" if any(marker in line.lower() for marker in ("פקס", "fax")) else "phone"
            contacts.append(ContactPoint(kind=kind, value=normalize_space(match.group(0))))

        return contacts

    def _extract_profile_links(self, profile_root: Tag, page_url: str) -> list[LinkRecord]:
        links: list[LinkRecord] = []
        for anchor in profile_root.select("a[href]"):
            href = anchor.get("href", "")
            if href.startswith(("javascript:", "#", "mailto:", "tel:")):
                continue
            url = urljoin(page_url, href)
            label = normalize_space(anchor.get_text(" ", strip=True)) or None
            if not self._should_keep_profile_link(anchor, url, label):
                continue
            kind = self.classify_link(url, label or "")
            links.append(LinkRecord(kind=kind, url=url, label=label))
        return links

    def _should_keep_profile_link(self, anchor: Tag, url: str, label: str | None) -> bool:
        lowered_url = url.lower()
        lowered_label = (label or "").lower()
        classes = {name.lower() for name in anchor.get("class", [])}
        kind = self.classify_link(url, label or "")

        if "bottom-link" in classes:
            return False
        if "apps4cloud.bgu.ac.il" in lowered_url or "staffupdatedtls" in lowered_url:
            return False
        if "כניסת סגל" in lowered_label or "עדכון פרטים" in lowered_label:
            return False

        return kind in {"personal_page", "cv", "publication", "scholar", "scopus", "orcid", "cris"}

    def _is_cris_profile(self, page_url: str) -> bool:
        host = (urlparse(page_url).hostname or "").lower()
        return host == "cris.bgu.ac.il"

    def _parse_cris_profile_page(self, soup: BeautifulSoup, page_url: str) -> PersonalPageData:
        profile_root = soup.select_one("#main-content") or soup.select_one("main") or soup
        text = normalize_space(profile_root.get_text("\n", strip=True))
        name = self._extract_cris_name(soup, profile_root)
        rank = self._extract_cris_rank(soup)
        research_interests = self._extract_cris_research_interests(profile_root)
        links = self._extract_cris_links(profile_root, page_url)
        source_evidence = self._extract_cris_source_evidence(text, page_url, name=name)
        return PersonalPageData(
            name=name,
            rank=rank,
            contacts=[],
            links=_dedupe_links(links),
            research_interests=research_interests,
            source_evidence=source_evidence,
        )

    def _extract_cris_name(self, soup: BeautifulSoup, profile_root: Tag) -> str | None:
        for scope in (profile_root, soup):
            for selector in ("h1", "title"):
                tag = scope.select_one(selector) if hasattr(scope, "select_one") else None
                if tag is None:
                    continue
                text = normalize_space(tag.get_text(" ", strip=True))
                if not text:
                    continue
                return text.replace("- Ben-Gurion University Research Portal", "").strip(" -")
        return None

    def _extract_cris_rank(self, soup: BeautifulSoup) -> str | None:
        for script_tag in soup.select("script[type='application/ld+json']"):
            raw = script_tag.string or script_tag.get_text()
            if not raw:
                continue
            try:
                payload = json.loads(raw)
            except ValueError:
                continue
            candidates = payload if isinstance(payload, list) else [payload]
            for item in candidates:
                if not isinstance(item, dict):
                    continue
                job_title = normalize_space(str(item.get("jobTitle", "")).strip())
                if job_title:
                    return job_title
        return None

    def _extract_cris_research_interests(self, profile_root: Tag) -> list[str]:
        topics: list[str] = []
        fingerprint_title = profile_root.find(string=lambda value: value and "Fingerprint" in value)
        if fingerprint_title is not None:
            container = fingerprint_title.parent
            for tag in container.find_all_next(["span", "div"], limit=40):
                text = normalize_space(tag.get_text(" ", strip=True))
                if not text or text in {"Fingerprint", "View full fingerprint"}:
                    continue
                if text == "Research output":
                    break
                if text.endswith("%") or text.isdigit():
                    continue
                if any(marker in text for marker in ("Research output", "Similar Profiles", "Scopus citations")):
                    continue
                if text in topics:
                    continue
                topics.append(text)
                if len(topics) >= 8:
                    break
        return topics

    def _extract_cris_links(self, profile_root: Tag, page_url: str) -> list[LinkRecord]:
        links: list[LinkRecord] = []
        for anchor in profile_root.select("a[href]"):
            href = anchor.get("href", "")
            if href.startswith(("javascript:", "#", "mailto:", "tel:")):
                continue
            url = urljoin(page_url, href)
            label = normalize_space(anchor.get_text(" ", strip=True)) or None
            kind = self.classify_link(url, label or "")
            if kind not in {"orcid", "scopus", "cris", "publication", "scholar"}:
                continue
            links.append(LinkRecord(kind=kind, url=url, label=label))
        return links

    def _extract_cris_source_evidence(
        self, text: str, page_url: str, *, name: str | None = None
    ) -> list[SourceEvidence]:
        if not text:
            return []
        start_index = text.find(name) if name else -1
        snippet = text[start_index:] if start_index > 0 else text
        return [
            SourceEvidence(
                field_name="cris_text",
                source_url=page_url,
                excerpt=snippet[:3500],
                confidence=0.9,
            )
        ]

    def _extract_bgu_profile_rank(self, profile_root: Tag) -> str | None:
        department_article = profile_root.select_one("section.departments-section article.member-department")
        if department_article is None:
            return None
        headings = [
            normalize_space(tag.get_text(" ", strip=True))
            for tag in department_article.select(".member-department__main-info h3")
            if normalize_space(tag.get_text(" ", strip=True))
        ]
        if not headings:
            return None
        return headings[0]

    def _extract_profile_source_evidence(self, profile_root: Tag, page_url: str) -> list[SourceEvidence]:
        lines = [
            line
            for line in self._text_lines(profile_root)
            if line and not self._is_profile_chrome_line(line)
        ]
        if not lines:
            return []
        profile_text = " ".join(lines)
        return [
            SourceEvidence(
                field_name="profile_text",
                source_url=page_url,
                excerpt=profile_text[:2000],
                confidence=0.85,
            )
        ]

    def _is_profile_chrome_line(self, line: str) -> bool:
        lowered = line.lower()
        if "כניסת סגל לעדכון פרטים בעמוד" in line:
            return True
        if "apps4cloud" in lowered or "staffupdatedtls" in lowered:
            return True
        return line in {"יחידות", "אזור צור קשר עם איש הסגל"}

    def _text_lines(self, tag: Tag) -> list[str]:
        return [
            normalize_space(line)
            for line in tag.get_text("\n", strip=True).splitlines()
            if normalize_space(line)
        ]

    def _normalize_photo_url(self, raw_url: str | None, page_url: str) -> str | None:
        if not raw_url:
            return None
        photo_url = urljoin(page_url, raw_url.strip())
        if "no-profile.png" in photo_url.lower():
            return None
        return photo_url

    def _load_page_data(self) -> dict | None:
        try:
            response = requests.post(
                BGU_PAGE_DATA_URL,
                json={"pageNodeId": BGU_PAGE_NODE_ID, "cultureCode": BGU_CULTURE_CODE},
                timeout=30,
            )
            response.raise_for_status()
        except requests.RequestException:
            return None

        try:
            return response.json()
        except ValueError:
            return None

    def _build_available_filters(self, page_data: dict) -> list[DiscoveryFilterGroup]:
        groups: list[DiscoveryFilterGroup] = []

        departments = self._build_options(page_data.get("departments", []))
        if departments:
            groups.append(
                DiscoveryFilterGroup(
                    key="unit",
                    label="Units / Research Institutes",
                    options=departments,
                )
            )

        staff_types = self._build_options(page_data.get("typesFiltersItems", []))
        if staff_types:
            groups.append(
                DiscoveryFilterGroup(
                    key="staff_type",
                    label="Staff Types",
                    options=staff_types,
                )
            )

        campuses = self._build_options(page_data.get("campuses", []))
        if campuses:
            groups.append(
                DiscoveryFilterGroup(
                    key="campus",
                    label="Campuses",
                    options=campuses,
                )
            )

        return groups

    def _build_options(self, items: list[dict]) -> list[DiscoveryOption]:
        options: list[DiscoveryOption] = []
        for item in items:
            code = str(item.get("key", "")).strip()
            label = normalize_space(str(item.get("value", "")).strip())
            if not code or not label:
                continue
            options.append(DiscoveryOption(code=code, label=label))
        return options
