from __future__ import annotations

import json
from typing import Any
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
BGU_SEARCH_URL = "https://www.bgu.ac.il/umbraco/api/staffMembersLobbyApi/searchStaffMembers"
BGU_PAGE_NODE_ID = "107837"
BGU_CULTURE_CODE = "he-IL"


class BguAdapter(UniversityAdapter):
    """Ben-Gurion University staff directory adapter.

    BGU's people page is a Vue.js SPA backed by public Umbraco JSON endpoints.
    The normal crawl path uses the public listing API. Rendered HTML cards are
    still parsed as a fallback for fixtures and manual testing.
    """

    name = "bgu"
    display_name = "Ben-Gurion University"
    default_start_url = "https://www.bgu.ac.il/people/"
    default_allowed_domains = ["bgu.ac.il", "www.bgu.ac.il", "in.bgu.ac.il", "apps4cloud.bgu.ac.il"]

    def requires_playwright(self) -> bool:
        return False

    def parse_discovery_page(self, html: str, start_url: str) -> DiscoverySnapshot:
        page_node_id, culture_code = self._extract_people_app_state(html)
        available_filters: list[DiscoveryFilterGroup] = []
        connector_state: dict[str, Any] = {
            "page_node_id": page_node_id,
            "culture_code": culture_code,
        }
        page_data = self._load_page_data(page_node_id=page_node_id, culture_code=culture_code)
        if page_data:
            available_filters = self._build_available_filters(page_data)
            page_size = page_data.get("pageSize")
            if isinstance(page_size, int):
                connector_state["page_size"] = page_size
        return DiscoverySnapshot(
            connector_name=self.name,
            start_url=start_url,
            available_filters=available_filters,
            connector_state=connector_state,
        )

    def generate_result_links(
        self, snapshot: DiscoverySnapshot, selected_filters: dict[str, list[str]]
    ) -> list[DiscoveryLink]:
        """Generate the first API-backed results request for the BGU people directory."""
        payload = self._build_search_payload(snapshot, selected_filters, current_page=1)
        return [
            DiscoveryLink(
                url=BGU_SEARCH_URL,
                method="POST",
                json_payload=payload,
                headers={"Content-Type": "application/json"},
                artifact_kind="json",
                label="BGU Search Results Page 1",
            )
        ]

    def parse_results_artifact(
        self,
        content: bytes,
        content_type: str | None,
        result_link: DiscoveryLink,
    ) -> ResultPageData:
        normalized_content_type = (content_type or "").split(";", 1)[0].strip().lower()
        if normalized_content_type == "application/json" or result_link.artifact_kind == "json":
            try:
                payload = json.loads(content.decode("utf-8", errors="replace"))
            except ValueError:
                return ResultPageData()
            if isinstance(payload, dict):
                return self._parse_search_results_payload(payload, result_link)
            return ResultPageData()
        return self.parse_results_page(content.decode("utf-8", errors="replace"), result_link.url)

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

        if people:
            return ResultPageData(
                people=people,
                pagination_links=[DiscoveryLink(url=url) for url in sorted(set(pagination_urls))],
            )
        if self._is_people_spa_shell(soup):
            return ResultPageData()
        return ResultPageData()

    def _parse_search_results_payload(
        self,
        payload: dict[str, Any],
        result_link: DiscoveryLink,
    ) -> ResultPageData:
        people: list[PersonRecord] = []
        for item in payload.get("staffMembers", []):
            if not isinstance(item, dict):
                continue
            record = self._parse_search_result_item(item, result_link.url)
            if record is not None:
                people.append(record)

        request_payload = result_link.json_payload or {}
        try:
            total_pages = int(payload.get("totalPages") or 1)
        except (TypeError, ValueError):
            total_pages = 1
        try:
            current_page = int(request_payload.get("currentPage") or 1)
        except (TypeError, ValueError):
            current_page = 1
        pagination_links: list[DiscoveryLink] = []
        for page_num in range(current_page + 1, total_pages + 1):
            next_payload = dict(request_payload)
            next_payload["currentPage"] = page_num
            pagination_links.append(
                DiscoveryLink(
                    url=result_link.url,
                    method="POST",
                    json_payload=next_payload,
                    headers=dict(result_link.headers),
                    artifact_kind="json",
                    label=f"BGU Search Results Page {page_num}",
                )
            )
        return ResultPageData(people=people, pagination_links=pagination_links)

    def _parse_search_result_item(self, item: dict[str, Any], page_url: str) -> PersonRecord | None:
        full_name = normalize_space(str(item.get("name", "")).strip())
        if not full_name:
            return None

        raw_email = normalize_space(str(item.get("email", "")).strip()) or None
        public_email = None if self._is_placeholder_email(raw_email) else raw_email
        person_id = PersonRecord.create_id(full_name, raw_email)

        contacts: list[ContactPoint] = []
        if public_email:
            contacts.append(ContactPoint(kind="email", value=public_email))
        phone = normalize_space(str(item.get("phone", "")).strip())
        if phone:
            contacts.append(ContactPoint(kind="phone", value=phone))

        affiliations: list[OrgAffiliation] = []
        rank: str | None = None
        for department in item.get("departments", []):
            if not isinstance(department, dict):
                continue
            department_name = normalize_space(str(department.get("name", "")).strip()) or None
            staff_type = normalize_space(str(department.get("memberPosition", "")).strip()) or None
            if rank is None:
                rank = (
                    normalize_space(str(department.get("stepInGradeDescription", "")).strip())
                    or normalize_space(str(department.get("gradeDescription", "")).strip())
                    or None
                )
            affiliations.append(
                OrgAffiliation(
                    organization="Ben-Gurion University of the Negev",
                    department=department_name,
                    faculty_or_unit=department_name,
                    staff_type=staff_type,
                )
            )
        if not affiliations:
            affiliations.append(OrgAffiliation(organization="Ben-Gurion University of the Negev"))

        links: list[LinkRecord] = []
        page_href = normalize_space(str(item.get("pageUrl", "")).strip())
        if page_href:
            profile_url = urljoin(page_url, page_href)
            links.append(LinkRecord(kind="personal_page", url=profile_url, label=full_name))
        orcid_url = normalize_space(str(item.get("orcLink", "")).strip())
        if orcid_url:
            links.append(LinkRecord(kind="orcid", url=orcid_url, label="ORCID"))

        role = normalize_space(str(item.get("responsibleFor", "")).strip()) or None
        photo_url = self._normalize_photo_url(item.get("image"), page_url)
        evidence_excerpt = json.dumps(
            {
                "name": item.get("name"),
                "email": public_email,
                "phone": phone or None,
                "departments": item.get("departments", []),
                "pageUrl": item.get("pageUrl"),
                "orcLink": item.get("orcLink"),
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )
        return PersonRecord(
            person_id=person_id,
            full_name=full_name,
            contacts=contacts,
            org_affiliations=affiliations,
            current_role=role,
            current_rank=rank or _extract_rank(full_name),
            photo_url=photo_url,
            links=_dedupe_links(links),
            source_evidence=[
                SourceEvidence(
                    field_name="directory_record",
                    source_url=page_url,
                    excerpt=evidence_excerpt[:500],
                    confidence=0.97,
                )
            ],
        )

    def _extract_people_app_state(self, html: str) -> tuple[str, str]:
        soup = BeautifulSoup(html, "html.parser")
        app_root = soup.select_one(
            "#staffMembersModernLobbyApp, #staffMembersLobbyApp, #staffMembersMainLobbyApp"
        )
        if app_root is None:
            return BGU_PAGE_NODE_ID, BGU_CULTURE_CODE
        page_node_id = normalize_space(app_root.get("page-node-id", "")).strip() or BGU_PAGE_NODE_ID
        culture_code = normalize_space(app_root.get("culture-code", "")).strip() or BGU_CULTURE_CODE
        return page_node_id, culture_code

    def _is_people_spa_shell(self, soup: BeautifulSoup) -> bool:
        return soup.select_one(
            "#staffMembersModernLobbyApp, #staffMembersLobbyApp, #staffMembersMainLobbyApp"
        ) is not None

    def _build_search_payload(
        self,
        snapshot: DiscoverySnapshot,
        selected_filters: dict[str, list[str]],
        *,
        current_page: int,
    ) -> dict[str, Any]:
        connector_state = snapshot.connector_state or {}
        return {
            "pageNodeId": str(connector_state.get("page_node_id") or BGU_PAGE_NODE_ID),
            "cultureCode": str(connector_state.get("culture_code") or BGU_CULTURE_CODE),
            "currentPage": current_page,
            "pageSize": int(connector_state.get("page_size") or 30),
            "term": "",
            "units": self._int_filter_values(selected_filters.get("unit", [])),
            "selectedTypes": self._int_filter_values(selected_filters.get("staff_type", [])),
            "selectedCampuses": self._int_filter_values(selected_filters.get("campus", [])),
            "currentStaff": False,
            "lookingForStudents": False,
        }

    def _int_filter_values(self, values: list[str]) -> list[int]:
        result: list[int] = []
        for value in values:
            try:
                result.append(int(value))
            except (TypeError, ValueError):
                continue
        return result

    def _is_placeholder_email(self, email: str | None) -> bool:
        lowered = (email or "").lower()
        return lowered.endswith("@bgu.no.email")

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
        lowered = photo_url.lower()
        if "no-profile.png" in lowered or "img_vector" in lowered:
            return None
        return photo_url

    def _load_page_data(
        self,
        page_node_id: str | None = None,
        culture_code: str | None = None,
    ) -> dict | None:
        try:
            response = requests.post(
                BGU_PAGE_DATA_URL,
                json={
                    "pageNodeId": page_node_id or BGU_PAGE_NODE_ID,
                    "cultureCode": culture_code or BGU_CULTURE_CODE,
                },
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
