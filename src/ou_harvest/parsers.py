from __future__ import annotations

from collections import defaultdict
import re
from urllib.parse import parse_qs, urldefrag, urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup, Tag

from .models import (
    ContactPoint,
    DiscoveryFilterGroup,
    DiscoveryLink,
    DiscoveryOption,
    DiscoverySnapshot,
    LinkRecord,
    OrgAffiliation,
    PersonRecord,
    PersonalPageData,
    ResultPageData,
    SourceEvidence,
)

EMAIL_RE = re.compile(r"[\w.+-]+@[\w.-]+\.\w+")
PHONE_RE = re.compile(r"0\d[\d -]{7,}")
RANK_PREFIXES = (
    "פרופ'",
    'פרופ׳',
    "Professor",
    "Prof.",
    "Prof",
    "ד\"ר",
    "ד׳ר",
    "Dr.",
    "Dr ",
    "Asst. Professor",
    "Associate Professor",
)

STAFF_TYPE_PATTERNS = (
    "סגל אקדמי בכיר",
    "סגל הוראה אקדמי",
    "סגל אקדמי",
    "סגל הוראה",
    "סגל מנהלי",
)


def parse_discovery_page(html: str, start_url: str) -> DiscoverySnapshot:
    soup = BeautifulSoup(html, "html.parser")
    result_links: dict[str, DiscoveryLink] = {}
    department_links: dict[str, LinkRecord] = {}

    for anchor in soup.find_all("a", href=True):
        href = urljoin(start_url, anchor["href"])
        text = normalize_space(anchor.get_text(" ", strip=True))
        parsed = urlparse(href)
        if parsed.path.endswith("/results.aspx"):
            query = parse_qs(parsed.query)
            result_links[href] = DiscoveryLink(
                url=href,
                unit=_first(query.get("unit")),
                staff=_first(query.get("staff")),
                label=text or None,
            )
            continue
        if "academic.openu.ac.il" in parsed.netloc and "סגל" in text:
            department_links[href] = LinkRecord(kind="department_staff", url=href, label=text)

    available_units = _extract_select_options(
        soup, lambda name: name and "OrganizationalUnits" in name
    )
    available_staff_types = _extract_select_options(
        soup, lambda name: name == "m_StaffType"
    )
    available_filters: list[DiscoveryFilterGroup] = []
    if available_units:
        available_filters.append(
            DiscoveryFilterGroup(key="unit", label="Departments", options=available_units)
        )
    if available_staff_types:
        available_filters.append(
            DiscoveryFilterGroup(key="staff_type", label="Staff Types", options=available_staff_types)
        )

    return DiscoverySnapshot(
        connector_name="openu",
        start_url=start_url,
        result_links=sorted(result_links.values(), key=lambda item: item.url),
        department_staff_links=sorted(department_links.values(), key=lambda item: item.url),
        available_filters=available_filters,
    )


def parse_results_page(html: str, page_url: str) -> ResultPageData:
    soup = BeautifulSoup(html, "html.parser")
    people: list[PersonRecord] = []

    rows = soup.select("tr.zebra-generic")
    if rows:
        for row in rows:
            record = _parse_result_table_row(row, page_url)
            if record is not None:
                people.append(record)
    else:
        containers = _locate_person_containers(soup)
        for container in containers:
            record = _parse_person_container(container, page_url)
            if record is not None:
                people.append(record)

    pagination_urls = sorted(
        {
            urljoin(page_url, anchor["href"])
            for anchor in soup.find_all("a", href=True)
            if _is_results_pagination_link(anchor, page_url)
        }
    )
    return ResultPageData(people=people, pagination_links=[DiscoveryLink(url=url) for url in pagination_urls])


def parse_personal_page(html: str, page_url: str) -> PersonalPageData:
    soup = BeautifulSoup(html, "html.parser")
    text = normalize_space(soup.get_text("\n", strip=True))
    name = None
    for selector in ("h1", "title", "h2"):
        tag = soup.find(selector)
        if tag and normalize_space(tag.get_text(" ", strip=True)):
            name = normalize_space(tag.get_text(" ", strip=True))
            break

    photo_url = _extract_figure_photo(soup, page_url)

    links: list[LinkRecord] = []
    research_interests: list[str] = []
    source_evidence: list[SourceEvidence] = []
    contacts = _extract_contacts_from_text(text)

    for anchor in soup.find_all("a", href=True):
        raw_href = anchor["href"]
        if raw_href.startswith(("javascript:", "#")):
            continue
        url = _normalize_openu_url(urljoin(page_url, raw_href))
        label = normalize_space(anchor.get_text(" ", strip=True)) or None
        kind = classify_link(url, label or "")
        canonical_url = urldefrag(url)[0]
        if not _should_keep_personal_link(canonical_url, page_url, kind):
            continue
        links.append(LinkRecord(kind=kind, url=canonical_url, label=label))

    for marker in ("research interests", "research", "תחום מחקר", "מחקר"):
        section = _extract_section_after_marker(text, marker)
        if section:
            research_interests.extend(_split_interests(section))
            source_evidence.append(
                SourceEvidence(field_name="research_interests", source_url=page_url, excerpt=section[:250], confidence=0.85)
            )
            break

    rank = _extract_rank(name or "") or _extract_rank(text)
    return PersonalPageData(
        name=name,
        rank=rank,
        contacts=contacts,
        links=_dedupe_links(links),
        research_interests=sorted(set(research_interests)),
        source_evidence=source_evidence,
        photo_url=photo_url,
    )


def normalize_space(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _normalize_openu_url(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    path = parsed.path or ""

    if host in {"www.dev.openu.ac.il", "dev.openu.ac.il"}:
        parsed = parsed._replace(scheme="https", netloc="www.openu.ac.il")
        host = "www.openu.ac.il"

    if host == "www.openu.ac.il" and path.startswith("/Personal_sites/"):
        parsed = parsed._replace(
            scheme="https",
            path="/personal_sites/" + path.removeprefix("/Personal_sites/"),
        )

    return urlunparse(parsed)


def _locate_person_containers(soup: BeautifulSoup) -> list[Tag]:
    best_by_email: dict[str, tuple[int, int, Tag]] = {}
    for tag in soup.find_all(["div", "li", "tr", "section", "article"]):
        text = normalize_space(tag.get_text("\n", strip=True))
        if "דואר אלקטרוני" not in text:
            continue
        if len(text) < 30 or len(text) > 4000:
            continue
        emails = sorted(set(EMAIL_RE.findall(text)))
        if len(emails) != 1:
            continue
        line_count = len([line for line in tag.get_text("\n", strip=True).splitlines() if normalize_space(line)])
        marker_count = sum(
            1 for marker in ("יחידת", "אתר אישי", "מחקר", "טלפון", "בניין", "חדר") if marker in text
        )
        if line_count < 5 or marker_count < 3:
            continue
        email = emails[0]
        score = (marker_count, line_count)
        current = best_by_email.get(email)
        if current is None or score > current[:2]:
            best_by_email[email] = (marker_count, line_count, tag)
    return [item[2] for item in best_by_email.values()]


def _parse_person_container(container: Tag, page_url: str) -> PersonRecord | None:
    raw_lines = [normalize_space(line) for line in container.get_text("\n", strip=True).splitlines()]
    lines = [line for line in raw_lines if line and line not in {"×", "סגור"}]
    if not lines:
        return None

    name, department_hint = _extract_header_from_lines(lines)
    if not name:
        return None

    contacts = _extract_contacts_from_lines(lines)
    primary_email = next((item.value for item in contacts if item.kind == "email"), None)
    person_id = PersonRecord.create_id(name, primary_email)

    sub_unit = _extract_prefixed_value(lines, ("יחידת מישנה", "יחידת משנה", "sub-unit"))
    building = _extract_prefixed_value(lines, ("בניין", "building"))
    room = _extract_prefixed_value(lines, ("חדר", "room"))
    research_main = _extract_prefixed_value(lines, ("תחום", "research area"))
    research_sub = _extract_prefixed_value(lines, ("תת תחום", "sub field"))
    summary_line = _find_summary_line(lines, name)
    staff_type, role = _extract_staff_type_and_role(summary_line, department_hint or sub_unit)
    rank = _extract_rank(name)

    links = []
    for anchor in container.find_all("a", href=True):
        url = _normalize_openu_url(urljoin(page_url, anchor["href"]))
        label = normalize_space(anchor.get_text(" ", strip=True)) or None
        links.append(LinkRecord(kind=classify_link(url, label or ""), url=url, label=label))

    office_parts = [part for part in (building, room) if part]
    if office_parts:
        contacts.append(ContactPoint(kind="office", value=", ".join(office_parts)))

    evidence = [
        SourceEvidence(
            field_name="directory_record",
            source_url=page_url,
            excerpt=normalize_space(" ".join(lines[:12]))[:250],
            confidence=0.95,
        )
    ]

    affiliation = OrgAffiliation(
        department=department_hint or sub_unit,
        faculty_or_unit=department_hint,
        sub_unit=sub_unit,
        staff_type=staff_type,
        role=role,
    )

    interests = [item for item in (research_main, research_sub) if item]
    return PersonRecord(
        person_id=person_id,
        full_name=name,
        contacts=_dedupe_contacts(contacts),
        org_affiliations=[affiliation],
        current_role=role,
        current_rank=rank,
        research_interests=interests,
        links=_dedupe_links(links),
        source_evidence=evidence,
        content_fingerprint=None,
    )


def _parse_result_table_row(row: Tag, page_url: str) -> PersonRecord | None:
    cells = row.find_all("td", recursive=False)
    if len(cells) < 8:
        return None

    modal = row.find("div", class_="staff-search-results-modal")
    detail_values = _extract_modal_detail_values(modal, page_url) if modal else {}
    header_tag = modal.find("th") if modal else None
    full_name, header_rank, header_department = _parse_modal_header(header_tag)
    if not full_name:
        first_cell_text = normalize_space(cells[0].get_text(" ", strip=True))
        full_name = _clean_name(first_cell_text)
    if not full_name:
        return None

    department_link = cells[2].find("a", href=True)
    department = normalize_space(cells[2].get_text(" ", strip=True)) or header_department
    sub_unit = normalize_space(cells[3].get_text(" ", strip=True)) or detail_values.get("יחידת מישנה") or detail_values.get("יחידת משנה")
    staff_type = empty_to_none(normalize_space(cells[4].get_text(" ", strip=True)))
    role = empty_to_none(normalize_space(cells[5].get_text(" ", strip=True)))
    row_phone = empty_to_none(normalize_space(cells[6].get_text(" ", strip=True)))

    contacts: list[ContactPoint] = []
    for label, kind in (
        ("דואר אלקטרוני", "email"),
        ("טלפון", "phone"),
        ("מס' פקס", "fax"),
        ("טלפון מחלקה", "phone"),
    ):
        value = detail_values.get(label)
        if value:
            contacts.append(ContactPoint(kind=kind, value=value, label=label))
    if row_phone and not any(contact.value == row_phone for contact in contacts):
        contacts.append(ContactPoint(kind="phone", value=row_phone, label="טלפון"))

    building = detail_values.get("בניין")
    room = detail_values.get("חדר")
    office_parts = [part for part in (building, room) if part]
    if office_parts:
        contacts.append(ContactPoint(kind="office", value=", ".join(office_parts)))

    current_rank = header_rank or _extract_rank(full_name)
    links: list[LinkRecord] = []
    if department_link:
        links.append(
            LinkRecord(
                kind="department_page",
                url=_normalize_openu_url(urljoin(page_url, department_link["href"])),
                label=normalize_space(department_link.get_text(" ", strip=True)) or None,
            )
        )

    for anchor in cells[7].find_all("a", href=True):
        url = _normalize_openu_url(urljoin(page_url, anchor["href"]))
        label = normalize_space(anchor.get_text(" ", strip=True)) or None
        kind = classify_link(url, label or "")
        if kind in {"personal_page", "cv", "cris"}:
            links.append(LinkRecord(kind=kind, url=url, label=label))

    for label in ("אתר אישי", "קורות חיים", "CV", "פורטל מחקר"):
        url = detail_values.get(f"__link__::{label}")
        if not url:
            continue
        kind = classify_link(url, label)
        links.append(LinkRecord(kind=kind, url=url, label=label))

    research_interests = [
        value
        for value in (detail_values.get("תחום"), detail_values.get("תת תחום"))
        if value
    ]

    evidence_excerpt = " ".join(
        [
            full_name,
            department or "",
            sub_unit or "",
            staff_type or "",
            role or "",
            *research_interests,
        ]
    ).strip()

    affiliation = OrgAffiliation(
        department=sub_unit or department,
        faculty_or_unit=department,
        sub_unit=sub_unit,
        staff_type=staff_type,
        role=role,
    )
    primary_email = next((item.value for item in contacts if item.kind == "email"), None)
    return PersonRecord(
        person_id=PersonRecord.create_id(full_name, primary_email),
        full_name=full_name,
        contacts=_dedupe_contacts(contacts),
        org_affiliations=[affiliation],
        current_role=role,
        current_rank=current_rank,
        research_interests=research_interests,
        links=_dedupe_links(links),
        source_evidence=[
            SourceEvidence(
                field_name="directory_record",
                source_url=page_url,
                excerpt=evidence_excerpt[:250],
                confidence=0.98,
            )
        ],
        content_fingerprint=None,
    )


def _parse_header(header: str) -> tuple[str | None, str | None]:
    if " - " in header:
        name, department = header.split(" - ", 1)
        return _clean_name(name), normalize_space(department)
    return _clean_name(header), None


def _extract_header_from_lines(lines: list[str]) -> tuple[str | None, str | None]:
    label_markers = ("יחידת", "בניין", "חדר", "טלפון", "מס' פקס", "דואר אלקטרוני", "מחקר", "תחום")
    cutoff = next((index for index, line in enumerate(lines) if line.startswith(label_markers)), len(lines))
    cluster = lines[:cutoff]
    if not cluster:
        return None, None

    for line in cluster:
        if " - " in line:
            return _parse_header(line)

    for index, line in enumerate(cluster):
        if line.endswith("-") and index + 1 < len(cluster):
            name = _extract_name_from_cluster(cluster[: index + 1])
            department = normalize_space(cluster[index + 1])
            return name, department

    return _parse_header(cluster[0])


def _extract_contacts_from_lines(lines: list[str]) -> list[ContactPoint]:
    contacts: list[ContactPoint] = []
    for line in lines:
        email_match = EMAIL_RE.search(line)
        if email_match:
            contacts.append(ContactPoint(kind="email", value=email_match.group(0)))
        phone_value = _extract_phone_from_line(line)
        if not phone_value:
            continue
        if line.startswith("מס' פקס"):
            contacts.append(ContactPoint(kind="fax", value=phone_value))
        else:
            contacts.append(ContactPoint(kind="phone", value=phone_value))
    return contacts


def _extract_contacts_from_text(text: str) -> list[ContactPoint]:
    contacts = [ContactPoint(kind="email", value=value) for value in sorted(set(EMAIL_RE.findall(text)))]
    for phone in sorted(set(PHONE_RE.findall(text))):
        contacts.append(ContactPoint(kind="phone", value=normalize_space(phone)))
    return contacts


def _extract_prefixed_value(lines: list[str], prefixes: tuple[str, ...]) -> str | None:
    for line in lines:
        for prefix in prefixes:
            if line.startswith(prefix):
                return normalize_space(line[len(prefix) :])
    return None


def _find_summary_line(lines: list[str], name: str) -> str:
    surname = name.split()[-1]
    for line in reversed(lines):
        if surname in line and "סגל" in line:
            return line
    return ""


def _extract_staff_type_and_role(summary_line: str, department_hint: str | None) -> tuple[str | None, str | None]:
    if not summary_line:
        return None, None
    staff_type = next((pattern for pattern in STAFF_TYPE_PATTERNS if pattern in summary_line), None)
    role = None
    if staff_type:
        after = summary_line.split(staff_type, 1)[1]
        after = re.sub(r"0\d[\d -]+$", "", after).strip(" ,")
        if department_hint:
            after = after.replace(department_hint, "").strip(" ,")
        role = normalize_space(after) or None
    return staff_type, role


def _extract_phone_from_line(line: str) -> str | None:
    match = PHONE_RE.search(line)
    if not match:
        return None
    return normalize_space(match.group(0))


def _extract_rank(text: str) -> str | None:
    for prefix in RANK_PREFIXES:
        if prefix in text:
            return prefix.strip()
    professor_match = re.search(r"\b(?:Professor|Associate Professor|Asst\. Professor)\b", text)
    if professor_match:
        return professor_match.group(0)
    return None


def _parse_modal_header(header_tag: Tag | None) -> tuple[str | None, str | None, str | None]:
    if header_tag is None:
        return None, None, None
    department_link = header_tag.find("a", href=True)
    department = (
        normalize_space(department_link.get_text(" ", strip=True))
        if department_link is not None
        else None
    )
    text = normalize_space(header_tag.get_text(" ", strip=True))
    if department:
        text = text.replace(department, "").strip(" -")
    text = normalize_space(text)

    left = re.split(r"\s*-\s*", text, maxsplit=1)[0]
    if "," in left:
        name_part, rank_part = left.split(",", 1)
        return normalize_space(name_part), empty_to_none(normalize_space(rank_part)), department
    return normalize_space(left), None, department


def _clean_name(value: str) -> str:
    name = normalize_space(value)
    name = re.split(r"\s*,\s*", name, maxsplit=1)[0]
    return normalize_space(name)


def _extract_name_from_cluster(cluster: list[str]) -> str | None:
    tokens: list[str] = []
    seen_tokens: set[str] = set()
    for line in cluster:
        if "," in line or line.endswith("-"):
            continue
        for token in normalize_space(line).split():
            if token in {"×", "סגור"}:
                continue
            if token in seen_tokens:
                continue
            tokens.append(token)
            seen_tokens.add(token)
    if not tokens:
        return None
    return normalize_space(" ".join(tokens))


def _extract_modal_detail_values(modal: Tag, page_url: str) -> dict[str, str]:
    values: dict[str, str] = {}
    if modal is None:
        return values
    for row in modal.find_all("tr"):
        cells = row.find_all("td", recursive=False)
        if len(cells) != 2:
            continue
        label_tag = cells[0].find("strong")
        if label_tag is None:
            continue
        label = normalize_space(label_tag.get_text(" ", strip=True))
        if not label:
            continue
        if cells[1].find("table"):
            continue
        link = cells[1].find("a", href=True)
        if link is not None:
            href = _normalize_openu_url(urljoin(page_url, link["href"]))
            if label == "דואר אלקטרוני":
                values[label] = normalize_space(link.get_text(" ", strip=True)) or href.removeprefix("mailto:")
            else:
                values[f"__link__::{label}"] = href
                values[label] = normalize_space(link.get_text(" ", strip=True)) or href
            continue
        value = empty_to_none(normalize_space(cells[1].get_text(" ", strip=True)))
        if value is not None:
            values[label] = value
    return values


def _is_results_pagination_link(anchor: Tag, page_url: str) -> bool:
    href = anchor.get("href")
    if not href:
        return False
    text = normalize_space(anchor.get_text(" ", strip=True))
    if text not in {"<<", ">>"} and not text.isdigit():
        return False
    parsed = urlparse(urljoin(page_url, href))
    if not parsed.path.endswith("/results.aspx"):
        return False
    query = parse_qs(parsed.query)
    return "page" in query


def empty_to_none(value: str) -> str | None:
    return value or None


def classify_link(url: str, label: str) -> str:
    lowered_url = url.lower()
    lowered_label = label.lower()
    is_cv = (
        "cv" in lowered_label
        or "קורות חיים" in label
        or "cv" in lowered_url.rsplit("/", 1)[-1]
    )
    if is_cv:
        return "cv"
    if lowered_url.endswith(".pdf"):
        return "publication"
    if "personalsites" in lowered_url or "personal_sites" in lowered_url or "/home/" in lowered_url:
        return "personal_page"
    if "scholar.google" in lowered_url:
        return "scholar"
    if "cris" in lowered_url:
        return "cris"
    if "orcid.org" in lowered_url:
        return "orcid"
    if "scopus.com" in lowered_url:
        return "scopus"
    if "pubmed" in lowered_url:
        return "pubmed"
    if "academic.openu.ac.il" in lowered_url:
        return "department_page"
    return "external"


def _should_keep_personal_link(url: str, page_url: str, kind: str) -> bool:
    lowered_url = url.lower()
    if lowered_url.startswith(("mailto:", "tel:")):
        return False
    if kind in {"cv", "scholar", "cris"}:
        return True
    if kind == "personal_page":
        return urldefrag(url)[0] != urldefrag(page_url)[0]
    parsed = urlparse(url)
    if not parsed.netloc:
        return False
    if parsed.path in {"", "/"}:
        return False
    if parsed.netloc.endswith("openu.ac.il"):
        return False
    return True


def _extract_section_after_marker(text: str, marker: str) -> str | None:
    lowered = text.lower()
    marker_index = lowered.find(marker.lower())
    if marker_index == -1:
        return None
    candidate = text[marker_index : marker_index + 400]
    parts = re.split(r"(?:About|Publications|Teaching|Contact|קשר|ייעוץ)", candidate, maxsplit=1)
    return normalize_space(parts[0])


def _split_interests(value: str) -> list[str]:
    cleaned = value.split(":", 1)[-1]
    cleaned = re.sub(
        r"(?i)\bresearch interests? are (?:in|include)?\s*",
        "",
        cleaned,
    ).strip()
    cleaned = re.sub(r"(?i)\band\s+", ", ", cleaned)
    items = re.split(r"[,;|/]", cleaned)
    return [normalize_space(item) for item in items if normalize_space(item)]


def _first(values: list[str] | None) -> str | None:
    if not values:
        return None
    return values[0]


def _dedupe_links(links: list[LinkRecord]) -> list[LinkRecord]:
    seen: set[tuple[str, str]] = set()
    result: list[LinkRecord] = []
    for link in links:
        key = (link.kind, link.url)
        if key in seen:
            continue
        seen.add(key)
        result.append(link)
    return result


def _dedupe_contacts(contacts: list[ContactPoint]) -> list[ContactPoint]:
    seen: set[tuple[str, str]] = set()
    result: list[ContactPoint] = []
    for contact in contacts:
        key = (contact.kind, contact.value)
        if key in seen:
            continue
        seen.add(key)
        result.append(contact)
    return result


_PHOTO_SKIP_PATTERNS = (
    "avatar-general",
    "logo",
    "back-en",
    "back-he",
    "/icon",
    "icon-o-",
    "icpdf",
    ".svg",
    "spacer",
    "empty.gif",
    "pixel",
    "1x1",
    "favicon",
    "gifs/logo",
)


def _extract_figure_photo(soup: BeautifulSoup, page_url: str) -> str | None:
    """Extract a profile photo from OpenU personal pages.

    OpenU personal pages have no standard template — staff can use the
    SharePoint aspx template (photos in MediaServer_Images/PersonalSites/)
    or fully custom HTML (photos as relative paths like ``ricagonen1.gif``).
    This function picks the first image that looks like a portrait by
    filtering out known site-chrome images (logos, icons, back buttons,
    PDF icons, avatar placeholders).
    """
    for img in soup.find_all("img"):
        src = img.get("src", "")
        if not src:
            continue
        lowered = src.lower()
        if any(skip in lowered for skip in _PHOTO_SKIP_PATTERNS):
            continue
        # Skip tiny tracking/decoration images by explicit dimensions
        w = img.get("width", "")
        h = img.get("height", "")
        if w and h:
            try:
                nw = int(str(w).replace("px", ""))
                nh = int(str(h).replace("px", ""))
                if nw < 30 or nh < 30:
                    continue
            except ValueError:
                pass
        return urljoin(page_url, src)
    return None


def _extract_select_options(soup: BeautifulSoup, name_matcher) -> list[DiscoveryOption]:
    select = soup.find("select", {"name": name_matcher})
    if select is None:
        return []
    options: list[DiscoveryOption] = []
    for opt in select.find_all("option"):
        code = (opt.get("value") or "").strip()
        if not code:
            continue
        label = normalize_space(opt.get_text(" ", strip=True))
        options.append(DiscoveryOption(code=code, label=label))
    return options
