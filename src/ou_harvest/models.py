from __future__ import annotations

from typing import Any
from datetime import datetime, timezone
from hashlib import sha1
from typing import Literal

from pydantic import BaseModel, Field


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


class SourceEvidence(BaseModel):
    field_name: str
    source_url: str
    excerpt: str
    confidence: float = 1.0
    artifact_id: str | None = None


class ReviewFlag(BaseModel):
    field_name: str
    reason: str
    confidence: float
    source_url: str | None = None


class ContactPoint(BaseModel):
    kind: Literal["email", "phone", "fax", "office"]
    value: str
    label: str | None = None


class LinkRecord(BaseModel):
    kind: str
    url: str
    label: str | None = None


class Artifact(BaseModel):
    artifact_id: str
    kind: Literal["html", "pdf", "text", "json", "image"]
    source_url: str
    path: str
    checksum: str
    content_type: str | None = None
    fetched_at: str = Field(default_factory=utc_now)


class DemographicEstimate(BaseModel):
    dominant_gender: str | None = None
    gender_scores: dict[str, float] = Field(default_factory=dict)
    dominant_race: str | None = None
    race_scores: dict[str, float] = Field(default_factory=dict)
    estimated_age: int | None = None
    dominant_emotion: str | None = None
    emotion_scores: dict[str, float] = Field(default_factory=dict)
    face_confidence: float | None = None
    face_region: dict[str, int] = Field(default_factory=dict)
    detector_backend: str = "retinaface"
    analyzed_at: str = Field(default_factory=utc_now)
    source_artifact_id: str | None = None


class OrgAffiliation(BaseModel):
    organization: str | None = None
    department: str | None = None
    faculty_or_unit: str | None = None
    sub_unit: str | None = None
    staff_type: str | None = None
    role: str | None = None


class EducationEntry(BaseModel):
    degree_level: str | None = None
    field: str | None = None
    institution: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    year: int | None = None
    evidence: list[SourceEvidence] = Field(default_factory=list)


class AppointmentEntry(BaseModel):
    title: str | None = None
    institution: str | None = None
    department: str | None = None
    faculty: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    is_current: bool | None = None
    evidence: list[SourceEvidence] = Field(default_factory=list)


class PublicationEntry(BaseModel):
    title: str | None = None
    venue: str | None = None
    year: int | None = None
    authors_text: str | None = None
    publication_type: str | None = None
    doi_or_url: str | None = None
    evidence: list[SourceEvidence] = Field(default_factory=list)


class AwardEntry(BaseModel):
    title: str | None = None
    organization: str | None = None
    year: int | None = None
    evidence: list[SourceEvidence] = Field(default_factory=list)


class AcademicServiceEntry(BaseModel):
    role: str | None = None
    organization: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    evidence: list[SourceEvidence] = Field(default_factory=list)


class PersonRecord(BaseModel):
    person_id: str
    full_name: str
    name_variants: list[str] = Field(default_factory=list)
    contacts: list[ContactPoint] = Field(default_factory=list)
    org_affiliations: list[OrgAffiliation] = Field(default_factory=list)
    current_role: str | None = None
    current_rank: str | None = None
    research_interests: list[str] = Field(default_factory=list)
    education: list[EducationEntry] = Field(default_factory=list)
    appointments: list[AppointmentEntry] = Field(default_factory=list)
    publications: list[PublicationEntry] = Field(default_factory=list)
    awards: list[AwardEntry] = Field(default_factory=list)
    academic_service: list[AcademicServiceEntry] = Field(default_factory=list)
    links: list[LinkRecord] = Field(default_factory=list)
    notable_links: list[LinkRecord] = Field(default_factory=list)
    photo_url: str | None = None
    photo_artifact_id: str | None = None
    demographics: DemographicEstimate | None = None
    artifacts: list[Artifact] = Field(default_factory=list)
    source_evidence: list[SourceEvidence] = Field(default_factory=list)
    review_flags: list[ReviewFlag] = Field(default_factory=list)
    confidence: float = 1.0
    content_fingerprint: str | None = None
    last_seen_at: str = Field(default_factory=utc_now)
    enriched_at: str | None = None
    enriched_by: list[str] = Field(default_factory=list)
    source_connectors: list[str] = Field(default_factory=list)

    @property
    def primary_email(self) -> str | None:
        for contact in self.contacts:
            if contact.kind == "email":
                return contact.value
        return None

    def merge(self, other: "PersonRecord") -> "PersonRecord":
        payload = self.model_dump()
        payload["name_variants"] = sorted(
            {self.full_name, other.full_name, *self.name_variants, *other.name_variants}
        )
        payload["contacts"] = _dedupe_as_dicts(self.contacts + other.contacts, key_fn=lambda c: (c.kind, c.value))
        payload["org_affiliations"] = _dedupe_as_dicts(
            self.org_affiliations + other.org_affiliations,
            key_fn=lambda item: (
                item.organization,
                item.department,
                item.faculty_or_unit,
                item.sub_unit,
                item.staff_type,
                item.role,
            ),
        )
        payload["research_interests"] = sorted({*self.research_interests, *other.research_interests})
        payload["education"] = _dedupe_as_dicts(
            self.education + other.education,
            key_fn=lambda item: (
                item.degree_level,
                item.field,
                item.institution,
                item.start_date,
                item.end_date,
                item.year,
            ),
        )
        payload["appointments"] = _dedupe_as_dicts(
            self.appointments + other.appointments,
            key_fn=lambda item: (
                item.title,
                item.institution,
                item.department,
                item.faculty,
                item.start_date,
                item.end_date,
            ),
        )
        payload["publications"] = _dedupe_as_dicts(
            self.publications + other.publications,
            key_fn=lambda item: (
                item.title,
                item.venue,
                item.year,
                item.publication_type,
                item.doi_or_url,
            ),
        )
        payload["awards"] = _dedupe_as_dicts(
            self.awards + other.awards,
            key_fn=lambda item: (item.title, item.organization, item.year),
        )
        payload["academic_service"] = _dedupe_as_dicts(
            self.academic_service + other.academic_service,
            key_fn=lambda item: (
                item.role,
                item.organization,
                item.start_date,
                item.end_date,
            ),
        )
        payload["links"] = _dedupe_as_dicts(self.links + other.links, key_fn=lambda item: (item.kind, item.url))
        payload["notable_links"] = _dedupe_as_dicts(
            self.notable_links + other.notable_links,
            key_fn=lambda item: (item.kind, item.url),
        )
        payload["artifacts"] = _dedupe_as_dicts(
            self.artifacts + other.artifacts, key_fn=lambda item: item.artifact_id
        )
        payload["source_evidence"] = _dedupe_as_dicts(
            self.source_evidence + other.source_evidence,
            key_fn=lambda item: (item.field_name, item.source_url, item.excerpt),
        )
        payload["review_flags"] = _dedupe_as_dicts(
            self.review_flags + other.review_flags,
            key_fn=lambda item: (item.field_name, item.reason, item.source_url),
        )
        payload["current_role"] = other.current_role or self.current_role
        payload["current_rank"] = other.current_rank or self.current_rank
        payload["photo_url"] = other.photo_url or self.photo_url
        payload["photo_artifact_id"] = other.photo_artifact_id or self.photo_artifact_id
        payload["demographics"] = _prefer_demographics(self.demographics, other.demographics)
        payload["confidence"] = min(self.confidence, other.confidence)
        payload["content_fingerprint"] = other.content_fingerprint or self.content_fingerprint
        payload["source_connectors"] = sorted(set(self.source_connectors) | set(other.source_connectors))
        payload["last_seen_at"] = utc_now()
        return PersonRecord.model_validate(payload)

    @classmethod
    def create_id(cls, full_name: str, email: str | None = None) -> str:
        normalized_email = (email or "").strip().lower()
        if normalized_email:
            seed = normalized_email
        else:
            seed = full_name.strip().lower()
        return sha1(seed.encode("utf-8")).hexdigest()[:16]


class DiscoveryLink(BaseModel):
    url: str
    method: Literal["GET", "POST"] = "GET"
    json_payload: dict[str, Any] | None = None
    headers: dict[str, str] = Field(default_factory=dict)
    artifact_kind: Literal["html", "json"] = "html"
    unit: str | None = None
    staff: str | None = None
    label: str | None = None


class DiscoveryOption(BaseModel):
    code: str
    label: str


class DiscoveryFilterGroup(BaseModel):
    key: str
    label: str
    multi_select: bool = True
    options: list[DiscoveryOption] = Field(default_factory=list)


class DiscoverySnapshot(BaseModel):
    connector_name: str | None = None
    start_url: str
    result_links: list[DiscoveryLink] = Field(default_factory=list)
    department_staff_links: list[LinkRecord] = Field(default_factory=list)
    available_filters: list[DiscoveryFilterGroup] = Field(default_factory=list)
    connector_state: dict[str, Any] = Field(default_factory=dict)

    @property
    def available_units(self) -> list[DiscoveryOption]:
        return self._options_for_key("unit")

    @property
    def available_staff_types(self) -> list[DiscoveryOption]:
        return self._options_for_key("staff_type")

    def _options_for_key(self, key: str) -> list[DiscoveryOption]:
        for group in self.available_filters:
            if group.key == key:
                return group.options
        return []


class ResultPageData(BaseModel):
    people: list[PersonRecord] = Field(default_factory=list)
    pagination_links: list[DiscoveryLink] = Field(default_factory=list)

    @property
    def pagination_urls(self) -> list[str]:
        return [link.url for link in self.pagination_links if link.method == "GET"]


class PersonalPageData(BaseModel):
    name: str | None = None
    rank: str | None = None
    photo_url: str | None = None
    contacts: list[ContactPoint] = Field(default_factory=list)
    links: list[LinkRecord] = Field(default_factory=list)
    research_interests: list[str] = Field(default_factory=list)
    source_evidence: list[SourceEvidence] = Field(default_factory=list)


def dedupe_models(items: list[BaseModel], key_fn) -> list[BaseModel]:
    seen: set[tuple | str] = set()
    result: list[BaseModel] = []
    for item in items:
        key = key_fn(item)
        if key in seen:
            continue
        seen.add(key)
        result.append(item)
    return result


def _dedupe_as_dicts(items: list[BaseModel], key_fn) -> list[dict]:
    return [item.model_dump() for item in dedupe_models(items, key_fn)]


def _prefer_demographics(
    current: DemographicEstimate | None,
    incoming: DemographicEstimate | None,
) -> DemographicEstimate | None:
    if current is None:
        return incoming
    if incoming is None:
        return current
    current_confidence = current.face_confidence if current.face_confidence is not None else -1.0
    incoming_confidence = incoming.face_confidence if incoming.face_confidence is not None else -1.0
    if incoming_confidence >= current_confidence:
        return incoming
    return current
