from __future__ import annotations

import logging
from pathlib import Path
from typing import Callable, Iterable

from .config import AppConfig

logger = logging.getLogger(__name__)
from .events import EventSink, RunCancelled, RunEvent
from .http import FetchResult, guess_fetcher
from .llm import build_extractor
from .models import (
    AcademicServiceEntry,
    AppointmentEntry,
    AwardEntry,
    DiscoveryLink,
    DiscoverySnapshot,
    EducationEntry,
    LinkRecord,
    PersonRecord,
    PublicationEntry,
    ReviewFlag,
    SourceEvidence,
    utc_now,
)
from .adapters import get_adapter
from .secrets import SecretStore
from .storage import Storage
from .text_extract import extract_pdf_text


class OuHarvestPipeline:
    def __init__(
        self,
        config: AppConfig,
        *,
        event_sink: EventSink | None = None,
        should_cancel: Callable[[], bool] | None = None,
        secret_store: SecretStore | None = None,
    ):
        self.config = config
        self.adapter = get_adapter(config.university)
        self.storage = Storage(config.output_path)
        self.fetcher = guess_fetcher(config, force_playwright=self.adapter.requires_playwright())
        self.event_sink = event_sink
        self.should_cancel = should_cancel or (lambda: False)
        self.secret_store = secret_store or SecretStore()
        self._reverse_checksum_map: dict[str, str] | None = None

    def discover(self) -> DiscoverySnapshot:
        self._check_cancel("discover")
        self._emit("progress", stage="discover", message="Fetching discovery page", url=self.config.start_url)
        fetched = self.fetcher.fetch(self.config.start_url)
        artifact = self.storage.write_artifact(
            kind="html",
            source_url=fetched.url,
            content=fetched.content,
            content_type=fetched.content_type,
        )
        self._emit(
            "artifact_saved",
            stage="discover",
            message="Saved discovery HTML",
            url=fetched.url,
            artifact_path=artifact.path,
        )
        snapshot = self.adapter.parse_discovery_page(fetched.text, fetched.url)

        # Generate result URLs from connector-specific filter selections.
        generated_links = self.adapter.generate_result_links(
            snapshot, self.config.selected_filters_for_connector(self.config.university)
        )

        # Merge: static links from HTML + generated from selects + seed URLs
        all_links: dict[str, DiscoveryLink] = {}
        for link in snapshot.result_links:
            all_links[link.url] = link
        for link in generated_links:
            all_links.setdefault(link.url, link)
        for url in self.config.seed_result_urls:
            all_links.setdefault(url, DiscoveryLink(url=url))

        result_links = list(all_links.values())
        snapshot = snapshot.model_copy(update={"result_links": sorted(result_links, key=lambda item: item.url)})
        self.storage.save_json("state/discovery.json", snapshot.model_dump(mode="json"))
        self.storage.update_fingerprint(fetched.url, artifact.checksum)
        self.storage.flush_fingerprints()
        self._emit(
            "log",
            stage="discover",
            message="Discovery completed",
            result_links=len(snapshot.result_links),
            department_staff_links=len(snapshot.department_staff_links),
        )
        return snapshot

    def crawl(self) -> list[str]:
        snapshot_data = self.storage.load_json("state/discovery.json")
        if not snapshot_data or snapshot_data.get("connector_name") not in {None, self.config.university}:
            snapshot = self.discover()
        else:
            snapshot = DiscoverySnapshot.model_validate(snapshot_data)

        urls = {item.url for item in snapshot.result_links}
        crawled_urls: list[str] = []
        seen: set[str] = set()
        queue = list(urls)
        personal_pages_remaining = self.config.personal_page_limit
        while queue:
            self._check_cancel("crawl")
            url = queue.pop(0)
            if url in seen:
                continue
            seen.add(url)
            self._emit(
                "progress",
                stage="crawl",
                message="Fetching results page",
                url=url,
                queue_size=len(queue),
                crawled_count=len(crawled_urls),
                current=len(crawled_urls),
                total=len(crawled_urls) + len(queue) + 1,
            )
            fetched = self.fetcher.fetch(url)
            artifact = self.storage.write_artifact(
                kind="html",
                source_url=fetched.url,
                content=fetched.content,
                content_type=fetched.content_type,
            )
            self.storage.update_fingerprint(fetched.url, artifact.checksum)
            crawled_urls.append(fetched.url)
            self._emit(
                "artifact_saved",
                stage="crawl",
                message="Saved HTML artifact",
                url=fetched.url,
                artifact_path=artifact.path,
                crawled_count=len(crawled_urls),
            )
            result_page = self.adapter.parse_results_page(fetched.text, fetched.url)
            for next_url in result_page.pagination_urls:
                if next_url not in seen:
                    queue.append(next_url)

            personal_links = [
                link.url
                for person in result_page.people
                for link in person.links
                if link.kind in {"personal_page", "cv", "cris"}
            ]
            if personal_pages_remaining > 0:
                personal_links = personal_links[:personal_pages_remaining]
            for personal_url in personal_links:
                self._check_cancel("crawl")
                if personal_url in seen:
                    continue
                seen.add(personal_url)
                self._emit(
                    "progress",
                    stage="crawl",
                    message="Fetching linked profile artifact",
                    url=personal_url,
                    crawled_count=len(crawled_urls),
                )
                personal_fetch = self.fetcher.fetch(personal_url)
                kind = "pdf" if personal_url.lower().endswith(".pdf") else "html"
                personal_artifact = self.storage.write_artifact(
                    kind=kind,
                    source_url=personal_fetch.url,
                    content=personal_fetch.content,
                    content_type=personal_fetch.content_type,
                )
                self.storage.update_fingerprint(personal_fetch.url, personal_artifact.checksum)
                crawled_urls.append(personal_fetch.url)
                self._emit(
                    "artifact_saved",
                    stage="crawl",
                    message="Saved linked artifact",
                    url=personal_fetch.url,
                    artifact_path=personal_artifact.path,
                    crawled_count=len(crawled_urls),
                )
                if personal_pages_remaining > 0:
                    personal_pages_remaining -= 1

                # Follow CV/PDF links found on personal pages
                if kind == "html":
                    page_data = self.adapter.parse_personal_page(personal_fetch.text, personal_fetch.url)
                    cv_links = [link.url for link in page_data.links if link.kind == "cv"]
                    for cv_url in cv_links:
                        self._check_cancel("crawl")
                        if cv_url in seen:
                            continue
                        seen.add(cv_url)
                        try:
                            self._emit("progress", stage="crawl", message="Fetching CV/PDF", url=cv_url, crawled_count=len(crawled_urls))
                            cv_fetch = self.fetcher.fetch(cv_url)
                            cv_kind = "pdf" if cv_url.lower().endswith(".pdf") else "html"
                            cv_artifact = self.storage.write_artifact(
                                kind=cv_kind, source_url=cv_fetch.url, content=cv_fetch.content, content_type=cv_fetch.content_type,
                            )
                            self.storage.update_fingerprint(cv_fetch.url, cv_artifact.checksum)
                            crawled_urls.append(cv_fetch.url)
                            self._emit("artifact_saved", stage="crawl", message="Saved CV artifact", url=cv_fetch.url, artifact_path=cv_artifact.path, crawled_count=len(crawled_urls))
                        except Exception as exc:
                            self._emit("log", stage="crawl", message=f"Failed to fetch CV: {exc}", url=cv_url)
        self.storage.flush_fingerprints()
        self.storage.save_json("state/crawl_manifest.json", {"urls": crawled_urls})
        self._emit("log", stage="crawl", message="Crawl completed", crawled_count=len(crawled_urls))
        return crawled_urls

    def parse(self) -> list[PersonRecord]:
        records: dict[str, PersonRecord] = {}

        # Only parse artifacts from the current crawl manifest
        manifest = self.storage.load_json("state/crawl_manifest.json", default={"urls": []})
        crawled_urls = set(manifest.get("urls", []))

        html_files = sorted(self.storage.raw_html.glob("*.html"))
        pdf_files = sorted(self.storage.raw_pdf.glob("*.pdf"))
        total_files = len(html_files) + len(pdf_files)
        pending_personal_pages: list[tuple[str, str]] = []

        for file_index, path in enumerate(html_files):
            self._check_cancel("parse")
            html = path.read_text(encoding="utf-8", errors="replace")
            source_url = self._source_url_for_checksum(path.stem)
            if not source_url:
                continue
            if crawled_urls and source_url not in crawled_urls:
                continue
            self._emit(
                "progress",
                stage="parse",
                message="Parsing HTML artifact",
                source_url=source_url,
                artifact_path=str(path),
                current=file_index,
                total=total_files,
            )
            result_page = self.adapter.parse_results_page(html, source_url)
            if result_page.people or result_page.pagination_urls:
                for person in result_page.people:
                    records[person.person_id] = self._merge_record(records.get(person.person_id), person)
            else:
                pending_personal_pages.append((source_url, html))

        pending = list(pending_personal_pages)
        while pending:
            self._check_cancel("parse")
            next_pending: list[tuple[str, str]] = []
            processed_in_pass = 0
            for source_url, html in pending:
                page = self.adapter.parse_personal_page(html, source_url)
                linked_record = self._match_personal_page(records, source_url, page.name)
                if linked_record is None:
                    next_pending.append((source_url, html))
                    continue
                updated = linked_record.model_copy(deep=True)
                if page.rank and not updated.current_rank:
                    updated.current_rank = page.rank
                updated.contacts = _dedupe_by_key(
                    updated.contacts + page.contacts, key=lambda item: (item.kind, item.value)
                )
                updated.links = _dedupe_by_key(
                    updated.links + page.links, key=lambda item: (item.kind, item.url)
                )
                updated.research_interests = sorted(
                    set(updated.research_interests + page.research_interests)
                )
                updated.source_evidence.extend(page.source_evidence)
                records[updated.person_id] = updated
                processed_in_pass += 1
            if processed_in_pass == 0:
                break
            pending = next_pending

        for pdf_index, path in enumerate(pdf_files):
            self._check_cancel("parse")
            source_url = self._source_url_for_checksum(path.stem)
            if not source_url:
                continue
            if crawled_urls and source_url not in crawled_urls:
                continue
            self._emit(
                "progress",
                stage="parse",
                message="Extracting PDF text",
                source_url=source_url,
                artifact_path=str(path),
                current=len(html_files) + pdf_index,
                total=total_files,
            )
            person = self._match_pdf_link(records, source_url)
            if person is None:
                continue
            text = extract_pdf_text(path)
            if not text:
                continue
            artifact = self.storage.write_artifact(
                kind="text",
                source_url=source_url,
                content=text.encode("utf-8"),
                content_type="text/plain",
            )
            updated = person.model_copy(deep=True)
            updated.artifacts.append(artifact)
            updated.source_evidence.append(
                SourceEvidence(
                    field_name="cv_text",
                    source_url=source_url,
                    excerpt=text[:250],
                    confidence=0.8,
                    artifact_id=artifact.artifact_id,
                )
            )
            records[updated.person_id] = updated

        for record in records.values():
            self.storage.save_record(record)
            if record.review_flags:
                self.storage.write_review_flags(record.person_id, record.review_flags)
            self._emit(
                "record_saved",
                stage="parse",
                message="Saved canonical record",
                person_id=record.person_id,
                full_name=record.full_name,
            )
        # Save the IDs from this parse run so enrich/review/export can scope to them
        self.storage.save_json("state/last_parse_ids.json", sorted(records.keys()))
        self._emit("log", stage="parse", message="Parse completed", record_count=len(records))
        return list(records.values())

    def _scoped_records(self) -> list[PersonRecord]:
        """Return records from the last parse run, or all records if no scope exists."""
        scope_ids = self.storage.load_json("state/last_parse_ids.json", default=None)
        all_records = self.storage.all_records()
        if scope_ids is None:
            return all_records
        scope_set = set(scope_ids)
        return [r for r in all_records if r.person_id in scope_set]

    def enrich(self, provider_name: str) -> list[PersonRecord]:
        extractor = build_extractor(self.config, provider_name, secret_store=self.secret_store)
        all_records = self._scoped_records()
        total_records = len(all_records)
        updated_records: list[PersonRecord] = []
        records_with_cv_text = 0
        cv_chunks_processed = 0
        records_with_cv_additions = 0
        skipped = 0
        for record_index, record in enumerate(all_records):
            self._check_cancel("enrich")

            # Skip records already enriched by this provider
            if record.enriched_at and provider_name in record.enriched_by:
                updated_records.append(record)
                skipped += 1
                self._emit(
                    "log",
                    stage="enrich",
                    message="Skipping (already enriched)",
                    person_id=record.person_id,
                    full_name=record.full_name,
                    provider=provider_name,
                    current=record_index,
                    total=total_records,
                )
                continue

            enriched = record.model_copy(deep=True)
            before_counts = self._section_counts(record)

            profile_text = self._collect_profile_enrichment_text(record)
            if profile_text and len(profile_text) > 50:
                self._emit(
                    "progress",
                    stage="enrich",
                    message="Enriching record from profile text",
                    provider=provider_name,
                    person_id=record.person_id,
                    full_name=record.full_name,
                    current=record_index,
                    total=total_records,
                )
                payload = extractor.extract(profile_text, source_kind="profile")
                enriched = self._apply_enrichment(
                    enriched,
                    payload,
                    source_url=self._preferred_profile_source(record),
                    artifact_id=None,
                    source_kind="profile",
                )

            cv_artifacts = self._collect_cv_text_artifacts(record)
            if cv_artifacts:
                records_with_cv_text += 1
            for artifact, cv_text in cv_artifacts:
                chunks = _chunk_text(cv_text)
                for index, chunk in enumerate(chunks, start=1):
                    self._check_cancel("enrich")
                    cv_chunks_processed += 1
                    self._emit(
                        "progress",
                        stage="enrich",
                        message="Enriching record from CV text",
                        provider=provider_name,
                        person_id=record.person_id,
                        full_name=record.full_name,
                        source_url=artifact.source_url,
                        artifact_id=artifact.artifact_id,
                        chunk_index=index,
                        chunk_count=len(chunks),
                    )
                    payload = extractor.extract(chunk, source_kind="cv")
                    enriched = self._apply_enrichment(
                        enriched,
                        payload,
                        source_url=artifact.source_url,
                        artifact_id=artifact.artifact_id,
                        source_kind="cv",
                    )

            after_counts = self._section_counts(enriched)
            if cv_artifacts and any(after_counts[key] > before_counts[key] for key in after_counts):
                records_with_cv_additions += 1

            # Stamp enrichment
            enriched.enriched_at = utc_now()
            if provider_name not in enriched.enriched_by:
                enriched.enriched_by.append(provider_name)

            self.storage.save_record(enriched)
            if enriched.review_flags:
                self.storage.write_review_flags(enriched.person_id, enriched.review_flags)
            updated_records.append(enriched)
        self._emit(
            "log",
            stage="enrich",
            message="Enrichment completed",
            provider=provider_name,
            record_count=len(updated_records),
            skipped=skipped,
            records_with_cv_text=records_with_cv_text,
            cv_chunks_processed=cv_chunks_processed,
            records_with_cv_additions=records_with_cv_additions,
            publications_total=sum(len(record.publications) for record in updated_records),
            awards_total=sum(len(record.awards) for record in updated_records),
            academic_service_total=sum(len(record.academic_service) for record in updated_records),
            notable_links_total=sum(len(record.notable_links) for record in updated_records),
        )
        return updated_records

    def export(self, fmt: str) -> Path:
        records = self._scoped_records()
        if fmt == "json":
            path = self.storage.export_records_json(records)
            self._emit("log", stage="export", message=f"Exported {len(records)} records to JSON", path=str(path), format=fmt)
            return path
        if fmt == "jsonl":
            path = self.storage.export_records_jsonl(records)
            self._emit("log", stage="export", message=f"Exported {len(records)} records to JSONL", path=str(path), format=fmt)
            return path
        raise ValueError(f"Unsupported export format: {fmt}")

    def review(self) -> list[dict]:
        output: list[dict] = []
        for record in self._scoped_records():
            if record.confidence < self.config.review.confidence_threshold or record.review_flags:
                item = {
                    "person_id": record.person_id,
                    "full_name": record.full_name,
                    "confidence": record.confidence,
                    "review_flags": [flag.model_dump() for flag in record.review_flags],
                }
                output.append(item)
        self.storage.save_json("review/queue.json", output)
        self._emit("log", stage="review", message="Review queue updated", review_queue_count=len(output))
        return output

    def _source_url_for_checksum(self, checksum_prefix: str) -> str | None:
        if self._reverse_checksum_map is None:
            fingerprints = self.storage.load_json("state/fingerprints.json", default={})
            by_checksum: dict[str, str] = {}
            for url, checksum in fingerprints.items():
                prefix = checksum[:16]
                if prefix in by_checksum:
                    logger.warning(
                        "Fingerprint prefix collision: %s -> %s and %s",
                        prefix, by_checksum[prefix], url,
                    )
                by_checksum[prefix] = url
            self._reverse_checksum_map = by_checksum
        return self._reverse_checksum_map.get(checksum_prefix)

    def _merge_record(self, current: PersonRecord | None, incoming: PersonRecord) -> PersonRecord:
        if current is None:
            return incoming
        return current.merge(incoming)

    def _match_personal_page(
        self, records: dict[str, PersonRecord], source_url: str, page_name: str | None
    ) -> PersonRecord | None:
        for record in records.values():
            if any(link.url == source_url for link in record.links):
                return record
        if page_name is None:
            return None
        normalized = page_name.lower()
        for record in records.values():
            if record.full_name.lower() in normalized or normalized in record.full_name.lower():
                return record
        return None

    def _match_pdf_link(self, records: dict[str, PersonRecord], source_url: str) -> PersonRecord | None:
        for record in records.values():
            if any(link.url == source_url and link.kind == "cv" for link in record.links):
                return record
        return None

    def _collect_profile_enrichment_text(self, record: PersonRecord) -> str:
        snippets = [
            evidence.excerpt
            for evidence in record.source_evidence
            if evidence.excerpt and evidence.field_name != "cv_text"
        ]
        return "\n\n".join(snippets[:4])[:4000]

    def _collect_cv_text_artifacts(self, record: PersonRecord) -> list[tuple]:
        items = []
        for artifact in record.artifacts:
            if artifact.kind != "text":
                continue
            text_path = Path(artifact.path)
            if text_path.exists():
                items.append((artifact, text_path.read_text(encoding="utf-8", errors="replace")))
        return items

    def _preferred_profile_source(self, record: PersonRecord) -> str:
        for link in record.links:
            if link.kind in {"personal_page", "department_page"}:
                return link.url
        return record.links[0].url if record.links else ""

    def _apply_enrichment(
        self,
        record: PersonRecord,
        payload: dict,
        *,
        source_url: str,
        artifact_id: str | None,
        source_kind: str,
    ) -> PersonRecord:
        updated = record.model_copy(deep=True)
        if payload.get("current_rank") and not updated.current_rank:
            updated.current_rank = payload["current_rank"]

        for interest in payload.get("research_interests") or []:
            if interest not in updated.research_interests:
                updated.research_interests.append(interest)

        confidences: list[float] = []
        for item in payload.get("education") or []:
            confidence = float(item.get("confidence", 0.0))
            confidences.append(confidence)
            evidence = _build_evidence(
                field_name="education",
                source_url=source_url,
                excerpt=item.get("excerpt"),
                confidence=confidence,
                artifact_id=artifact_id,
            )
            entry = EducationEntry(
                degree_level=item.get("degree_level"),
                field=item.get("field"),
                institution=item.get("institution"),
                start_date=item.get("start_date"),
                end_date=item.get("end_date"),
                year=item.get("year"),
                evidence=[evidence] if evidence else [],
            )
            _append_unique_model(
                updated.education,
                entry,
                key=lambda existing: (
                    existing.degree_level,
                    existing.field,
                    existing.institution,
                    existing.start_date,
                    existing.end_date,
                    existing.year,
                ),
            )
            if confidence < self.config.review.confidence_threshold:
                updated.review_flags.append(
                    ReviewFlag(
                        field_name="education",
                        reason="low_confidence_llm_extraction",
                        confidence=confidence,
                        source_url=source_url,
                    )
                )

        for item in payload.get("appointments") or []:
            confidence = float(item.get("confidence", 0.0))
            confidences.append(confidence)
            evidence = _build_evidence(
                field_name="appointments",
                source_url=source_url,
                excerpt=item.get("excerpt"),
                confidence=confidence,
                artifact_id=artifact_id,
            )
            entry = AppointmentEntry(
                title=item.get("title"),
                institution=item.get("institution"),
                department=item.get("department"),
                faculty=item.get("faculty"),
                start_date=item.get("start_date"),
                end_date=item.get("end_date"),
                is_current=item.get("is_current"),
                evidence=[evidence] if evidence else [],
            )
            _append_unique_model(
                updated.appointments,
                entry,
                key=lambda existing: (
                    existing.title,
                    existing.institution,
                    existing.department,
                    existing.faculty,
                    existing.start_date,
                    existing.end_date,
                ),
            )
            if confidence < self.config.review.confidence_threshold:
                updated.review_flags.append(
                    ReviewFlag(
                        field_name="appointments",
                        reason="low_confidence_llm_extraction",
                        confidence=confidence,
                        source_url=source_url,
                    )
                )

        for item in payload.get("publications") or []:
            confidence = float(item.get("confidence", 0.0))
            confidences.append(confidence)
            evidence = _build_evidence(
                field_name="publications",
                source_url=source_url,
                excerpt=item.get("excerpt"),
                confidence=confidence,
                artifact_id=artifact_id,
            )
            entry = PublicationEntry(
                title=item.get("title"),
                venue=item.get("venue"),
                year=item.get("year"),
                authors_text=item.get("authors_text"),
                publication_type=item.get("publication_type"),
                doi_or_url=item.get("doi_or_url"),
                evidence=[evidence] if evidence else [],
            )
            _append_unique_model(
                updated.publications,
                entry,
                key=lambda existing: (
                    existing.title,
                    existing.venue,
                    existing.year,
                    existing.publication_type,
                    existing.doi_or_url,
                ),
            )
            if confidence < self.config.review.confidence_threshold:
                updated.review_flags.append(
                    ReviewFlag(
                        field_name="publications",
                        reason="low_confidence_llm_extraction",
                        confidence=confidence,
                        source_url=source_url,
                    )
                )

        for item in payload.get("awards") or []:
            confidence = float(item.get("confidence", 0.0))
            confidences.append(confidence)
            evidence = _build_evidence(
                field_name="awards",
                source_url=source_url,
                excerpt=item.get("excerpt"),
                confidence=confidence,
                artifact_id=artifact_id,
            )
            entry = AwardEntry(
                title=item.get("title"),
                organization=item.get("organization"),
                year=item.get("year"),
                evidence=[evidence] if evidence else [],
            )
            _append_unique_model(
                updated.awards,
                entry,
                key=lambda existing: (existing.title, existing.organization, existing.year),
            )
            if confidence < self.config.review.confidence_threshold:
                updated.review_flags.append(
                    ReviewFlag(
                        field_name="awards",
                        reason="low_confidence_llm_extraction",
                        confidence=confidence,
                        source_url=source_url,
                    )
                )

        for item in payload.get("academic_service") or []:
            confidence = float(item.get("confidence", 0.0))
            confidences.append(confidence)
            evidence = _build_evidence(
                field_name="academic_service",
                source_url=source_url,
                excerpt=item.get("excerpt"),
                confidence=confidence,
                artifact_id=artifact_id,
            )
            entry = AcademicServiceEntry(
                role=item.get("role"),
                organization=item.get("organization"),
                start_date=item.get("start_date"),
                end_date=item.get("end_date"),
                evidence=[evidence] if evidence else [],
            )
            _append_unique_model(
                updated.academic_service,
                entry,
                key=lambda existing: (
                    existing.role,
                    existing.organization,
                    existing.start_date,
                    existing.end_date,
                ),
            )
            if confidence < self.config.review.confidence_threshold:
                updated.review_flags.append(
                    ReviewFlag(
                        field_name="academic_service",
                        reason="low_confidence_llm_extraction",
                        confidence=confidence,
                        source_url=source_url,
                    )
                )

        for item in payload.get("notable_links") or []:
            confidence = float(item.get("confidence", 0.0))
            confidences.append(confidence)
            url = item.get("url")
            if not url:
                continue
            link = LinkRecord(
                kind=item.get("kind") or ("cv_link" if source_kind == "cv" else "external"),
                url=url,
                label=item.get("label"),
            )
            _append_unique_model(
                updated.notable_links,
                link,
                key=lambda existing: (existing.kind, existing.url),
            )
            evidence = _build_evidence(
                field_name="notable_links",
                source_url=source_url,
                excerpt=item.get("excerpt"),
                confidence=confidence,
                artifact_id=artifact_id,
            )
            if evidence is not None:
                updated.source_evidence.append(evidence)
            if confidence < self.config.review.confidence_threshold:
                updated.review_flags.append(
                    ReviewFlag(
                        field_name="notable_links",
                        reason="low_confidence_llm_extraction",
                        confidence=confidence,
                        source_url=source_url,
                    )
                )

        if confidences:
            updated.confidence = min([updated.confidence, *confidences])
        return updated

    def _section_counts(self, record: PersonRecord) -> dict[str, int]:
        return {
            "education": len(record.education),
            "appointments": len(record.appointments),
            "publications": len(record.publications),
            "awards": len(record.awards),
            "academic_service": len(record.academic_service),
            "notable_links": len(record.notable_links),
        }

    def _emit(self, kind: str, *, stage: str, message: str | None = None, **data) -> None:
        if self.event_sink is None:
            return
        self.event_sink(RunEvent(kind=kind, stage=stage, message=message, data=data))

    def _check_cancel(self, stage: str) -> None:
        if self.should_cancel():
            raise RunCancelled(f"Stage '{stage}' was cancelled")


def _dedupe_by_key(items: Iterable, key):
    seen = set()
    result = []
    for item in items:
        marker = key(item)
        if marker in seen:
            continue
        seen.add(marker)
        result.append(item)
    return result


def _build_evidence(
    field_name: str,
    source_url: str,
    excerpt: str | None,
    confidence: float,
    artifact_id: str | None = None,
) -> SourceEvidence | None:
    if not excerpt:
        return None
    return SourceEvidence(
        field_name=field_name,
        source_url=source_url,
        excerpt=excerpt[:250],
        confidence=confidence,
        artifact_id=artifact_id,
    )


def _append_unique_model(items: list, item, key):
    marker = key(item)
    for existing in items:
        if key(existing) == marker:
            return
    items.append(item)


def _chunk_text(text: str, max_chars: int = 6000) -> list[str]:
    normalized = text.strip()
    if not normalized:
        return []
    paragraphs = [paragraph.strip() for paragraph in normalized.split("\n\n") if paragraph.strip()]
    if not paragraphs:
        return [normalized[:max_chars]]
    chunks: list[str] = []
    current: list[str] = []
    current_len = 0
    for paragraph in paragraphs:
        if len(paragraph) > max_chars:
            if current:
                chunks.append("\n\n".join(current))
                current = []
                current_len = 0
            for start in range(0, len(paragraph), max_chars):
                chunks.append(paragraph[start : start + max_chars])
            continue
        addition = len(paragraph) + (2 if current else 0)
        if current and current_len + addition > max_chars:
            chunks.append("\n\n".join(current))
            current = [paragraph]
            current_len = len(paragraph)
        else:
            current.append(paragraph)
            current_len += addition
    if current:
        chunks.append("\n\n".join(current))
    return chunks
