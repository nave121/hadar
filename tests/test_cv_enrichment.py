from __future__ import annotations

from pathlib import Path

from ou_harvest.config import AppConfig
from ou_harvest.models import LinkRecord, PersonRecord, SourceEvidence
from ou_harvest.pipeline import OuHarvestPipeline, _chunk_text
from ou_harvest.storage import Storage


def test_chunk_text_splits_large_cv_into_multiple_chunks():
    text = "\n\n".join(
        [
            "Education " + ("A" * 4500),
            "Appointments " + ("B" * 4500),
            "Publications " + ("C" * 4500),
        ]
    )

    chunks = _chunk_text(text, max_chars=5000)

    assert len(chunks) == 3
    assert chunks[0].startswith("Education")
    assert chunks[-1].startswith("Publications")


def test_enrich_reads_cv_text_and_adds_structured_sections(monkeypatch, tmp_path: Path):
    class FakeExtractor:
        def __init__(self):
            self.calls: list[str] = []

        def extract(self, text: str, *, source_kind: str = "profile") -> dict:
            self.calls.append(source_kind)
            if source_kind == "profile":
                return {
                    "current_rank": "Professor",
                    "research_interests": ["Machine learning"],
                    "education": [],
                    "appointments": [],
                    "publications": [],
                    "awards": [],
                    "academic_service": [],
                    "notable_links": [],
                }
            return {
                "education": [
                    {
                        "degree_level": "PhD",
                        "field": "Computer Science",
                        "institution": "Technion",
                        "year": 2018,
                        "excerpt": "PhD in Computer Science, Technion, 2018",
                        "confidence": 0.92,
                    }
                ],
                "appointments": [
                    {
                        "title": "Assistant Professor",
                        "institution": "Open University of Israel",
                        "department": "Computer Science",
                        "faculty": "CS",
                        "start_date": "2022",
                        "end_date": None,
                        "is_current": True,
                        "excerpt": "Assistant Professor, Open University of Israel, 2022-present",
                        "confidence": 0.88,
                    }
                ],
                "publications": [
                    {
                        "title": "Learning Graphs",
                        "venue": "NeurIPS",
                        "year": 2023,
                        "authors_text": "A. Person; B. Person",
                        "publication_type": "conference",
                        "doi_or_url": "https://example.com/paper",
                        "excerpt": "Learning Graphs. NeurIPS 2023",
                        "confidence": 0.95,
                    }
                ],
                "awards": [
                    {
                        "title": "Best Paper Award",
                        "organization": "NeurIPS",
                        "year": 2023,
                        "excerpt": "Best Paper Award, NeurIPS 2023",
                        "confidence": 0.84,
                    }
                ],
                "academic_service": [
                    {
                        "role": "Program Committee Member",
                        "organization": "ICML",
                        "start_date": "2024",
                        "end_date": None,
                        "excerpt": "Program Committee Member, ICML 2024",
                        "confidence": 0.7,
                    }
                ],
                "notable_links": [
                    {
                        "kind": "publication_profile",
                        "url": "https://scholar.example.com/person",
                        "label": "Scholar Profile",
                        "excerpt": "Scholar profile link",
                        "confidence": 0.9,
                    }
                ],
                "research_interests": ["Graph learning"],
            }

    fake_extractor = FakeExtractor()
    monkeypatch.setattr("ou_harvest.pipeline.build_extractor", lambda *args, **kwargs: fake_extractor)

    config = AppConfig(output_root=str(tmp_path / "data"))
    storage = Storage(config.output_path)
    artifact = storage.write_artifact(
        kind="text",
        source_url="https://example.com/cv.pdf",
        content=b"PhD in Computer Science\n\nLearning Graphs. NeurIPS 2023\n\nProgram Committee Member, ICML 2024",
        content_type="text/plain",
    )
    record = PersonRecord(
        person_id="person-1",
        full_name="Prof. Test Person",
        current_role="Head of Department",
        links=[LinkRecord(kind="personal_page", url="https://example.com/profile")],
        artifacts=[artifact],
        source_evidence=[
            SourceEvidence(
                field_name="directory_record",
                source_url="https://example.com/profile",
                excerpt="Current OU profile overview with research interests in machine learning, graph neural networks, and optimization methods for large-scale systems",
                confidence=0.95,
            )
        ],
    )
    storage.save_record(record)

    pipeline = OuHarvestPipeline(config)
    enriched_records = pipeline.enrich("ollama")

    assert len(enriched_records) == 1
    enriched = storage.load_record("person-1")
    assert enriched is not None
    assert fake_extractor.calls == ["profile", "cv"]
    assert enriched.current_rank == "Professor"
    assert enriched.current_role == "Head of Department"
    assert sorted(enriched.research_interests) == ["Graph learning", "Machine learning"]
    assert len(enriched.education) == 1
    assert len(enriched.appointments) == 1
    assert len(enriched.publications) == 1
    assert len(enriched.awards) == 1
    assert len(enriched.academic_service) == 1
    assert len(enriched.notable_links) == 1
    assert enriched.publications[0].evidence[0].artifact_id == artifact.artifact_id
    assert enriched.academic_service[0].evidence[0].source_url == "https://example.com/cv.pdf"
    assert any(flag.field_name == "academic_service" for flag in enriched.review_flags)
    assert enriched.confidence == 0.7


def test_cv_enrichment_dedupes_repeated_chunk_results(monkeypatch, tmp_path: Path):
    class FakeExtractor:
        def extract(self, text: str, *, source_kind: str = "profile") -> dict:
            if source_kind == "profile":
                return {
                    "education": [],
                    "appointments": [],
                    "publications": [],
                    "awards": [],
                    "academic_service": [],
                    "notable_links": [],
                    "research_interests": [],
                }
            return {
                "education": [],
                "appointments": [],
                "publications": [
                    {
                        "title": "Repeated Paper",
                        "venue": "ICML",
                        "year": 2022,
                        "authors_text": "A. Person",
                        "publication_type": "conference",
                        "doi_or_url": None,
                        "excerpt": "Repeated Paper, ICML 2022",
                        "confidence": 0.9,
                    }
                ],
                "awards": [],
                "academic_service": [],
                "notable_links": [],
                "research_interests": [],
            }

    monkeypatch.setattr("ou_harvest.pipeline.build_extractor", lambda *args, **kwargs: FakeExtractor())

    config = AppConfig(output_root=str(tmp_path / "data"))
    storage = Storage(config.output_path)
    artifact = storage.write_artifact(
        kind="text",
        source_url="https://example.com/cv.pdf",
        content=("Paragraph " * 2000).encode("utf-8"),
        content_type="text/plain",
    )
    record = PersonRecord(
        person_id="person-2",
        full_name="Prof. Chunk Person",
        links=[LinkRecord(kind="personal_page", url="https://example.com/profile")],
        artifacts=[artifact],
        source_evidence=[
            SourceEvidence(
                field_name="directory_record",
                source_url="https://example.com/profile",
                excerpt="Profile",
                confidence=0.95,
            )
        ],
    )
    storage.save_record(record)

    pipeline = OuHarvestPipeline(config)
    pipeline.enrich("ollama")

    enriched = storage.load_record("person-2")
    assert enriched is not None
    assert len(enriched.publications) == 1
