from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path
from typing import Any

from .models import Artifact, PersonRecord, ReviewFlag


class Storage:
    def __init__(self, root: Path):
        self.root = root
        self.raw_html = root / "raw" / "html"
        self.raw_pdf = root / "raw" / "pdf"
        self.raw_text = root / "raw" / "text"
        self.raw_image = root / "raw" / "image"
        self.records = root / "records"
        self.review = root / "review"
        self.exports = root / "exports"
        self.state = root / "state"
        for path in (
            self.raw_html,
            self.raw_pdf,
            self.raw_text,
            self.raw_image,
            self.records,
            self.review,
            self.exports,
            self.state,
        ):
            path.mkdir(parents=True, exist_ok=True)
        self._fingerprints: dict | None = None

    def write_artifact(
        self, *, kind: str, source_url: str, content: bytes, content_type: str | None = None
    ) -> Artifact:
        checksum = sha256(content).hexdigest()
        normalized_content_type = (content_type or "").split(";", 1)[0].strip().lower()
        extension = {
            "html": ".html",
            "pdf": ".pdf",
            "text": ".txt",
            "json": ".json",
            "image": {
                "image/jpeg": ".jpg",
                "image/png": ".png",
                "image/webp": ".webp",
            }.get(normalized_content_type, ".bin"),
        }.get(kind, ".bin")
        artifact_id = checksum[:16]
        target_dir = {
            "html": self.raw_html,
            "pdf": self.raw_pdf,
            "text": self.raw_text,
            "image": self.raw_image,
            "json": self.state,
        }.get(kind, self.state)
        path = target_dir / f"{artifact_id}{extension}"
        if not path.exists():
            path.write_bytes(content)
        return Artifact(
            artifact_id=artifact_id,
            kind=kind,
            source_url=source_url,
            path=str(path),
            checksum=checksum,
            content_type=content_type,
        )

    def save_record(self, record: PersonRecord) -> Path:
        path = self.records / f"{record.person_id}.json"
        path.write_text(record.model_dump_json(indent=2), encoding="utf-8")
        return path

    def load_record(self, person_id: str) -> PersonRecord | None:
        path = self.records / f"{person_id}.json"
        if not path.exists():
            return None
        return PersonRecord.model_validate_json(path.read_text(encoding="utf-8"))

    def all_records(self) -> list[PersonRecord]:
        return [
            PersonRecord.model_validate_json(path.read_text(encoding="utf-8"))
            for path in sorted(self.records.glob("*.json"))
        ]

    def save_json(self, relative_path: str, payload: Any) -> Path:
        path = self.root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return path

    def load_json(self, relative_path: str, default: Any = None) -> Any:
        path = self.root / relative_path
        if not path.exists():
            return default
        return json.loads(path.read_text(encoding="utf-8"))

    def write_review_flags(self, person_id: str, flags: list[ReviewFlag]) -> Path:
        path = self.review / f"{person_id}.json"
        path.write_text(
            json.dumps([flag.model_dump() for flag in flags], ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return path

    def prune_generated_files(self, *, keep_record_ids: set[str], keep_review_ids: set[str]) -> None:
        for path in self.records.glob("*.json"):
            if path.stem not in keep_record_ids:
                path.unlink()
        for path in self.review.glob("*.json"):
            if path.stem not in keep_review_ids and path.name != "queue.json":
                path.unlink()

    def export_json(self, filename: str = "people.json") -> Path:
        return self.export_records_json(self.all_records(), filename)

    def export_jsonl(self, filename: str = "people.jsonl") -> Path:
        return self.export_records_jsonl(self.all_records(), filename)

    def export_records_json(self, records: list[PersonRecord], filename: str = "people.json") -> Path:
        data = [record.model_dump(mode="json") for record in records]
        return self.save_json(f"exports/{filename}", data)

    def export_records_jsonl(self, records: list[PersonRecord], filename: str = "people.jsonl") -> Path:
        path = self.exports / filename
        lines = [record.model_dump_json() for record in records]
        path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
        return path

    def _load_fingerprints(self) -> dict:
        if self._fingerprints is None:
            self._fingerprints = self.load_json("state/fingerprints.json", default={})
        return self._fingerprints

    def update_fingerprint(self, url: str, checksum: str) -> None:
        self._load_fingerprints()[url] = checksum

    def flush_fingerprints(self) -> None:
        if self._fingerprints is not None:
            self.save_json("state/fingerprints.json", self._fingerprints)

    def should_process(self, url: str, checksum: str) -> bool:
        return self._load_fingerprints().get(url) != checksum
