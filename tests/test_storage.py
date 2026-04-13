from pathlib import Path

from ou_harvest.models import ContactPoint, PersonRecord, ReviewFlag
from ou_harvest.storage import Storage


def test_storage_writes_and_loads_record(tmp_path: Path):
    storage = Storage(tmp_path)
    record = PersonRecord(
        person_id="abc123",
        full_name="Dr. Test Person",
        contacts=[ContactPoint(kind="email", value="test@example.com")],
    )
    storage.save_record(record)

    loaded = storage.load_record("abc123")
    assert loaded is not None
    assert loaded.primary_email == "test@example.com"


def test_storage_artifact_fingerprint_and_review_queue(tmp_path: Path):
    storage = Storage(tmp_path)
    artifact = storage.write_artifact(
        kind="html",
        source_url="https://www.openu.ac.il/staff/pages/results.aspx?unit=311",
        content=b"<html></html>",
        content_type="text/html",
    )
    assert artifact.checksum
    assert storage.should_process("https://www.openu.ac.il/staff/pages/results.aspx?unit=311", artifact.checksum)

    storage.update_fingerprint("https://www.openu.ac.il/staff/pages/results.aspx?unit=311", artifact.checksum)
    assert not storage.should_process("https://www.openu.ac.il/staff/pages/results.aspx?unit=311", artifact.checksum)

    review_path = storage.write_review_flags(
        "abc123",
        [ReviewFlag(field_name="education", reason="low_confidence_llm_extraction", confidence=0.5)],
    )
    assert review_path.exists()


def test_storage_writes_image_artifact(tmp_path: Path):
    storage = Storage(tmp_path)
    artifact = storage.write_artifact(
        kind="image",
        source_url="https://example.com/photo.jpg",
        content=b"jpeg-bytes",
        content_type="image/jpeg",
    )

    assert artifact.kind == "image"
    assert Path(artifact.path).parent == tmp_path / "raw" / "image"
    assert Path(artifact.path).suffix == ".jpg"
    assert Path(artifact.path).read_bytes() == b"jpeg-bytes"


def test_storage_image_png_extension(tmp_path: Path):
    storage = Storage(tmp_path)
    artifact = storage.write_artifact(
        kind="image",
        source_url="https://example.com/photo.png",
        content=b"png-bytes",
        content_type="image/png",
    )

    assert artifact.kind == "image"
    assert Path(artifact.path).parent == tmp_path / "raw" / "image"
    assert Path(artifact.path).suffix == ".png"
    assert Path(artifact.path).read_bytes() == b"png-bytes"


def test_fingerprint_cache_defers_writes(tmp_path: Path):
    storage = Storage(tmp_path)
    fp_path = tmp_path / "state" / "fingerprints.json"

    storage.update_fingerprint("https://example.com/a", "checksum_a")
    storage.update_fingerprint("https://example.com/b", "checksum_b")

    # Before flush, disk should not have the updates (file may not exist or be empty default)
    import json
    if fp_path.exists():
        on_disk = json.loads(fp_path.read_text())
    else:
        on_disk = {}
    assert "https://example.com/a" not in on_disk

    # In-memory cache should work
    assert not storage.should_process("https://example.com/a", "checksum_a")
    assert storage.should_process("https://example.com/a", "different")

    # After flush, disk should have updates
    storage.flush_fingerprints()
    on_disk = json.loads(fp_path.read_text())
    assert on_disk["https://example.com/a"] == "checksum_a"
    assert on_disk["https://example.com/b"] == "checksum_b"
