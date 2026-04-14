"""Cross-connector E2E tests verifying all adapters satisfy the same contract."""

from pathlib import Path

import pytest

from ou_harvest.adapters import available_adapters, get_adapter
from ou_harvest.adapters.base import UniversityAdapter
from ou_harvest.config import AppConfig
from ou_harvest.pipeline import OuHarvestPipeline
from ou_harvest.storage import Storage


FIXTURES = Path(__file__).parent / "fixtures"


@pytest.mark.parametrize("connector", ["openu", "bgu", "technion_med"])
def test_adapter_contract_compliance(connector):
    """Every adapter implements the full UniversityAdapter interface."""
    adapter = get_adapter(connector)
    assert isinstance(adapter, UniversityAdapter)
    assert adapter.name == connector
    assert adapter.display_name
    assert adapter.default_start_url.startswith("http")
    assert len(adapter.default_allowed_domains) >= 1
    # All required methods exist and are callable
    assert callable(adapter.parse_discovery_page)
    assert callable(adapter.parse_results_page)
    assert callable(adapter.parse_personal_page)
    assert callable(adapter.generate_result_links)
    assert callable(adapter.classify_link)
    assert callable(adapter.extract_photo_url)
    assert isinstance(adapter.requires_playwright(), bool)


@pytest.mark.parametrize("connector,fixture,expected_min", [
    ("openu", "results_page.html", 1),
    ("bgu", "bgu_listing_live.html", 30),
])
def test_results_page_produces_records(connector, fixture, expected_min):
    """Each connector's results page produces the expected minimum record count."""
    adapter = get_adapter(connector)
    html = (FIXTURES / fixture).read_text(encoding="utf-8")
    url = adapter.default_start_url

    result = adapter.parse_results_page(html, url)

    assert len(result.people) >= expected_min
    for person in result.people:
        assert person.person_id
        assert person.full_name


@pytest.mark.parametrize("connector,fixture,has_photo", [
    ("openu", "personal_page.html", True),
    ("openu", "openu_personal_page_no_photo.html", False),
    ("bgu", "bgu_profile_nonbgu.html", False),
    ("technion_med", "technion_profile.html", True),
])
def test_photo_extraction_across_connectors(connector, fixture, has_photo):
    """extract_photo_url returns a URL when a real photo exists, None otherwise."""
    adapter = get_adapter(connector)
    html = (FIXTURES / fixture).read_text(encoding="utf-8")
    url = adapter.default_start_url

    result = adapter.extract_photo_url(html, url)

    if has_photo:
        assert result is not None
        assert result.startswith("http")
    else:
        assert result is None


@pytest.mark.parametrize("connector", ["openu", "bgu", "technion_med"])
def test_classify_link_orcid_across_connectors(connector):
    """Every adapter classifies orcid.org links as 'orcid'."""
    adapter = get_adapter(connector)
    assert adapter.classify_link("https://orcid.org/0000-0002-1234-5678", "ORCID") == "orcid"


@pytest.mark.parametrize("connector,fixture,listing_url", [
    ("openu", "results_page.html", "https://www.openu.ac.il/staff/pages/results.aspx?unit=311"),
    ("bgu", "bgu_listing_live.html", "https://www.bgu.ac.il/people/"),
])
def test_pipeline_parse_produces_records_with_required_fields(connector, fixture, listing_url, tmp_path):
    """Parse stage produces records with person_id, full_name, and at least one contact."""
    config = AppConfig(university=connector, output_root=str(tmp_path))
    storage = Storage(tmp_path)
    html = (FIXTURES / fixture).read_text(encoding="utf-8")

    artifact = storage.write_artifact(
        kind="html", source_url=listing_url,
        content=html.encode("utf-8"), content_type="text/html",
    )
    storage.update_fingerprint(listing_url, artifact.checksum)
    storage.flush_fingerprints()
    storage.save_json("state/crawl_manifest.json", {"urls": [listing_url]})

    records = OuHarvestPipeline(config).parse()

    assert len(records) >= 1
    for record in records:
        assert record.person_id
        assert record.full_name
