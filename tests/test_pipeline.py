from pathlib import Path

from ou_harvest.config import AppConfig
from ou_harvest.pipeline import OuHarvestPipeline
from ou_harvest.storage import Storage


FIXTURES = Path(__file__).parent / "fixtures"


def test_pipeline_parse_recognizes_bgu_listing_pages_without_results_aspx(tmp_path: Path):
    config = AppConfig(university="bgu", output_root=str(tmp_path))
    storage = Storage(tmp_path)
    listing_url = "https://www.bgu.ac.il/people/"
    listing_html = (FIXTURES / "bgu_listing_live.html").read_text(encoding="utf-8")

    artifact = storage.write_artifact(
        kind="html",
        source_url=listing_url,
        content=listing_html.encode("utf-8"),
        content_type="text/html",
    )
    storage.update_fingerprint(listing_url, artifact.checksum)
    storage.flush_fingerprints()
    storage.save_json("state/crawl_manifest.json", {"urls": [listing_url]})

    records = OuHarvestPipeline(config).parse()

    assert len(records) == 30
    suleiman = next(record for record in records if record.full_name == 'ד"ר סלימאן אבו בדר')
    assert suleiman.current_rank == "מרצה בכיר"
    assert suleiman.org_affiliations[0].staff_type == "חבר/ת סגל אקדמי בכיר"
    assert suleiman.org_affiliations[0].department == "הפקולטה למדעי הרוח והחברה, כלכלה"
    khalil = next(record for record in records if record.full_name == "חליל אבו יונס")
    assert any(
        link.kind == "orcid" and link.url == "https://orcid.org/0009-0006-4362-267X"
        for link in khalil.links
    )


def _seed_bgu_listing_with_photo(
    tmp_path: Path,
    *,
    include_photo_artifact: bool,
) -> tuple[AppConfig, str, str, object | None]:
    config = AppConfig(university="bgu", output_root=str(tmp_path))
    storage = Storage(tmp_path)
    listing_url = "https://www.bgu.ac.il/people/"
    photo_url = "https://apps4cloud.bgu.ac.il/media/photos/test-person.jpg?width=300&format=webp"
    listing_html = f"""
    <html>
      <body>
        <a class="staff-member-item" href="/people/test-person/">
          <div class="member-image">
            <img src="{photo_url}" />
          </div>
          <div class="member-content">
            <div class="top-section">
              <h2 class="member-name">ד"ר טסט</h2>
            </div>
            <div class="department">
              <span>מרצה בכיר</span>
              <div class="department-separator"></div>
              <span>חבר/ת סגל אקדמי בכיר</span>
              <div class="department-separator"></div>
              <span>הפקולטה למדעי הרוח והחברה, כלכלה</span>
            </div>
            <div class="bottom-section">
              <a href="mailto:test@bgu.ac.il">test@bgu.ac.il</a>
            </div>
          </div>
        </a>
      </body>
    </html>
    """

    listing_artifact = storage.write_artifact(
        kind="html",
        source_url=listing_url,
        content=listing_html.encode("utf-8"),
        content_type="text/html",
    )
    storage.update_fingerprint(listing_url, listing_artifact.checksum)

    photo_artifact = None
    if include_photo_artifact:
        photo_artifact = storage.write_artifact(
            kind="image",
            source_url=photo_url,
            content=b"photo-bytes",
            content_type="image/jpeg",
        )
        storage.update_fingerprint(photo_url, photo_artifact.checksum)

    storage.flush_fingerprints()
    storage.save_json("state/crawl_manifest.json", {"urls": [listing_url]})
    return config, photo_url, listing_url, photo_artifact


def test_pipeline_parse_preserves_photo_url_from_bgu_listing(tmp_path: Path):
    config, photo_url, _, _ = _seed_bgu_listing_with_photo(tmp_path, include_photo_artifact=False)

    records = OuHarvestPipeline(config).parse()

    assert len(records) == 1
    record = records[0]
    assert record.photo_url == photo_url
    assert record.photo_artifact_id is None


def test_pipeline_photo_artifact_linkage(tmp_path: Path):
    config, photo_url, _, photo_artifact = _seed_bgu_listing_with_photo(
        tmp_path,
        include_photo_artifact=True,
    )

    records = OuHarvestPipeline(config).parse()

    assert len(records) == 1
    record = records[0]
    assert record.photo_url == photo_url
    assert photo_artifact is not None
    assert record.photo_artifact_id == photo_artifact.artifact_id
    assert any(artifact.kind == "image" and artifact.artifact_id == photo_artifact.artifact_id for artifact in record.artifacts)


def test_pipeline_crawl_downloads_photos_when_demographics_enabled(tmp_path: Path, monkeypatch):
    """Crawl stage downloads photo URLs from result-page cards when demographics is enabled."""
    from unittest.mock import MagicMock
    from ou_harvest.http import FetchResult
    from ou_harvest.models import DiscoveryLink, DiscoverySnapshot

    photo_url = "https://apps4cloud.bgu.ac.il/media/photos/test.jpg"
    listing_url = "https://www.bgu.ac.il/people/"
    listing_html = f"""
    <html><body>
    <a class="staff-member-item" href="/people/test/">
      <div class="member-image"><img src="{photo_url}" /></div>
      <div class="member-content">
        <div class="top-section"><h2 class="member-name">ד"ר טסט</h2></div>
        <div class="department">
          <span>מרצה בכיר</span><div class="department-separator"></div>
          <span>חבר/ת סגל אקדמי בכיר</span><div class="department-separator"></div>
          <span>מדעים</span>
        </div>
        <div class="bottom-section"><a href="mailto:t@bgu.ac.il">t@bgu.ac.il</a></div>
      </div>
    </a>
    </body></html>
    """
    config = AppConfig(
        university="bgu",
        output_root=str(tmp_path),
        allowed_domains=["bgu.ac.il", "apps4cloud.bgu.ac.il"],
        personal_page_limit=0,
    )
    config.demographics.enabled = True

    pipeline = OuHarvestPipeline(config)
    storage = pipeline.storage

    # Seed discovery state so crawl skips discover()
    snapshot = DiscoverySnapshot(
        start_url=listing_url,
        connector_name="bgu",
        result_links=[DiscoveryLink(url=listing_url)],
    )
    storage.save_json("state/discovery.json", snapshot.model_dump(mode="json"))

    # Mock the fetchers so nothing hits the network
    listing_result = FetchResult(
        url=listing_url, status_code=200,
        content=listing_html.encode(), content_type="text/html",
    )
    photo_result = FetchResult(
        url=photo_url, status_code=200,
        content=b"\xff\xd8\xff\xe0fake-jpeg", content_type="image/jpeg",
    )
    personal_url = "https://www.bgu.ac.il/people/test/"
    personal_result = FetchResult(
        url=personal_url, status_code=200,
        content=b"<html><body>empty profile</body></html>", content_type="text/html",
    )

    def fake_fetch(url):
        if url == listing_url:
            return listing_result
        if url == photo_url:
            return photo_result
        if url == personal_url:
            return personal_result
        raise ValueError(f"Unexpected fetch: {url}")

    pipeline.fetcher = MagicMock()
    pipeline.fetcher.fetch = MagicMock(side_effect=fake_fetch)
    pipeline.binary_fetcher = MagicMock()
    pipeline.binary_fetcher.fetch = MagicMock(side_effect=fake_fetch)

    crawled = pipeline.crawl()

    assert photo_url in crawled
    # Verify image artifact was stored
    image_files = list(storage.raw_image.glob("*.jpg"))
    assert len(image_files) == 1


def test_pipeline_crawl_skips_photos_when_demographics_disabled(tmp_path: Path, monkeypatch):
    """Crawl stage does NOT download photos when demographics.enabled is False."""
    from unittest.mock import MagicMock
    from ou_harvest.http import FetchResult
    from ou_harvest.models import DiscoveryLink, DiscoverySnapshot

    photo_url = "https://apps4cloud.bgu.ac.il/media/photos/test.jpg"
    listing_url = "https://www.bgu.ac.il/people/"
    listing_html = f"""
    <html><body>
    <a class="staff-member-item" href="/people/test/">
      <div class="member-image"><img src="{photo_url}" /></div>
      <div class="member-content">
        <div class="top-section"><h2 class="member-name">ד"ר טסט</h2></div>
        <div class="department"><span>מרצה בכיר</span></div>
        <div class="bottom-section"><a href="mailto:t@bgu.ac.il">t@bgu.ac.il</a></div>
      </div>
    </a>
    </body></html>
    """
    config = AppConfig(
        university="bgu",
        output_root=str(tmp_path),
        allowed_domains=["bgu.ac.il", "apps4cloud.bgu.ac.il"],
        personal_page_limit=0,
    )
    assert config.demographics.enabled is False

    pipeline = OuHarvestPipeline(config)
    storage = pipeline.storage

    snapshot = DiscoverySnapshot(
        start_url=listing_url,
        connector_name="bgu",
        result_links=[DiscoveryLink(url=listing_url)],
    )
    storage.save_json("state/discovery.json", snapshot.model_dump(mode="json"))

    listing_result = FetchResult(
        url=listing_url, status_code=200,
        content=listing_html.encode(), content_type="text/html",
    )
    pipeline.fetcher = MagicMock()
    pipeline.fetcher.fetch = MagicMock(return_value=listing_result)
    pipeline.binary_fetcher = MagicMock()

    crawled = pipeline.crawl()

    assert photo_url not in crawled
    pipeline.binary_fetcher.fetch.assert_not_called()
    assert list(storage.raw_image.glob("*.jpg")) == []


def test_pipeline_parse_resolves_bgu_cris_page_after_profile_adds_matching_link(tmp_path: Path):
    config = AppConfig(university="bgu", output_root=str(tmp_path))
    storage = Storage(tmp_path)

    listing_url = "https://www.bgu.ac.il/people/"
    profile_url = "https://www.bgu.ac.il/people/test-person/"
    cris_url = "https://cris.bgu.ac.il/en/persons/test-person"

    listing_html = """
    <html>
      <body>
        <a class="staff-member-item" href="/people/test-person/">
          <h2 class="member-name">ד"ר טסט ביו</h2>
          <div class="department">
            <span>מרצה בכיר</span>
            <div class="department-separator"></div>
            <span>חבר/ת סגל אקדמי בכיר</span>
            <div class="department-separator"></div>
            <span>הפקולטה למדעי הרוח והחברה, כלכלה</span>
          </div>
          <div class="orc-link-container">
            <span>אורקיד</span>
            <a href="https://orcid.org/0000-0000-0000-0001" class="orc-link">https://orcid.org/0000-0000-0000-0001</a>
          </div>
          <div class="bottom-section">
            <a href="mailto:test@bgu.ac.il">test@bgu.ac.il</a>
          </div>
        </a>
      </body>
    </html>
    """
    profile_html = """
    <html>
      <body>
        <section class="profile-data-container">
          <header class="top-section">
            <h1>ד"ר טסט ביו</h1>
            <section class="member-contacts">
              <a href="mailto:test@bgu.ac.il">test@bgu.ac.il</a>
            </section>
            <nav class="member-big-links">
              <a href="https://orcid.org/0000-0000-0000-0001">ORCID</a>
              <a href="https://cris.bgu.ac.il/en/persons/test-person">קישור לפרופיל מחקר</a>
            </nav>
          </header>
        </section>
      </body>
    </html>
    """
    cris_html = """
    <html>
      <head>
        <title>Test Person - Ben-Gurion University Research Portal</title>
        <script type="application/ld+json">
          {
            "@context": "http://schema.org",
            "@type": "Person",
            "name": "Test Person",
            "jobTitle": "Senior Lecturer"
          }
        </script>
      </head>
      <body>
        <main id="main-content">
          <h1>Test Person</h1>
          <a href="https://orcid.org/0000-0000-0000-0001">ORCID Profile</a>
          <div>Fingerprint</div>
          <div>Economic Growth</div>
          <div>63%</div>
          <div>Research output</div>
          <div>Example publication in economics.</div>
        </main>
      </body>
    </html>
    """

    for url, html in (
        (listing_url, listing_html),
        (profile_url, profile_html),
        (cris_url, cris_html),
    ):
        artifact = storage.write_artifact(
            kind="html",
            source_url=url,
            content=html.encode("utf-8"),
            content_type="text/html",
        )
        storage.update_fingerprint(url, artifact.checksum)
    storage.flush_fingerprints()
    storage.save_json("state/crawl_manifest.json", {"urls": [listing_url, profile_url, cris_url]})

    records = OuHarvestPipeline(config).parse()

    assert len(records) == 1
    record = records[0]
    assert any(link.kind == "cris" and link.url == cris_url for link in record.links)
    assert sum(link.kind == "orcid" and link.url == "https://orcid.org/0000-0000-0000-0001" for link in record.links) == 1
    assert "Economic Growth" in record.research_interests
    assert any(evidence.field_name == "cris_text" for evidence in record.source_evidence)
