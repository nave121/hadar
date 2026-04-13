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
