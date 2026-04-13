from pathlib import Path

import pytest

from ou_harvest.adapters import available_adapters, get_adapter
from ou_harvest.adapters.base import UniversityAdapter

FIXTURES = Path(__file__).parent / "fixtures"


def test_available_adapters_returns_all_three():
    adapters = available_adapters()
    assert "openu" in adapters
    assert "bgu" in adapters
    assert "technion_med" in adapters
    assert len(adapters) == 3


def test_get_adapter_returns_correct_type():
    adapter = get_adapter("openu")
    assert isinstance(adapter, UniversityAdapter)
    assert adapter.name == "openu"
    assert adapter.display_name == "Open University of Israel"
    assert "openu.ac.il" in adapter.default_allowed_domains
    assert not adapter.requires_playwright()


def test_bgu_adapter_requires_playwright():
    adapter = get_adapter("bgu")
    assert adapter.name == "bgu"
    assert adapter.requires_playwright()
    assert "bgu.ac.il" in adapter.default_allowed_domains


def test_technion_med_adapter_requires_playwright():
    adapter = get_adapter("technion_med")
    assert adapter.name == "technion_med"
    assert adapter.requires_playwright()
    assert "md.technion.ac.il" in adapter.default_allowed_domains


def test_each_adapter_has_default_start_url():
    for name in available_adapters():
        adapter = get_adapter(name)
        assert adapter.default_start_url, f"{name} has no default_start_url"
        assert adapter.default_start_url.startswith("https://"), f"{name} start_url not HTTPS"


def test_bgu_parse_results_page_extracts_staff_cards():
    fixture = FIXTURES / "bgu_results.html"
    if not fixture.exists():
        return  # skip if fixture not available
    adapter = get_adapter("bgu")
    html = fixture.read_text(encoding="utf-8")
    result = adapter.parse_results_page(html, "https://www.bgu.ac.il/people/")

    assert len(result.people) > 0
    person = result.people[0]
    assert person.full_name
    assert person.person_id
    assert person.org_affiliations
    assert person.org_affiliations[0].organization == "Ben-Gurion University of the Negev"


def test_bgu_parse_live_results_page_extracts_expected_cards():
    fixture = FIXTURES / "bgu_listing_live.html"
    adapter = get_adapter("bgu")
    html = fixture.read_text(encoding="utf-8")
    result = adapter.parse_results_page(html, "https://www.bgu.ac.il/people/")

    assert len(result.people) == 30
    assert result.people[0].full_name == "ד\"ר אנג'ליקה אבדלימוב"
    assert result.people[0].contacts[0].value == "nonbgu@bgu.ac.il"
    assert result.people[0].links[0].kind == "personal_page"
    assert result.people[0].links[0].url == "https://www.bgu.ac.il/people/nonbgu/"


def test_bgu_parse_live_results_page_keeps_orcid_links_from_listing_cards():
    fixture = FIXTURES / "bgu_listing_live.html"
    adapter = get_adapter("bgu")
    html = fixture.read_text(encoding="utf-8")
    result = adapter.parse_results_page(html, "https://www.bgu.ac.il/people/")

    person = next(record for record in result.people if record.full_name == "חליל אבו יונס")
    assert any(
        link.kind == "orcid" and link.url == "https://orcid.org/0009-0006-4362-267X"
        for link in person.links
    )


def test_bgu_parse_live_results_page_preserves_rank_staff_type_and_department():
    fixture = FIXTURES / "bgu_listing_live.html"
    adapter = get_adapter("bgu")
    html = fixture.read_text(encoding="utf-8")
    result = adapter.parse_results_page(html, "https://www.bgu.ac.il/people/")

    person = next(record for record in result.people if record.full_name == 'ד"ר סלימאן אבו בדר')
    assert person.current_rank == "מרצה בכיר"
    assert person.org_affiliations[0].staff_type == "חבר/ת סגל אקדמי בכיר"
    assert person.org_affiliations[0].department == "הפקולטה למדעי הרוח והחברה, כלכלה"
    assert person.org_affiliations[0].faculty_or_unit == "הפקולטה למדעי הרוח והחברה, כלכלה"


def test_bgu_generate_result_links_creates_pagination():
    from ou_harvest.models import DiscoverySnapshot
    adapter = get_adapter("bgu")
    snapshot = DiscoverySnapshot(connector_name="bgu", start_url="https://www.bgu.ac.il/people/")
    links = adapter.generate_result_links(snapshot, {})
    assert len(links) > 100  # should generate many pages
    assert links[0].url == "https://www.bgu.ac.il/people/"
    assert "?page=2" in links[1].url


def test_bgu_parse_discovery_page_builds_filter_groups_from_page_data(monkeypatch):
    adapter = get_adapter("bgu")

    monkeypatch.setattr(
        adapter,
        "_load_page_data",
        lambda: {
            "departments": [{"key": 117531, "value": "המכונים לחקר המדבר"}],
            "typesFiltersItems": [
                {"key": 1, "value": "סגל אקדמי בכיר"},
                {"key": 18, "value": "סגל קליני"},
            ],
            "campuses": [{"key": 5, "value": "קמפוס מרקוס"}],
        },
    )

    snapshot = adapter.parse_discovery_page("<html></html>", "https://www.bgu.ac.il/people/")
    groups = {group.key: group for group in snapshot.available_filters}

    assert snapshot.connector_name == "bgu"
    assert set(groups) == {"unit", "staff_type", "campus"}
    assert groups["unit"].options[0].code == "117531"
    assert groups["staff_type"].options[1].code == "18"
    assert groups["campus"].options[0].code == "5"


def test_bgu_generate_result_links_uses_generic_filter_map():
    from ou_harvest.models import DiscoverySnapshot

    adapter = get_adapter("bgu")
    snapshot = DiscoverySnapshot(connector_name="bgu", start_url="https://www.bgu.ac.il/people/")

    links = adapter.generate_result_links(
        snapshot,
        {"unit": ["117531"], "staff_type": ["18"], "campus": ["5"]},
    )

    assert links[0].url == "https://www.bgu.ac.il/people/?unit=117531&types=18&campuses=5"
    assert links[1].url == "https://www.bgu.ac.il/people/?unit=117531&types=18&campuses=5&page=2"


def test_technion_parse_discovery_from_sitemap():
    fixture = FIXTURES / "technion_sitemap.xml"
    if not fixture.exists():
        return  # skip if fixture not available
    adapter = get_adapter("technion_med")
    xml = fixture.read_text(encoding="utf-8")
    snapshot = adapter.parse_discovery_page(xml, "https://md.technion.ac.il/page-sitemap.xml")

    assert len(snapshot.result_links) > 0
    assert snapshot.available_filters == []
    # Should filter to profile URLs only
    for link in snapshot.result_links:
        assert "md.technion.ac.il" in link.url
        assert link.label  # should have a name-like label


def test_technion_parse_profile_page():
    fixture = FIXTURES / "technion_profile.html"
    if not fixture.exists():
        return  # skip if fixture not available
    adapter = get_adapter("technion_med")
    html = fixture.read_text(encoding="utf-8")
    result = adapter.parse_results_page(html, "https://md.technion.ac.il/aaron-ciechanover/")

    assert len(result.people) == 1
    person = result.people[0]
    assert "Aaron" in person.full_name or "Ciechanover" in person.full_name
    assert person.person_id
    assert any(c.kind == "email" for c in person.contacts)


@pytest.mark.parametrize(
    ("fixture_name", "page_url", "expected_name", "expected_email"),
    [
        (
            "bgu_profile_nonbgu.html",
            "https://www.bgu.ac.il/people/nonbgu/",
            "יבגניה קורוטינסקי",
            "nonbgu@bgu.ac.il",
        ),
        (
            "bgu_profile_abu.html",
            "https://www.bgu.ac.il/people/1000454264/",
            "מיתר אבו",
            "abum@post.bgu.ac.il",
        ),
        (
            "bgu_profile_sapirabu.html",
            "https://www.bgu.ac.il/people/sapirabu/",
            "ספיר אבו",
            "sapirabu@bgu.ac.il",
        ),
    ],
)
def test_bgu_parse_profile_page_scopes_contacts_and_filters_site_chrome(
    fixture_name: str,
    page_url: str,
    expected_name: str,
    expected_email: str,
):
    fixture = FIXTURES / fixture_name
    adapter = get_adapter("bgu")
    html = fixture.read_text(encoding="utf-8")
    result = adapter.parse_personal_page(html, page_url)

    assert result.name == expected_name
    assert [(contact.kind, contact.value) for contact in result.contacts] == [("email", expected_email)]
    assert not any("apps4cloud" in link.url for link in result.links)


def test_bgu_parse_profile_page_keeps_orcid_without_turning_it_into_phone():
    fixture = FIXTURES / "bgu_profile_nonbgu.html"
    adapter = get_adapter("bgu")
    html = fixture.read_text(encoding="utf-8")
    result = adapter.parse_personal_page(html, "https://www.bgu.ac.il/people/nonbgu/")

    assert [(link.kind, link.url) for link in result.links] == [
        ("orcid", "https://orcid.org/0000-0003-4075-8919")
    ]
    assert not any(contact.kind in {"phone", "fax"} for contact in result.contacts)
    assert not any("0000-0003-4075-8919" in contact.value for contact in result.contacts)


def test_bgu_parse_profile_page_emits_profile_text_evidence_without_update_link_chrome():
    fixture = FIXTURES / "bgu_profile_nonbgu.html"
    adapter = get_adapter("bgu")
    html = fixture.read_text(encoding="utf-8")
    result = adapter.parse_personal_page(html, "https://www.bgu.ac.il/people/nonbgu/")

    assert len(result.source_evidence) == 1
    evidence = result.source_evidence[0]
    assert evidence.field_name == "profile_text"
    assert evidence.source_url == "https://www.bgu.ac.il/people/nonbgu/"
    assert "יבגניה קורוטינסקי" in evidence.excerpt
    assert "אקדמי לא פעיל" in evidence.excerpt
    assert "כניסת סגל לעדכון פרטים בעמוד" not in evidence.excerpt


def test_bgu_parse_cris_profile_page_extracts_rank_links_and_cris_text():
    adapter = get_adapter("bgu")
    html = """
    <html>
      <head>
        <title>Suleiman Abu-Bader - Ben-Gurion University Research Portal</title>
        <script type="application/ld+json">
          {
            "@context": "http://schema.org",
            "@type": "Person",
            "name": "Suleiman Abu-Bader",
            "jobTitle": "Senior Lecturer"
          }
        </script>
      </head>
      <body>
        <main id="main-content">
          <h1>Suleiman Abu-Bader</h1>
          <a href="https://orcid.org/0000-0003-2871-8789">ORCID Profile</a>
          <a href="https://www.scopus.com/authid/detail.uri?authorId=57195948470">View Scopus Profile</a>
          <div>Fingerprint</div>
          <div>Israel</div>
          <div>100%</div>
          <div>Structural Breaks</div>
          <div>69%</div>
          <div>Economic Growth</div>
          <div>63%</div>
          <div>Research output</div>
          <div>Polarization, foreign military intervention, and civil conflict Abu-Bader, S. &amp; Ianchovichina, E., 1 Nov 2019, In: Journal of Development Economics.</div>
        </main>
      </body>
    </html>
    """

    result = adapter.parse_personal_page(html, "https://cris.bgu.ac.il/en/persons/suleiman-abu-bader")

    assert result.name == "Suleiman Abu-Bader"
    assert result.rank == "Senior Lecturer"
    assert result.research_interests == ["Israel", "Structural Breaks", "Economic Growth"]
    assert [(link.kind, link.url) for link in result.links] == [
        ("orcid", "https://orcid.org/0000-0003-2871-8789"),
        ("scopus", "https://www.scopus.com/authid/detail.uri?authorId=57195948470"),
    ]
    assert len(result.source_evidence) == 1
    assert result.source_evidence[0].field_name == "cris_text"
    assert "Polarization, foreign military intervention, and civil conflict" in result.source_evidence[0].excerpt
