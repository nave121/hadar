import json
from pathlib import Path

import pytest

from ou_harvest.adapters import available_adapters, get_adapter
from ou_harvest.adapters.bgu import BGU_SEARCH_URL
from ou_harvest.adapters.base import UniversityAdapter
from ou_harvest.models import DiscoveryFilterGroup, DiscoveryLink, DiscoveryOption, DiscoverySnapshot
from tests.fixture_helpers import require_fixture

FIXTURES = Path(__file__).parent / "fixtures"


def _parse_bgu_search_fixture():
    adapter = get_adapter("bgu")
    payload = json.loads(require_fixture("bgu_search_page_1.json").read_text(encoding="utf-8"))
    page_size = len(payload["staffMembers"])
    result = adapter.parse_results_artifact(
        require_fixture("bgu_search_page_1.json").read_bytes(),
        "application/json",
        DiscoveryLink(
            url=BGU_SEARCH_URL,
            method="POST",
            artifact_kind="json",
            headers={"Content-Type": "application/json"},
            json_payload={
                "pageNodeId": "107837",
                "cultureCode": "he-IL",
                "currentPage": 1,
                "pageSize": page_size,
                "term": "",
                "units": [],
                "selectedTypes": [],
                "selectedCampuses": [],
                "currentStaff": False,
                "lookingForStudents": False,
            },
        ),
    )
    return result


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


def test_bgu_adapter_does_not_require_playwright():
    adapter = get_adapter("bgu")
    assert adapter.name == "bgu"
    assert not adapter.requires_playwright()
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
    adapter = get_adapter("bgu")
    html = """
    <html>
      <body>
        <a class="staff-member-item" href="/people/test-person/">
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
    result = adapter.parse_results_page(html, "https://www.bgu.ac.il/people/")

    assert len(result.people) > 0
    person = result.people[0]
    assert person.full_name
    assert person.person_id
    assert person.org_affiliations
    assert person.org_affiliations[0].organization == "Ben-Gurion University of the Negev"


def test_bgu_parse_api_results_extracts_expected_records():
    result = _parse_bgu_search_fixture()

    assert len(result.people) == 30
    person = next(record for record in result.people if record.full_name == "ד\"ר אנג'ליקה אבדלימוב")
    assert person.contacts[0].value == "nonbgu@bgu.ac.il"
    assert person.links[0].kind == "personal_page"
    assert person.links[0].url == "https://www.bgu.ac.il/people/nonbgu/"


def test_bgu_parse_api_results_keeps_orcid_links():
    result = _parse_bgu_search_fixture()

    person = next(record for record in result.people if record.full_name == "מוחמד אבו אחמד")
    assert any(
        link.kind == "orcid" and link.url == "https://orcid.org/0009-0001-6613-2044"
        for link in person.links
    )


def test_bgu_listing_extracts_photo_url_for_staff_with_real_photo():
    adapter = get_adapter("bgu")
    html = """
    <html>
      <body>
        <a class="staff-member-item" href="/people/test-person/">
          <div class="member-image">
            <img src="https://apps4cloud.bgu.ac.il/media/photos/test-person.jpg?width=300&format=webp" />
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

    result = adapter.parse_results_page(html, "https://www.bgu.ac.il/people/")

    assert result.people[0].photo_url == (
        "https://apps4cloud.bgu.ac.il/media/photos/test-person.jpg?width=300&format=webp"
    )


def test_bgu_api_results_filter_placeholder_photo():
    result = _parse_bgu_search_fixture()

    person = next(record for record in result.people if record.full_name == "ד\"ר סלים אבו ג'אבר")
    assert person.photo_url is None


def test_bgu_parse_api_results_preserves_rank_staff_type_and_department():
    result = _parse_bgu_search_fixture()

    person = next(record for record in result.people if record.full_name == 'ד"ר סלימאן אבו בדר')
    assert person.current_rank == "מרצה בכיר"
    assert person.org_affiliations[0].staff_type == "חבר/ת סגל אקדמי בכיר"
    assert person.org_affiliations[0].department == "הפקולטה למדעי הרוח והחברה, כלכלה"
    assert person.org_affiliations[0].faculty_or_unit == "הפקולטה למדעי הרוח והחברה, כלכלה"


def test_bgu_parse_api_results_skips_placeholder_email_contacts():
    result = _parse_bgu_search_fixture()

    person = next(record for record in result.people if record.full_name == "גירום אבאי")
    assert person.contacts == []


def test_bgu_generate_result_links_creates_first_api_request():
    adapter = get_adapter("bgu")
    snapshot = DiscoverySnapshot(
        connector_name="bgu",
        start_url="https://www.bgu.ac.il/people/",
        connector_state={"page_node_id": "107837", "culture_code": "he-IL", "page_size": 30},
    )
    links = adapter.generate_result_links(snapshot, {})
    assert len(links) == 1
    assert links[0].url == BGU_SEARCH_URL
    assert links[0].method == "POST"
    assert links[0].artifact_kind == "json"
    assert links[0].json_payload["currentPage"] == 1


def test_bgu_parse_discovery_page_builds_filter_groups_from_page_data(monkeypatch):
    adapter = get_adapter("bgu")

    monkeypatch.setattr(
        adapter,
        "_load_page_data",
        lambda *args, **kwargs: {
            "departments": [{"key": 117531, "value": "המכונים לחקר המדבר"}],
            "typesFiltersItems": [
                {"key": 1, "value": "סגל אקדמי בכיר"},
                {"key": 18, "value": "סגל קליני"},
            ],
            "campuses": [{"key": 5, "value": "קמפוס מרקוס"}],
            "pageSize": 30,
        },
    )

    snapshot = adapter.parse_discovery_page(
        '<div id="staffMembersModernLobbyApp" page-node-id="107837" culture-code="he-IL"></div>',
        "https://www.bgu.ac.il/people/",
    )
    groups = {group.key: group for group in snapshot.available_filters}

    assert snapshot.connector_name == "bgu"
    assert set(groups) == {"unit", "staff_type", "campus"}
    assert groups["unit"].options[0].code == "117531"
    assert groups["staff_type"].options[1].code == "18"
    assert groups["campus"].options[0].code == "5"
    assert snapshot.connector_state == {
        "page_node_id": "107837",
        "culture_code": "he-IL",
        "page_size": 30,
    }


def test_bgu_generate_result_links_uses_generic_filter_map():
    adapter = get_adapter("bgu")
    snapshot = DiscoverySnapshot(
        connector_name="bgu",
        start_url="https://www.bgu.ac.il/people/",
        connector_state={"page_node_id": "107837", "culture_code": "he-IL", "page_size": 30},
    )

    links = adapter.generate_result_links(
        snapshot,
        {"unit": ["117531"], "staff_type": ["18"], "campus": ["5"]},
    )

    assert links[0].url == BGU_SEARCH_URL
    assert links[0].json_payload["units"] == [117531]
    assert links[0].json_payload["selectedTypes"] == [18]
    assert links[0].json_payload["selectedCampuses"] == [5]


def test_technion_parse_discovery_from_sitemap():
    fixture = require_fixture("technion_sitemap.xml")
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
    fixture = require_fixture("technion_profile.html")
    adapter = get_adapter("technion_med")
    html = fixture.read_text(encoding="utf-8")
    result = adapter.parse_results_page(html, "https://md.technion.ac.il/aaron-ciechanover/")

    assert len(result.people) == 1
    person = result.people[0]
    assert "Aaron" in person.full_name or "Ciechanover" in person.full_name
    assert person.person_id
    assert any(c.kind == "email" for c in person.contacts)
    assert person.photo_url == "https://md.technion.ac.il/wp-content/uploads/2020/08/ciehanover-250x375.jpg"


def test_openu_extract_photo_url_returns_none_for_chrome_only_page():
    """A page with only site-chrome images (logo, icons) returns None."""
    adapter = get_adapter("openu")
    html = """<html><body>
        <img src="/_layouts/15/OpenU_WWW/Theming/Global/images/LOGO_OU_BLACK.svg">
        <img src="/_layouts/15/images/icpdf.png" width="16" height="16">
        <img src="/Personal_sites/gifs/logo.jpg" width="59" height="52">
    </body></html>"""

    assert adapter.extract_photo_url(html, "https://www.openu.ac.il/staff/pages/results.aspx") is None


def test_openu_extract_photo_url_returns_url_from_figure():
    adapter = get_adapter("openu")
    html = (FIXTURES / "personal_page.html").read_text(encoding="utf-8")

    url = adapter.extract_photo_url(html, "https://www.openu.ac.il/en/personalsites/OrenBarkan.aspx")

    assert url == "https://www.openu.ac.il/Lists/MediaServer_Images/PersonalSites/OrenBarkan.jpg"


def test_openu_extract_photo_url_filters_avatar_placeholder():
    adapter = get_adapter("openu")
    html = (FIXTURES / "openu_personal_page_no_photo.html").read_text(encoding="utf-8")

    url = adapter.extract_photo_url(html, "https://www.openu.ac.il/en/personalsites/test.aspx")

    assert url is None


def test_openu_extract_photo_url_from_freeform_page():
    adapter = get_adapter("openu")
    html = (FIXTURES / "openu_freeform_page.html").read_text(encoding="utf-8")

    url = adapter.extract_photo_url(html, "https://www.openu.ac.il/personal_sites/rica-gonen/index.html")

    assert url == "https://www.openu.ac.il/personal_sites/rica-gonen/ricagonen1.gif"


def test_openu_adapter_does_not_require_playwright():
    adapter = get_adapter("openu")
    assert adapter.requires_playwright() is False


def test_openu_classify_link_orcid():
    adapter = get_adapter("openu")
    assert adapter.classify_link("https://orcid.org/0000-0002-1234-5678", "ORCID") == "orcid"


def test_openu_classify_link_scopus():
    adapter = get_adapter("openu")
    assert adapter.classify_link("https://www.scopus.com/authid/detail.uri?authorId=123", "Scopus") == "scopus"


def test_openu_classify_link_pubmed():
    adapter = get_adapter("openu")
    assert adapter.classify_link("https://pubmed.ncbi.nlm.nih.gov/?term=test", "PubMed") == "pubmed"


def test_openu_generate_result_links_creates_cartesian_product():
    adapter = get_adapter("openu")
    snapshot = DiscoverySnapshot(
        start_url="https://www.openu.ac.il/staff/pages/default.aspx",
        available_filters=[
            DiscoveryFilterGroup(key="unit", label="Units", options=[
                DiscoveryOption(code="307", label="CS"),
                DiscoveryOption(code="311", label="Edu"),
            ]),
            DiscoveryFilterGroup(key="staff_type", label="Staff", options=[
                DiscoveryOption(code="10", label="Senior"),
                DiscoveryOption(code="20", label="Teaching"),
            ]),
        ],
    )

    links = adapter.generate_result_links(snapshot, {})

    assert len(links) == 4
    urls = {link.url for link in links}
    assert "https://www.openu.ac.il/staff/pages/results.aspx?unit=307&staff=10" in urls
    assert "https://www.openu.ac.il/staff/pages/results.aspx?unit=311&staff=20" in urls


def test_openu_generate_result_links_respects_selected_filters():
    adapter = get_adapter("openu")
    snapshot = DiscoverySnapshot(
        start_url="https://www.openu.ac.il/staff/pages/default.aspx",
        available_filters=[
            DiscoveryFilterGroup(key="unit", label="Units", options=[
                DiscoveryOption(code="307", label="CS"),
                DiscoveryOption(code="311", label="Edu"),
            ]),
            DiscoveryFilterGroup(key="staff_type", label="Staff", options=[
                DiscoveryOption(code="10", label="Senior"),
                DiscoveryOption(code="20", label="Teaching"),
            ]),
        ],
    )

    links = adapter.generate_result_links(snapshot, {"unit": ["307"]})

    assert len(links) == 2
    assert all("unit=307" in link.url for link in links)


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
    fixture = require_fixture(fixture_name)
    adapter = get_adapter("bgu")
    html = fixture.read_text(encoding="utf-8")
    result = adapter.parse_personal_page(html, page_url)

    assert result.name == expected_name
    assert [(contact.kind, contact.value) for contact in result.contacts] == [("email", expected_email)]
    assert not any("apps4cloud" in link.url for link in result.links)
    assert result.photo_url is None


def test_bgu_parse_profile_page_keeps_orcid_without_turning_it_into_phone():
    fixture = require_fixture("bgu_profile_nonbgu.html")
    adapter = get_adapter("bgu")
    html = fixture.read_text(encoding="utf-8")
    result = adapter.parse_personal_page(html, "https://www.bgu.ac.il/people/nonbgu/")

    assert [(link.kind, link.url) for link in result.links] == [
        ("orcid", "https://orcid.org/0000-0003-4075-8919")
    ]
    assert not any(contact.kind in {"phone", "fax"} for contact in result.contacts)
    assert not any("0000-0003-4075-8919" in contact.value for contact in result.contacts)


def test_bgu_parse_profile_page_emits_profile_text_evidence_without_update_link_chrome():
    fixture = require_fixture("bgu_profile_nonbgu.html")
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
