from pathlib import Path

from ou_harvest.parsers import parse_discovery_page, parse_personal_page, parse_results_page


FIXTURES = Path(__file__).parent / "fixtures"


def test_parse_discovery_page_collects_result_and_department_staff_links():
    html = (FIXTURES / "discovery_default.html").read_text(encoding="utf-8")
    snapshot = parse_discovery_page(html, "https://www.openu.ac.il/staff/pages/default.aspx")

    assert len(snapshot.result_links) == 2
    assert snapshot.result_links[0].url.startswith("https://www.openu.ac.il/staff/pages/results.aspx")
    assert len(snapshot.department_staff_links) == 2
    assert snapshot.department_staff_links[0].kind == "department_staff"


def test_parse_discovery_page_extracts_unit_and_staff_type_options():
    html = (FIXTURES / "discovery_default.html").read_text(encoding="utf-8")
    snapshot = parse_discovery_page(html, "https://www.openu.ac.il/staff/pages/default.aspx")

    assert snapshot.connector_name == "openu"
    assert len(snapshot.available_filters) == 2
    groups = {group.key: group for group in snapshot.available_filters}

    assert len(groups["unit"].options) == 3
    codes = [u.code for u in groups["unit"].options]
    assert "307" in codes
    assert "311" in codes
    assert "306" in codes
    assert groups["unit"].options[0].label  # has Hebrew label

    assert len(groups["staff_type"].options) == 2
    assert groups["staff_type"].options[0].code == "10"
    assert groups["staff_type"].options[1].code == "20"


def test_parse_results_page_extracts_person_record_and_pagination():
    html = (FIXTURES / "results_page.html").read_text(encoding="utf-8")
    page = parse_results_page(html, "https://www.openu.ac.il/staff/pages/results.aspx?unit=311")

    assert len(page.people) == 1
    person = page.people[0]
    assert person.full_name.startswith("פרופ' אבנר כספי")
    assert person.primary_email == "avnerca@openu.ac.il"
    assert person.current_role == "ראש התכנית לתואר שלישי"
    assert person.current_rank == "פרופ'"
    assert "Psychology, Psychology and education, Education" in person.research_interests
    assert any(link.kind == "personal_page" for link in person.links)
    assert any(link.kind == "cv" for link in person.links)
    assert any(url.endswith("page=2") for url in page.pagination_urls)


def test_parse_personal_page_extracts_rank_links_and_research():
    html = (FIXTURES / "personal_page.html").read_text(encoding="utf-8")
    page = parse_personal_page(html, "https://www.openu.ac.il/en/personalsites/orenbarkan.aspx")

    assert page.name == "Dr. Oren Barkan"
    assert page.rank == "Dr."
    assert "deep learning" in page.research_interests
    assert any(contact.kind == "email" for contact in page.contacts)
    assert any(link.kind == "cv" for link in page.links)


def test_parse_results_table_row_extracts_role_rank_and_ignores_course_links_as_pagination():
    html = (FIXTURES / "results_table_page.html").read_text(encoding="utf-8")
    page = parse_results_page(html, "https://www.openu.ac.il/staff/pages/results.aspx?unit=307&staff=10")

    assert len(page.people) == 1
    person = page.people[0]
    assert person.full_name == 'ד"ר בועז סלומקה'
    assert person.current_rank == "מרצה בכיר"
    assert person.current_role == "ראש תחום"
    assert person.primary_email == "slomka@openu.ac.il"
    assert "Mathematics" in person.research_interests
    assert "Graph theory" in person.research_interests
    assert any(link.kind == "personal_page" for link in person.links)
    assert page.pagination_urls == [
        "https://www.openu.ac.il/staff/pages/results.aspx?first=&last=&unit=307&subunit=&staff=10&job=&page=1&sort=&asc="
    ]


def test_parse_results_page_normalizes_legacy_openu_dev_personal_page_links():
    html = """
    <table>
      <tr class="zebra-generic">
        <td>ד"ר אורית נאור אלאיזה</td>
        <td></td>
        <td><a href="/dept">מחלקה</a></td>
        <td>יחידה</td>
        <td>סגל אקדמי</td>
        <td>מרצה</td>
        <td>08-1234567</td>
        <td><a href="http://www.dev.openu.ac.il/Personal_sites/orit-naor-elaiza.html">בית</a></td>
      </tr>
    </table>
    """

    page = parse_results_page(html, "https://www.openu.ac.il/staff/pages/results.aspx?unit=029&staff=30")

    assert len(page.people) == 1
    personal_links = [link for link in page.people[0].links if link.kind == "personal_page"]
    assert len(personal_links) == 1
    assert personal_links[0].url == "https://www.openu.ac.il/personal_sites/orit-naor-elaiza.html"
