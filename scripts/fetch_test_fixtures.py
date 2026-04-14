from __future__ import annotations

import argparse
import json
from pathlib import Path
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parent.parent
FIXTURES_DIR = ROOT / "tests" / "fixtures"

BGU_PAGE_DATA_URL = "https://www.bgu.ac.il/umbraco/api/staffMembersLobbyApi/GetPageData"
BGU_SEARCH_URL = "https://www.bgu.ac.il/umbraco/api/staffMembersLobbyApi/searchStaffMembers"
BGU_PAGE_NODE_ID = "107837"
BGU_CULTURE_CODE = "he-IL"

STATIC_FIXTURES: dict[str, str] = {
    "bgu_profile_nonbgu.html": "https://www.bgu.ac.il/people/nonbgu/",
    "bgu_profile_abu.html": "https://www.bgu.ac.il/people/1000454264/",
    "bgu_profile_sapirabu.html": "https://www.bgu.ac.il/people/sapirabu/",
    "technion_profile.html": "https://md.technion.ac.il/aaron-ciechanover/",
    "technion_sitemap.xml": "https://md.technion.ac.il/page-sitemap.xml",
}


def fetch(
    url: str,
    *,
    accept: str,
    json_payload: dict | None = None,
) -> bytes:
    data = None
    headers = {
        "User-Agent": "ou-harvest-test-fixtures/1.0 (+https://github.com/)",
        "Accept": accept,
    }
    if json_payload is not None:
        data = json.dumps(json_payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = Request(url, data=data, headers=headers, method="POST" if data is not None else "GET")
    with urlopen(request, timeout=30) as response:
        return response.read()


def write_fixture(name: str, content: bytes, *, force: bool) -> None:
    target = FIXTURES_DIR / name
    if target.exists() and not force:
        print(f"skip {name} (already exists)")
        return
    print(f"write {name}")
    target.write_bytes(content)


def normalize_json_bytes(content: bytes) -> tuple[bytes, dict]:
    payload = json.loads(content.decode("utf-8"))
    if isinstance(payload, str):
        payload = json.loads(payload)
    normalized = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    return normalized, payload


def build_bgu_search_payload(*, page_size: int) -> dict:
    return {
        "pageNodeId": BGU_PAGE_NODE_ID,
        "cultureCode": BGU_CULTURE_CODE,
        "currentPage": 1,
        "pageSize": page_size,
        "term": "",
        "units": [],
        "selectedTypes": [],
        "selectedCampuses": [],
        "currentStaff": False,
        "lookingForStudents": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch real-site test fixtures into tests/fixtures/.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite fixtures that already exist locally.",
    )
    args = parser.parse_args()

    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)

    page_data_bytes, page_data = normalize_json_bytes(
        fetch(
        BGU_PAGE_DATA_URL,
        accept="application/json",
        json_payload={"pageNodeId": BGU_PAGE_NODE_ID, "cultureCode": BGU_CULTURE_CODE},
    )
    )
    write_fixture("bgu_page_data.json", page_data_bytes, force=args.force)
    page_size = int(page_data.get("pageSize") or 30)
    search_bytes, _ = normalize_json_bytes(
        fetch(
            BGU_SEARCH_URL,
            accept="application/json",
            json_payload=build_bgu_search_payload(page_size=page_size),
        )
    )
    write_fixture("bgu_search_page_1.json", search_bytes, force=args.force)

    for name, url in STATIC_FIXTURES.items():
        content = fetch(url, accept="text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
        write_fixture(name, content, force=args.force)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
