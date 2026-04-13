# University Support Summary

## Summary

`ou-harvest` started as an Open University harvester and now supports multiple university connectors through the same shared pipeline, record model, CLI, and TUI flow.

The current supported connectors are:

- `openu` — Open University of Israel
- `bgu` — Ben-Gurion University of the Negev
- `technion_med` — Technion Faculty of Medicine

Both the CLI and the TUI execute stages through the same `PipelineRunner`, so discovery, crawl, parse, enrich, review, and export logic are shared.

## Open University status

Open University remains the most mature connector.

Implemented behavior includes:

- discovery from the OU staff landing page
- result-page parsing for the legacy OU table layouts
- department/staff result-link generation
- personal-page parsing
- normalization of legacy `www.dev.openu.ac.il` and `/Personal_sites/` links to canonical OU profile URLs
- crawl/parse/enrich/export coverage through the shared pipeline

Known OU fix already in place:

- legacy `www.dev.openu.ac.il` personal-site links are normalized to `https://www.openu.ac.il/...` so the crawler does not fail on dead dev hosts

## BGU status

BGU support was added as a connector-specific adapter on top of the same shared pipeline.

Implemented behavior includes:

- discovery filter loading from the BGU SPA page-data API
- connector-aware filter generation for `unit`, `staff_type`, and `campus`
- BGU listing-page parsing from `.staff-member-item` cards
- extraction of rank, staff type, department, and email from listing cards
- BGU personal-page parsing with profile-text evidence
- CRIS profile parsing for rank, research interests, and academic links
- pipeline parsing that recognizes non-OU result pages instead of assuming `results.aspx`
- deferred parsing passes so CRIS pages discovered from personal pages are merged into the same canonical record
- BGU ORCID extraction from:
  - listing cards
  - personal pages
  - CRIS pages

This means a BGU researcher can now accumulate links from multiple sources into one canonical record without requiring a separate BGU-specific export path or TUI path.

## Shared architecture decisions

Important decisions made during the multi-university work:

- TUI and CLI must use the same `PipelineRunner` and `OuHarvestPipeline` logic
- connector differences belong in adapters, not in separate UI or export code paths
- enrichment consumes the same canonical records regardless of connector
- per-run scoping uses `state/last_parse_ids.json`, so enrich/review/export can operate on the same parsed subset in both CLI and TUI flows

## Tests added during this work

Coverage now includes:

- BGU listing parsing from live fixtures
- BGU preservation of rank, staff type, and department
- BGU personal-page ORCID extraction without false phone parsing
- BGU CRIS parsing for ORCID, Scopus, rank, and CRIS text evidence
- pipeline recognition of BGU listing pages that do not use OU-style `results.aspx`
- pipeline merging of BGU profile + CRIS pages across multiple parse passes
- end-to-end merge dedupe for repeated ORCID links across BGU listing/profile/CRIS sources

## Known limitations

Current known gaps worth addressing next:

- BGU CRIS link extraction is still noisy and keeps many non-primary portal URLs
- CRIS-derived research-interest extraction still includes some noisy text in certain profiles
- mixed-source enrichment attribution is still coarse when one enriched record combines BGU page text and CRIS text
- some enrichment outputs are thin when the public source material is sparse or mostly link-based

## Recommended next steps

If continuing university support work, the highest-value follow-ups are:

- tighten BGU CRIS link filtering to keep only truly useful profile/publication identifiers
- tighten CRIS fingerprint/topic extraction
- improve enrichment source attribution when one record is enriched from multiple upstream pages
- keep README and examples university-agnostic instead of OU-only
