# Entity Workflow Audit

Date: 2026-05-13

Scope: current job-based workflows in `app/models/jobs.py`, their scripts in `src/`, and the shared Pure apply/rollback path. This is an end-to-end review from data collection through review artifacts and Pure updates.

## Summary

The application still has the five expected production workflows:

- Internal persons
- External persons
- External organizations
- Research outputs
- Datasets

The main issue is not that everything is gone. The main issue is that the workflows are uneven. Internal persons and external persons still contain most of the expected logic, although they need performance work. External organizations recently lost useful logic but is now wired back toward Ricgraph organization data plus an OpenAlex institution snapshot. Research outputs and datasets are the weakest production candidates: they have narrower or less reliable selection logic, and datasets still has an unsafe `all` mode.

## Priority Findings

1. Datasets `all` mode is not safely faculty-scoped.
   `src/update_datasets_from_ricgraph.py` uses a global Ricgraph `advanced_search(category=data set)` when faculty choice is `all`. That can include all datasets in the graph instead of datasets reached through selected UU faculties.

2. Datasets now uses the configured Ricgraph base URL, but its selection logic still needs production review.
   The previous hard-coded `http://127.0.0.1:3030/api/...` calls were replaced with `RIC_BASE_URL` on 2026-05-20. The remaining dataset risk is the `all` mode scope and whether dataset selection should be strictly faculty-traversed.

3. Research output import only selects `journal article`.
   `src/update_researchoutput_from_ricgraph.py` ignores the shared configured category set used by external person/org workflows. If the intended import scope is broader, this will miss records.

4. Research output import decides "not in Pure" from Ricgraph source labels.
   It treats Ricgraph `_source` as truth for whether Pure already has the output. That is fast, but production import should verify against Pure by DOI before proposing a create.

5. External organization flow was fragile and needs live validation.
   The improved target shape is correct: Ricgraph should provide publication-person-organization links, and the OpenAlex institution snapshot should enrich ROR/address metadata. This should be tested on a small real faculty before trusting a full university run.

6. Large jobs still make many small HTTP calls.
   Internal persons, external persons, external organizations, and non-`all` datasets traverse person roots and neighbours with many Ricgraph calls. This is understandable for Ricgraph's API shape, but it is the main reason full-university runs feel slow.

7. Apply and rollback are useful but still artifact-coupled.
   `src/apply_updates_to_pure.py` relies on exact CSV/JSON contracts. `app/services/jobs.py` captures change sets around that, which is good, but if a workflow writes incomplete JSON or mismatched CSV rows, apply/rollback quality drops quickly.

8. Legacy/interactive paths remain.
   Some functions still contain `input()` prompts or old script-style behavior. They are not normally used by the job backend, but they make the codebase harder for a new person to understand and trust.

## Workflow Matrix

| Entity | Get data | Prepare/match | Review artifacts | Apply to Pure | Rollback | Current risk |
| --- | --- | --- | --- | --- | --- | --- |
| Internal persons | Ricgraph faculty -> person roots -> person IDs; Pure persons by UUID batch | Compare Ricgraph identifiers to Pure person identifiers | `datatotal.json`, `personstobeupdated_*.csv` | PUT `/persons/{uuid}` | Restores ORCID/identifier if unchanged since apply | Medium |
| External persons | Ricgraph faculty -> person roots -> research outputs -> external person neighbours; Pure outputs by UUID/DOI | Name match Pure contributors to Ricgraph persons; optional OpenAlex fallback disabled by default | `to_be_updated.json`, `ext_pers_update.csv` | PUT `/external-persons/{uuid}` | Removes identifiers added by job | Medium-high |
| External organizations | Ricgraph output -> person -> organization; OpenAlex institution snapshot for ROR/address metadata; Pure orgs by UUID | Match Pure external orgs to Ricgraph/OpenAlex institution data | `external_orgs_updates.json`, `external_orgs_to_update.csv` | PUT `/external-organizations/{uuid}` | Removes ROR/address updates if unchanged since apply | High until live-tested |
| Research outputs | Ricgraph faculty -> person roots -> `journal article` neighbours; OpenAlex works by DOI | Transform OpenAlex work to Pure JSON | `output_to_be_updated.json`, `to_be_updated.csv` | POST/create research output | Deletes created record by recorded UUID | High |
| Datasets | Ricgraph datasets by faculty, except `all` currently uses global dataset search; DataCite by DOI | Transform DataCite result and contributor details to Pure JSON | `datasets_to_be_updated.json`, `to_be_updated.csv` | POST/create dataset | Deletes created record by recorded UUID | High |

## Internal Persons

### Current Flow

1. Validate Ricgraph and Pure availability.
2. Select primary faculties only.
3. For each faculty, get person roots from Ricgraph.
4. For each person root, get connected person identifiers.
5. Pivot Ricgraph identifiers into person rows.
6. Fetch full Pure person records in batches of 100.
7. Compare Ricgraph identifiers against existing Pure identifiers.
8. Write review CSV and JSON.
9. Apply selected updates by PUT to Pure persons.
10. Rollback can restore ORCID or remove an added identifier when the current Pure value still matches the applied value.

### What Is Good

- Uses primary-faculty filtering.
- Batches Pure person fetches.
- Produces reviewable CSV and source JSON.
- Rollback captures old values for ORCID and identifiers.
- This is the most production-shaped workflow.

### What Is Not Good

- Ricgraph traversal is sequential per person root. Full-university scale will be slow.
- Empty Ricgraph result handling is fragile before the later `empty` checks.
- Existing conflicting identifiers are not surfaced clearly enough. The code mostly proposes missing identifiers, but it does not give reviewers a strong conflict report when Pure has a different value for the same identifier type.
- There is an unused `update_person` function in `src/enrich_internal_persons_with_ids.py` that can confuse maintainers because actual apply now happens in `src/apply_updates_to_pure.py`.

### Recommended Fixes

- Add a clear conflict CSV for "Pure has different value for same identifier type".
- Make `select_persons` safe when Ricgraph returns no person rows.
- Thread Ricgraph person-root neighbour calls with a conservative worker limit.
- Remove or mark unused direct-update functions as legacy.

## External Persons

### Current Flow

1. Select primary faculties.
2. Traverse faculty -> person roots -> research outputs.
3. Fetch Pure research outputs by UUID, with DOI fallback.
4. From each Ricgraph output, collect external person nodes and their ORCID/OpenAlex IDs.
5. Match Ricgraph external persons to Pure external contributors, mainly by name.
6. Consolidate duplicate/conflicting candidate updates.
7. Write review CSV and JSON.
8. Apply selected updates by PUT to Pure external persons.
9. Rollback removes identifiers added by the job if still present.

### What Is Good

- The default path is now Ricgraph-first, which fits the available data model.
- Optional OpenAlex fallback is disabled by default.
- There is caching of person details during Ricgraph collection.
- Fetching Pure research outputs is batched.

### What Is Not Good

- The workflow still makes one Ricgraph output-neighbour call per research output and one person-detail call per unique person. On full-university runs this can be very slow.
- Name matching is inherently risky. It should be treated as candidate generation, not proof.
- Optional OpenAlex fallback code remains in the main module. Even disabled, it makes the current logic harder to read.
- Conflict reporting exists but should be more visible in the UI/review artifacts.

### Recommended Fixes

- Add timing counters for each stage: collect outputs, fetch Pure outputs, Ricgraph person collection, matching, write artifacts.
- Separate optional OpenAlex fallback into a clearly named legacy/helper module.
- Add a review column explaining match confidence and source.
- Consider persisting Ricgraph external person lookups per run so failed jobs can resume without redoing the whole traversal.

## External Organizations

### Current Flow

1. Select primary faculties.
2. Collect research outputs from faculty-linked internal persons.
3. Fetch Pure research output JSON.
4. For each output, use Ricgraph publication-person links.
5. For those persons, use Ricgraph person-organization links.
6. Extract Pure external organization UUIDs from Pure research outputs.
7. Fetch those Pure external organization records.
8. Enrich Ricgraph organization names with local OpenAlex institution snapshot metadata by ROR/name.
9. Compare against Pure organization records.
10. Write review CSV and JSON.
11. Apply selected updates by PUT to Pure external organizations.
12. Rollback removes ROR/address changes if current Pure still matches the job-applied value.

### What Is Good

- The intended logic now matches the data source better: Ricgraph supplies organization relationships; OpenAlex institution snapshot supplies metadata.
- The job no longer needs OpenAlex works by DOI for external organizations.
- Local snapshot lookup is the right efficiency direction for institution metadata.

### What Is Not Good

- This is newly rewired and not yet proven on a real small faculty run.
- Name-based institution lookup can be ambiguous. ROR from Ricgraph should be preferred whenever available.
- The snapshot dependency is operational: the job needs `output/openalex_cache/openalex_institutions_snapshot_by_ror.json`, produced by `snapshot_openalex_institutions`.
- The code still has more moving parts than the others and needs cleanup after validation.

### Recommended Fixes

- Run one small faculty job and inspect every proposed update before full-university use.
- Make missing snapshot a clear UI/job warning with instructions, not just a low-level log.
- Add counters: outputs scanned, Ricgraph org nodes found, Pure org UUIDs found, ROR matches, name-only matches, proposed updates.
- Treat name-only matches as lower confidence in the review CSV.
- After validation, remove any unused helper functions left from the old OpenAlex-works path.

### Live Test 2026-05-13

Command used:

```bash
BTP_OUTPUT_DIR=output/live_tests/external_orgs_geestes_live .venv/bin/python -u src/enrich_pure_external_orgs.py 'uu faculty: faculteit geesteswetenschappen|organization_name' no
```

Result after the first run:

- Ricgraph selected 1,709 unique research outputs.
- Pure fetched 1,610 research outputs.
- Ricgraph organization traversal found 458 publications with Pure external organization UUIDs.
- The job saw 922 external organization UUID references.
- The first run produced 0 proposals because `fetch_pure_extorgs()` flattened UUID lists twice, turning UUID strings into individual characters before Pure search.
- A second issue was found after this test: `output/openalex_cache/openalex_institutions_by_ror.json` was not a full OpenAlex snapshot lookup. It was the older API cache built from requested RORs only. The external-org job now uses `output/openalex_cache/openalex_institutions_snapshot_by_ror.json` so the downloaded snapshot and the API cache cannot be confused.

Fix applied:

- `fetch_pure_extorgs()` now flattens list/tuple/set UUID groups once and keeps UUID strings intact.
- The batch log now correctly says UUIDs instead of DOIs.
- The OpenAlex institution snapshot lookup now has a separate filename from the legacy API cache.
- If the compact snapshot lookup is missing, the external-org job stops before the expensive Ricgraph/Pure harvest with an instruction to run `snapshot_openalex_institutions --download` once.
- `snapshot_openalex_institutions` now downloads the public OpenAlex institution snapshot directly over HTTPS, so it no longer requires the AWS CLI.
- The local compact snapshot was built on 2026-05-13: `output/openalex_cache/openalex_institutions_snapshot_by_ror.json` contains 121,511 unique ROR records.

Result after rerun:

- 922 external organization UUID references processed.
- 246 unique external organization update proposals written to `external_orgs_to_update.csv`.
- 246 JSON update payloads written to `external_orgs_updates.json`.
- 171 no-name-match cases written to `external_orgs_no_name_match.json`.
- 35 ambiguous-match cases written to `external_orgs_ambiguous_matches.json`.
- 220 proposals are exact display-name matches.
- 23 proposals are exact alternative-name matches.
- 3 proposals are fuzzy matches.

Safety changes after reviewing the live artifacts:

- Fuzzy matches are still written to the review CSV, but are no longer selected by default.
- Address enrichment now fills missing city/country/geolocation fields only; it does not replace an existing Pure address field with snapshot data.
- The latest live artifacts contain 243 preselected exact-match updates and 3 unselected fuzzy-match rows.
- 189 rows still have `needs_geo_update=True`, mostly because Pure has incomplete address fields and the job can fill missing city/country/geolocation data.

Live-test concerns:

- The logs are too noisy: DOI fallback logs and repeated no-match/queued messages make it hard to see run health.
- Ambiguous matches sometimes appear to contain duplicate labels for the same display name. The ambiguity artifact should dedupe by ROR/OpenAlex ID before deciding a match is ambiguous.
- Exact alternative-name matches are currently selected by default. That is probably acceptable, but should be reviewed once before applying at scale.

## Research Outputs

### Current Flow

1. Select primary faculties.
2. Traverse faculty -> person roots -> Ricgraph `journal article` neighbours.
3. Split records into "new" and "duplicate" based on Ricgraph source labels.
4. Fetch OpenAlex works for candidate DOIs.
5. Transform OpenAlex works to Pure research output JSON.
6. Drop rows without journal ISSN.
7. Write review CSV and JSON.
8. Apply selected rows by creating research outputs in Pure.
9. Apply writes a manifest with created Pure UUIDs.
10. Rollback deletes created Pure research outputs by recorded UUID after DOI check.

### What Is Good

- The create path records created Pure UUIDs for rollback.
- DOI normalization exists in apply/rollback.
- The review-before-apply model is consistent with the rest of the app.

### What Is Not Good

- It only selects `journal article`; this may be too narrow.
- It trusts Ricgraph `_source` to decide whether Pure already has the record.
- It depends on OpenAlex works metadata for creation, so missing/stale OpenAlex data means missing imports.
- The old interactive `test_or_not` function remains.
- It does not call `raise_for_status()` in the research-output neighbour request.

### Recommended Fixes

- Decide the intended research-output categories and make them explicit in one shared config.
- Verify candidate DOI absence against Pure before writing review artifacts.
- Add a clear skipped-reason CSV: already in Pure, no OpenAlex data, no journal, invalid DOI, transform error.
- Remove unused interactive functions from the job path.

## Datasets

### Current Flow

1. Select primary faculties.
2. If a single faculty is selected, traverse faculty -> person roots -> data set neighbours.
3. If `all` is selected, currently query all Ricgraph data set nodes globally.
4. Fetch DataCite metadata by DOI.
5. For each DataCite row, check whether Pure already has the dataset.
6. Build Pure dataset JSON with contributor details.
7. Write review CSV and JSON.
8. Apply selected rows by creating datasets in Pure.
9. Apply writes a manifest with created Pure UUIDs.
10. Rollback deletes created Pure datasets by recorded UUID after DOI check.

### What Is Good

- Single-faculty traversal follows the expected faculty/person-root/data-set model.
- Pure existence check by DOI exists before proposing creates.
- Created Pure UUIDs are recorded for rollback.

### What Is Not Good

- `all` mode is not equivalent to all selected UU faculties. It is a global dataset search.
- Ricgraph URLs now use `RIC_BASE_URL`, but the dataset workflow still needs a stronger remote-Ricgraph smoke test.
- Several HTTP calls do not call `raise_for_status()`.
- DataCite/Pure/contributor processing is mostly sequential.
- There are interactive/legacy code paths in `src/pure_datasets.py`.

### Recommended Fixes

- Fix `all` to iterate selected primary faculties, matching other workflows.
- Add regression/smoke coverage for configured local and remote Ricgraph sources.
- Add explicit skipped reasons for no DataCite record, already in Pure, missing contributor details, and transform errors.
- Add caching for contributor details during one job run.
- Remove or isolate interactive script-only behavior from the job backend.

## Apply And Rollback

### Current Flow

1. Job scripts write CSV and JSON artifacts.
2. `JobService.apply_job()` sets environment variables to point the shared apply script at the job artifact directory.
3. Before apply, `JobService` captures expected changes from the CSV/JSON files.
4. `src/apply_updates_to_pure.py` updates or creates records in Pure.
5. CSV rows are marked updated on success.
6. `JobService` finalizes the change set by reading updated CSV rows and apply manifests.
7. Rollback uses captured changes to undo identifiers/address changes or delete created records.

### What Is Good

- Apply is tied to tracked job artifacts, not whatever files happen to be in global output directories.
- Rollback checks current Pure values before undoing updates, which prevents blind overwrite.
- Research output and dataset creation now records created UUIDs in `apply_manifest.jsonl`.

### What Is Not Good

- The shared apply script has a lot of entity-specific behavior in one file.
- External person and external org apply status is entity-based: if one update on an entity succeeds, the change set can mark the entity applied rather than proving every field-level change.
- CSV/JSON schema drift is dangerous. There is no central typed artifact schema.
- The script uses `verify=False` for some Pure calls.
- Fixed `time.sleep(0.1)` throttling is crude and slows large jobs even when Pure can handle more.

### Recommended Fixes

- Introduce small per-entity apply modules with typed input validation.
- Validate artifact schemas before enabling Apply.
- Make external person/org applied detection field-level where possible.
- Remove `verify=False` unless the certificate problem is explicitly documented and configured.
- Replace fixed sleeps with retry/backoff driven by response status.

## Efficiency Hotspots

| Hotspot | Impact | Better Direction |
| --- | --- | --- |
| Per-person-root Ricgraph neighbour calls | Slow full-university runs | Conservative thread pools, progress counters, resumable caches |
| Per-output Ricgraph collection for external persons/orgs | Very slow when many outputs are selected | Cache output/person/org lookups per job and dedupe earlier |
| Pure/DataCite checks in dataset creation | Slow and hard to resume | Batch where API allows; cache by DOI/person |
| OpenAlex work fetch for research outputs | Slow and dependent on remote/cache quality | Use cache snapshots and explicit skipped reasons |
| Shared apply script | Maintenance risk | Split by entity after behavior is stable |

## Legacy And Cleanup Candidates

- Unused direct-update functions inside enrichment scripts.
- Interactive `input()` functions in job-era modules.
- Old OpenAlex fallback code in external persons should be isolated.
- Any external organization helpers from the former OpenAlex-works-by-DOI path should be removed after the new Ricgraph/snapshot path is live-tested.
- Hard-coded Ricgraph URLs in dataset code.

## Suggested Production-Ready Order

1. Fix datasets scope/config first.
   This is the clearest correctness issue.

2. Stabilize external organizations on a small real run.
   Confirm Ricgraph organization traversal and snapshot enrichment produce expected review rows.

3. Add stage timing and counters to all large workflows.
   This will make slow jobs understandable instead of mysterious.

4. Make research output selection and Pure existence checks explicit.
   Decide categories and verify DOI absence in Pure.

5. Clean apply contracts.
   Add schema validation and split apply logic after the entity behavior is stable.

6. Remove legacy interactive/dead code.
   Do this last, after tests prove the intended job paths.
