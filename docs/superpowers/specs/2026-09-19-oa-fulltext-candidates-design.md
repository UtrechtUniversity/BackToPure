# OA full-text candidates: dry-run job

Date: 2026-09-19
Issue: #6
Status: design approved, not implemented

## Goal

Report which publications already in Pure could have an open-access full text
attached, so the repository team can set policy against real numbers before
anything is deposited.

This slice reports only. It does not upload files.

## Policy input

Answered by the repository side: **publisher version only**. Accepted
manuscripts and preprints are not deposited.

This makes the filter load-bearing rather than cosmetic. `doi_resolver`
hardcodes `versionType: publishersversion` on the file entry it builds, which
issue #6 correctly calls a bug. Under a published-only policy that hardcoded
value becomes true by construction -- but only because candidates are filtered
to `best_oa_location.version == "publishedVersion"` first. Both ends need a
comment saying so, or a later relaxation of the policy silently mislabels
accepted manuscripts as published versions.

## Scope decisions

| Decision | Choice |
|---|---|
| Population | Publications already in Pure, not newly imported ones |
| Selection | Ricgraph, by faculty, reusing `select_persons_researchoutput` |
| Verification | Fetch and validate every candidate, not metadata only |
| Job model | A new job type, following the existing two-phase run/apply model |

Validating every candidate is the expensive choice, taken deliberately: a
metadata-only count is an upper bound, and the point of the report is to be
trusted.

## Components

### `src/fulltext_candidates.py` -- pure, no I/O

Ported from `doi_resolver/backend/app/services/fulltext.py` and the mappers in
`pure_enrichment.py`:

- `_canonical_url`, `_extract_domain`
- `_access_rank`, `_link_type_rank`, `_format_rank`, `_ranking_key`
- the merge logic inside `_dedupe_candidates`
- `_map_access_type`, `_map_license_type` -- the Pure access-permission and CC
  licence URIs
- `_infer_pdf_filename`

The `confidence` column in the review file is derived here, from the existing
rank helpers rather than a separate scorer: `direct` when the candidate is a
PDF link with open access status, `likely` when it ranks as open or likely-open
but the link type is a landing page, `weak` otherwise. This is the
`direct`/`likely`/`weak` scoring issue #6 refers to, expressed in terms of the
ranks already being ported instead of as a second mechanism.

`aggregate_full_text_data` itself is **not** ported. It is pydantic-based and
exists to merge five sources (Unpaywall, Crossref, EuropePMC, Semantic Scholar,
OpenAlex). This job uses OpenAlex alone, so the multi-source machinery is not
needed and pydantic would be a new dependency in a four-line requirements file.
Candidates are plain dicts here.

### `src/fulltext_fetch.py` -- network

Ported from `pure_enrichment.py`, rewritten from async `httpx` to sync
`requests` to match the rest of `src/`:

- `preflight` -- HEAD check; rejects `text/html` (landing and challenge pages),
  rejects 401/403/404, and deliberately continues on an unknown content-type
- `download_and_validate` -- `%PDF-` magic bytes, content-type, size cap, and
  the HTML-on-error case that catches Cloudflare challenges

Plus a per-host rate limiter ported from `doi_resolver/backend/app/utils/ratelimit.py`
(`TokenBucket` and `RateLimiterRegistry` port cleanly to threads; the httpx
transport wrapper becomes a small helper that acquires a slot before each call).

The limiter is in this slice, not a later one, because without it the report is
untrustworthy. Measured over a 300-work sample of UU publications:

```
117  doi.org                    (44%)
 20  onlinelibrary.wiley.com
 16  dspace.library.uu.nl
 16  link.springer.com
top 3 hosts = 58% of all fetches
```

A global thread pool would hammer a handful of hosts. Publishers answer with
429s and Cloudflare challenges, which arrive as HTML and are correctly rejected
by the validator -- so throttling would show up in the report as "no full text
available" for papers that are perfectly available. `dspace.library.uu.nl` is
UU's own repository, which makes this a self-inflicted outage risk as well as a
rude one. `Retry-After` is honoured rather than counted as a failure.

### `src/harvest_fulltext_candidates.py` -- the job script

```
Ricgraph (faculty)  -> DOIs + Pure UUIDs      [select_persons_researchoutput]
  -> Pure           -> skip anything that already has a file attached
  -> OpenAlex       -> best_oa_location and all locations
  -> policy filter  -> version == publishedVersion only
  -> fetch+validate -> preflight, %PDF-, content-type, size cap
  -> review CSV
```

Validation runs through a `ThreadPoolExecutor`, as `select_persons_researchoutput`
already does, with the phase timing added in #21. This will be the slowest job
in the application; the timing is what makes that diagnosable.

### Job registration

A new `JobType.FULL_TEXT = "full_text"` with a `JobTypeDefinition`:
`script_path` the harvest script, `artifact_dir` `output/full_text`,
`identity_columns` `("doi",)`, `allowed_params` the usual faculty choice.

The dry run is this job's phase 1. The review gate the application already has
becomes the policy checkpoint before any deposit -- no separate "dry-run mode"
is needed.

## Review file

`output/full_text/to_be_updated.csv`, one row per publication examined:

`to_be_updated, updated, doi, pure_uuid, title, candidate_url, version,
licence, access_status, confidence, validation, size_bytes, reason`

Every rejection carries a reason rather than being dropped -- no OA location,
not the published version, dead URL, HTML not PDF, too large, already has a
file. The report is then useful as a coverage document even where the answer is
no, which is what the repository team needs in order to set the remaining
policy.

## Error handling

A failed candidate is a recorded row, not a lost one. Network failures are
retried per the limiter's backoff and then recorded with their reason. A run
where every fetch fails must fail loudly rather than report zero coverage --
the same rule applied to the harvests in #22.

## Testing

- `fulltext_candidates`: unit tests, including the dedupe-and-rank case ported
  from `backend/tests/test_fulltext_service.py` (53 lines, one test -- a
  starting point, not a suite)
- `fulltext_fetch`: stubbed responses covering HTML landing page, challenge
  page, wrong content-type, oversize, good PDF, and per-host throttling
- the job script: a smoke test
- no live network in tests

## Out of scope

The upload path (`PUT /research-outputs/file-uploads`), the `FileElectronicVersion`
entry, the apply hook and rollback of deposited files. Those follow once the
report shows what the real coverage is.

A byte cache so apply need not re-download is deliberately deferred: it only
pays off once apply exists.

## Open questions (repository side, not blocking this slice)

From issue #6, still unanswered: who owns the licence check; what to do about
duplicates with the existing OA workflow; storage growth in Pure at faculty
scale.
