# Ricgraph API Contract

Last updated: 2026-05-20
Scope: Ricgraph HTTP routes and data shapes BackToPure expects.

## Purpose

BackToPure can use a local Ricgraph instance or a compatible remote Ricgraph endpoint. The endpoint must contain data that matches the target Pure tenant. Route compatibility alone is not enough for production Pure updates.

Configure the endpoint in `src/config.ini`:

```ini
[RICGRAPH-API]
BaseURL = https://your-ricgraph.example.org/api/
FacultyPrefix = your faculty search prefix
PrimaryOrganizationPrefixes = your primary organization key prefix:
ExcludedOrganizationPrefixes = your excluded organization key prefix:
```

## Cursor pagination in Ricgraph 3.5

Collection reads use `ricgraph_client.read_results`. Ricgraph 3.5 returns up to
`page_size` results per response (default 50, supported maximum 250) and provides
`meta.next_cursor` when more results remain. Keep the same route and search filters
and send that opaque value as the next request's `cursor`. Stop when the cursor is
empty or absent. `max_nr_items=0` alone does not retrieve all pages in 3.5.

The shared reader preserves legacy parameters for older servers and accepts
single-page legacy responses without cursor metadata. A failed later page,
invalid results, repeated cursor/page or empty intermediate page stops the harvest
with an explicit error rather than returning a successful partial collection.

This applies to faculty selection, person roots, person identifiers, author and
organization neighbours, and advanced searches used by imports. The
`person/enrich` endpoint may still return a single legacy-style response.

## Required Routes

### `GET organization/search`

Used to populate organization/faculty choices.

Typical parameters:

```text
value=<FacultyPrefix>
```

Expected response:

```json
{
  "results": [
    {
      "_key": "uu faculty: example|organization_name",
      "value": "Example Faculty"
    }
  ]
}
```

BackToPure filters `_key` with `PrimaryOrganizationPrefixes` and `ExcludedOrganizationPrefixes`.

### `GET get_all_personroot_nodes`

Used to traverse from an organization/faculty to person roots.

Typical parameters:

```text
key=<organization_key>
max_nr_items=0
```

Expected response:

```json
{
  "results": [
    {
      "_key": "person-root-key"
    }
  ]
}
```

### `GET get_all_neighbor_nodes`

Used to traverse from person roots to identifiers, research outputs, datasets, external persons, and organizations.

Typical parameters vary by workflow:

```text
key=<node_key>
category_want=<category>
max_nr_items=<number>
```

Expected response:

```json
{
  "results": [
    {
      "_key": "node-key",
      "value": "display value",
      "category": "category"
    }
  ]
}
```

Important fields used by different workflows:

- `_key`: stable Ricgraph node key.
- `value`: display value such as organization or person name.
- `url_other`: Pure URL or Pure UUID-bearing URL when present.
- `_source`: source labels used by some import workflows.

In Ricgraph 3.5, `url_main` and `url_other` are lists. Earlier releases used
strings. Both representations are supported when resolving Pure output UUIDs.
For a typed `PURE_ID_RESOUT` node without a DOI, its UUID is available in `value`
or in the typed `_key`; its Pure publication URL may be in `url_main`. UUID values
are validated, and unrelated identifiers or non-publication URLs are ignored.

### `GET advanced_search`

Used by import workflows to find datasets or outputs by category/source.

Typical parameters:

```text
category=<category>
max_nr_items=<number>
```

or:

```text
name_want=<field>
category_want=<category>
value_want=<value>
```

Expected response:

```json
{
  "results": [
    {
      "_key": "10.1234/example|data set",
      "value": "10.1234/example"
    }
  ]
}
```

### `GET person/enrich`

Used by internal-person enrichment support paths.

Expected response shape is a JSON object with `results` when matches are found. Empty responses should still return valid JSON.

## Data Required By Workflow

### Internal Persons

Ricgraph must contain:

- organization/faculty nodes
- person-root nodes linked to the selected organizations
- person identifier neighbours for ORCID, Scopus, ISNI, Digital Author ID, Researcher ID, or OpenAlex as configured
- Pure person UUIDs or Pure-linked identifiers where the workflow expects them

### External Persons

Ricgraph must contain:

- selected organization/faculty to person-root links
- person-root to research-output links
- research-output Pure UUIDs or DOI values
- research-output to external-person links
- external-person ORCID/OpenAlex identifiers where available

### External Organizations

Ricgraph must contain:

- selected organization/faculty to person-root links
- person-root to research-output links
- research-output Pure UUIDs
- research-output to person links
- person to organization links
- Pure external-organization UUIDs in the organization nodes or URLs

OpenAlex institution metadata is supplied separately by the local snapshot documented in `docs/first-run-setup.md`.

### Research Outputs

Ricgraph must contain:

- selected organization/faculty to person-root links
- person-root to research-output links
- DOI values
- enough source metadata to decide whether a record already exists in Pure

Production import should still verify against Pure before create.

### Datasets

Ricgraph must contain:

- selected organization/faculty to person-root links
- person-root to dataset links
- dataset DOI values
- contributor/person details needed to format Pure dataset contributors

Dataset `all` mode is still a production-review item; prefer scoped runs until it is tightened.

## Public Explorer Usage

`https://explorer.ricgraph.eu/api/` can validate route shape and can support demos where its data is intentionally the source.

For production Pure updates, use a Ricgraph endpoint known to contain data for the target Pure tenant. If the Pure UUID links come from a different Pure instance, BackToPure can generate incorrect or unusable update proposals.

Use public Explorer for:

- Demonstrating the UI and route compatibility.
- Checking that BackToPure can speak to a Ricgraph-like HTTP API.
- Exploring public/demo data where no Pure update will be applied.
- UU-specific workflows only when the public Explorer data is explicitly the intended source.

Do not use public Explorer for:

- Production updates for another organization's Pure tenant.
- Private or institution-specific data that is not present in public Explorer.
- Jobs where Pure UUID links must match a non-UU Pure tenant.
- Any apply workflow unless the operator has verified that the public Ricgraph data belongs to the same Pure target.

## Smoke Check

Opt-in live checks compare production readers with an independent cursor
traversal, using current API counts rather than fixed totals. All requests are
restricted to GET/HEAD on the selected Ricgraph API origin and path, including
redirects. No updates to Pure are sent.

```bash
BTP_RICGRAPH_TEST_BASE_URL=http://127.0.0.1:3030/api/ \
BTP_RICGRAPH_TEST_ORG_VALUE='uu faculty' \
BTP_RICGRAPH_TEST_OUTPUT_KEY='10.1038/s41586-025-09962-4|doi' \
  .venv/bin/python -m pytest tests/test_ricgraph_smoke.py -q
```

`BTP_RICGRAPH_TEST_FACULTY_KEY` can select a particular faculty. The output key is
optional, but should identify a publication with more than 50 authors to exercise
the old truncation bug. Graph-specific book checks skip when suitable source data
is absent. Live checks include a selector bounded to one real author and verify
that a DOI-less book survives research-output selection.

Run the doctor command for local checks and optional API reachability:

```bash
.venv/bin/python src/doctor.py
```

Use this when network checks are not wanted:

```bash
.venv/bin/python src/doctor.py --skip-network
```
