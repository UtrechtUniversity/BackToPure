# Ricgraph 3.5 completeness fixes

Approved scope: the user's agreement to the proposed paging, UUID and live verification repairs, plus clear GitHub issues. Keep the working Ricgraph 3.5/2026 graph. Make BackToPure consume complete results and retain DOI-less Pure outputs. Do not apply updates to Pure.

## Approach

Use one shared `ricgraph_client.read_results(send, url, *, params=None, timeout=30)` reader around the existing read/retry helpers. It collects cursor pages, preserves filters, accepts legacy responses without cursor metadata, and raises `FatalWorkflowError` on malformed/incomplete/repeated pagination. Existing workflow signatures remain stable. A shared reader avoids divergent per-workflow fixes; changing the server's page size would still truncate large selections.

The existing `extract_researchoutput_pure_uuid` helper will validate UUIDs and accept explicit fields, typed PURE_ID_RESOUT values/keys and publication URLs in both URL fields. Both legacy strings and new lists are supported. DOI matching stays available. No unrelated app/jobs/apply behavior changes.

## Delivery and validation

Work in an isolated local clone based on the newer clean checkout, preserve server-local edits, and backport only reviewed compatibility changes to the older server source. Test paging and DOI-less workflow retention before implementation, run the backend suite, then repeat the live read-only comparison and one small proposal-only check. Create three GitHub issues with reproduction evidence and acceptance criteria. Do not merge shared branches or apply Pure updates.
