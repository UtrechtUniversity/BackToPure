# Inline Review Plan

Last updated: 2026-04-21
Status: Planned
Scope: Bring review of tracked CSV files into the new UI without breaking the current download-based workflow

## Goal

Let users inspect and control reviewed rows directly inside `/app`, starting with the existing tracked workflows:

- `internal_persons`
- `external_persons`
- `external_orgs`

The first version should remove the most awkward part of the current flow:

- download CSV
- edit outside the app
- rely on manual file handling

It should do that without trying to become a full spreadsheet editor on day one.

## Product Direction

### What the first release should do

- Show the review CSV inside the job detail page
- Support search and basic filtering
- Let users toggle the selection column in the UI
- Save the edited selection back to the tracked job file

### What the first release should not try to do

- Full inline editing of every CSV cell
- Excel-like keyboard navigation
- Inline editing for JSON payloads
- Simultaneous multi-user editing

This should be a controlled review tool, not a generic data grid.

## Design Principles

- Keep the tracked file model intact: the CSV on disk remains the source of truth
- Keep job behavior consistent across migrated workflows
- Handle `1000+` rows without making the page sluggish
- Make selection state and save state obvious to the user
- Preserve the existing download flow as a fallback during rollout

## Technical Approach

## Backend

### New endpoints

- `GET /api/jobs/<id>/review-table`
- `POST /api/jobs/<id>/review-table`

### `GET /api/jobs/<id>/review-table`

Purpose:

- Load the current tracked CSV in a UI-friendly shape

Response shape:

- `jobId`
- `fileName`
- `columns`
- `rows`
- `rowCount`
- `selectionColumn`
- `editableColumns`
- `lastModified`

Rules:

- Only allow known tracked review CSV files for the job type
- Prefer the primary CSV for that workflow
- Return rows in file order
- Preserve empty values as empty strings, not `null` where possible

### `POST /api/jobs/<id>/review-table`

Purpose:

- Persist reviewed selection changes back to the tracked CSV

First release scope:

- Only persist changes to the selection column

Request shape:

- `fileName`
- `updates`

Each update:

- `rowIndex`
- `to_be_updated`

Rules:

- Reject writes for jobs without review files
- Reject writes for unsupported job types
- Reject updates to columns other than the approved selection column
- Write back to the same tracked CSV file in the same job directory
- Recompute tracked result summaries after save

### Backend implementation notes

- Reuse `pandas` for read/write because the review files already depend on it
- Keep row identity simple in V1: use row index from the current file order
- Re-read the file at save time and apply updates against current rows
- If row count changed unexpectedly, reject with a clear conflict error

## Frontend

### New UI surface

Add a new panel on the job detail page:

- `Review Table`

V1 contents:

- search input
- quick filters
- row count summary
- virtualized table
- save button

### Table behavior

- Default to showing the primary review CSV for the workflow
- Render only visible rows using virtualization
- Keep columns read-only except the selection column
- Show unsaved changes count
- Require explicit save

### Filters

Start with:

- `All rows`
- `Selected only`
- `Unselected only`
- text search

### Workflow-specific defaults

- `internal_persons`
  - editable column: `to_be_updated`
- `external_persons`
  - editable column: `to_be_updated`
- `external_orgs`
  - editable column: `to_be_updated`

### Reasonably sized column set

Do not show every possible CSV column by default if it makes the table unreadable.

Prefer:

- selection state
- updated state
- primary label column
- key identifier columns
- workflow-specific reason columns where present

## Performance

For `1000+` rows this should still be fine if the UI uses virtualization.

Expected approach:

- `@tanstack/react-virtual` or equivalent
- client-side filtering for V1

V1 performance assumptions:

- `1000` rows: easy
- `5000` rows: still acceptable with virtualization

If later job types produce much larger CSV files:

- move filtering and pagination server-side

## Rollout Plan

### Phase A: Read-only review table

Deliverables:

- backend `GET /review-table`
- frontend review table panel
- search and simple filters
- virtualized rendering

Acceptance criteria:

- users can inspect tracked review CSVs in the app
- the page remains responsive for `1000+` rows

### Phase B: Inline selection editing

Deliverables:

- backend `POST /review-table`
- toggle `to_be_updated` in the UI
- unsaved changes tracking
- save flow

Acceptance criteria:

- a user can change row selection without leaving the app
- the saved CSV remains compatible with existing apply logic
- job result counts refresh after save

### Phase C: Bulk actions

Deliverables:

- select all visible
- deselect all visible
- select filtered subset

Acceptance criteria:

- users can review larger files quickly without row-by-row clicking

### Phase D: Workflow-specific polish

Deliverables:

- clearer visible reason columns for `external_orgs`
- clearer identifier columns for persons workflows
- better empty-state and save-state messages

Acceptance criteria:

- each workflow is easier to review without opening the raw CSV first

## Risks

- CSV row identity based on row index is simple but not ideal if files are regenerated externally
- CSV formatting must remain stable enough that apply scripts still accept it
- very wide files may need a custom visible-column strategy
- introducing full cell editing too early would add more risk than value

## Recommendation

Build this in phases and stop after Phase B for the first usable release.

That gives the biggest user-value jump:

- review inside the app
- selection editing inside the app
- no need for upload support

without turning this into a broad spreadsheet project.
