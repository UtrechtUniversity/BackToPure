# UI Density Reduction Plan

Last updated: 2026-05-12
Owner: BackToPure maintainers
Scope: Make the React UI calmer, easier to scan, and less visually crowded while keeping the UU yellow as a brand color.

## Design Direction

- Keep UU yellow.
- Stop using yellow as a dominant full-surface background.
- Use yellow as an accent for actions, highlights, active states, and small emphasis areas.
- Move the overall UI toward a quiet operational tool rather than a hero-style branded page.

## Current Problems

- The sidebar is too visually dominant.
- The background, gradients, blur, and shadows add too much visual noise.
- Headings are too large for a workflow application.
- Too many sections are presented as separate cards.
- Dashboard content competes for attention instead of establishing a clear order.
- Navigation labels are too long and make scanning harder.
- Metrics, workflow summaries, and job cards all use similar emphasis, so the eye has no stable priority.

## Goals

- A user should be able to understand the page structure in a few seconds.
- The primary workflow actions should be obvious without the page feeling loud.
- Repeated content should become denser and easier to scan.
- Brand identity should still read as UU through restrained yellow usage.

## Phase 1: Tone Down the Shell

### UI-101: Reduce sidebar dominance

Status: Completed

Deliverables:

- Reduce sidebar width.
- Remove the full-intensity yellow panel treatment.
- Keep the logo and UU yellow, but use a calmer sidebar background.
- Shorten navigation labels where possible.

Acceptance criteria:

- The main content visually leads the page.
- The sidebar still feels branded, but no longer carries most of the visual weight.

### UI-102: Calm the global surface styles

Status: Completed

Deliverables:

- Replace the current high-energy page background with a quieter neutral background.
- Reduce or remove heavy blur and large shadows.
- Flatten the panel system so surfaces feel more consistent.
- Keep yellow for selected controls, badges, and focused highlights.

Acceptance criteria:

- Pages feel lighter and more stable.
- Panels are readable without looking decorative.

## Phase 2: Tighten Typography and Spacing

### UI-201: Scale headings for an operations tool

Status: Completed

Deliverables:

- Reduce hero/header type sizes.
- Limit serif display treatment to a small number of places, if any.
- Tighten vertical spacing between headings, copy, and sections.

Acceptance criteria:

- No page reads like a landing page.
- Headings support scanning instead of dominating the screen.

### UI-202: Make repeated UI denser

Status: Completed

Deliverables:

- Reduce padding in cards, panels, metrics, and job rows.
- Tighten gaps in grids and stacked sections.
- Keep tap/click targets usable while reducing wasted space.

Acceptance criteria:

- More useful information fits above the fold.
- Lists and dashboards feel efficient instead of bulky.

## Phase 3: Rework Dashboard Hierarchy

### UI-301: Remove the hero feel from dashboard

Status: Completed

Deliverables:

- Replace the current hero-style dashboard header with a compact operational header.
- Reduce the number of top-level sections competing for attention.
- Reorder content so recent jobs and actionable status come before explanatory content.

Acceptance criteria:

- Dashboard reads as a work queue and status view.
- Important actions and recent work are visible without scrolling through decorative framing.

### UI-302: Differentiate summary from detail

Status: Completed

Deliverables:

- Make metrics visually lighter than primary task content.
- Simplify workflow analytics cards so they do not compete with the job list.
- Reduce repeated badge and pill emphasis where it adds noise.

Acceptance criteria:

- The page has a clear visual order.
- Summary data supports decisions without overpowering the operational list.

## Phase 4: Simplify Workflow Screens

### UI-401: Reduce panel nesting and fragmentation

Status: Completed

Deliverables:

- Remove unnecessary panel wrappers.
- Avoid treating every block as its own visual card.
- Keep tables, logs, and metadata sections clearly separated but less boxed-in.

Acceptance criteria:

- Job detail pages feel structured without feeling crowded.
- The number of visually distinct containers per screen is materially lower.

### UI-402: Shorten UI copy where it affects scanability

Status: Completed

Deliverables:

- Shorten long navigation labels.
- Shorten repeated panel titles and helper copy.
- Keep domain accuracy while reducing wordiness.

Acceptance criteria:

- Users can scan navigation and screen structure quickly.
- Text supports the workflow without filling the interface.

## Phase 5: Verify Across Pages

### UI-501: Review all main views after cleanup

Status: Completed

Deliverables:

- Check home, dashboard, history, guide, create-job pages, and job detail pages.
- Verify desktop and smaller-width layouts.
- Confirm yellow remains present but controlled.

Acceptance criteria:

- The UI feels consistent across the app.
- No page still stands out as oversized, crowded, or overly decorative.

## Tracking Rules

- Update `Last updated` whenever this file changes.
- Move ticket status through `Not started`, `In progress`, `Blocked`, and `Completed`.
- Record a short note under `Progress Log` after each meaningful change.
- Keep yellow as a brand accent throughout the work; do not remove UU identity.
- Prefer small reviewable commits per phase.

## Progress Log

### 2026-05-12

- Created the UI density reduction plan after reviewing the current React layout and global styles.
- Confirmed that UU yellow stays, but its usage should move from dominant surface color to controlled accent color.
- Completed Phase 1 by calming the sidebar, shortening navigation labels, flattening global surfaces, and moving yellow into a more restrained accent role.
- Completed Phase 2 by reducing header sizes, removing remaining display-like typography in shared surfaces, and tightening repeated spacing across cards, panels, metrics, lists, forms, tables, and logs.
- Completed Phase 3 by turning the dashboard header into a compact control bar, moving recent jobs ahead of analytics, removing the separate review-flow panel, and reducing the visual weight of the results section relative to the operational queue.
- Completed Phase 4 by removing the workflow banner from job detail, shortening repeated panel titles and helper copy, tightening create-job page copy, and reducing the boxed-in feel of files, results, review, and rollback sections without dropping the UU yellow accent.
- Completed Phase 5 by reviewing home, guide, history, dashboard, create-job, and job-detail screens together, removing redundant workflow action buttons, replacing the remaining hero-like home treatment with a calmer operational header, tightening history summary labels, and confirming the restrained yellow accent stayed consistent across the app.
