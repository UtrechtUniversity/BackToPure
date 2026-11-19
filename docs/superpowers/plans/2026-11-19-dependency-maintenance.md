# Dependency Maintenance Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans, with a final independent review.

**Goal:** Clear the existing PR backlog with verified fixes and reduce future dependency PR duplication.

**Architecture:** One replacement frontend dependency update from current main, then weekly grouped Dependabot configuration and a concise triage procedure.

**Tech Stack:** Node 22, npm, React 18, Vite/Vitest, GitHub Actions and Dependabot.

**Spec:** `docs/superpowers/specs/2026-11-19-dependency-maintenance-design.md`

## Global Constraints

- No server deployments or Pure writes.
- Preserve the prior frontend browser target and application behavior.
- Do not close old PRs before verified replacement changes are merged.
- No blanket automatic merging.

## Review Focus

- Vite and plugin-react peer versions must agree without force/legacy-peer-deps.
- Tests must continue to exercise the existing UI; do not weaken tests to pass upgrades.
- Clean npm ci must reproduce the lockfile and npm audit must cover development dependencies too.
- Weekly version PR caps do not cap security PRs; document that distinction.
- Each obsolete PR must point to the merged replacement containing its fix.

## Task 1: Replacement dependency update

- [x] Establish baseline tests, exact browser target and npm audit findings.
- [x] Update compatible patched dependency versions and the required Node floor.
- [x] Fix migration-related configuration failures, then verify npm ci, npm test, npm run build and npm audit.
- [ ] Verify the full Python suite and lint; commit the repair.

## Task 2: Recurring maintenance

- [x] Add weekly Dependabot configuration grouping security, toolchain, routing and routine minor/patch updates.
- [x] Document the weekly review and red-CI/superseded-PR procedure and Node setup.
- [ ] Validate configuration, obtain an independent read-only review and push a reviewable replacement PR.

## Task 3: Finish backlog

- [ ] Require green GitHub CI on the exact replacement head, then merge.
- [ ] Close superseded dependency PRs with concrete version/fix references.
- [ ] Update issue #19 and verify remaining open PRs/alerts before reporting completion.
