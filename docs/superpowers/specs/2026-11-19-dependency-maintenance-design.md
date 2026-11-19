# Dependency backlog and recurring maintenance

The user approved finishing PR #41, cleaning the ten remaining dependency PRs and establishing weekly maintenance. PR #41 has been merged with green CI. All ten old PRs modify only frontend dependency manifests/locks; several overlap, and their suggested versions do not resolve all current advisories.

Use one tested replacement update on current main, rather than merging ten stale dependency trees. Upgrade Vite, its matching React plugin, Vitest and React Router together to compatible patched releases; refresh remaining vulnerable transitive dependencies. Keep React 18 and existing application behavior, preserve the prior browser build target, and make the Node floor explicit. The old Vite/Vitest PR fails because plugin-react 4 excludes Vite 8.

Verify a clean npm install, all frontend tests, production build, full backend tests and npm audit. Obtain an independent review and green GitHub CI before merging the replacement. Close old PRs as superseded only after their fixes are represented in merged main. Update security issue #19 with actual evidence.

Add weekly Dependabot checks, group related toolchain/router updates and security updates, and cap version-update PRs at three. Add a short maintenance procedure: review weekly, investigate red CI within the week, merge reviewed green work, and close superseded work with links. No permanent auto-merge rule, deployment, or changes to the server checkout are part of this maintenance task.
