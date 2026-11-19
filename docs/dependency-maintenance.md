# Dependency and pull-request maintenance

Dependabot checks `/frontend` every Monday at 09:00 Europe/Amsterdam. Related
Vite/Vitest/React-plugin updates share one PR; routing updates share another;
remaining minor/patch version updates are grouped. At most three version-update
PRs are open at once. Major updates outside those related groups remain separate
so their migration requirements can be reviewed explicitly.

Security updates are grouped separately. They are event-driven rather than held
until Monday, and have a separate GitHub limit: the three-PR cap applies only to
version updates. Grouping does not approve or merge a change automatically.

CI runs `npm audit` on every PR, including development dependencies, so known
vulnerabilities block merging until addressed. The weekly maintainer review
still checks alerts and PRs that have not had a fresh CI run.

## Weekly triage

The repository maintainer reviews the open queue each week:

1. Check security alerts and the latest CI result on the current PR head. Prioritize
   critical/high findings; do not rely on CI from an old base branch.
2. Compare overlapping updates. Update related tools together when their peer
   requirements demand it. Keep one active replacement rather than merging
   several stale lockfiles independently.
3. Give every PR a concrete next action: review and merge, repair, or close as
   superseded. Investigate red CI within the week and record the cause and owner
   in the PR or a linked issue. The PR author owns a manual repair; a maintainer
   owns follow-up on bot PRs.
4. Before merging, review the diff and run `npm ci`, `npm test`, `npm run build`,
   `npm audit`, Python tests and the CI lint command. Changes to test/build tools
   must preserve the behavior exercised by the tests. Do not use `--force` or
   `--legacy-peer-deps` to mask incompatible peer requirements.
5. Merge only after the exact reviewed head has green backend and frontend CI.
   Close an older PR only once a merged replacement contains its fix; leave a
   comment linking that replacement and the versions/fixes it contains.
6. Check the open queue and security alerts again after merging. Record any
   unresolved finding with a next action rather than leaving it unattended.

For the frontend, use Node 22.12+ on the supported Node 22 line, Node 24, or Node
26+. `frontend/.nvmrc` selects Node 22, matching CI. React remains on version 18.
The frontend build keeps the previous Vite 5 browser target explicitly.

Useful commands:

```bash
gh pr list --repo UtrechtUniversity/BackToPure
cd frontend
nvm use
npm ci
npm test
npm run build
npm audit
```

For backend verification, from the repository root:

```bash
BTP_CONFIG_PATH=src/config.example.ini .venv/bin/python -m pytest -q
.venv/bin/python -m ruff check --select I,F app tests BackToPure.py
```
