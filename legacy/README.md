# Legacy scripts

Scripts kept here are **not part of the current application**. Nothing in `app/`,
`src/`, the job registry (`app/models/jobs.py`) or the test suite imports them.

They are parked rather than deleted so that any manual workflow still depending on
one can be spotted before it disappears. If nothing has needed them after a release
or two, delete them.

| File | Why it is here |
| --- | --- |
| `personsperpublication.py` | No inbound import, route, or job-registry entry. |
| `pure_api_utils.py` | Older Pure helper code, superseded by the current `src/pure_*` modules. |

Moved 2026-09-17 as part of the legacy cleanup tracked in
`docs/legacy-file-inventory-plan.md`.

Note: `src/merge_external_orgs.py` was **not** moved. The inventory lists it as
"needs review" rather than unused, and it received recent work (a `main()` guard and
output-directory routing). Whether a supported external-org merge command should
exist is still an open question.
