from __future__ import annotations

import argparse
import importlib
import os
import sys
from dataclasses import dataclass
from pathlib import Path

import requests

from config import (
    FACULTY_PREFIX,
    PURE_BASE_URL,
    PURE_HEADERS,
    RIC_BASE_URL,
    config_path,
    validate_pure_uri_config,
)


@dataclass
class CheckResult:
    name: str
    ok: bool
    message: str
    required: bool = True


def run_config_checks() -> list[str]:
    return validate_pure_uri_config()


def check_imports() -> CheckResult:
    modules = ["app", "pandas", "requests"]
    missing = []
    for module in modules:
        try:
            importlib.import_module(module)
        except Exception as exc:
            missing.append(f"{module}: {exc}")
    if missing:
        return CheckResult("imports", False, "; ".join(missing))
    return CheckResult("imports", True, "required Python modules import")


def check_frontend_dist() -> CheckResult:
    frontend_dist = Path(os.environ.get("BTP_FRONTEND_DIST", "frontend/dist"))
    index_html = frontend_dist / "index.html"
    if index_html.exists():
        return CheckResult("frontend build", True, f"found {index_html}", required=False)
    return CheckResult(
        "frontend build",
        False,
        f"missing {index_html}; run `cd frontend && npm run build` for the React UI",
        required=False,
    )


def check_openalex_snapshot() -> CheckResult:
    path = Path(
        os.environ.get(
            "BTP_OPENALEX_INSTITUTIONS_LOOKUP",
            "output/openalex_cache/openalex_institutions_snapshot_by_ror.json",
        )
    )
    if path.exists() and path.stat().st_size > 0:
        return CheckResult("OpenAlex institution snapshot", True, f"found {path}", required=False)
    return CheckResult(
        "OpenAlex institution snapshot",
        False,
        f"missing {path}; run `.venv/bin/python src/snapshot_openalex_institutions.py --download` before external organization jobs",
        required=False,
    )


def check_runtime_paths() -> CheckResult:
    runtime_root = Path(os.environ.get("BTP_RUNTIME_ROOT", "."))
    targets = [
        Path(os.environ.get("BTP_DATA_DIR", runtime_root / "data")),
        Path(os.environ.get("BTP_LOGS_DIR", runtime_root / "logs" / "jobs")),
        runtime_root / "output",
    ]
    missing_parents = [str(path.parent) for path in targets if not path.exists() and not path.parent.exists()]
    unwritable = [str(path) for path in targets if path.exists() and not os.access(path, os.W_OK)]
    if missing_parents or unwritable:
        details = []
        if missing_parents:
            details.append(f"missing parent directories: {', '.join(missing_parents)}")
        if unwritable:
            details.append(f"not writable: {', '.join(unwritable)}")
        return CheckResult("runtime paths", False, "; ".join(details))
    return CheckResult("runtime paths", True, "runtime directories or their parents are writable")


def _get_json(url, *, params=None, headers=None, timeout=10):
    response = requests.get(url, params=params, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response.json()


def check_pure(timeout: int) -> CheckResult:
    try:
        _get_json(PURE_BASE_URL + "persons/", params={"size": 1, "offset": 0}, headers=PURE_HEADERS, timeout=timeout)
    except Exception as exc:
        return CheckResult("Pure API", False, str(exc))
    return CheckResult("Pure API", True, f"reachable at {PURE_BASE_URL}")


def check_ricgraph(timeout: int) -> CheckResult:
    checks = [
        ("organization/search", {"value": FACULTY_PREFIX}),
        ("get_all_personroot_nodes", {"key": "__btp_doctor__", "max_nr_items": "1"}),
        ("get_all_neighbor_nodes", {"key": "__btp_doctor__", "max_nr_items": "1"}),
        ("advanced_search", {"category": "data set", "max_nr_items": "1"}),
        ("person/enrich", {"key": "__btp_doctor__"}),
    ]
    failures = []
    for route, params in checks:
        try:
            payload = _get_json(RIC_BASE_URL + route, params=params, timeout=timeout)
        except Exception as exc:
            failures.append(f"{route}: {exc}")
            continue
        if not isinstance(payload, dict):
            failures.append(f"{route}: response is not a JSON object")
    if failures:
        return CheckResult("Ricgraph API", False, "; ".join(failures))
    return CheckResult("Ricgraph API", True, f"required routes reachable at {RIC_BASE_URL}")


def run_environment_checks(skip_network=False, timeout=10) -> list[CheckResult]:
    results = [
        check_imports(),
        check_frontend_dist(),
        check_openalex_snapshot(),
        check_runtime_paths(),
    ]
    if not skip_network:
        results.extend([check_pure(timeout), check_ricgraph(timeout)])
    return results


def print_results(results: list[CheckResult]) -> None:
    for result in results:
        status = "OK" if result.ok else ("WARN" if not result.required else "FAIL")
        print(f"[{status}] {result.name}: {result.message}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate BackToPure environment prerequisites")
    parser.add_argument(
        "--config-only",
        action="store_true",
        help="Run only local configuration validation checks",
    )
    parser.add_argument(
        "--skip-network",
        action="store_true",
        help="Skip Pure and Ricgraph HTTP reachability checks",
    )
    parser.add_argument("--timeout", type=int, default=10, help="HTTP timeout in seconds for network checks")
    args = parser.parse_args()

    issues = run_config_checks()
    if issues:
        print(f"Configuration check failed for {config_path}:")
        for issue in issues:
            print(f"- {issue}")
        return 1

    if args.config_only:
        print(f"Configuration check passed for {config_path}")
        return 0

    print(f"Configuration check passed for {config_path}")
    results = run_environment_checks(skip_network=args.skip_network, timeout=args.timeout)
    print_results(results)
    return 0 if all(result.ok or not result.required for result in results) else 1


if __name__ == "__main__":
    sys.exit(main())
