from __future__ import annotations

import argparse
import gzip
import json
import os
from pathlib import Path
from urllib.parse import quote, urlencode
from urllib.request import urlopen
import xml.etree.ElementTree as ET

from logging_config import setup_logging

logger = setup_logging("btp")

DEFAULT_SNAPSHOT_DIR = Path("data/openalex-snapshot/institutions")
DEFAULT_OUTPUT_PATH = Path("output/openalex_cache/openalex_institutions_snapshot_by_ror.json")
DEFAULT_STATE_PATH = Path("output/openalex_cache/openalex_institutions_snapshot_state.json")
S3_INSTITUTIONS_PREFIX = "s3://openalex/data/institutions"
S3_BUCKET_URL = "https://openalex.s3.amazonaws.com"
S3_INSTITUTIONS_KEY_PREFIX = "data/institutions/"
DOWNLOAD_CHUNK_SIZE = 1024 * 1024


def clean_ror(value):
    if not value:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def compact_institution(record):
    ids = record.get("ids") if isinstance(record.get("ids"), dict) else {}
    ror = clean_ror(ids.get("ror") or record.get("ror"))
    if not ror:
        return None, None

    compact = {
        "id": record.get("id"),
        "ror": ror,
        "display_name": record.get("display_name"),
        "country_code": record.get("country_code"),
        "type": record.get("type"),
        "homepage_url": record.get("homepage_url"),
        "display_name_acronyms": record.get("display_name_acronyms") or [],
        "display_name_alternatives": record.get("display_name_alternatives") or [],
        "works_count": record.get("works_count"),
        "cited_by_count": record.get("cited_by_count"),
        "ids": {
            "openalex": ids.get("openalex") or record.get("id"),
            "ror": ror,
            "wikidata": ids.get("wikidata"),
        },
        "geo": record.get("geo") or {},
        "updated_date": record.get("updated_date"),
    }
    return ror, compact


def iter_snapshot_records(snapshot_dir):
    for path in sorted(Path(snapshot_dir).rglob("*.gz")):
        logger.info(f"Reading {path}")
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError as exc:
                    logger.warning(f"Skipping invalid JSON line in {path}: {exc}")


def load_existing_lookup(output_path):
    path = Path(output_path)
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return payload if isinstance(payload, dict) else {}


def write_json(path, payload):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(f"{path.suffix}.tmp")
    with open(tmp_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    os.replace(tmp_path, path)


def build_institution_lookup(snapshot_dir, output_path=DEFAULT_OUTPUT_PATH, state_path=DEFAULT_STATE_PATH, merge_existing=False):
    lookup = load_existing_lookup(output_path) if merge_existing else {}
    processed = 0
    with_ror = 0

    for record in iter_snapshot_records(snapshot_dir):
        processed += 1
        ror, compact = compact_institution(record)
        if not ror:
            continue
        lookup[ror] = compact
        with_ror += 1
        if processed % 100000 == 0:
            logger.info(f"Processed {processed} institution record(s), {len(lookup)} unique ROR(s)")

    write_json(output_path, lookup)
    write_json(
        state_path,
        {
            "snapshot_dir": str(snapshot_dir),
            "processed_records": processed,
            "records_with_ror": with_ror,
            "unique_rors": len(lookup),
        },
    )
    logger.info(f"Wrote {len(lookup)} institution ROR record(s) to {output_path}")
    return lookup


def parse_s3_listing(payload):
    root = ET.fromstring(payload)
    namespace = ""
    if root.tag.startswith("{"):
        namespace = root.tag.split("}", 1)[0] + "}"

    keys = []
    for contents in root.findall(f"{namespace}Contents"):
        key = contents.findtext(f"{namespace}Key")
        size_text = contents.findtext(f"{namespace}Size")
        if not key:
            continue
        keys.append({"key": key, "size": int(size_text or 0)})

    next_token = root.findtext(f"{namespace}NextContinuationToken")
    is_truncated = (root.findtext(f"{namespace}IsTruncated") or "").lower() == "true"
    return keys, next_token if is_truncated else None


def list_openalex_institution_objects():
    continuation_token = None
    while True:
        params = {
            "list-type": "2",
            "prefix": S3_INSTITUTIONS_KEY_PREFIX,
        }
        if continuation_token:
            params["continuation-token"] = continuation_token

        url = f"{S3_BUCKET_URL}/?{urlencode(params)}"
        with urlopen(url) as response:
            keys, continuation_token = parse_s3_listing(response.read())

        for item in keys:
            if item["key"].endswith(".gz"):
                yield item

        if not continuation_token:
            break


def download_file(url, destination, expected_size=None):
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = destination.with_suffix(f"{destination.suffix}.tmp")

    with urlopen(url) as response, open(tmp_path, "wb") as handle:
        while True:
            chunk = response.read(DOWNLOAD_CHUNK_SIZE)
            if not chunk:
                break
            handle.write(chunk)

    if expected_size is not None and tmp_path.stat().st_size != expected_size:
        tmp_path.unlink(missing_ok=True)
        raise IOError(f"Downloaded {url} with unexpected size")

    os.replace(tmp_path, destination)


def download_institutions_snapshot(snapshot_dir=DEFAULT_SNAPSHOT_DIR, delete=False):
    snapshot_dir = Path(snapshot_dir)
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Downloading OpenAlex institutions snapshot from public S3 HTTPS endpoint")

    expected_paths = set()
    downloaded = 0
    skipped = 0
    for item in list_openalex_institution_objects():
        key = item["key"]
        relative_path = key.removeprefix(S3_INSTITUTIONS_KEY_PREFIX)
        destination = snapshot_dir / relative_path
        expected_paths.add(destination)

        if destination.exists() and destination.stat().st_size == item["size"]:
            skipped += 1
            continue

        url = f"{S3_BUCKET_URL}/{quote(key)}"
        logger.info(f"Downloading {key} to {destination}")
        download_file(url, destination, expected_size=item["size"])
        downloaded += 1

    if delete:
        for path in snapshot_dir.rglob("*.gz"):
            if path not in expected_paths:
                logger.info(f"Deleting stale snapshot file {path}")
                path.unlink()

    logger.info(f"OpenAlex institutions snapshot download complete: {downloaded} downloaded, {skipped} unchanged")


def main():
    parser = argparse.ArgumentParser(description="Download and compact the OpenAlex institutions snapshot")
    parser.add_argument("--snapshot-dir", default=str(DEFAULT_SNAPSHOT_DIR))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH))
    parser.add_argument("--state", default=str(DEFAULT_STATE_PATH))
    parser.add_argument("--download", action="store_true", help="Download institutions from the public OpenAlex S3 bucket first")
    parser.add_argument("--delete", action="store_true", help="Pass --delete to aws s3 sync when downloading")
    parser.add_argument("--merge-existing", action="store_true", help="Merge parsed records into an existing lookup file")
    args = parser.parse_args()

    snapshot_dir = Path(args.snapshot_dir)
    if args.download:
        download_institutions_snapshot(snapshot_dir, delete=args.delete)
    if not snapshot_dir.exists():
        raise SystemExit(f"Snapshot directory does not exist: {snapshot_dir}")

    build_institution_lookup(
        snapshot_dir,
        output_path=Path(args.output),
        state_path=Path(args.state),
        merge_existing=args.merge_existing,
    )


if __name__ == "__main__":
    main()
