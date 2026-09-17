import argparse
import logging

from logging_config import setup_logging
import enrich_pure_external_persons as enrich
from openalex_cache import fetch_openalex_works_cached, extract_rors_from_openalex_works, fetch_openalex_institutions_cached

logger = setup_logging("btp", level=logging.INFO)


def main(faculty_choice):
    logger.info("Script to prefetch OpenAlex metadata cache has started")
    faculties = enrich.select_faculties(faculty_choice)
    logger.info(f"Selected {len(faculties)} faculty key(s)")

    researchoutputs = enrich.select_persons_researchoutput(faculties)
    dois = [entry.get("doi") for entry in researchoutputs if entry.get("doi")]
    logger.info(f"Prepared {len(dois)} DOI(s) from Ricgraph for OpenAlex cache")

    works = fetch_openalex_works_cached(dois, batch_size=10, request_delay=1.5, force_refresh=False)
    by_doi = works.get("by_doi", {})
    logger.info(f"OpenAlex works cache now contains {len(by_doi)} DOI record(s) for this selection")

    rors = extract_rors_from_openalex_works(by_doi)
    logger.info(f"Extracted {len(rors)} unique ROR(s) from cached OpenAlex works")

    institutions = fetch_openalex_institutions_cached(rors, chunk_size=20, request_delay=1.0, force_refresh=False)
    logger.info(f"OpenAlex institutions cache returned {len(institutions.get('results', []))} record(s) for this selection")
    logger.info("Script to prefetch OpenAlex metadata cache has ended")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prefetch OpenAlex works/institutions cache from Ricgraph DOIs")
    parser.add_argument(
        "faculty_choice",
        type=str,
        nargs="?",
        default="all",
        help='Faculty choice or "all"',
    )
    args = parser.parse_args()
    main(args.faculty_choice)
