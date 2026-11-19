"""Complete, read-only Ricgraph collections for legacy and cursor-based APIs."""

from workflow_http import (
    FatalWorkflowError,
    check_page_progress,
    json_object,
    request_read,
)


def read_results(send, url, *, params=None, timeout=30):
    """Collect every page, or fail without returning a partial collection.

    Keep legacy parameters such as max_nr_items for older servers. Ricgraph
    3.5 uses page_size and an opaque cursor returned in response metadata.
    """
    query = dict(params or {})
    query.setdefault("page_size", 250)
    seen_cursors = set()
    if query.get("cursor"):
        seen_cursors.add(query["cursor"])
    seen_pages = set()
    result = []
    while True:
        response = request_read(send, url, params=dict(query), timeout=timeout)
        payload = json_object(response)
        rows = payload.get("results")
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            raise FatalWorkflowError("Invalid Ricgraph results; harvest stopped before returning partial data.")
        meta = payload.get("meta")
        if meta is None:
            meta = {}
        if not isinstance(meta, dict):
            raise FatalWorkflowError("Invalid Ricgraph page metadata; harvest stopped.")
        cursor = meta.get("next_cursor")
        if cursor is not None and not isinstance(cursor, str):
            raise FatalWorkflowError("Invalid Ricgraph continuation cursor; harvest stopped.")
        check_page_progress(rows, seen_pages)
        result.extend(rows)
        if not cursor:
            return result
        if not rows or cursor in seen_cursors:
            raise FatalWorkflowError("Ricgraph pagination made no progress; harvest stopped before returning partial data.")
        seen_cursors.add(cursor)
        query["cursor"] = cursor
