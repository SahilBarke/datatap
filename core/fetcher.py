"""
Fetcher
Handles HTTP requests with auth, custom headers, pagination,
rate-limit retries, and returns raw JSON records.
"""

from __future__ import annotations
import time
import httpx
from core.config_loader import SourceConfig


def _build_headers(config: SourceConfig) -> dict:
    headers = dict(config.headers)
    auth = config.auth
    if auth.type == "bearer" and auth.token:
        headers["Authorization"] = f"Bearer {auth.token}"
    elif auth.type == "api_key" and auth.api_key:
        headers[auth.api_key_header] = auth.api_key
    return headers


def _fetch_page(
    client: httpx.Client,
    url: str,
    method: str,
    headers: dict,
    params: dict,
    retries: int = 3,
) -> dict | list:
    """Fetch a single page with retry on rate limit (429) or server errors."""
    for attempt in range(retries):
        try:
            response = client.request(
                method, url, headers=headers, params=params, timeout=30
            )
            if response.status_code == 429:
                wait = int(response.headers.get("Retry-After", 5))
                print(f"[fetcher] Rate limited. Waiting {wait}s...")
                time.sleep(wait)
                continue
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            if attempt == retries - 1:
                raise
            print(
                f"[fetcher] HTTP error {e.response.status_code}, retrying ({attempt+1}/{retries})..."
            )
            time.sleep(2**attempt)
    raise RuntimeError("Max retries exceeded")


def fetch_all(config: SourceConfig) -> list[dict]:
    """
    Fetch all records from the API defined in config,
    handling pagination automatically.
    Returns a flat list of raw JSON records (before flattening).
    """
    from core.flattener import extract_results

    headers = _build_headers(config)
    params = dict(config.params)
    pagination = config.pagination
    all_records = []

    with httpx.Client() as client:

        if pagination.type == "none":
            raw = _fetch_page(client, config.url, config.method, headers, params)
            records = extract_results(raw, pagination.results_path)
            all_records.extend(records)

        elif pagination.type == "offset":
            params[pagination.limit_param] = pagination.limit
            offset = 0
            for page_num in range(pagination.max_pages):
                params[pagination.offset_param] = offset
                raw = _fetch_page(client, config.url, config.method, headers, params)
                records = extract_results(raw, pagination.results_path)
                if not records:
                    break
                all_records.extend(records)
                offset += pagination.limit
                if len(records) < pagination.limit:
                    break  # Last page

        elif pagination.type == "page":
            for page_num in range(1, pagination.max_pages + 1):
                params[pagination.page_param] = page_num
                raw = _fetch_page(client, config.url, config.method, headers, params)
                records = extract_results(raw, pagination.results_path)
                if not records:
                    break
                all_records.extend(records)
                if len(records) < pagination.limit:
                    break

        elif pagination.type == "cursor":
            cursor = None
            for _ in range(pagination.max_pages):
                if cursor:
                    params[pagination.cursor_param] = cursor
                raw = _fetch_page(client, config.url, config.method, headers, params)
                records = extract_results(raw, pagination.results_path)
                if not records:
                    break
                all_records.extend(records)
                # Extract next cursor
                cursor = _get_nested(raw, pagination.next_cursor_path)
                if not cursor:
                    break

    print(f"[fetcher] Fetched {len(all_records)} records from '{config.name}'")
    return all_records


def _get_nested(obj: dict, path: str | None) -> any:
    """Navigate a dot-notation path in a dict."""
    if not path or not obj:
        return None
    parts = path.split(".")
    current = obj
    for part in parts:
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return None
    return current
