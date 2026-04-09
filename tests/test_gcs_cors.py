"""Regression tests for GCS bucket CORS settings and public access.

These tests verify that:
1. The GCS bucket is publicly readable (no auth required).
2. The CORS policy allows GET/HEAD with Range requests from any origin.
3. Range requests work correctly (partial content delivery).
"""

from __future__ import annotations

import io

import pytest
import requests  # type: ignore[import-untyped]

GCS_BASE = (
    "https://storage.googleapis.com/idc-index-data-artifacts/current/release_artifacts"
)
# Small file to keep tests fast; idc_index.parquet is the canonical artefact.
TEST_FILE = f"{GCS_BASE}/idc_index.parquet"

# Origin sent in CORS preflight / simple requests
TEST_ORIGIN = "https://example.com"


def _get(url: str, **kwargs) -> requests.Response:
    return requests.get(url, timeout=30, **kwargs)


def _head(url: str, **kwargs) -> requests.Response:
    return requests.head(url, timeout=30, **kwargs)


# ---------------------------------------------------------------------------
# Public access
# ---------------------------------------------------------------------------


def test_public_get_returns_200():
    """Bucket must be publicly readable without credentials."""
    resp = _get(TEST_FILE)
    assert resp.status_code == 200, (
        f"Expected 200, got {resp.status_code}. Bucket may not be publicly readable."
    )


def test_public_head_returns_200():
    """HEAD must work without credentials."""
    resp = _head(TEST_FILE)
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}."


def test_content_type_is_parquet():
    """Sanity-check that we're fetching a parquet file."""
    resp = _head(TEST_FILE)
    assert resp.status_code == 200
    ct = resp.headers.get("Content-Type", "")
    # GCS serves parquet as application/octet-stream or application/x-parquet
    assert ct != "", "Content-Type header must be present"


def test_accept_ranges_header_present():
    """Server must advertise byte-range support."""
    resp = _head(TEST_FILE)
    assert resp.status_code == 200
    assert resp.headers.get("Accept-Ranges", "").lower() == "bytes", (
        "Accept-Ranges: bytes header is required for range-request support"
    )


# ---------------------------------------------------------------------------
# Range requests
# ---------------------------------------------------------------------------


def test_range_request_returns_206():
    """Partial GET (Range: bytes=0-1023) must return 206 Partial Content."""
    resp = _get(TEST_FILE, headers={"Range": "bytes=0-1023"})
    assert resp.status_code == 206, (
        f"Expected 206, got {resp.status_code}. Range requests may be broken."
    )
    assert len(resp.content) > 0, "Range response body must not be empty"


def test_range_response_has_content_range_header():
    """206 response must include Content-Range header."""
    resp = _get(TEST_FILE, headers={"Range": "bytes=0-1023"})
    assert resp.status_code == 206
    cr = resp.headers.get("Content-Range", "")
    assert cr.startswith("bytes 0-1023/"), f"Unexpected Content-Range: {cr!r}"


# ---------------------------------------------------------------------------
# CORS — simple requests
# ---------------------------------------------------------------------------


def test_cors_simple_get_exposes_origin():
    """GET with Origin must return Access-Control-Allow-Origin: *."""
    resp = _get(TEST_FILE, headers={"Origin": TEST_ORIGIN})
    assert resp.status_code == 200
    acao = resp.headers.get("Access-Control-Allow-Origin", "")
    assert acao == "*", f"Access-Control-Allow-Origin should be '*', got {acao!r}"


def test_cors_simple_get_exposes_content_range():
    """GET response must expose Content-Range in Access-Control-Expose-Headers."""
    resp = _get(
        TEST_FILE,
        headers={"Origin": TEST_ORIGIN, "Range": "bytes=0-1023"},
    )
    assert resp.status_code == 206
    expose = resp.headers.get("Access-Control-Expose-Headers", "")
    exposed = {h.strip().lower() for h in expose.split(",")}
    assert "content-range" in exposed, (
        f"Content-Range must be in Access-Control-Expose-Headers, got: {expose!r}"
    )


# ---------------------------------------------------------------------------
# CORS — preflight
# ---------------------------------------------------------------------------


def test_cors_preflight_returns_204_or_200():
    """OPTIONS preflight must succeed."""
    resp = requests.options(
        TEST_FILE,
        headers={
            "Origin": TEST_ORIGIN,
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Headers": "Range",
        },
        timeout=30,
    )
    assert resp.status_code in (200, 204), (
        f"Preflight should return 200 or 204, got {resp.status_code}"
    )


def test_cors_preflight_allows_get():
    """Preflight must allow GET method."""
    resp = requests.options(
        TEST_FILE,
        headers={
            "Origin": TEST_ORIGIN,
            "Access-Control-Request-Method": "GET",
        },
        timeout=30,
    )
    allow_methods = resp.headers.get("Access-Control-Allow-Methods", "")
    assert "GET" in allow_methods.upper(), (
        f"Access-Control-Allow-Methods should include GET, got: {allow_methods!r}"
    )


def test_cors_preflight_allows_range_header():
    """Preflight must allow the Range request header."""
    resp = requests.options(
        TEST_FILE,
        headers={
            "Origin": TEST_ORIGIN,
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Headers": "Range",
        },
        timeout=30,
    )
    allow_headers = resp.headers.get("Access-Control-Allow-Headers", "")
    assert "range" in allow_headers.lower(), (
        f"Access-Control-Allow-Headers should include Range, got: {allow_headers!r}"
    )


def test_cors_max_age_is_set():
    """Preflight should return Access-Control-Max-Age to allow caching."""
    resp = requests.options(
        TEST_FILE,
        headers={
            "Origin": TEST_ORIGIN,
            "Access-Control-Request-Method": "GET",
        },
        timeout=30,
    )
    max_age = resp.headers.get("Access-Control-Max-Age", "")
    assert max_age != "", (
        "Access-Control-Max-Age should be set to allow preflight caching"
    )
    assert int(max_age) > 0, (
        f"Access-Control-Max-Age should be positive, got {max_age!r}"
    )


# ---------------------------------------------------------------------------
# End-to-end: download and parse parquet via range requests
# ---------------------------------------------------------------------------


def test_parquet_readable_via_range_requests():
    """Download the full file and verify it can be parsed as parquet."""
    resp = _get(TEST_FILE)
    assert resp.status_code == 200
    df = pytest.importorskip("pandas").read_parquet(io.BytesIO(resp.content))
    assert not df.empty, "idc_index.parquet should contain rows"
