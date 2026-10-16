"""Resilient REST API ingestion template.

Expected API response:
{
  "data": [{"id": "stable-source-id", "...": "..."}],
  "next_cursor": "opaque-cursor-or-null"
}

Required environment variables:
  API_URL      HTTPS endpoint without query parameters
  API_TOKEN    Bearer token
  DATABASE_URL PostgreSQL DSN, e.g. postgresql://user:pass@host:5432/dbname
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit

# pyrefly: ignore [missing-import]
import psycopg
import requests

# pyrefly: ignore [missing-import]
from psycopg.types.json import Jsonb
from requests.adapters import HTTPAdapter
from urllib3.util import Retry


CONNECT_TIMEOUT_SECONDS = 3.05
READ_TIMEOUT_SECONDS = 30
MAX_PAGES = 100_000
RETRYABLE_STATUS_CODES = (429, 500, 502, 503, 504)
UPSERT_SQL = """
INSERT INTO api_items (source_id, payload)
VALUES (%s, %s::jsonb)
ON CONFLICT (source_id)
DO UPDATE SET
    payload = EXCLUDED.payload,
    last_changed_at = NOW()
WHERE api_items.payload IS DISTINCT FROM EXCLUDED.payload
"""


class DataIngestionError(RuntimeError):
    """Base exception for an ingestion job failure."""


class AuthenticationError(DataIngestionError):
    """The source API rejected authentication or authorization."""


class ConfigurationError(DataIngestionError):
    """Required configuration is missing or invalid."""


class JsonFormatter(logging.Formatter):
    """Emit one structured JSON object per log event."""

    EXTRA_FIELDS = (
        "event",
        "endpoint",
        "page",
        "records",
        "status_code",
        "cursor_present",
    )

    def format(self, record: logging.LogRecord) -> str:
        timestamp = (
            datetime.fromtimestamp(record.created, tz=timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z")
        )
        payload: dict[str, Any] = {
            "timestamp": timestamp,
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for field in self.EXTRA_FIELDS:
            value = getattr(record, field, None)
            if value is not None:
                payload[field] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def configure_logging() -> logging.Logger:
    root = logging.getLogger()
    root.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
    root.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root.addHandler(handler)
    return logging.getLogger("data_ingestion")


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ConfigurationError(f"Required environment variable {name} is missing")
    return value


def validate_api_url(api_url: str) -> str:
    parsed = urlsplit(api_url)
    if parsed.scheme.lower() != "https" or not parsed.netloc:
        raise ConfigurationError("API_URL must be a valid HTTPS URL")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ConfigurationError(
            "API_URL must not include credentials, query parameters, or a fragment; "
            "pass query parameters separately"
        )
    return parsed.path or "/"


def build_session(api_token: str) -> requests.Session:
    retry_policy = Retry(
        total=5,
        connect=3,
        read=3,
        status=5,
        other=0,
        backoff_factor=1.0,
        backoff_jitter=0.3,
        status_forcelist=RETRYABLE_STATUS_CODES,
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
        raise_on_status=True,
    )
    adapter = HTTPAdapter(
        max_retries=retry_policy,
        pool_connections=10,
        pool_maxsize=10,
    )

    session = requests.Session()
    session.headers.update(
        {
            "Authorization": f"Bearer {api_token}",
            "Accept": "application/json",
            "User-Agent": "resilient-data-ingestor/1.0",
        }
    )
    session.mount("https://", adapter)
    return session


def fetch_page(
    session: requests.Session,
    api_url: str,
    cursor: str | None,
    endpoint: str,
    logger: logging.Logger,
    page_number: int,
) -> dict[str, Any]:
    params = {"cursor": cursor} if cursor is not None else None

    try:
        response = session.get(
            api_url,
            params=params,
            timeout=(CONNECT_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS),
        )
    except requests.exceptions.RetryError as exc:
        raise DataIngestionError("API retry budget exhausted") from exc
    except requests.exceptions.RequestException as exc:
        raise DataIngestionError(
            f"API network request failed ({type(exc).__name__})"
        ) from exc

    if response.status_code in (401, 403):
        raise AuthenticationError(
            f"API authentication/authorization failed (HTTP {response.status_code})"
        )

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as exc:
        raise DataIngestionError(
            f"API returned non-success HTTP status {response.status_code}"
        ) from exc

    try:
        body = response.json()
    except ValueError as exc:
        raise DataIngestionError("API response was not valid JSON") from exc

    if not isinstance(body, dict) or not isinstance(body.get("data"), list):
        raise DataIngestionError(
            "Unexpected API schema: expected an object containing a data list"
        )

    logger.info(
        "Fetched API page",
        extra={
            "event": "api_page_fetched",
            "endpoint": endpoint,
            "page": page_number,
            "records": len(body["data"]),
            "status_code": response.status_code,
            "cursor_present": cursor is not None,
        },
    )
    return body


def ensure_schema(conn: psycopg.Connection[Any]) -> None:
    with conn.transaction():
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS api_items (
                source_id TEXT PRIMARY KEY,
                payload JSONB NOT NULL,
                first_ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )


def upsert_page(
    conn: psycopg.Connection[Any],
    records: list[Any],
    logger: logging.Logger,
    page_number: int,
) -> int:
    # Validate and serialize the whole page before opening its transaction.
    # Duplicate IDs with conflicting payloads are treated as a source-quality error.
    rows_by_id: dict[str, str] = {}
    for record in records:
        if not isinstance(record, dict):
            raise DataIngestionError("An API record was not a JSON object")

        raw_id = record.get("id")
        if raw_id is None or str(raw_id).strip() == "":
            raise DataIngestionError("An API record is missing its stable id field")

        source_id = str(raw_id).strip()
        try:
            payload_json = json.dumps(
                record, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            )
        except (TypeError, ValueError) as exc:
            raise DataIngestionError(
                "An API record could not be serialized as JSON"
            ) from exc

        previous_payload = rows_by_id.get(source_id)
        if previous_payload is not None and previous_payload != payload_json:
            raise DataIngestionError(
                "Conflicting records with the same source id occurred in one API page"
            )
        rows_by_id[source_id] = payload_json

    rows = [
        (source_id, Jsonb(json.loads(payload)))
        for source_id, payload in rows_by_id.items()
    ]
    if not rows:
        return 0

    # With autocommit enabled on the connection, this is a real transaction per page.
    # A failed page rolls back as a unit; pages committed earlier remain safe to rerun.
    with conn.transaction():
        with conn.cursor() as cursor:
            cursor.executemany(UPSERT_SQL, rows)

    if len(rows) < len(records):
        logger.warning(
            "Duplicate identical IDs were deduplicated within the API page",
            extra={
                "event": "duplicate_records_deduplicated",
                "page": page_number,
                "records": len(records) - len(rows),
            },
        )
    return len(rows)


def ingest(
    session: requests.Session,
    conn: psycopg.Connection[Any],
    api_url: str,
    endpoint: str,
    logger: logging.Logger,
) -> int:
    cursor: str | None = None
    seen_cursors: set[str] = set()
    total_records = 0

    for page_number in range(1, MAX_PAGES + 1):
        body = fetch_page(session, api_url, cursor, endpoint, logger, page_number)
        records = body["data"]
        processed = upsert_page(conn, records, logger, page_number)
        total_records += processed

        logger.info(
            "Committed API page to the database",
            extra={
                "event": "database_page_committed",
                "page": page_number,
                "records": processed,
            },
        )

        next_cursor = body.get("next_cursor")
        if next_cursor is None or next_cursor == "":
            logger.info(
                "API pagination completed",
                extra={"event": "pagination_completed", "records": total_records},
            )
            return total_records

        if isinstance(next_cursor, bool) or not isinstance(next_cursor, (str, int)):
            raise DataIngestionError(
                "API next_cursor must be a string, integer, or null"
            )

        cursor = str(next_cursor)
        if cursor in seen_cursors:
            raise DataIngestionError(
                "API repeated a pagination cursor; refusing an infinite loop"
            )
        seen_cursors.add(cursor)

    raise DataIngestionError(
        f"API pagination exceeded the safety limit of {MAX_PAGES} pages"
    )


def main() -> int:
    logger = configure_logging()
    try:
        api_url = required_env("API_URL")
        endpoint = validate_api_url(api_url)
        api_token = required_env("API_TOKEN")
        database_url = required_env("DATABASE_URL")

        logger.info(
            "Starting API ingestion",
            extra={"event": "pipeline_started", "endpoint": endpoint},
        )
        with build_session(api_token) as session:
            with psycopg.connect(database_url, autocommit=True) as conn:
                ensure_schema(conn)
                total = ingest(session, conn, api_url, endpoint, logger)

        logger.info(
            "API ingestion completed successfully",
            extra={"event": "pipeline_succeeded", "records": total},
        )
        return 0
    except AuthenticationError:
        logger.exception(
            "API authentication/authorization failed", extra={"event": "auth_failed"}
        )
        return 2
    except DataIngestionError:
        logger.exception("Data ingestion failed", extra={"event": "ingestion_failed"})
        return 1
    except Exception:
        # Unexpected bugs and infrastructure failures must still be visible with a traceback.
        logger.exception(
            "Unexpected fatal pipeline error", extra={"event": "fatal_error"}
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
