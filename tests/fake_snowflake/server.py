"""The gosnowflake wire protocol over ``http.server``.

Routes (all ``POST``; absolute-form request targets are accepted so the
server can also play an HTTP proxy):

- ``/session/v1/login-request`` → opens a :class:`Session`, returns a token.
  Password logins accept anything; ``AUTHENTICATOR=OAUTH`` must carry a token
  this server's ``/oauth/token`` issued.
- ``/oauth/token`` → client-credentials grant for the configured client.
- ``/queries/v1/query-request`` → ``Session.execute`` on the session named by
  ``Authorization: Snowflake Token="..."``.
- ``/session/token-request``, ``/session`` (delete), ``/telemetry/send`` and
  anything else → ``{"success": true}``.

Everything the driver sends is recorded (``logins``, ``queries``,
``requests``, ``proxied_hosts``, ``token_requests``) so tests can assert on
what reached the "server".
"""

from __future__ import annotations

import base64
import gzip
import json
import os
import secrets
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from tests.fake_snowflake.sqlengine import Database, Result, Session, SnowflakeError

_LOGIN_PARAMETERS = [
    {"name": "TIMESTAMP_OUTPUT_FORMAT", "value": "YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM"},
    {"name": "CLIENT_PREFETCH_THREADS", "value": 4},
    {"name": "CLIENT_RESULT_CHUNK_SIZE", "value": 160},
    {"name": "TIMEZONE", "value": "UTC"},
]


def endpoint_db_kwargs(host: str, port: int) -> dict[str, str]:
    """Driver options that point ``adbc-driver-snowflake`` at ``http://host:port``."""
    return {
        "adbc.snowflake.sql.uri.host": host,
        "adbc.snowflake.sql.uri.port": str(port),
        "adbc.snowflake.sql.uri.protocol": "http",
        "adbc.snowflake.sql.client_option.login_timeout": "8s",
        "adbc.snowflake.sql.client_option.request_timeout": "8s",
        "adbc.snowflake.sql.client_option.ocsp_fail_open_mode": "true",
    }


class FakeSnowflake:
    def __init__(
        self,
        db_path: str | os.PathLike[str],
        *,
        database: str = "FAKEDB",
        schema: str = "PUBLIC",
        oauth_client: tuple[str, str] | None = None,
    ) -> None:
        self.database = Database(db_path, name=database)
        self.schema = schema.upper()
        self.oauth_client = oauth_client
        self._sessions: dict[str, Session] = {}
        self._lock = threading.Lock()
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self.issued_tokens: set[str] = set()
        self.logins: list[dict[str, Any]] = []
        self.queries: list[str] = []
        self.requests: list[tuple[str, dict[str, Any]]] = []
        self.proxied_hosts: list[str] = []
        self.token_requests: list[dict[str, str]] = []

    # ── lifecycle ────────────────────────────────────────────────────

    def start(self) -> FakeSnowflake:
        fake = self

        class Handler(_Handler):
            server_ref = fake

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._server.daemon_threads = True
        self._thread = threading.Thread(
            target=self._server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True
        )
        self._thread.start()
        return self

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        with self._lock:
            for s in self._sessions.values():
                s.close()
            self._sessions.clear()
        self.database.close()

    def __enter__(self) -> FakeSnowflake:
        return self.start()

    def __exit__(self, *exc: object) -> None:
        self.stop()

    @property
    def host(self) -> str:
        return "127.0.0.1"

    @property
    def port(self) -> int:
        assert self._server is not None, "server not started"
        return int(self._server.server_address[1])

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    def db_kwargs(self, host: str | None = None) -> dict[str, str]:
        return endpoint_db_kwargs(host or self.host, self.port)

    def reset_log(self) -> None:
        with self._lock:
            self.logins.clear()
            self.queries.clear()
            self.requests.clear()
            self.proxied_hosts.clear()
            self.token_requests.clear()

    # ── protocol ─────────────────────────────────────────────────────

    def handle(
        self, path: str, query: dict[str, str], headers: Any, body: bytes
    ) -> tuple[int, dict[str, Any]]:
        if path == "/oauth/token":
            return self._oauth_token(headers, body)
        payload: dict[str, Any] = {}
        if body:
            try:
                payload = json.loads(body)
            except ValueError:
                return 400, {"success": False, "message": "malformed JSON body", "code": "000000"}
        if path == "/session/v1/login-request":
            return 200, self._login(query, payload.get("data") or {})
        if path == "/queries/v1/query-request":
            return 200, self._query(headers, payload)
        if path == "/session" and query.get("delete") == "true":
            token = _session_token(headers)
            with self._lock:
                session = self._sessions.pop(token, None)
            if session is not None:
                session.close()
        return 200, {"success": True, "data": None, "code": None, "message": None}

    def _login(self, query: dict[str, str], data: dict[str, Any]) -> dict[str, Any]:
        record = dict(data)
        if record.get("PASSWORD"):
            record["PASSWORD"] = "***"
        record.pop("CLIENT_ENVIRONMENT", None)
        with self._lock:
            self.logins.append(record)
        authenticator = str(data.get("AUTHENTICATOR") or "SNOWFLAKE").upper()
        if authenticator == "OAUTH":
            if data.get("TOKEN") not in self.issued_tokens:
                return _error("Invalid OAuth access token. ", "390303", "08001")
        elif authenticator not in ("SNOWFLAKE", ""):
            return _error(f"Authenticator {authenticator} is not supported by the fake.", "390100")
        elif not data.get("PASSWORD"):
            return _error("Incorrect username or password was specified.", "390100", "08001")
        schema = (query.get("schemaName") or self.schema).upper()
        token = secrets.token_hex(16)
        with self._lock:
            self._sessions[token] = self.database.session(schema)
            session_id = len(self.logins)
        return {
            "success": True,
            "code": None,
            "message": None,
            "data": {
                "token": token,
                "masterToken": "master-" + token,
                "validityInSeconds": 3600,
                "masterValidityInSeconds": 14400,
                "sessionId": session_id,
                "parameters": _LOGIN_PARAMETERS,
                "sessionInfo": {
                    "databaseName": (query.get("databaseName") or self.database.name).upper(),
                    "schemaName": schema,
                    "warehouseName": query.get("warehouse") or "",
                    "roleName": query.get("roleName") or "",
                },
            },
        }

    def _query(self, headers: Any, payload: dict[str, Any]) -> dict[str, Any]:
        token = _session_token(headers)
        with self._lock:
            session = self._sessions.get(token)
            self.requests.append(("/queries/v1/query-request", payload))
            self.queries.append(str(payload.get("sqlText", "")))
        if session is None:
            return _error(
                "Session no longer exists. New login required to access the service.",
                "390104",
                "08001",
            )
        try:
            result = session.execute(str(payload.get("sqlText", "")), payload.get("bindings"))
        except SnowflakeError as exc:
            return _error(str(exc), exc.code, exc.sql_state)
        except Exception as exc:  # noqa: BLE001 - a bug in the fake must not drop the connection
            return _error(f"Internal error in fake Snowflake: {exc!r}", "000603", "XX000")
        return _result_payload(result)

    def _oauth_token(self, headers: Any, body: bytes) -> tuple[int, dict[str, Any]]:
        if self.oauth_client is None:
            return 404, {
                "error": "invalid_request",
                "error_description": "no OAuth client configured",
            }
        auth = headers.get("Authorization", "")
        form = urllib.parse.parse_qs(body.decode())
        flat = {k: v[-1] for k, v in form.items()}
        client_id, client_secret = self.oauth_client
        presented: tuple[str, str] | None = None
        if auth.startswith("Basic "):
            try:
                user, _, pw = base64.b64decode(auth[6:]).decode().partition(":")
                presented = (user, pw)
            except (ValueError, UnicodeDecodeError):
                presented = None
        elif "client_id" in flat:
            presented = (flat["client_id"], flat.get("client_secret", ""))
        if presented != (client_id, client_secret):
            return 401, {"error": "invalid_client"}
        if flat.get("grant_type") != "client_credentials":
            return 400, {"error": "unsupported_grant_type"}
        token = secrets.token_urlsafe(24)
        with self._lock:
            self.issued_tokens.add(token)
            self.token_requests.append({k: v for k, v in flat.items() if k != "client_secret"})
        return 200, {"access_token": token, "token_type": "Bearer", "expires_in": 3600}


def _session_token(headers: Any) -> str:
    auth = headers.get("Authorization", "")
    prefix = 'Snowflake Token="'
    if auth.startswith(prefix) and auth.endswith('"'):
        return auth[len(prefix) : -1]
    return ""


def _error(message: str, code: str, sql_state: str = "42000") -> dict[str, Any]:
    return {
        "success": False,
        "message": message,
        "code": code,
        "data": {"sqlState": sql_state, "errorCode": code, "queryId": secrets.token_hex(8)},
    }


def _result_payload(result: Result) -> dict[str, Any]:
    data: dict[str, Any] = {
        "parameters": [],
        "rowtype": result.rowtype,
        "total": result.total,
        "returned": result.total,
        "queryId": secrets.token_hex(8),
        "chunks": [],
        "statementTypeId": result.statement_type_id,
        "sqlState": "00000",
        "finalDatabaseName": None,
        "finalSchemaName": None,
    }
    if result.rowset_json is not None:
        # DML/DDL: Snowflake answers in JSON; the driver's ExecuteUpdate sums
        # the row counts from ``rowset`` and its query path builds Arrow from it.
        data["queryResultFormat"] = "json"
        data["rowset"] = result.rowset_json
    else:
        data["queryResultFormat"] = "arrow"
        data["rowsetBase64"] = result.rowset_b64
    return {"success": True, "code": None, "message": None, "data": data}


class _Handler(BaseHTTPRequestHandler):
    server_ref: FakeSnowflake
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002 - stdlib signature
        return

    def _read_body(self) -> bytes:
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        if self.headers.get("Content-Encoding", "").lower() == "gzip" and raw:
            raw = gzip.decompress(raw)
        return raw

    def _respond(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:  # noqa: N802 - stdlib naming
        parts = urllib.parse.urlsplit(self.path)
        if parts.scheme and parts.netloc:
            with self.server_ref._lock:
                self.server_ref.proxied_hosts.append(parts.hostname or parts.netloc)
        path = parts.path or "/"
        query = {k: v[-1] for k, v in urllib.parse.parse_qs(parts.query).items()}
        body = self._read_body()
        if path != "/queries/v1/query-request":
            with self.server_ref._lock:
                try:
                    recorded = json.loads(body) if body else {}
                except ValueError:
                    recorded = {}
                self.server_ref.requests.append(
                    (path, recorded if isinstance(recorded, dict) else {})
                )
        status, payload = self.server_ref.handle(path, query, self.headers, body)
        self._respond(status, payload)

    def do_GET(self) -> None:  # noqa: N802 - stdlib naming
        self._respond(200, {"success": True, "message": "fake snowflake"})
