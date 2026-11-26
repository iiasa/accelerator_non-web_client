"""`accli copy --release` end to end, against a real local HTTP server.

Everything except the Xet CAS upload (a Rust binary talking its own protocol) runs for real:
the login state in TinyDB, the refresh-token exchange, the GC ID check, bulk-register and the
release-files call all go over HTTP to a stdlib server on 127.0.0.1. HOME points at tmp_path,
so the tests never touch the user's real ~/.accli (tokens, state.json, Xet cache).
"""
import base64
import importlib
import json
import re
import threading
import types
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from typer.testing import CliRunner

from accli import cli, xet_cache

token_module = importlib.import_module("accli.token")

PROJECT = "demo"
REFRESH_TOKEN = "refresh-token-1"
GC_ID = "gc-42"


def make_jwt(payload: dict) -> str:
    def b64(obj):
        return base64.urlsafe_b64encode(json.dumps(obj).encode()).decode().rstrip("=")
    return f"{b64({'alg': 'none'})}.{b64(payload)}.sig"


ACCESS_TOKEN = make_jwt({"type": "TERM_CLI", "xet_gc_id": GC_ID})


class FakeAcceleratorServer:
    """The Accelerator API endpoints `accli copy` uploads use, with knobs for error cases."""

    def __init__(self):
        self.lock = threading.Lock()
        self.requests = []          # (method, path) of every request received
        self.registered = []        # filenames passed to bulk-register
        self.released = []          # (project_slug, object id) released
        self.next_id = 1
        # Knobs
        self.register_returns_ids = True
        self.release_error = None   # (status, json body) to fail the release with
        self.release_stream_error = None  # "Error: ..." line to append to a 200 release body
        self.ids_to_filename = {}

        server = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _body(self):
                length = int(self.headers.get("Content-Length") or 0)
                return json.loads(self.rfile.read(length) or b"null")

            def _send(self, status, body, content_type="application/json"):
                data = body if isinstance(body, bytes) else (
                    body.encode() if isinstance(body, str) else json.dumps(body).encode())
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_POST(self):
                server.record(self)
                body = self._body()
                if self.path == "/api/v1/oauth/device/access-token/":
                    if body != {"refresh_token": REFRESH_TOKEN}:
                        return self._send(401, {"detail": "bad refresh token"})
                    return self._send(200, {"access_token": ACCESS_TOKEN, "refresh_token": REFRESH_TOKEN})
                if self.path == "/api/xet-cas/v1/cas/bulk-register":
                    return self._send(200, server.bulk_register(body["items"]))
                self._send(404, {"detail": "not found"})

            def do_PUT(self):
                server.record(self)
                match = re.fullmatch(r"/api/v1/projects/([^/]+)/release-files/", self.path)
                if not match:
                    return self._send(404, {"detail": "not found"})
                if self.headers.get("x-authorization") != ACCESS_TOKEN:
                    return self._send(401, {"detail": "Invalid token."})
                body = self._body()
                if set(body) != {"objects_ids_list", "nodes_ids_list"}:
                    return self._send(400, {"detail": f"unexpected body {body}"})
                if server.release_error:
                    status, detail = server.release_error
                    return self._send(status, detail)
                self._send(200, server.release(match.group(1), body["objects_ids_list"]), "text/plain")

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}"

    def record(self, handler):
        with self.lock:
            self.requests.append((handler.command, handler.path))

    def bulk_register(self, items):
        rows = []
        with self.lock:
            for item in items:
                row = {"filename": item["filename"]}
                self.registered.append(item["filename"])
                if self.register_returns_ids:
                    row["id"] = self.next_id
                    self.ids_to_filename[self.next_id] = item["filename"]
                    self.next_id += 1
                rows.append(row)
        return rows

    def release(self, slug, ids):
        lines = []
        with self.lock:
            for object_id in ids:
                filename = self.ids_to_filename.get(object_id)
                if filename is None:
                    lines.append(f"Error: unknown object {object_id}\n")
                    continue
                self.released.append((slug, filename))
                lines.append(f"Releasing {filename} \nReleased  {filename} \n")
        if self.release_stream_error:
            lines.append(self.release_stream_error + "\n")
        return "".join(lines)

    def released_files(self):
        return sorted(f for _, f in self.released)

    def hits(self, path_fragment):
        return [r for r in self.requests if path_fragment in r[1]]


class FakeXetSession:
    """Stands in for hf_xet.XetSession: the CAS upload is a Rust binary with its own protocol."""

    @contextmanager
    def new_upload_commit(self, **kwargs):
        yield self

    def start_upload_file(self, path, sha256):
        with open(path, "rb") as f:
            size = len(f.read())
        info = types.SimpleNamespace(xet_info=types.SimpleNamespace(hash=f"xet-{sha256[:16]}", file_size=size))
        return types.SimpleNamespace(result=lambda: info)


@pytest.fixture
def server():
    srv = FakeAcceleratorServer()
    thread = threading.Thread(target=srv.httpd.serve_forever, daemon=True)
    thread.start()
    yield srv
    srv.httpd.shutdown()
    srv.httpd.server_close()


@pytest.fixture(autouse=True)
def home(monkeypatch, tmp_path, server):
    """A logged-in user whose ~/.accli lives in tmp_path and whose server is the local one."""
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.delenv("SUDO_USER", raising=False)
    monkeypatch.delenv("PKEXEC_UID", raising=False)
    # copy() overwrites HF_XET_CACHE in os.environ; setenv makes monkeypatch restore it afterwards.
    monkeypatch.setenv("HF_XET_CACHE", str(tmp_path / "xet"))
    # Don't reuse an access token cached in-process by an earlier test.
    monkeypatch.setattr(token_module, "_cached_cas_token", None)
    monkeypatch.setattr(token_module, "_cached_access_token", None)
    monkeypatch.setattr(token_module, "_cached_expires_at", 0)
    # The root callback checks PyPI for updates; keep tests offline.
    monkeypatch.setattr(cli, "check_for_updates", lambda: (False, None))
    monkeypatch.setattr(cli.hf_xet, "XetSession", FakeXetSession)

    token_module.save_token_details(REFRESH_TOKEN, server.url, server.url)
    xet_cache.save_gc_id(server.url, GC_ID)  # this machine's cache is in sync with the server
    return home


def copy(*args, exit_code=0):
    result = CliRunner().invoke(cli.app, ["copy", *args], env={"COLUMNS": "200"})
    assert result.exit_code == exit_code, result.output
    return result.output


def write(path, text="data"):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return path


def test_release_on_download_is_rejected_without_contacting_server(server, tmp_path):
    output = copy("acc://demo/data", str(tmp_path / "out"), "--release", exit_code=1)

    assert "only supported when uploading" in output
    assert server.requests == []


def test_upload_without_release_releases_nothing(server, tmp_path):
    src = write(tmp_path / "a.csv")
    copy(str(src), "acc://demo/inputs/")

    assert server.registered == ["demo/inputs/a.csv"]
    assert server.hits("release-files") == []
    assert server.released == []


def test_upload_single_file_with_release(server, tmp_path):
    src = write(tmp_path / "a.csv")
    output = copy(str(src), "acc://demo/inputs/", "--release")

    assert server.released == [(PROJECT, "demo/inputs/a.csv")]
    assert "Released  demo/inputs/a.csv" in output


def test_upload_directory_with_release_releases_every_file(server, tmp_path):
    src = tmp_path / "dir"
    for name in ("a.csv", "b.csv", "sub/c.csv", "sub/deeper/d.csv"):
        write(src / name)

    output = copy(str(src), "acc://demo/x", "--release")

    expected = ["demo/x/a.csv", "demo/x/b.csv", "demo/x/sub/c.csv", "demo/x/sub/deeper/d.csv"]
    assert server.released_files() == expected
    for filename in expected:
        assert f"Released  {filename}" in output


def test_upload_many_files_with_release_releases_all(server, tmp_path):
    src = tmp_path / "many"
    for i in range(150):
        write(src / f"f{i:03}.txt", text=f"file {i}")

    copy(str(src), "acc://demo", "--release")

    assert server.released_files() == [f"demo/f{i:03}.txt" for i in range(150)]


def test_release_forbidden_shows_server_detail(server, tmp_path):
    server.release_error = (403, {"detail": "User not authorized to perform this action."})
    src = write(tmp_path / "a.csv")

    output = copy(str(src), "acc://demo/", "--release", exit_code=1)

    assert "User not authorized to perform this action." in output
    assert server.released == []


def test_release_stream_error_line_fails(server, tmp_path):
    server.release_stream_error = "Error: demo/a.csv is not valid yet"
    src = write(tmp_path / "a.csv")

    output = copy(str(src), "acc://demo/", "--release", exit_code=1)

    assert "Error: demo/a.csv is not valid yet" in output


def test_register_without_ids_fails_and_never_releases(server, tmp_path):
    server.register_returns_ids = False
    src = write(tmp_path / "a.csv")

    output = copy(str(src), "acc://demo/", "--release", exit_code=1)

    assert server.registered == ["demo/a.csv"]
    assert server.hits("release-files") == []
    assert "IDs" in output
