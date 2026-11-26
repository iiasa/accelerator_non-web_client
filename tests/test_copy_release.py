"""`accli copy --release` and the release-files API call, against fakes."""
import json
import types
from contextlib import contextmanager

import pytest
from typer.testing import CliRunner

from accli import cli
from accli.AcceleratorTerminalCliProjectService import AcceleratorTerminalCliProjectService, AccAPIError


class FakeService:
    def __init__(self, release_output="", error=None):
        self.release_output = release_output
        self.error = error
        self.release_calls = []

    def release_files(self, project_slug, objects_ids_list, nodes_ids_list=None):
        self.release_calls.append((project_slug, list(objects_ids_list)))
        if self.error:
            raise self.error
        return self.release_output


class FakeXetSession:
    """Stands in for hf_xet.XetSession: every file "uploads" instantly."""

    @contextmanager
    def new_upload_commit(self, **kwargs):
        yield self

    def start_upload_file(self, path, sha256):
        size = len(open(path, "rb").read())
        info = types.SimpleNamespace(xet_info=types.SimpleNamespace(hash=f"hash-{sha256[:8]}", file_size=size))
        return types.SimpleNamespace(result=lambda: info)


class FakeResponse:
    def __init__(self, data):
        self.data = data

    def raise_for_status(self):
        pass

    def json(self):
        return self.data


class FakeRegister:
    """Fake bulk-register endpoint: hands out ids in order, one per registered item."""

    def __init__(self, ids=True):
        self.ids = ids
        self.next_id = 100
        self.calls = []

    def __call__(self, url, json, headers, verify):
        self.calls.append(dict(url=url, items=json["items"], headers=headers))
        rows = []
        for item in json["items"]:
            row = dict(filename=item["filename"])
            if self.ids:
                row["id"] = self.next_id
                self.next_id += 1
            rows.append(row)
        return FakeResponse(rows)


@pytest.fixture
def env(monkeypatch, tmp_path):
    """Patch every network/auth/xet touchpoint of `accli copy`; record what was called."""
    calls = dict(auth=0)

    def exchange(slug):
        calls["auth"] += 1
        return ("cas-token", "access-token", 0)

    register = FakeRegister()
    monkeypatch.setattr(cli, "get_server_url", lambda: "https://acc.example")
    monkeypatch.setattr(cli, "exchange_refresh_token", exchange)
    monkeypatch.setattr(cli.xet_cache, "xet_cache_dir", lambda server_url: tmp_path / "xet")
    monkeypatch.setattr(cli.xet_cache, "check_gc_id", lambda server_url, token: None)
    monkeypatch.setattr(cli, "hf_xet", types.SimpleNamespace(XetSession=FakeXetSession))
    monkeypatch.setattr(cli.requests, "post", lambda *a, **kw: env.register(*a, **kw))
    # copy() writes HF_XET_CACHE into os.environ; setenv makes monkeypatch restore it afterwards.
    monkeypatch.setenv("HF_XET_CACHE", str(tmp_path / "xet"))

    env = types.SimpleNamespace(calls=calls, register=register, service=FakeService(), tmp=tmp_path)
    monkeypatch.setattr(cli, "AcceleratorTerminalCliProjectService", lambda **kw: env.service)
    return env


@pytest.fixture(autouse=True)
def cache_in_use(monkeypatch):
    class Marker:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            pass

    monkeypatch.setattr(cli.xet_cache, "CacheInUse", Marker)


def copy(*args, exit_code=0):
    result = CliRunner().invoke(cli.app, ["copy", *args], env={"COLUMNS": "200"})
    assert result.exit_code == exit_code, result.output
    return result.output


def write(path, text="data"):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return path


def test_release_on_download_fails_before_any_call(monkeypatch, tmp_path):
    monkeypatch.setattr(cli, "get_server_url", lambda: pytest.fail("server url looked up"))
    monkeypatch.setattr(cli, "exchange_refresh_token", lambda slug: pytest.fail("auth called"))
    monkeypatch.setattr(cli, "AcceleratorTerminalCliProjectService", lambda **kw: pytest.fail("API called"))
    monkeypatch.setattr(cli.requests, "post", lambda *a, **kw: pytest.fail("network called"))

    output = copy("acc://demo/data", str(tmp_path / "out"), "--release", exit_code=1)
    assert "--release is only supported when uploading to acc://" in output


def test_upload_without_release_does_not_release(env):
    src = write(env.tmp / "a.csv")
    output = copy(str(src), "acc://demo/inputs/", exit_code=0)

    assert env.service.release_calls == []
    assert env.register.calls[0]["items"][0]["filename"] == "demo/inputs/a.csv"
    assert "Upload completed successfully" in output
    assert "Releas" not in output


def test_upload_single_file_with_release(env):
    env.service.release_output = "Releasing demo/inputs/a.csv \nReleased  demo/inputs/a.csv \n"
    src = write(env.tmp / "a.csv")
    output = copy(str(src), "acc://demo/inputs/", "--release")

    assert env.service.release_calls == [("demo", [100])]
    assert "Releasing 1 file(s)..." in output
    assert "Released  demo/inputs/a.csv" in output
    assert "[OK] Released 1 file(s) successfully!" in output
    assert output.index("Upload completed successfully") < output.index("Releasing 1 file(s)")


def test_upload_directory_with_release_releases_all_ids_once(env):
    src = env.tmp / "dir"
    for name in ("a.csv", "b.csv", "sub/c.csv"):
        write(src / name)
    env.service.release_output = "\n".join(f"Released  demo/x/{n}" for n in ("a.csv", "b.csv", "sub/c.csv"))

    output = copy(str(src), "acc://demo/x", "--release")

    assert len(env.service.release_calls) == 1
    slug, ids = env.service.release_calls[0]
    assert slug == "demo" and sorted(ids) == [100, 101, 102]
    registered = {i["filename"] for c in env.register.calls for i in c["items"]}
    assert registered == {"demo/x/a.csv", "demo/x/b.csv", "demo/x/sub/c.csv"}
    assert "[OK] Released 3 file(s) successfully!" in output


def test_ids_collected_across_batches(env):
    src = env.tmp / "dir"
    for i in range(150):  # more than MAX_FILES_PER_BATCH (100)
        write(src / f"f{i:03}.txt")

    copy(str(src), "acc://demo", "--release")

    assert len(env.register.calls) == 2
    assert len(env.service.release_calls) == 1
    assert sorted(env.service.release_calls[0][1]) == list(range(100, 250))


def test_release_api_error_shows_detail(env):
    class Res:
        data = b'{"detail": "Only project admins can release files."}'

    env.service.error = AccAPIError("forbidden", Res(), 403)
    src = write(env.tmp / "a.csv")
    output = copy(str(src), "acc://demo/", "--release", exit_code=1)

    assert env.service.release_calls == [("demo", [100])]
    assert "Failed to release files: Only project admins can release files." in output
    assert "Released 1 file(s) successfully" not in output


def test_release_without_ids_from_register_fails(env):
    env.register.ids = False
    src = write(env.tmp / "a.csv")
    output = copy(str(src), "acc://demo/", "--release", exit_code=1)

    assert env.service.release_calls == []
    assert "did not return any file IDs" in output


def test_release_stream_error_line_fails(env):
    env.service.release_output = "Releasing demo/a.csv \nError: demo/a.csv is not valid yet\n"
    src = write(env.tmp / "a.csv")
    output = copy(str(src), "acc://demo/", "--release", exit_code=1)

    assert "Releasing demo/a.csv" in output
    assert "Error: demo/a.csv is not valid yet" in output
    assert "successfully!" not in output.split("Upload completed successfully!")[1]


def test_release_files_sends_put_with_ids():
    sent = {}

    class FakeHttp:
        def request(self, method, url, **kwargs):
            sent.update(method=method, url=url, **kwargs)
            return types.SimpleNamespace(status=200, data=b"Releasing demo/a.csv \nReleased  demo/a.csv \n")

    service = AcceleratorTerminalCliProjectService(user_token="tok", server_url="https://acc.example/")
    service.http_client = FakeHttp()

    text = service.release_files("demo", (7, 8))

    assert sent["method"] == "PUT"
    assert sent["url"] == "https://acc.example/api/v1/projects/demo/release-files/"
    assert sent["headers"] == {"x-authorization": "tok", "Content-Type": "application/json"}
    body = sent["json"] if "json" in sent else json.loads(sent["body"])
    assert body == {"objects_ids_list": [7, 8], "nodes_ids_list": []}
    assert text == "Releasing demo/a.csv \nReleased  demo/a.csv \n"


def test_release_files_passes_node_ids_and_raises_on_error():
    class FakeHttp:
        def request(self, method, url, **kwargs):
            self.kwargs = kwargs
            return types.SimpleNamespace(status=403, data=b'{"detail": "nope"}')

    service = AcceleratorTerminalCliProjectService(user_token="tok", server_url="https://acc.example")
    service.http_client = FakeHttp()

    with pytest.raises(AccAPIError) as err:
        service.release_files("demo", [1], nodes_ids_list=[5])
    assert err.value.status_code == 403
    kwargs = service.http_client.kwargs
    body = kwargs["json"] if "json" in kwargs else json.loads(kwargs["body"])
    assert body == {"objects_ids_list": [1], "nodes_ids_list": [5]}
