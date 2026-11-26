"""`accli rename` against a fake API."""
import pytest
from typer.testing import CliRunner

from accli import cli
from accli.AcceleratorTerminalCliProjectService import (
    AccAPIError,
    AcceleratorTerminalCliProjectService,
    validate_rename_segment,
)


class FakeService:
    def __init__(self, items=None, error=None):
        self.items = items or {"nodes": [], "objects": []}
        self.error = error
        self.calls = []

    def list_node_items(self, project_slug, absolute_node_path):
        self.calls.append(("list", project_slug, absolute_node_path))
        if self.error and self.error[0] == "list":
            raise self.error[1]
        return self.items

    def rename_file(self, project_slug, object_id, new_name):
        self.calls.append(("rename_file", project_slug, object_id, new_name))
        if self.error and self.error[0] == "rename":
            raise self.error[1]
        return True

    def rename_folder(self, project_slug, node_id, new_name):
        self.calls.append(("rename_folder", project_slug, node_id, new_name))
        if self.error and self.error[0] == "rename":
            raise self.error[1]
        return True


def run(monkeypatch, service, *args, exit_code=0):
    monkeypatch.setenv("ACCLI_NO_UPDATE_CHECK", "1")
    monkeypatch.setattr(cli, "exchange_refresh_token", lambda slug: ("", "token", 0))
    monkeypatch.setattr(cli, "validate_project_slug", lambda server_url, slug: None)
    monkeypatch.setattr(cli, "get_server_url", lambda: "https://acc.example")
    monkeypatch.setattr(cli, "AcceleratorTerminalCliProjectService", lambda **kw: service)
    result = CliRunner().invoke(cli.app, ["rename", *args], env={"COLUMNS": "200", "ACCLI_NO_UPDATE_CHECK": "1"})
    assert result.exit_code == exit_code, result.output
    return result.output


def test_rename_help():
    result = CliRunner().invoke(cli.app, ["rename", "--help"], env={"ACCLI_NO_UPDATE_CHECK": "1"})
    assert result.exit_code == 0
    assert "Rename a remote file or folder in place." in result.output


def test_rename_file_auto_detect(monkeypatch):
    service = FakeService(items={"nodes": [], "objects": [{"id": 42, "filename": "demo/dir/old.csv"}]})
    output = run(monkeypatch, service, "acc://demo/dir/old.csv", "new.csv")

    assert service.calls == [
        ("list", "demo", "demo/dir"),
        ("rename_file", "demo", 42, "new.csv"),
    ]
    assert "Renamed file 'dir/old.csv' -> 'dir/new.csv'" in output


def test_rename_folder_auto_detect(monkeypatch):
    service = FakeService(items={"nodes": [{"id": 7, "absolute_node_name": "demo/old_dir"}], "objects": []})
    output = run(monkeypatch, service, "acc://demo/old_dir/", "new_dir")

    assert service.calls == [
        ("list", "demo", "demo"),
        ("rename_folder", "demo", 7, "new_dir"),
    ]
    assert "Renamed folder 'old_dir' -> 'new_dir'" in output


def test_rename_missing_target_errors(monkeypatch):
    service = FakeService(items={"nodes": [], "objects": []})
    output = run(monkeypatch, service, "acc://demo/missing.txt", "new.txt", exit_code=1)
    assert "Path 'missing.txt' not found in project 'demo'." in output


def test_rename_rejects_invalid_segment_and_root_path(monkeypatch):
    service = FakeService()
    out1 = run(monkeypatch, service, "acc://demo/a.txt", "sub/b.txt", exit_code=1)
    assert "Invalid name" in out1
    out2 = run(monkeypatch, service, "acc://demo", "new_name", exit_code=1)
    assert "project root" in out2
    assert service.calls == []


def test_rename_surfaces_api_error(monkeypatch):
    class Res:
        data = b'{"detail": "File with the provided filename already exists."}'

    service = FakeService(
        items={"nodes": [], "objects": [{"id": 42, "filename": "demo/a.txt"}]},
        error=("rename", AccAPIError("conflict", Res(), 403)),
    )
    output = run(monkeypatch, service, "acc://demo/a.txt", "b.txt", exit_code=1)
    assert "File with the provided filename already exists." in output


def test_service_rename_methods_and_validation():
    calls = []

    class DummyResponse:
        data = b"true"

    svc = AcceleratorTerminalCliProjectService(user_token="tok", server_url="https://acc.example", verify_cert=False)
    svc.http_client_request = lambda method, url, **kw: calls.append((method, url, kw)) or DummyResponse()

    assert svc.rename_file("demo", 10, "b.txt") is True
    assert svc.rename_folder("demo", 20, "new_dir") is True
    assert calls[0][0] == "PUT" and calls[0][1] == "https://acc.example/api/v1/projects/demo/rename-file/"
    assert calls[0][2]["json"] == {"object_id": 10, "new_name": "b.txt"}
    assert calls[1][0] == "PUT" and calls[1][1] == "https://acc.example/api/v1/projects/demo/rename-folder/"
    assert calls[1][2]["json"] == {"node_id": 20, "new_name": "new_dir"}

    for bad in ("", ".", "..", "a/b", "a b"):
        with pytest.raises(ValueError):
            validate_rename_segment(bad)
