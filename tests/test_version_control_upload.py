"""Version-control folders (.git, .svn, ...) are only uploaded on purpose."""
import os
from types import SimpleNamespace
from unittest import mock

import pytest
from typer.testing import CliRunner

from accli import file_filter
from accli.file_filter import UploadNotConfirmed, confirm_version_control, version_control_folders


def fmt(n):
    return f"{n} B"


def test_finds_version_control_folders_anywhere():
    files = [("main.py", 1), (".git/config", 10), (".git/objects/a", 20), ("lib/.svn/entries", 5),
             ("notes/git.txt", 3), (".github/workflow.yml", 2)]
    assert version_control_folders(files) == [(".git/", 30), ("lib/.svn/", 5)]


def test_nothing_to_confirm_without_version_control():
    confirm_version_control([("main.py", 1)], False, "exclude them", fmt)  # no error, no question


def test_yes_uploads_without_asking(capsys):
    with mock.patch("builtins.input") as ask:
        confirm_version_control([(".git/config", 10)], True, "exclude them", fmt)
    ask.assert_not_called()
    assert ".git/ (10 B)" in capsys.readouterr().err


def _terminal(is_tty):
    return mock.patch.object(file_filter.sys, "stdin", SimpleNamespace(isatty=lambda: is_tty))


def test_without_a_terminal_it_stops_and_explains(monkeypatch):
    monkeypatch.delenv("ACCLI_NO_PROMPT", raising=False)
    with _terminal(False), mock.patch("builtins.input") as ask:
        with pytest.raises(UploadNotConfirmed) as stop:
            confirm_version_control([(".git/config", 10)], False, "add them to --exclude", fmt)
    ask.assert_not_called()  # never waits for an answer
    message = str(stop.value)
    assert message.startswith(file_filter.VERSION_CONTROL_MARKER)
    assert "add them to --exclude" in message
    assert "run the command again with -y" in message


@pytest.mark.parametrize("answer, uploads", [("y", True), ("YES", True), ("n", False), ("", False)])
def test_at_a_terminal_it_asks(monkeypatch, answer, uploads):
    monkeypatch.delenv("ACCLI_NO_PROMPT", raising=False)
    with _terminal(True), mock.patch("builtins.input", return_value=answer):
        if uploads:
            confirm_version_control([(".git/config", 10)], False, "exclude them", fmt)
        else:
            with pytest.raises(UploadNotConfirmed, match="Upload stopped"):
                confirm_version_control([(".git/config", 10)], False, "exclude them", fmt)


def test_no_prompt_setting_stops_even_at_a_terminal(monkeypatch):
    monkeypatch.setenv("ACCLI_NO_PROMPT", "1")
    with _terminal(True), mock.patch("builtins.input") as ask:
        with pytest.raises(UploadNotConfirmed):
            confirm_version_control([(".git/config", 10)], False, "exclude them", fmt)
    ask.assert_not_called()


# --- accli copy -------------------------------------------------------------------------------


@pytest.fixture
def repo(tmp_path):
    for name in ["main.py", ".git/config", ".git/objects/abc"]:
        path = tmp_path / "repo" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x")
    return tmp_path / "repo"


def _copy(folder, *extra, cache_dir):
    from accli import cli, xet_cache

    uploaded = []

    class FakeCommit:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def start_upload_file(self, path, sha256):
            uploaded.append(os.path.relpath(path, folder).replace(os.sep, "/"))
            info = SimpleNamespace(xet_info=SimpleNamespace(hash="h", file_size=1))
            return SimpleNamespace(result=lambda: info)

    session = SimpleNamespace(new_upload_commit=lambda **kw: FakeCommit())
    with mock.patch.object(cli, "check_for_updates", return_value=(False, None)), \
            mock.patch.object(cli, "get_server_url", return_value="https://acc.example"), \
            mock.patch.object(cli, "exchange_refresh_token", return_value=("cas", "access", 9999999999)), \
            mock.patch.object(xet_cache, "check_gc_id"), \
            mock.patch.object(xet_cache, "xet_cache_dir", return_value=cache_dir), \
            mock.patch.object(cli.hf_xet, "XetSession", return_value=session), \
            mock.patch.object(cli.requests, "post", return_value=SimpleNamespace(raise_for_status=lambda: None)):
        result = CliRunner().invoke(cli.app, ["copy", str(folder), "acc://proj/code", *extra])
    return result, sorted(uploaded)


def test_copy_stops_on_git_folder(repo, tmp_path_factory, monkeypatch):
    monkeypatch.delenv("ACCLI_NO_PROMPT", raising=False)
    result, uploaded = _copy(repo, cache_dir=tmp_path_factory.mktemp("c"))  # CliRunner has no terminal
    assert result.exit_code == 1
    assert file_filter.VERSION_CONTROL_MARKER in result.output
    assert '--exclude ".git"' in result.output and "-y" in result.output
    assert uploaded == []  # nothing was sent


def test_copy_with_yes_uploads_git_folder(repo, tmp_path_factory):
    result, uploaded = _copy(repo, "-y", cache_dir=tmp_path_factory.mktemp("c"))
    assert result.exit_code == 0, result.output
    assert uploaded == [".git/config", ".git/objects/abc", "main.py"]


def test_copy_with_git_excluded_does_not_ask(repo, tmp_path_factory):
    result, uploaded = _copy(repo, "--exclude", ".git", cache_dir=tmp_path_factory.mktemp("c"))
    assert result.exit_code == 0, result.output
    assert uploaded == ["main.py"]


# --- accli dispatch ---------------------------------------------------------------------------


def _push(folder, exclude=None):
    from accli import AcceleratorTaskDispatcher as dispatcher

    dispatcher.push_folder_job.cache_clear()
    with mock.patch.object(dispatcher, "get_server_url", return_value="https://acc.example"), \
            mock.patch.object(dispatcher, "get_project_slug", return_value="proj"), \
            mock.patch.object(dispatcher, "exchange_refresh_token", return_value=("c", "a", 0)), \
            mock.patch.object(dispatcher, "AcceleratorTerminalCliProjectService"), \
            mock.patch.object(dispatcher, "MAX_JOB_FOLDER_BYTES", 0):  # stop at the size check, before uploading
        dispatcher.push_folder_job(str(folder), None, exclude)


def test_dispatch_stops_on_git_folder(repo, monkeypatch):
    from accli import AcceleratorTaskDispatcher as dispatcher

    monkeypatch.setattr(dispatcher, "UPLOAD_VERSION_CONTROL", False)
    with _terminal(False), pytest.raises(UploadNotConfirmed) as stop:
        _push(repo)
    assert 'exclude=".git"' in str(stop.value)


def test_dispatch_with_yes_or_exclude_goes_on(repo, monkeypatch):
    from accli import AcceleratorTaskDispatcher as dispatcher

    monkeypatch.setattr(dispatcher, "UPLOAD_VERSION_CONTROL", True)
    with pytest.raises(ValueError, match="Job folder too large"):  # passed the version-control check
        _push(repo)
    monkeypatch.setattr(dispatcher, "UPLOAD_VERSION_CONTROL", False)
    with pytest.raises(ValueError, match="Job folder too large"):
        _push(repo, exclude=".git")


def test_dispatch_command_passes_yes(tmp_path, monkeypatch):
    from accli import cli, AcceleratorTaskDispatcher as dispatcher

    monkeypatch.setattr(dispatcher, "UPLOAD_VERSION_CONTROL", False)
    monkeypatch.chdir(tmp_path)
    with mock.patch.object(cli, "check_for_updates", return_value=(False, None)), \
            mock.patch.object(cli, "set_project_slug"), \
            mock.patch.object(cli, "get_server_url", return_value="https://acc.example"), \
            mock.patch.object(cli, "exchange_refresh_token", return_value=("c", "a", 0)):
        CliRunner().invoke(cli.app, ["dispatch", "proj", "task", "-f", "missing.py", "-y"])
    assert dispatcher.UPLOAD_VERSION_CONTROL is True


# --- GUI --------------------------------------------------------------------------------------


def test_gui_recognises_the_stop():
    gui = pytest.importorskip("accli.gui")  # needs tkinter
    output = (
        "Enumerating...\n"
        "Value error, Version-control folders in the upload: .git/ (60.0 MB)\n"
        "They hold the project's version history...\n"
    )
    assert gui.version_control_question(1, output) == "Version-control folders in the upload: .git/ (60.0 MB)"
    assert gui.version_control_question(1, "some other error") is None
    assert gui.version_control_question(0, output) is None
