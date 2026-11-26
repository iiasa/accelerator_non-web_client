"""include/exclude rules shared by wkube.py job folders and `accli copy`."""
import os

import pytest

from accli.file_filter import is_selected, iter_selected_files, split_patterns

FILES = [
    "main.py", "Dockerfile", "requirements.txt", "data/input.csv", "data/big.bin",
    ".venv/lib/site.py", ".git/config", "src/model.py", "src/__pycache__/model.pyc",
    "outputs/run1.txt", "sub/requirements.txt",
]


@pytest.fixture
def folder(tmp_path):
    for name in FILES:
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x")
    return tmp_path


def selected(folder, include=None, exclude=None, skip_dirs=()):
    return sorted(rel for _, rel in iter_selected_files(folder, include, exclude, skip_dirs))


def test_split_patterns():
    assert split_patterns(" a, b,,c ") == ["a", "b", "c"]
    assert split_patterns(None) == [] and split_patterns("") == []


def test_no_patterns_keeps_everything(folder):
    assert selected(folder) == sorted(FILES)


def test_exclude_folder_name_covers_everything_inside(folder):
    kept = selected(folder, exclude=".git, .venv, __pycache__")
    assert not any(f.startswith((".git/", ".venv/")) or "__pycache__" in f for f in kept)
    assert "src/model.py" in kept


def test_exclude_by_relative_path_and_wildcard(folder):
    kept = selected(folder, exclude="data/*, *.txt")
    assert "data/input.csv" not in kept and "requirements.txt" not in kept and "sub/requirements.txt" not in kept
    assert "main.py" in kept


def test_build_files_only_with_exclude_everything(folder):
    # The wkube.py setup the "job folder too large" message suggests.
    assert selected(folder, include="Dockerfile, requirements.txt", exclude="*") == [
        "Dockerfile", "requirements.txt", "sub/requirements.txt"]


def test_only_include_keeps_nothing_else(folder):
    assert selected(folder, include="*.py") == [".venv/lib/site.py", "main.py", "src/model.py"]


def test_include_wins_over_exclude(folder):
    kept = selected(folder, include="data/input.csv", exclude="data")
    assert "data/input.csv" in kept and "data/big.bin" not in kept


def test_excluded_folder_stays_excluded_when_include_is_also_given(folder):
    # The old dispatch filter uploaded .venv/lib/site.py here.
    kept = selected(folder, include="*.txt", exclude=".venv")
    assert ".venv/lib/site.py" not in kept
    assert "requirements.txt" in kept and "main.py" in kept


def test_include_a_whole_folder(folder):
    assert selected(folder, include="src", exclude="*") == ["src/__pycache__/model.pyc", "src/model.py"]


def test_skip_dirs_are_never_entered(folder):
    assert not any(f.startswith(".git/") for f in selected(folder, include="*", skip_dirs=(".git",)))


def test_paths_use_forward_slashes(folder):
    for abs_path, rel in iter_selected_files(folder):
        assert "\\" not in rel and os.path.isfile(abs_path)


def test_is_selected_matches_iterated_files(folder):
    for include, exclude in [(None, ".venv"), ("*.py", "*"), ("*.txt", "data")]:
        expected = sorted(f for f in FILES if is_selected(f, include, exclude))
        assert selected(folder, include, exclude) == expected


def test_dispatch_job_folder_uses_the_same_rules(folder, tmp_path_factory):
    from accli.AcceleratorTaskDispatcher import copy_tree

    out = tmp_path_factory.mktemp("job")
    copy_tree(str(folder), str(out), "Dockerfile, requirements.txt", "*")
    copied = sorted(os.path.relpath(os.path.join(r, f), out).replace(os.sep, "/")
                    for r, _, fs in os.walk(out) for f in fs)
    assert copied == selected(folder, "Dockerfile, requirements.txt", "*", skip_dirs=(".git",))


def test_copy_rejects_patterns_for_downloads():
    from unittest import mock
    from typer.testing import CliRunner
    from accli import cli

    with mock.patch.object(cli, "check_for_updates", return_value=(False, None)):
        result = CliRunner().invoke(cli.app, ["copy", "acc://proj/data", "./data", "--exclude", ".git"])
    assert result.exit_code == 1
    assert "only work when uploading" in result.output


def test_copy_upload_sends_only_the_selected_files(folder, tmp_path_factory):
    """Runs `accli copy` with --exclude; the storage upload and the server are faked."""
    from types import SimpleNamespace
    from unittest import mock
    from typer.testing import CliRunner
    from accli import cli, xet_cache

    uploaded, registered = [], []

    class FakeCommit:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def start_upload_file(self, path, sha256):
            uploaded.append(path)
            info = SimpleNamespace(xet_info=SimpleNamespace(hash="h", file_size=1))
            return SimpleNamespace(result=lambda: info)

    class FakeSession:
        def new_upload_commit(self, **kwargs):
            return FakeCommit()

    def fake_post(url, json, **kwargs):
        registered.extend(item["filename"] for item in json["items"])
        return SimpleNamespace(raise_for_status=lambda: None)

    cache_dir = tmp_path_factory.mktemp("xet-cache")
    with mock.patch.object(cli, "check_for_updates", return_value=(False, None)), \
            mock.patch.object(cli, "get_server_url", return_value="https://acc.example"), \
            mock.patch.object(cli, "exchange_refresh_token", return_value=("cas", "access", 9999999999)), \
            mock.patch.object(xet_cache, "check_gc_id"), \
            mock.patch.object(xet_cache, "xet_cache_dir", return_value=cache_dir), \
            mock.patch.object(cli.hf_xet, "XetSession", FakeSession), \
            mock.patch.object(cli.requests, "post", side_effect=fake_post):
        result = CliRunner().invoke(cli.app, [
            "copy", str(folder), "acc://proj/jobstore/20261126/abc",
            "--exclude", ".git, .venv, __pycache__, data",
        ])

    assert result.exit_code == 0, result.output
    expected = ["Dockerfile", "main.py", "outputs/run1.txt", "requirements.txt", "src/model.py", "sub/requirements.txt"]
    assert sorted(os.path.relpath(p, folder).replace(os.sep, "/") for p in uploaded) == expected
    assert sorted(registered) == [f"proj/jobstore/20261126/abc/{f}" for f in expected]
