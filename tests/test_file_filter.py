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


def selected(folder, include=None, exclude=None):
    return sorted(rel for _, rel in iter_selected_files(folder, include, exclude))


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


def test_git_folder_is_kept_unless_excluded(folder):
    # No folder is left out automatically, in accli copy or the dispatch job folder upload.
    assert ".git/config" in selected(folder)
    assert ".git/config" not in selected(folder, exclude=".git")


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
    assert copied == selected(folder, "Dockerfile, requirements.txt", "*")


def test_dispatch_uploads_git_folder_unless_excluded(folder, tmp_path_factory):
    from accli.AcceleratorTaskDispatcher import copy_tree

    for exclude, expected in ((None, True), (".git", False)):
        out = tmp_path_factory.mktemp("job")
        copy_tree(str(folder), str(out), None, exclude)
        assert os.path.isfile(os.path.join(out, ".git", "config")) is expected


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


# --- accli size ---------------------------------------------------------------------------


def _size_folder(tmp_path):
    files = {"main.py": 10, "data/raw/a.parquet": 900, "data/raw/b.gpkg": 150, "outputs/run.nc": 350,
             ".venv/lib/m1.py": 120, ".venv/lib/m2.py": 120, ".venv/lib/m3.py": 120}
    for name, n in files.items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"x" * n)
    return tmp_path


def test_folder_stats_orders_files_and_folders(tmp_path):
    from accli.file_filter import folder_stats

    total, files, folders = folder_stats(_size_folder(tmp_path))
    assert total == 1770
    assert [name for name, _ in files][:3] == ["data/raw/a.parquet", "outputs/run.nc", "data/raw/b.gpkg"]
    # A folder of many small files shows up in the folder list even though no file of it is large.
    assert folders[:4] == [("data/", 1050), ("data/raw/", 1050), (".venv/", 360), (".venv/lib/", 360)]


def test_folder_stats_applies_include_exclude(tmp_path):
    from accli.file_filter import folder_stats

    total, files, folders = folder_stats(_size_folder(tmp_path), exclude=".venv, outputs")
    assert total == 1060
    assert not any(name.startswith((".venv", "outputs")) for name, _ in files + folders)


def _run_size(args):
    from unittest import mock
    from typer.testing import CliRunner
    from accli import cli

    with mock.patch.object(cli, "check_for_updates", return_value=(False, None)):
        return CliRunner().invoke(cli.app, ["size", *args])


def test_size_command_output(tmp_path):
    folder = _size_folder(tmp_path)
    result = _run_size([str(folder), "--top", "2"])
    assert result.exit_code == 0, result.output
    lines = result.output.splitlines()
    assert lines[1] == "Total: 1.7 KB in 7 files"
    files_at = lines.index("Largest files:")
    assert lines[files_at + 1].split() == ["900", "B", "data/raw/a.parquet"]
    assert lines[files_at + 2].split() == ["350", "B", "outputs/run.nc"]
    assert lines[files_at + 3].strip() == "... 5 more; use --top to list more"
    folders_at = lines.index("Largest folders (including everything inside):")
    assert lines[folders_at + 1].split() == ["1.0", "KB", "data/"]


def test_size_command_with_patterns(tmp_path):
    folder = _size_folder(tmp_path)
    result = _run_size([str(folder), "--exclude", "*", "--include", "main.py"])
    assert result.exit_code == 0, result.output
    assert "Total: 10 B in 1 files after --include/--exclude" in result.output


def test_size_command_rejects_a_file(tmp_path):
    (tmp_path / "f.txt").write_text("x")
    result = _run_size([str(tmp_path / "f.txt")])
    assert result.exit_code == 1
    assert "Not a folder" in result.output
