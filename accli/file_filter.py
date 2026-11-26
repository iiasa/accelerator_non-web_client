"""Include/exclude patterns for choosing which files of a folder to upload.

Used for the job folder in wkube.py (include=..., exclude=...) and by `accli copy`
(--include, --exclude), so a pattern means the same in both places:

- Patterns are comma-separated shell-style wildcards (*, ?, [abc]), e.g. '.git, *.pyc, data/*'.
- A pattern is matched against the file's name, its path relative to the folder, and the
  name and relative path of every folder it is in. So '.git' or 'node_modules' covers
  everything inside those folders, wherever they are, and '*' covers everything.
- A file that matches an include pattern is kept, even if it also matches an exclude pattern.
- A file that matches an exclude pattern (and no include pattern) is left out.
- With only include patterns, every file that does not match one is left out.
- Nothing is left out automatically. Version-control folders (.git, .svn, ...) are not
  dropped silently either: confirm_version_control() asks before uploading them.
"""

from __future__ import annotations

import fnmatch
import os
import sys
from pathlib import PurePosixPath
from typing import Callable, Iterable, Iterator


def split_patterns(patterns: str | None) -> list[str]:
    """'a, b,,c ' -> ['a', 'b', 'c']"""
    return [p.strip() for p in patterns.split(",") if p.strip()] if patterns else []


def _names_to_match(rel_path: str) -> list[str]:
    """The file's path and name, and the path and name of every folder it is in."""
    parts = PurePosixPath(rel_path).parts
    names = []
    for i in range(1, len(parts) + 1):
        names.append("/".join(parts[:i]))  # relative path of this folder / the file
        names.append(parts[i - 1])  # its own name
    return names


def _matches(rel_path: str, patterns: list[str]) -> bool:
    names = _names_to_match(rel_path)
    # fnmatch ignores case on Windows, like its file system; elsewhere case matters.
    return any(fnmatch.fnmatch(name, pattern) for pattern in patterns for name in names)


def is_selected(rel_path: str, include: str | None = None, exclude: str | None = None) -> bool:
    """Whether a file (path relative to the folder, '/'-separated) is kept by include/exclude."""
    includes, excludes = split_patterns(include), split_patterns(exclude)
    if includes and _matches(rel_path, includes):
        return True
    if excludes:
        return not _matches(rel_path, excludes)
    return not includes  # only includes given: nothing else is kept


def iter_selected_files(
    root: str | os.PathLike,
    include: str | None = None,
    exclude: str | None = None,
) -> Iterator[tuple[str, str]]:
    """Yield (absolute path, '/'-separated relative path) of the files kept, in a stable order.

    Excluded folders are not entered, unless include patterns could pick files out of them.
    """
    includes, excludes = split_patterns(include), split_patterns(exclude)
    root = os.path.abspath(root)
    for dirpath, dirnames, filenames in os.walk(root):
        rel_dir = os.path.relpath(dirpath, root)
        rel_dir = "" if rel_dir == "." else PurePosixPath(*rel_dir.split(os.sep)).as_posix()

        kept_dirs = []
        for name in sorted(dirnames):
            rel = f"{rel_dir}/{name}" if rel_dir else name
            if excludes and not includes and _matches(rel, excludes):
                continue  # nothing inside can be kept
            kept_dirs.append(name)
        dirnames[:] = kept_dirs

        for name in sorted(filenames):
            rel = f"{rel_dir}/{name}" if rel_dir else name
            if is_selected(rel, include, exclude):
                yield os.path.join(dirpath, name), rel


def folder_stats(root, include=None, exclude=None):
    """Sizes of the files include/exclude keep in a folder.

    Returns (total bytes, files, folders): files as [(relative path, bytes)] and folders as
    [(relative path + '/', total bytes of the kept files inside)], each largest first.
    """
    files = []
    folder_sizes = {}
    for abs_path, rel in iter_selected_files(root, include, exclude):
        try:
            size = os.path.getsize(abs_path)
        except OSError:
            continue  # e.g. a broken link
        files.append((rel, size))
        parts = rel.split("/")[:-1]
        for i in range(1, len(parts) + 1):
            folder = "/".join(parts[:i]) + "/"
            folder_sizes[folder] = folder_sizes.get(folder, 0) + size

    by_size = lambda item: (-item[1], item[0])  # noqa: E731
    return sum(size for _, size in files), sorted(files, key=by_size), sorted(folder_sizes.items(), key=by_size)


# --- Version-control folders in an upload -------------------------------------------------

VERSION_CONTROL_FOLDERS = (".git", ".svn", ".hg", ".bzr")

# Starts the message when an upload stops because of version-control folders.
# The GUI looks for it to ask the user in a dialog instead.
VERSION_CONTROL_MARKER = "Version-control folders in the upload:"


class UploadNotConfirmed(ValueError):
    """The upload has version-control folders and nobody confirmed uploading them."""


def version_control_folders(files: Iterable[tuple[str, int]]) -> list[tuple[str, int]]:
    """Version-control folders among (relative path, bytes) files, as [(folder + '/', bytes)]."""
    found: dict[str, int] = {}
    for rel, size in files:
        parts = rel.split("/")
        for i, part in enumerate(parts[:-1]):
            if part in VERSION_CONTROL_FOLDERS:
                folder = "/".join(parts[: i + 1]) + "/"
                found[folder] = found.get(folder, 0) + size
                break
    return sorted(found.items())


def _can_ask() -> bool:
    """A person can answer a typed question: an interactive terminal, prompts not turned off."""
    if os.environ.get("ACCLI_NO_PROMPT"):
        return False
    try:
        return sys.stdin is not None and sys.stdin.isatty()
    except Exception:
        return False


def confirm_version_control(
    files: Iterable[tuple[str, int]],
    assume_yes: bool,
    how_to_exclude: str,
    format_size: Callable[[int], str],
) -> None:
    """Make sure version-control folders are only uploaded on purpose.

    Returns when there are none, when assume_yes (-y) is set, or when a person at a
    terminal answers yes. Otherwise raises UploadNotConfirmed with what to do; without a
    terminal (AI agents, scripts, the GUI) it never waits for an answer.
    """
    folders = version_control_folders(files)
    if not folders:
        return
    listing = ", ".join(f"{name} ({format_size(size)})" for name, size in folders)
    if assume_yes:
        print(f"Uploading version-control folders because of -y: {listing}", file=sys.stderr)
        return

    message = (
        f"{VERSION_CONTROL_MARKER} {listing}\n"
        f"They hold the project's version history, which a job normally does not use. "
        f"To leave them out, {how_to_exclude}."
    )
    if _can_ask():
        print(message, file=sys.stderr)
        if input("Upload them anyway? [y/N] ").strip().lower() in ("y", "yes"):
            return
        raise UploadNotConfirmed(f"Upload stopped. To leave them out, {how_to_exclude}.")
    raise UploadNotConfirmed(f"{message}\nTo upload them anyway, run the command again with -y.")
