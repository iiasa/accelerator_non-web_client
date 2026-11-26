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
"""

from __future__ import annotations

import fnmatch
import os
from pathlib import PurePosixPath
from typing import Iterator


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
    skip_dirs: tuple[str, ...] = (),
) -> Iterator[tuple[str, str]]:
    """Yield (absolute path, '/'-separated relative path) of the files kept, in a stable order.

    Folders named in skip_dirs are never entered. Excluded folders are not entered either,
    unless include patterns could pick files out of them.
    """
    includes, excludes = split_patterns(include), split_patterns(exclude)
    root = os.path.abspath(root)
    for dirpath, dirnames, filenames in os.walk(root):
        rel_dir = os.path.relpath(dirpath, root)
        rel_dir = "" if rel_dir == "." else PurePosixPath(*rel_dir.split(os.sep)).as_posix()

        kept_dirs = []
        for name in sorted(dirnames):
            if name in skip_dirs:
                continue
            rel = f"{rel_dir}/{name}" if rel_dir else name
            if excludes and not includes and _matches(rel, excludes):
                continue  # nothing inside can be kept
            kept_dirs.append(name)
        dirnames[:] = kept_dirs

        for name in sorted(filenames):
            rel = f"{rel_dir}/{name}" if rel_dir else name
            if is_selected(rel, include, exclude):
                yield os.path.join(dirpath, name), rel
