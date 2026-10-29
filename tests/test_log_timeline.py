"""Shared cases with frontend/src/common/__fixtures__/log_timeline_cases.json (keep the copies identical)."""

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from accli.log_timeline import build_timeline

CASES = json.loads((Path(__file__).parent / "fixtures" / "log_timeline_cases.json").read_text())


def iso(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@pytest.mark.parametrize("case", CASES, ids=[c["name"] for c in CASES])
def test_build_timeline(case):
    lines = build_timeline(case["chunks"], case["events"])
    assert [[line.source, iso(line.time), line.text] for line in lines] == case["expected"]


def test_only_the_unterminated_last_line_is_partial():
    chunks = [{"source": "wkube", "seq": 0, "start": None, "end": "2026-10-29T09:30:10Z",
               "text": "2026-10-29T09:30:01.000Z\tdone\n2026-10-29T09:30:02.000Z\tdownloading 40%"}]
    lines = build_timeline(chunks, [])
    assert [(line.text, line.partial) for line in lines] == [("done", False), ("downloading 40%", True)]
