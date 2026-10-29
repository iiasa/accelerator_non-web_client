"""Builds one merged, line-accurate timeline from job log chunks and cluster events.

Writers prefix every line with an RFC 3339 UTC timestamp and a tab. Chunks are cut
by bytes, so a line can span two chunks: chunks are joined per source in seq order
before splitting. Lines without a valid prefix (continuations, chunks written before
timestamps existed) inherit the time of the line before them; a chunk with no
timestamped lines at all spreads its lines evenly between its start and end.
Keep in sync with frontend/src/common/log_timeline.ts.
"""

import re
from dataclasses import dataclass
from datetime import datetime, timezone

EVENTS_SOURCE = "events"

PREFIX_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2}))\t")


@dataclass
class TimelineLine:
    key: str
    time: float  # epoch milliseconds
    source: str
    text: str
    is_event: bool
    partial: bool  # last line of a source with no newline yet; it may still grow


def to_ms(iso):
    if not iso:
        return None
    dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp() * 1000


def _split_source(chunks):
    lines = []  # (chunk, text, partial)
    carry = ""
    carry_chunk = None
    previous_seq = None

    for chunk in chunks:
        # Only join with the previous chunk when it really is the previous one.
        if carry_chunk is not None and previous_seq != chunk["seq"] - 1:
            lines.append((carry_chunk, carry, False))
            carry, carry_chunk = "", None
        pieces = (carry + chunk["text"]).split("\n")
        tail = pieces.pop()
        for i, text in enumerate(pieces):
            lines.append((carry_chunk if i == 0 and carry_chunk is not None else chunk, text, False))
        if tail:
            carry_chunk = carry_chunk if not pieces and carry_chunk is not None else chunk
        else:
            carry_chunk = None
        carry = tail
        previous_seq = chunk["seq"]
    if carry_chunk is not None:
        lines.append((carry_chunk, carry, True))
    return lines


def _time_source_lines(source, raw):
    stamped_chunks = set()
    per_chunk_count = {}
    for chunk, text, _ in raw:
        if PREFIX_RE.match(text):
            stamped_chunks.add(chunk["seq"])
        per_chunk_count[chunk["seq"]] = per_chunk_count.get(chunk["seq"], 0) + 1

    out = []
    per_chunk_index = {}
    previous_time = None
    for chunk, text, partial in raw:
        seq = chunk["seq"]
        index = per_chunk_index.get(seq, 0)
        per_chunk_index[seq] = index + 1

        end = to_ms(chunk["end"])
        start = to_ms(chunk.get("start"))
        start = end if start is None else start
        match = PREFIX_RE.match(text)

        if match:
            time = to_ms(match.group(1))
            text = text[match.end() :]
        elif seq not in stamped_chunks:
            count = per_chunk_count.get(seq, 1)
            time = start + (end - start) * (index + 1) / count
        else:
            time = start if previous_time is None else previous_time
        previous_time = time
        out.append(TimelineLine(f"{source}:{seq}:{index}", time, source, text, False, partial))
    return out


def format_event(event):
    return " · ".join(p for p in (event.get("kind"), event.get("reason"), event.get("message")) if p)


def build_timeline(chunks, events):
    """chunks and events are the dicts from the logs API, chunks with their downloaded `text`."""
    by_source = {}
    for chunk in chunks:
        by_source.setdefault(chunk["source"], {})[chunk["seq"]] = chunk  # dedupe by (source, seq)

    # Events go first so that, at equal times, an event sorts before executor lines.
    # Second-precision events sit at .000 and so come before that second's output.
    unique_events = {e["id"]: e for e in events}
    lines = [
        TimelineLine(f"{EVENTS_SOURCE}:{e['id']}", to_ms(e["time"]), EVENTS_SOURCE, format_event(e), True, False)
        for e in unique_events.values()
    ]

    for source in sorted(by_source):
        ordered = sorted(by_source[source].values(), key=lambda c: c["seq"])
        lines.extend(_time_source_lines(source, _split_source(ordered)))

    return sorted(lines, key=lambda line: line.time)  # stable: keeps per-source order on ties
