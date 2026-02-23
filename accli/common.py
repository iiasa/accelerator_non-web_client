from __future__ import annotations

import json


def todict(bytestring: bytes):
    return json.loads(
        bytestring.decode("utf-8")
    )
