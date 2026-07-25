from hashlib import sha256
from typing import Hashable


def edge_identity(u: Hashable, v: Hashable) -> str:
    source = str(u).encode()
    target = str(v).encode()
    payload = len(source).to_bytes(8, "big") + source + target
    return "v1_" + sha256(payload).hexdigest()
