"""HTTP helpers shared across sources and destinations.

Anything HTTP-protocol-shaped that more than one connector reaches for
should live here, so neither side ends up importing the other just to
borrow a single function.
"""

from __future__ import annotations

import re


def extract_next_link(link_header: str) -> str | None:
    """Return the URL of the ``rel="next"`` entry in an RFC 5988 ``Link`` header.

    Format::

        <https://api.example.com?page=2>; rel="next",
        <https://api.example.com?page=50>; rel="last"

    Tolerates single or double quotes around the ``rel`` value and ignores
    surrounding whitespace. Returns ``None`` when no ``rel="next"`` entry
    is present or the header is empty.
    """
    for link in link_header.split(","):
        if re.search(r'rel\s*=\s*["\']next["\']', link, re.IGNORECASE):
            match = re.search(r"<([^>]+)>", link)
            if match:
                return match.group(1)
    return None
