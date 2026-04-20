"""Connectors package — Source and destination registry system."""

from drt.connectors.registry import (
    get_destination,
    get_source,
    register_destination,
    register_source,
)

__all__ = [
    "get_destination",
    "get_source",
    "register_destination",
    "register_source",
]
