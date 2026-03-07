"""Demoflight Python client for Arrow Flight server."""

from demoflight._client import Client
from demoflight._query import QueryHandle
from demoflight._session import DemoflightSession

__all__ = ["Client", "DemoflightSession", "QueryHandle"]
