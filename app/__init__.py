"""Robtaxi digest pipeline package."""

from __future__ import annotations

import sys


if sys.version_info[:2] != (3, 11):
    raise RuntimeError(
        f"Robtaxi digest 只支持 Python 3.11；当前为 "
        f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    )
