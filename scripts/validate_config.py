#!/usr/bin/env python3
from __future__ import annotations

import subprocess
import sys


def main() -> int:
    cfg = sys.argv[1] if len(sys.argv) > 1 else "./sources.json"
    proc = subprocess.run([sys.executable, "-m", "app.validate_sources", cfg])
    return proc.returncode


if __name__ == "__main__":
    raise SystemExit(main())
