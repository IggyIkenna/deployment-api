#!/usr/bin/env python3
"""Generate static docs/specs/openapi.json from the FastAPI app.

Usage:
    CLOUD_PROVIDER=local CLOUD_MOCK_MODE=true DISABLE_AUTH=true \
        python3 scripts/generate_openapi.py

Writes docs/specs/openapi.json (committed to repo for review / tooling).
"""

from __future__ import annotations

import json
import os
import sys
import warnings
from pathlib import Path

os.environ.setdefault("CLOUD_PROVIDER", "local")
os.environ.setdefault("CLOUD_MOCK_MODE", "true")
os.environ.setdefault("DISABLE_AUTH", "true")

with warnings.catch_warnings(record=True) as _w:
    warnings.simplefilter("always")
    from deployment_api.main import app

    schema = app.openapi()

# Surface duplicate OperationId warnings — these should be fixed eventually.
dup_warnings = [str(w.message) for w in _w if "Duplicate Operation ID" in str(w.message)]
if dup_warnings:
    for dw in dup_warnings:
        print(f"WARNING: {dw}", file=sys.stderr)

out = Path("docs/specs/openapi.json")
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps(schema, indent=2))

paths: dict[str, object] = schema.get("paths", {})  # pyright: ignore[reportAny]
endpoint_count = sum(
    len([m for m in methods if m in {"get", "post", "put", "patch", "delete"}])  # type: ignore[misc]
    for methods in paths.values()  # type: ignore[misc]
    if isinstance(methods, dict)
)
missing_summary = [
    f"{method.upper()} {path}"  # type: ignore[misc]
    for path, methods in paths.items()  # type: ignore[misc]
    if isinstance(methods, dict)
    for method, op in methods.items()  # type: ignore[misc]
    if method in {"get", "post", "put", "patch", "delete"}
    if isinstance(op, dict) and not op.get("summary") and not op.get("description")  # type: ignore[misc]
]

print(f"Written: {out} ({out.stat().st_size // 1024}KB)")
print(f"Paths: {len(paths)}  Endpoints: {endpoint_count}")
if missing_summary:
    print(f"WARNING: {len(missing_summary)} endpoints missing summary/description:")
    for m in missing_summary:
        print(f"  {m}")
else:
    print("All endpoints have summary or description.")

if dup_warnings:
    print(f"WARNING: {len(dup_warnings)} duplicate OperationId(s) — fix in source.", file=sys.stderr)
    sys.exit(1)
