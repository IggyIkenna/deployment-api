# Epic: observability_master
# Lifecycle: permanent
# Delete-when: mock mode is retired, or the parity contract is enforced by a QG/contract test instead
"""Diff deployment-api's LIVE responses against its MOCK responses — shape + currency.

WHY THIS EXISTS
---------------
Mock mode is what the UI's Playwright suite and every offline dev session run against. When
the mock drifts from live, the mock LIES: tests go green against a contract production no
longer has, and an operator "checks" a page that is quietly showing a different shape. This
has bitten the fleet for real — deployment-ui@0c817d2 (2026-07-17) fixed a mock catch-all
that had shadowed *every* specific `/api/data-status/*` endpoint added since 2026-06-16.
Nothing detects that class of rot except comparing the two modes endpoint-by-endpoint.

RE-RUN THIS; THE ANSWER HAS A DATE ON IT. Parity is not a property you fix once — it decays
every time an endpoint is added to one side only. A finding from a previous run is evidence
about that day, not today.

USAGE
-----
Needs BOTH modes up on separate ports (mock mode is per-process, so it is two servers):

    # live
    GCP_PROJECT_ID=<proj> DISABLE_AUTH=true ENVIRONMENT=development \
      .venv/bin/python -m uvicorn deployment_api.main:app --port 8005
    # mock
    CLOUD_MOCK_MODE=true GCP_PROJECT_ID=<proj> DISABLE_AUTH=true ENVIRONMENT=development \
      .venv/bin/python -m uvicorn deployment_api.main:app --port 8006

    .venv/bin/python scripts/compare_live_mock_parity.py [--live URL] [--mock URL]

TRAPS (measured 2026-07-16 — each one produced a WRONG answer before it was handled)
------------------------------------------------------------------------------------
1. RATE LIMITING FAKES A PARITY GAP. The live side rate-limits; hammering it concurrently
   returns 429 and the naive diff reports "live=429 mock=200 — status mismatch" for
   endpoints that are perfectly fine. Five endpoints were mis-reported this way on the first
   run. Hence `--serial` + the automatic 429 retry pass below. A 429 is NEVER a parity
   finding; treat it as "not measured".
2. `python -m deployment_api` IGNORES $PORT (it is not wired into UnifiedCloudConfig and
   hardcodes 8004) — you MUST launch via `uvicorn ... --port` or both servers collide.
3. 422 ON BOTH SIDES IS NOT A GAP. It means the endpoint requires query params this script
   does not synthesise. Reported separately, never as a diff.
4. PARAMETERLESS GETs ONLY. Paths with `{...}` are skipped — they need real ids. Their
   parity is genuinely unmeasured by this tool; do not read a clean run as "everything
   matches".
5. Shape-compare, not value-compare: mock values SHOULD differ from live. Only the KEY SET
   and the currency of dates are meaningful signals here.
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import re
import time
import urllib.error
import urllib.request

DATE_RE = re.compile(r"\b(20\d{2}-\d{2}-\d{2})")
_MAX_DEPTH = 6
_SAMPLE = 50


def fetch(base: str, path: str, timeout: int = 60) -> tuple[int, object | None, str]:
    """GET base+path → (status, parsed-json-or-None, raw-body). -1 = transport failure."""
    try:
        req = urllib.request.Request(base + path, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read().decode("utf-8", "replace")
            try:
                return r.status, json.loads(body), body
            except json.JSONDecodeError:
                return r.status, None, body[:200]
    except urllib.error.HTTPError as e:
        return e.code, None, e.read().decode("utf-8", "replace")[:200]
    except Exception as e:
        return -1, None, str(e)[:200]


def sig(obj: object, depth: int = 0) -> object:
    """Recursive KEY-SHAPE signature: dict → {key: sig}, list → union of element sigs.

    Values are deliberately discarded — mock values are meant to differ from live. Only the
    key set carries the contract.
    """
    if depth > _MAX_DEPTH:
        return "..."
    if isinstance(obj, dict):
        return {k: sig(v, depth + 1) for k, v in sorted(obj.items())}
    if isinstance(obj, list):
        merged: dict[str, object] = {}
        scalars: set[str] = set()
        for el in obj[:_SAMPLE]:
            s = sig(el, depth + 1)
            if isinstance(s, dict):
                for k, v in s.items():
                    merged.setdefault(k, v)
            else:
                scalars.add(s if isinstance(s, str) else json.dumps(s))
        if merged and not scalars:
            return [merged]
        if scalars and not merged:
            return [sorted(scalars)]
        if not merged and not scalars:
            return ["<empty>"]
        return [merged, sorted(scalars)]
    if obj is None:
        return "null"
    return type(obj).__name__


def key_diff(a: object, b: object, prefix: str = "") -> list[str]:
    """Keys in live(a) but not mock(b), and vice versa."""
    out: list[str] = []
    if isinstance(a, dict) and isinstance(b, dict):
        for k in sorted(set(a) | set(b)):
            p = f"{prefix}.{k}" if prefix else k
            if k not in b:
                out.append(f"live-only key: {p}")
            elif k not in a:
                out.append(f"mock-only key: {p}")
            else:
                out.extend(key_diff(a[k], b[k], p))
    elif isinstance(a, list) and isinstance(b, list):
        ea = a[0] if a else None
        eb = b[0] if b else None
        # An empty list on either side proves nothing about the element shape.
        if ea == ["<empty>"] or eb == ["<empty>"]:
            return out
        if isinstance(ea, dict) and isinstance(eb, dict):
            out.extend(key_diff(ea, eb, prefix + "[]"))
    return out


def max_date(body: str) -> str | None:
    """Newest ISO date in the payload — the currency signal (mock fixtures freeze in time)."""
    dates = [d for d in DATE_RE.findall(body) if d <= "2999-12-31"]
    return max(dates) if dates else None


def compare_one(live: str, mock: str, path: str) -> dict[str, object]:
    ls, lj, lb = fetch(live, path)
    ms, mj, mb = fetch(mock, path)
    row: dict[str, object] = {"path": path, "live_status": ls, "mock_status": ms}
    if ls == 200 and ms == 200 and lj is not None and mj is not None:
        diffs = key_diff(sig(lj), sig(mj))
        row["shape"] = "SAME" if not diffs else "DIFF"
        row["diffs"] = diffs[:12]
        row["live_max_date"] = max_date(lb)
        row["mock_max_date"] = max_date(mb)
    elif 429 in (ls, ms):
        row["shape"] = "RATE-LIMITED"  # Trap 1: not a finding.
    elif ls == ms == 422:
        row["shape"] = "NEEDS-PARAMS"  # Trap 3: not a finding.
    else:
        row["shape"] = "STATUS-MISMATCH" if ls != ms else "BOTH-NON-200"
        row["live_body"] = lb[:120]
        row["mock_body"] = mb[:120]
    return row


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--live", default="http://localhost:8005")
    ap.add_argument("--mock", default="http://localhost:8006")
    ap.add_argument(
        "--serial",
        action="store_true",
        help="One request at a time (slower, but avoids the live rate-limiter faking gaps — see Trap 1)",
    )
    ap.add_argument("--json-out", help="Write the full machine-readable report here")
    args = ap.parse_args()

    spec = json.load(urllib.request.urlopen(f"{args.live}/openapi.json", timeout=30))
    paths = sorted(p for p, m in spec["paths"].items() if "get" in m and "{" not in p)
    print(f"Comparing {len(paths)} parameterless GET endpoints (paths with {{}} are NOT measured — Trap 4)\n")

    if args.serial:
        results = [compare_one(args.live, args.mock, p) for p in paths]
    else:
        with cf.ThreadPoolExecutor(max_workers=8) as ex:
            results = list(ex.map(lambda p: compare_one(args.live, args.mock, p), paths))

    # Trap 1: re-measure anything the rate limiter ate, serially, after the window clears.
    limited = [r for r in results if r["shape"] == "RATE-LIMITED"]
    if limited:
        print(f"{len(limited)} endpoint(s) hit the rate limiter — waiting 65s to re-measure serially...\n")
        time.sleep(65)
        for r in limited:
            results[results.index(r)] = compare_one(args.live, args.mock, str(r["path"]))
            time.sleep(2)

    by = lambda kind: [r for r in results if r["shape"] == kind]  # noqa: E731
    print(
        f"total={len(results)} same={len(by('SAME'))} DIFF={len(by('DIFF'))} "
        f"status-mismatch={len(by('STATUS-MISMATCH'))} both-non-200={len(by('BOTH-NON-200'))} "
        f"needs-params={len(by('NEEDS-PARAMS'))} still-rate-limited={len(by('RATE-LIMITED'))}"
    )

    print("\n=== SHAPE DIFFS (the mock's contract has drifted from live) ===")
    for r in by("DIFF"):
        print(f"{r['path']}:")
        for d in r["diffs"]:  # pyright: ignore[reportGeneralTypeIssues]
            print(f"   {d}")

    print("\n=== STATUS MISMATCHES ===")
    for r in by("STATUS-MISMATCH"):
        print(f"{r['path']}: live={r['live_status']} mock={r['mock_status']} :: {r.get('live_body')!r}")

    print("\n=== CURRENCY (mock fixtures freeze; live moves) ===")
    for r in sorted(by("SAME") + by("DIFF"), key=lambda x: str(x["path"])):
        ld, md = r.get("live_max_date"), r.get("mock_max_date")
        if ld and md and str(ld) > str(md):
            print(f"{r['path']}: live={ld} mock={md}  <-- mock behind")

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as fh:
            json.dump(results, fh, indent=2)
        print(f"\nFull report: {args.json_out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
