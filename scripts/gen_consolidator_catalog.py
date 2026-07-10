# Epic: observability_master
# Lifecycle: permanent
# Delete-when: the manifest-consolidator estate stops being terraform-declared
"""Generate the consolidator catalog from the deployment-service terraform SSOT.

The manifest-consolidator estate is declared ONCE, in terraform — the
``manifest_consolidator_buckets`` (+ ``_extended``) locals of
``deployment-service/terraform/gcp/manifest_consolidator_scheduler.tf``, which
``for_each`` deploys one Cloud Run job + Cloud Scheduler cron per entry. Rather than
hand-maintain a second copy in deployment-api (which would drift and need a policing
test), this script PROJECTS that terraform map into a committed JSON catalog the
consolidator-health endpoint reads at runtime.

Why a committed artifact (not a runtime read of the sibling repo): on the deployed
Cloud Run image only deployment-api is present — deployment-service is not — so the
catalog must travel with the image. Same pattern as ``DOC_INDEX.generated.md``.

Re-run whenever the terraform map changes (i.e. a consolidator is added/removed) — it
is the single source; this file is its projection. ``--check`` exits non-zero if the
committed catalog is stale, for an optional CI *sync* (regenerate + commit), never a
hard-fail gate.

Usage::

    python scripts/gen_consolidator_catalog.py            # regenerate the catalog
    python scripts/gen_consolidator_catalog.py --check     # verify it is up-to-date
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

# Cloud Run job names + the env-prefix that terraform stamps
# (`${local.env_prefix}-manifest-consolidator-${category}`). ``uts-prod`` is the prod
# fleet prefix (verified live: ``uts-prod-manifest-consolidator-market-data-cefi``).
_ENV_PREFIX = "uts-prod"
_JOB_NAME_TMPL = _ENV_PREFIX + "-manifest-consolidator-{category}"

# Known asset-group suffixes, longest-first so ``prediction`` matches before a bare
# token could. A category is ``{kind}-{ag}`` when it ends in one of these, else flat.
_ASSET_GROUPS: tuple[str, ...] = ("prediction", "cefi", "defi", "tradfi", "sports")

# The two terraform locals that declare the estate. ``_extended`` = the Group-B
# (features/execution/strategy/ml/gas-fees) buckets.
_TF_MAP_NAMES: tuple[str, ...] = (
    "manifest_consolidator_buckets",
    "manifest_consolidator_buckets_extended",
)

_PAIR_RE = re.compile(r'"(?P<key>[a-z0-9\-]+)"\s*=\s*"(?P<val>[^"]+)"')


def _workspace_root(start: Path) -> Path:
    """Walk up from ``start`` to the slot/workspace root that holds sibling repos."""
    for parent in [start, *start.parents]:
        if (parent / "deployment-service").is_dir() and (parent / "deployment-api").is_dir():
            return parent
    raise FileNotFoundError(
        "could not locate the workspace root (a dir holding both deployment-service/ and deployment-api/) "
        f"walking up from {start}"
    )


def _terraform_path(root: Path) -> Path:
    tf = root / "deployment-service" / "terraform" / "gcp" / "manifest_consolidator_scheduler.tf"
    if not tf.is_file():
        raise FileNotFoundError(f"terraform SSOT not found at {tf}")
    return tf


def _extract_map_block(text: str, map_name: str) -> str:
    """Return the ``{...}`` body of a top-level ``<map_name> = { ... }`` terraform local."""
    anchor = text.find(f"{map_name} = {{")
    if anchor == -1:
        raise ValueError(f"terraform local {map_name!r} not found")
    depth = 0
    body_start = text.index("{", anchor)
    for i in range(body_start, len(text)):
        if text[i] == "{":
            depth += 1
        elif text[i] == "}":
            depth -= 1
            if depth == 0:
                return text[body_start + 1 : i]
    raise ValueError(f"unbalanced braces parsing {map_name!r}")


def _decode_category(category: str) -> tuple[str, str | None]:
    """Split ``{kind}-{asset_group}`` → (kind, asset_group); flat categories → (category, None)."""
    for ag in _ASSET_GROUPS:
        suffix = f"-{ag}"
        if category.endswith(suffix) and len(category) > len(suffix):
            return category[: -len(suffix)], ag
    return category, None


def _bucket_template(tf_value: str) -> str:
    """Terraform bucket template → a runtime-fillable one with ``{env}`` / ``{project}`` slots."""
    return tf_value.replace("${local.deployment_env_short}", "{env}").replace("${var.project_id}", "{project}")


def build_catalog(tf_text: str) -> list[dict[str, str | None]]:
    """Project the terraform locals into the ordered consolidator catalog (legacy excluded)."""
    entries: list[dict[str, str | None]] = []
    for map_name in _TF_MAP_NAMES:
        body = _extract_map_block(tf_text, map_name)
        for m in _PAIR_RE.finditer(body):
            category, tf_value = m.group("key"), m.group("val")
            if category.endswith("-legacy"):
                continue  # pending-decommission duplicates — not shown in the cockpit
            kind, asset_group = _decode_category(category)
            entries.append(
                {
                    "category": category,
                    "kind": kind,
                    "asset_group": asset_group,
                    "job_name": _JOB_NAME_TMPL.format(category=category),
                    "bucket_template": _bucket_template(tf_value),
                }
            )
    return entries


def render_catalog(entries: list[dict[str, str | None]], *, source_rel: str) -> str:
    """Serialise the catalog with a provenance header (stable ordering → clean diffs)."""
    doc = {
        "_generated_by": "deployment-api/scripts/gen_consolidator_catalog.py",
        "_source": source_rel,
        "_note": "PROJECTION of the terraform consolidator locals — do not hand-edit; re-run the generator.",
        "consolidators": entries,
    }
    return json.dumps(doc, indent=2, ensure_ascii=False) + "\n"


def _output_path(root: Path) -> Path:
    return root / "deployment-api" / "deployment_api" / "consolidator_catalog.generated.json"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check", action="store_true", help="verify the committed catalog is up-to-date (exit 1 if stale)"
    )
    args = parser.parse_args(argv)

    root = _workspace_root(Path(__file__).resolve())
    tf_path = _terraform_path(root)
    source_rel = str(tf_path.relative_to(root))
    entries = build_catalog(tf_path.read_text(encoding="utf-8"))
    rendered = render_catalog(entries, source_rel=source_rel)
    out_path = _output_path(root)

    if args.check:
        current = out_path.read_text(encoding="utf-8") if out_path.exists() else ""
        if current != rendered:
            print(
                f"STALE: {out_path.relative_to(root)} is out of date — re-run gen_consolidator_catalog.py",
                file=sys.stderr,
            )
            return 1
        print(f"up-to-date: {len(entries)} consolidators")
        return 0

    out_path.write_text(rendered, encoding="utf-8")
    print(f"wrote {out_path.relative_to(root)} — {len(entries)} consolidators from {source_rel}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
