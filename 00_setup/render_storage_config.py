#!/usr/bin/env python3
"""Render 00_setup/adf_storage_config.json with the values from config.env.

Writes the rendered file to data/adf_storage_config.rendered.json (git-ignored)
so it can be pasted into the Atlas UI or applied with the Atlas CLI. Nothing is
sent to Atlas by this script.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from common.config import load_config  # noqa: E402

TEMPLATE = Path(__file__).with_name("adf_storage_config.json")
OUTPUT = REPO_ROOT / "data" / "adf_storage_config.rendered.json"


def main() -> int:
    cfg = load_config()
    cfg.require("S3_BUCKET", "AWS_REGION", "ATLAS_CLUSTER", "ATLAS_PROJECT_ID")
    values = {
        "S3_BUCKET": cfg.s3_bucket,
        "AWS_REGION": cfg.aws_region,
        "ATLAS_CLUSTER": cfg.atlas_cluster,
        "ATLAS_PROJECT_ID": cfg.atlas_project_id,
        "TARGET_DB": cfg.target_db,
    }
    text = TEMPLATE.read_text(encoding="utf-8")
    rendered = re.sub(r"\$\{(\w+)\}", lambda m: values[m.group(1)], text)
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(rendered, encoding="utf-8")
    print(f"Rendered storage config -> {OUTPUT}")
    print("Apply with:")
    print(f"  atlas datafederation update <FEDERATED_INSTANCE_NAME> --projectId {cfg.atlas_project_id} --file {OUTPUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
