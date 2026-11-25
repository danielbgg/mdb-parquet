"""Load config.env (KEY=VALUE, '#' comments) and expose typed settings.

Nothing is hardcoded: every value comes from config.env or the environment
(environment variables take precedence over the file).
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_FILE = REPO_ROOT / "config.env"


def read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        value = value.split(" #", 1)[0].strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        values[key.strip()] = value
    return values


@dataclass
class Config:
    values: dict[str, str] = field(default_factory=dict)
    source: str = ""

    def get(self, key: str, default: str | None = None) -> str | None:
        return os.environ.get(key, self.values.get(key, default))

    def require(self, *keys: str) -> None:
        missing = [k for k in keys if not self.get(k)]
        if missing:
            raise SystemExit(
                f"Missing config keys: {', '.join(missing)}. "
                f"Set them in {self.source or DEFAULT_CONFIG_FILE} (see config.env.example)."
            )

    @property
    def mongodb_uri(self) -> str:
        return self.get("MONGODB_URI") or ""

    @property
    def adf_uri(self) -> str:
        return self.get("ADF_URI") or ""

    @property
    def atlas_project_id(self) -> str:
        return self.get("ATLAS_PROJECT_ID") or ""

    @property
    def atlas_cluster(self) -> str:
        return self.get("ATLAS_CLUSTER") or ""

    @property
    def atlas_tier(self) -> str:
        return self.get("ATLAS_TIER", "M40") or "M40"

    @property
    def s3_bucket(self) -> str:
        return self.get("S3_BUCKET") or ""

    @property
    def aws_region(self) -> str:
        return self.get("AWS_REGION") or ""

    @property
    def target_db(self) -> str:
        return self.get("TARGET_DB", "ratings_poc") or "ratings_poc"

    @property
    def adf_db(self) -> str:
        return self.get("ADF_DB", "dbimport") or "dbimport"


def load_config(path: str | Path | None = None) -> Config:
    p = Path(path) if path else DEFAULT_CONFIG_FILE
    return Config(values=read_env_file(p), source=str(p))


def load_manifest(path: str | Path) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def redact_uri(uri: str) -> str:
    """mongodb+srv://user:pass@host -> mongodb+srv://user:***@host"""
    if "@" not in uri or "://" not in uri:
        return uri
    scheme, rest = uri.split("://", 1)
    creds, host = rest.rsplit("@", 1)
    user = creds.split(":", 1)[0]
    return f"{scheme}://{user}:***@{host}"


def rel(path: Path) -> str:
    """Path relative to the repo root when possible (for log messages)."""
    try:
        return str(Path(path).resolve().relative_to(REPO_ROOT))
    except ValueError:
        return str(path)
