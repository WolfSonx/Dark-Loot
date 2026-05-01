from __future__ import annotations

import gzip
import pickle
import time
from pathlib import Path

from loot_spawn_web import APP_VERSION, CacheUnpickler


def main() -> int:
    source = Path("loot_spawn_cache.pkl.gz")
    if not source.exists():
        print("No loot_spawn_cache.pkl.gz found; nothing to bundle.")
        return 1

    output_dir = Path("bundle_cache")
    output_dir.mkdir(exist_ok=True)
    target = output_dir / "loot_spawn_cache.pkl.gz"

    with gzip.open(source, "rb") as handle:
        payload = CacheUnpickler(handle).load()

    # Keep the bundled cache small and frozen-safe. The EXE rebuilds this
    # in-memory index on startup from the scan result.
    payload.pop("index", None)
    payload["index_version"] = 0
    payload["app_version"] = f"{APP_VERSION}-bundle"
    payload["created_at"] = time.time()

    with gzip.open(target, "wb", compresslevel=5) as handle:
        pickle.dump(payload, handle, protocol=pickle.HIGHEST_PROTOCOL)

    print(f"Wrote {target} ({target.stat().st_size:,} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
