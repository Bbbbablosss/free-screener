"""Create a transfer archive containing only images referenced by formations.json."""
import json
import os
import tarfile
from pathlib import Path

base = Path("/opt/screener/backend")
data = json.loads((base / "data/formations.json").read_text("utf-8"))
names = [
    os.path.basename(item.get("chart_url", "").split("?", 1)[0])
    for item in data.get("items", [])
    if item.get("chart_url")
]
target = Path("/tmp/free-screener-formations.tgz")
with tarfile.open(target, "w:gz") as archive:
    for name in names:
        source = base / "static/formations" / name
        if source.is_file():
            archive.add(source, arcname=f"formations/{name}")
print(f"referenced={len(names)} archive_bytes={target.stat().st_size}")
