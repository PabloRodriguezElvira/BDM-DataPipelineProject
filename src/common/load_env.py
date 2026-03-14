import os
from pathlib import Path


def load_env_file(env_path: str = ".env") -> None:
    """Load KEY=VALUE entries from a .env file into os.environ if missing."""
    path = Path(env_path)
    if not path.exists():
        return

    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))
