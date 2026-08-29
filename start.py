import os
from pathlib import Path

key_file = Path("/etc/secrets/kalshi_private_key.pem")

if key_file.exists():
    os.environ["KALSHI_PRIVATE_KEY"] = key_file.read_text(
        encoding="utf-8"
    )

from app import app
