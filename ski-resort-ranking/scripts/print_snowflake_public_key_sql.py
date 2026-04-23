import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.storage.snowflake_utils import get_required_env, load_environment


def get_public_key_file() -> Path:
    configured_path = os.getenv("SNOWFLAKE_PUBLIC_KEY_FILE")
    if configured_path:
        path = Path(configured_path).expanduser()
    else:
        private_key_file = Path(get_required_env("SNOWFLAKE_PRIVATE_KEY_FILE")).expanduser()
        path = private_key_file.with_suffix(".pub")

    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    if not path.exists():
        raise FileNotFoundError(f"Public key file not found: {path}")
    return path


def extract_public_key_body(path: Path) -> str:
    lines = path.read_text(encoding="utf-8").splitlines()
    body_lines = [
        line.strip()
        for line in lines
        if line.strip()
        and "BEGIN PUBLIC KEY" not in line
        and "END PUBLIC KEY" not in line
    ]
    if not body_lines:
        raise ValueError(f"No public key content found in: {path}")
    return "".join(body_lines)


def main() -> None:
    load_environment()
    user_name = get_required_env("SNOWFLAKE_USER")
    public_key_file = get_public_key_file()
    public_key_body = extract_public_key_body(public_key_file)

    print("Run this in Snowsight:")
    print()
    print(f"ALTER USER {user_name} SET RSA_PUBLIC_KEY='{public_key_body}';")
    print()
    print(f"Public key source: {public_key_file}")


if __name__ == "__main__":
    main()
