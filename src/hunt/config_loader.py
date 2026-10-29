# ./src/hunt/config_loader.py
import os
from pathlib import Path

import yaml

# Repo root (or /app in the container): src/hunt/config_loader.py -> ../../
PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_ENV_VAR = "HUNT_CONFIG"


def default_config_path() -> str:
    """$HUNT_CONFIG if set, else config/config.yaml in the project root."""
    return os.environ.get(CONFIG_ENV_VAR) or str(PROJECT_ROOT / "config" / "config.yaml")


def load_yaml_config(path: str = None) -> dict:
    """Load YAML configuration file (default: default_config_path())."""
    path = path or default_config_path()
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    return cfg
