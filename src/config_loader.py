from pathlib import Path

import yaml


def load_yaml_config(path: str = "config.yaml") -> dict:
    """Load YAML configuration file."""
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    return cfg
