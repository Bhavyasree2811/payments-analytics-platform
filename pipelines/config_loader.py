import yaml
from pathlib import Path


def load_config():
    project_root = Path(__file__).resolve().parent.parent
    config_path = project_root / "config.yaml"

    with open(config_path, "r") as file:
        return yaml.safe_load(file)


def get_full_path(relative_path):
    project_root = Path(__file__).resolve().parent.parent
    return project_root / relative_path
