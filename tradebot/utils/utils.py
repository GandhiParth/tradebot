from typing import Dict, Any
import yaml


def read_yaml_file(filename: str) -> Dict[str, Any]:
    with open(filename, "r") as file:
        return yaml.safe_load(file)
