"""
Global configuration object for the CLI. Use the `get_cli_config()` method
to access and modify the configuration.
"""

from dataclasses import dataclass
from pathlib import Path


@dataclass
class CliConfig:
    config_file_path: Path
    key_dir: Path
    verbose: bool
