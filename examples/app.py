from pathlib import Path

from framework.app import FrameworkApp
from .config import Config

framework = FrameworkApp.from_config(Config, app_root=Path(__file__).parent)
app = framework.build_app()

if __name__ == "__main__":
    framework.serve()
