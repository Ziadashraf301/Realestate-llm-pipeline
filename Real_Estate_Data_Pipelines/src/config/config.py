from pathlib import Path
import json
from .config_model import RealEstateConfigModel

class Config:
    """Main configuration loader and validator."""
    def __init__(self, json_path: str):
        self.json_path = Path(json_path)
        self._load_config()

    def _load_config(self):
        if not self.json_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.json_path}")
        
        # Load JSON
        with open(self.json_path, "r", encoding="utf-8") as f:
            raw = json.load(f)

        # Remove comments (keys starting with '_comment')
        cleaned_keys = {k: v for k, v in raw.items() if not k.startswith("_comment")}

        # Validate using Pydantic
        self._model = RealEstateConfigModel(**cleaned_keys)

         # Set all attributes dynamically
        for key, value in self._model.dict().items():
            setattr(self, key, value)

    def reload(self):
        """Reload configuration at runtime."""
        self._load_config()

