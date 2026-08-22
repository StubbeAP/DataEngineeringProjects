import yaml
from pathlib import Path

def load_config(config_path: str = "config/producer_config.yaml") -> dict:
    base_dir = Path(__file__).resolve().parent.parent.parent
    full_path = base_dir / config_path
    
    if not full_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {full_path}")
        
    with open(full_path, "r") as f:
        return yaml.safe_load(f)
