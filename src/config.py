# src/config_manager.py
import json
import sys
from pathlib import Path

def get_config_path():
    """
    Returns a writable, persistent path for config.json.
    When frozen by PyInstaller, puts it next to the .exe instead of inside the temp folder.
    """
    if getattr(sys, 'frozen', False):
        # Running inside PyInstaller
        base_path = Path(sys.executable).parent
    else:
        # Running as normal script
        base_path = Path(__file__).parent
    return base_path / "config.json"


def create_default_config(config_path: Path):
    """Prompts user to create a new config.json interactively."""
    print("Please input the AE info for your SCP (Local listener):")
    scp_aetitle = input("SCP_AETITLE: ")
    scp_host = input("SCP_HOST: ")
    scp_port = int(input("SCP_PORT: "))

    print("\nPlease input the AE info for the Clinical Server:")
    clinical_aetitle = input("CLINICAL_AETITLE: ")
    clinical_host = input("CLINICAL_HOST: ")
    clinical_port = int(input("CLINICAL_PORT: "))

    ae_info = {
        "CLINICAL_SERVER": {
            "AETITLE": clinical_aetitle,
            "HOST": clinical_host,
            "PORT": clinical_port,
        },
        "SCP_SERVER": {
            "AETITLE": scp_aetitle,
            "HOST": scp_host,
            "PORT": scp_port,
        },
    }

    with open(config_path, "w") as f:
        json.dump(ae_info, f, indent=4)
    print(f"\n✅ Config saved to: {config_path}\n")
    return ae_info


def load_config():
    """
    Loads config.json, prompting the user to create it if it doesn't exist.
    Returns the config dictionary.
    """
    config_path = get_config_path()

    if not config_path.exists():
        print(f"No config found. Creating new one at {config_path}...")
        return create_default_config(config_path)

    with open(config_path, "r") as f:
        config = json.load(f)
    return config
