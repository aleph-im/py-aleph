import json
from pathlib import Path
from typing import Dict, List


def load_fixture_messages(fixture: str) -> List[Dict]:
    fixture_path = Path(__file__).parent / "fixtures" / fixture

    with open(fixture_path) as f:
        return json.load(f)["content"]["messages"]


def load_fixture_message(fixture: str) -> Dict:
    fixture_path = Path(__file__).parent / "fixtures" / fixture

    with open(fixture_path) as f:
        return json.load(f)


def load_fixture_message_list(fixture: str) -> List[Dict]:
    fixture_path = Path(__file__).parent / "fixtures" / fixture

    with open(fixture_path) as f:
        return json.load(f)
