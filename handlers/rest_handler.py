# handlers/rest_handler.py
import json
from pathlib import Path
import requests

def handle_rest_sample(file_or_url):
    """
    Accepts either a local JSON file path or a sandbox URL.
    Returns normalized sample_records (list of dicts) and raw_text.
    """
    raw_text = ""
    sample_records = []

    # if looks like URL, try fetching it
    if str(file_or_url).startswith("http://") or str(file_or_url).startswith("https://"):
        r = requests.get(file_or_url, timeout=10)
        r.raise_for_status()
        raw_text = r.text
        data = r.json()
    else:
        p = Path(file_or_url)
        if not p.exists():
            raise FileNotFoundError(f"{file_or_url} not found")
        raw_text = p.read_text(encoding="utf-8")
        data = json.loads(raw_text)

    # attempt to find list of shipments
    if isinstance(data, dict):
        # common keys
        for key in ["shipments", "data", "items"]:
            if key in data and isinstance(data[key], list):
                sample_records = data[key][:3]
                break
        # fallback: if dict contains records directly
        if not sample_records:
            # if it looks like a single record with many keys, wrap it
            sample_records = [data]
    elif isinstance(data, list):
        sample_records = data[:3]
    else:
        sample_records = [{"raw": str(data)}]

    return {
        "transport": "rest",
        "sample_records": sample_records,
        "raw_text": raw_text,
        "metadata": {"source": file_or_url}
    }
