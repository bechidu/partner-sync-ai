# handlers/webhook_handler.py
import xmltodict
from pathlib import Path
import json

def handle_webhook_sample(file_path):
    p = Path(file_path)
    if not p.exists():
        raise FileNotFoundError(f"{file_path} not found")
    raw_text = p.read_text(encoding="utf-8")
    try:
        obj = xmltodict.parse(raw_text)
    except Exception:
        # fallback: return raw
        return {
            "transport": "webhook",
            "sample_records": [{"raw": raw_text[:200]}],
            "raw_text": raw_text,
            "metadata": {"filename": str(p)}
        }

    # Try to find the root shipment element and convert to dict(s)
    # If it is a single shipment, make list of one
    # xmltodict returns OrderedDicts - convert to plain dict
    def to_plain(d):
        return json.loads(json.dumps(d))

    # Heuristic: if root has child that is plural e.g., Shipments -> Shipment list
    root = obj
    # get the first child (typical webhook single shipment)
    if isinstance(root, dict):
        # If the root contains a list under a plural key, use it
        for k,v in root.items():
            if isinstance(v, list):
                sample_records = [to_plain(x) for x in v[:3]]
                break
        else:
            # use the root as single record
            sample_records = [to_plain(root)]
    else:
        sample_records = [{"raw": raw_text[:200]}]

    return {
        "transport": "webhook",
        "sample_records": sample_records,
        "raw_text": raw_text,
        "metadata": {"filename": str(p)}
    }
