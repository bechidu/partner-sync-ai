# debug_status_mapping.py
import json
from pathlib import Path
from jsonschema import validate, ValidationError

PARTNER_SCHEMA = "partner_schema_FASTCO.json"
CANONICAL_OUT = "canonical_output_FASTCO.json"
CANONICAL_SCHEMA = "canonical_schema.json"

def load_json(p):
    return json.loads(Path(p).read_text(encoding="utf-8"))

def print_mapped_fields():
    schema = load_json(PARTNER_SCHEMA)
    print("Partner schema fields and their mapped_to values:")
    for f in schema.get("fields", []):
        print(f" - partner_field: '{f.get('partner_field')}' -> mapped_to: {f.get('mapped_to')!r} (confidence={f.get('confidence')})")
    print("----\n")

def inspect_canonical_objects():
    objs = load_json(CANONICAL_OUT)
    print(f"Loaded {len(objs)} canonical objects from {CANONICAL_OUT}")
    for i, o in enumerate(objs):
        keys = list(o.keys())
        print(f"Object {i} keys: {keys}")
        # if nested, also print top-level nested keys for customer_contact/destination if present
        if "customer_contact" in o:
            print(f"  customer_contact keys: {list(o['customer_contact'].keys())}")
        if "destination" in o:
            print(f"  destination keys: {list(o['destination'].keys())}")
    print("----\n")
    return objs

def validate_objects(objs):
    schema = load_json(CANONICAL_SCHEMA)
    for i, o in enumerate(objs):
        try:
            validate(instance=o, schema=schema)
            print(f"Record {i}: VALID")
        except ValidationError as e:
            print(f"Record {i}: INVALID")
            # print concise error path + message
            print("  jsonschema message:", e.message)
            print("  failing path:", list(e.path))
            print("  failing schema path (where requirement was):", list(e.schema_path))
            # show the object so we can inspect what is missing / misnamed
            print("  object snapshot (top-level keys):", list(o.keys()))
            # print the object itself (small)
            import pprint
            pprint.pprint(o)
            # stop after first failure to keep output short
            break

if __name__ == "__main__":
    if not Path(PARTNER_SCHEMA).exists():
        print(f"File not found: {PARTNER_SCHEMA}")
    else:
        print_mapped_fields()
    if not Path(CANONICAL_OUT).exists():
        print(f"File not found: {CANONICAL_OUT}")
    else:
        objs = inspect_canonical_objects()
        validate_objects(objs)
