# inspect_schema.py
import json
import argparse
from jsonschema import validate, ValidationError

PARTNER_SCHEMA_VALIDATOR = {
    "type": "object",
    "required": ["partner_name", "transport", "fields", "transform_snippet"],
    "properties": {
        "partner_name": {"type": "string"},
        "transport": {"type": "string"},
        "fields": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["partner_field", "inferred_type", "example_value", "mapped_to", "confidence", "short_description"],
                "properties": {
                    "partner_field": {"type": "string"},
                    "inferred_type": {"type": "string"},
                    "example_value": {},
                    "mapped_to": {},
                    "confidence": {"type": "number"},
                    "short_description": {"type": "string"}
                }
            }
        },
        "transform_snippet": {"type": "string"},
        "notes": {"type": "string"}
    },
    "additionalProperties": True
}

def print_field_table(fields):
    # column widths
    c1, c2, c3, c4 = 28, 12, 20, 9
    hdr = f"{'Partner Field':{c1}} {'Type':{c2}} {'Mapped To':{c3}} {'Confidence':{c4}}"
    print(hdr)
    print("-" * (c1 + c2 + c3 + c4 + 6))
    for f in fields:
        pf = str(f.get("partner_field", ""))[:c1-1]
        it = str(f.get("inferred_type", ""))[:c2-1]
        mt = str(f.get("mapped_to", ""))[:c3-1]
        conf = f.get("confidence", "")
        # format confidence
        try:
            confs = float(conf)
            confs = f"{confs:.2f}"
        except Exception:
            confs = str(conf)
        print(f"{pf:{c1}} {it:{c2}} {mt:{c3}} {confs:{c4}}")

def main():
    parser = argparse.ArgumentParser(description="Inspect & validate a partner schema JSON produced by the LLM")
    parser.add_argument("--file", required=False, default="partner_schema_FASTCO.json", help="Path to partner schema JSON file")
    args = parser.parse_args()

    with open(args.file, "r", encoding="utf-8") as f:
        obj = json.load(f)

    print(f"\nLoaded schema file: {args.file}\n")
    partner = obj.get("partner_name", "<unknown>")
    transport = obj.get("transport", "<unknown>")
    fields = obj.get("fields", [])
    print(f"Partner: {partner}")
    print(f"Transport: {transport}")
    print(f"Number of fields extracted: {len(fields)}\n")

    # Basic JSON Schema validation
    print("Running JSON Schema validation...")
    try:
        validate(instance=obj, schema=PARTNER_SCHEMA_VALIDATOR)
        print(" - JSON shape: OK")
    except ValidationError as e:
        print(" - JSON shape: FAILED")
        print("   Validation error:", e.message)
        print("   At path:", list(e.path))
    except Exception as e:
        print(" - JSON validation threw exception:", str(e))

    # Field-level checks
    print("\nField summary:")
    if not fields:
        print(" No fields found in schema.")
    else:
        print_field_table(fields)

        # Additional checks: confidence range and type sanity
        print("\nField-level checks:")
        bad_conf = []
        for f in fields:
            try:
                c = float(f.get("confidence", 0))
                if c < 0.0 or c > 1.0:
                    bad_conf.append((f.get("partner_field"), c))
            except Exception:
                bad_conf.append((f.get("partner_field"), f.get("confidence")))
        if not bad_conf:
            print(" - All confidences in [0.0,1.0].")
        else:
            print(" - Confidence issues found:")
            for p,c in bad_conf:
                print(f"    * {p}: {c}")

    # Show transform snippet (if present)
    snippet = obj.get("transform_snippet", "")
    if snippet:
        print("\nTransform snippet (preview, first 400 chars):\n")
        print(snippet[:400])
        # optionally save full snippet to a file for review
        fname = f"transform_{partner}.py"
        try:
            with open(fname, "w", encoding="utf-8") as f:
                f.write(snippet)
            print(f"\nFull transform snippet saved to: {fname}")
        except Exception as e:
            print("Could not write transform snippet to file:", e)

    print("\nDone.\n")

if __name__ == "__main__":
    main()
