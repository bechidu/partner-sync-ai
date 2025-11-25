# generate_and_run_transform.py
import json
import os
from pathlib import Path
from datetime import datetime
from dateutil import parser as dateparser
from jsonschema import validate, ValidationError

# use existing handler for reading sample records
from handlers.sftp_handler import handle_sftp_sample

CANONICAL_SCHEMA_FILE = "canonical_schema.json"
PARTNER_SCHEMA_FILE = "partner_schema_FASTCO.json"
SAMPLE_FILE = "partner_sftp.csv"   # change if you want to test other files
OUTPUT_FILE = "canonical_output_FASTCO.json"

# small helper: normalize names for fuzzy matching
def normalize_name(s):
    if s is None:
        return ""
    return "".join(ch.lower() for ch in str(s) if ch.isalnum())

# mapping heuristics: map partner field into canonical structure
def heuristic_map_field(pname, pvalue):
    n = normalize_name(pname)
    # tracking id candidates
    if any(k in n for k in ("awb", "airwaybill", "tracking", "awbno", "awbnumber")):
        return ("tracking_id", str(pvalue))
    # dates
    if any(k in n for k in ("date", "time", "ts", "timestamp")) and "status" in n:
        # status timestamp
        try:
            dt = dateparser.parse(str(pvalue))
            return ("status_timestamp", dt.isoformat())
        except Exception:
            return ("status_timestamp", str(pvalue))
    if any(k in n for k in ("pickupdate", "pickupdate")):
        try:
            dt = dateparser.parse(str(pvalue))
            return ("pickup_date", dt.isoformat())
        except Exception:
            return ("pickup_date", str(pvalue))
    # city mapping
    if any(k in n for k in ("fromcity", "origincity", "origin")):
        return ("origin.city", str(pvalue))
    if any(k in n for k in ("tocity", "destinationcity", "destcity", "to")):
        return ("destination.city", str(pvalue))
    # weight/dimensions
    if "weight" in n:
        try:
            return ("weight_kg", float(pvalue) if pvalue != "" else None)
        except Exception:
            return ("weight_kg", None)
    if any(k in n for k in ("length", "lcm", "lengthcm")):
        try: return ("dimensions_cm.l", float(pvalue))
        except: return ("dimensions_cm.l", None)
    if any(k in n for k in ("width", "wcm", "widthcm")):
        try: return ("dimensions_cm.w", float(pvalue))
        except: return ("dimensions_cm.w", None)
    if any(k in n for k in ("height", "hcm", "heightcm")):
        try: return ("dimensions_cm.h", float(pvalue))
        except: return ("dimensions_cm.h", None)
    # service
    if "service" in n or "servicetype" in n:
        return ("service_type", str(pvalue))
    # status
    if n in ("status", "currentstatus", "eventcode"):
        return ("status", str(pvalue))
    # contact
    if any(k in n for k in ("receivername","name","contactname")):
        return ("customer_contact.name", str(pvalue))
    if any(k in n for k in ("receiverphone","phone","contactphone")):
        return ("customer_contact.phone", str(pvalue))
    if "email" in n:
        return ("customer_contact.email", str(pvalue))
    # fallback: put into notes
    return ("notes", f"{pname}:{pvalue}")

def set_nested(d, path, value):
    """
    path like 'destination.city' sets nested dict keys
    """
    parts = path.split(".")
    cur = d
    for p in parts[:-1]:
        if p not in cur or not isinstance(cur[p], dict):
            cur[p] = {}
        cur = cur[p]
    cur[parts[-1]] = value

def build_canonical_from_partner_samples(partner_schema_file, sample_file):
    # load partner schema if needed (not strictly necessary for heuristics)
    if not Path(partner_schema_file).exists():
        raise FileNotFoundError(partner_schema_file)
    ps = json.loads(Path(partner_schema_file).read_text(encoding="utf-8"))

    # use existing handler to load partner sample_records
    sample_out = handle_sftp_sample(sample_file)
    records = sample_out["sample_records"]
    canonical_list = []

    for rec in records:
        canon = {}
        # iterate partner fields
        for k, v in rec.items():
            # best-effort: skip empty strings
            if v == "" or v is None:
                continue
            mapped_key, mapped_val = heuristic_map_field(k, v)
            # normalize mapped_val for basic types
            if mapped_key in ("customer_contact.phone",):
                # always cast phone to string
                try:
                    mapped_val = str(mapped_val)
                except Exception:
                    mapped_val = "" if mapped_val is None else str(mapped_val)
            if mapped_key in ("status_timestamp", "pickup_date"):
                # turn into ISO if possible
                try:
                    import dateutil.parser as _dp
                    dt = _dp.parse(str(mapped_val))
                    mapped_val = dt.isoformat()
                except Exception:
                    mapped_val = str(mapped_val)
            set_nested(canon, mapped_key, mapped_val)
        # post-processing: ensure some fields exist
        # If tracking_id missing, try id or AWB variants
        if "tracking_id" not in canon:
            for alt in ("AWB", "awb_no", "id"):
                if alt in rec and rec.get(alt):
                    canon["tracking_id"] = str(rec.get(alt))
                    break
        # post-processing: ensure customer_contact.phone is string
        if "customer_contact" in canon:
            ph = canon["customer_contact"].get("phone")
            if ph is not None and not isinstance(ph, str):
                canon["customer_contact"]["phone"] = str(ph)
        # add default customer_contact object if missing
        if "customer_contact" not in canon:
            canon["customer_contact"] = {"name": "", "phone": ""}
        canonical_list.append(canon)
    return canonical_list

def validate_and_write(canonical_list, schema_file, out_file):
    schema = json.loads(Path(schema_file).read_text(encoding="utf-8"))
    valid_count = 0
    results = []
    for idx, obj in enumerate(canonical_list):
        try:
            validate(instance=obj, schema=schema)
            ok = True
            valid_count += 1
            errors = None
        except ValidationError as e:
            ok = False
            errors = str(e.message)
        results.append({"index": idx, "valid": ok, "errors": errors, "object": obj})
    # write output file
    Path(out_file).write_text(json.dumps([r["object"] for r in results], indent=2, ensure_ascii=False), encoding="utf-8")
    return results, valid_count

def main():
    print("Building canonical output from partner samples...")
    canonical_list = build_canonical_from_partner_samples(PARTNER_SCHEMA_FILE, SAMPLE_FILE)
    print(f"Built {len(canonical_list)} canonical records. Validating against {CANONICAL_SCHEMA_FILE}...")
    results, valid_count = validate_and_write(canonical_list, CANONICAL_SCHEMA_FILE, OUTPUT_FILE)
    print(f"Validation: {valid_count}/{len(canonical_list)} records valid.")
    # print per-record summary
    for r in results:
        print(f"- Record {r['index']}: valid={r['valid']}; errors={r['errors']}")
    print(f"Canonical output written to: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
