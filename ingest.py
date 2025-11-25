# ingest.py
"""
Ingest helpers for GenAI partner onboarding (Stage-1 extraction, Stage-2 mapping).
Option A implementation: uses Groq SDK if available.
"""

import json
import re
from pathlib import Path
from datetime import datetime
import csv
import io
import xml.etree.ElementTree as ET
from jsonschema import validate as js_validate, ValidationError

CANONICAL_SCHEMA_FILE = "canonical_schema.json"

# -------------------------
# Utility helpers
# -------------------------
def normalize_partner_field_name(s: str) -> str:
    """Normalize partner field names: strip BOMs/nulls, replace spaces/underscores with dots,
    collapse multiple dots, lower-case is not enforced to keep original label but UI uses exact keys."""
    if s is None:
        return ""
    out = str(s).strip()
    out = out.replace("\ufeff", "").replace("\ufffe", "").replace("\x00", "")
    out = out.replace(" ", ".").replace("_", ".")
    while ".." in out:
        out = out.replace("..", ".")
    return out

def read_file_bytes_to_text(p: Path) -> str:
    """Read bytes and try decoding common encodings (handles BOM/UTF-16)."""
    b = p.read_bytes()
    # BOM UTF-16 little / big
    if b.startswith(b"\xff\xfe") or b.startswith(b"\xfe\xff"):
        text = b.decode("utf-16", errors="replace")
        return text.replace("\x00", "")
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return b.decode(enc)
        except Exception:
            continue
    return b.decode("latin-1", errors="replace")

def detect_csv_delimiter(text: str) -> str:
    lines = [ln for ln in text.splitlines() if ln.strip()]
    sample = "\n".join(lines[:10]) if lines else text[:2000]
    best, best_cnt = ',', -1
    for c in [',', '\t', ';', '|']:
        cnt = sample.count(c)
        if cnt > best_cnt:
            best, best_cnt = c, cnt
    return best

def flatten_record(rec):
    """Flatten a nested dict into dotted paths."""
    def _flatten(obj, prefix=''):
        items = []
        if isinstance(obj, dict):
            for k, v in obj.items():
                new_prefix = f"{prefix}.{k}" if prefix else k
                items.extend(_flatten(v, new_prefix))
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                new_prefix = f"{prefix}[{i}]" if prefix else f"[{i}]"
                items.extend(_flatten(item, new_prefix))
        else:
            items.append((prefix, obj))
        return items
    return dict(_flatten(rec))

def get_nested_value(obj, path):
    """Get value from nested dict by dotted path, or direct key (even with dots), or normalized underscores."""
    if not path:
        return obj
    # First, check if it's a direct key (even with dots in the key name)
    if path in obj:
        return obj[path]
    # Then try as nested path
    parts = path.split(".")
    current = obj
    for part in parts:
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            # If not nested, try as direct key with dots replaced by underscores
            direct_key = path.replace(".", "_")
            return obj.get(direct_key)
    return current

def flatten_element_to_dict(el: ET.Element, prefix: str = "") -> dict:
    """Flatten XML Element into dotted-path keys."""
    out = {}
    for child in el:
        key = f"{prefix}.{child.tag}" if prefix else child.tag
        if list(child):
            out.update(flatten_element_to_dict(child, key))
        else:
            out[key] = (child.text or "").strip()
    return out

# -------------------------
# Parse sample into records
# -------------------------
def parse_sample_to_records(path: str, max_rows: int = 200):
    """
    Parse a sample file (CSV/JSON/XML/TXT) into a list of JSON-like records and raw_text.
    Returns: (records: list[dict], raw_text: str)
    """
    p = Path(path)
    if not p.exists():
        return [], ""
    txt = read_file_bytes_to_text(p)
    raw_text = txt
    s = txt.strip()

    # JSON: list or object containing list
    if s.startswith("{") or s.startswith("["):
        try:
            obj = json.loads(s)
            if isinstance(obj, list):
                return obj[:max_rows], raw_text
            if isinstance(obj, dict):
                # common arrays in REST responses
                for candidate in ("shipments", "items", "data", "records", "payload"):
                    if candidate in obj and isinstance(obj[candidate], list):
                        return obj[candidate][:max_rows], raw_text
                list_values = [v for v in obj.values() if isinstance(v, list)]
                if list_values:
                    return list_values[0][:max_rows], raw_text
                return [obj], raw_text
        except Exception:
            # fallthrough to other attempts
            pass

    # XML
    if s.startswith("<"):
        try:
            # Clean malformed XML with double quotes
            cleaned_s = s.replace('""', '"')
            root = ET.fromstring(cleaned_s)
            # For XML, assume single record and flatten the root
            rec = flatten_element_to_dict(root)
            return [rec], raw_text
        except Exception:
            pass

    # CSV fallback
    lines = [ln for ln in txt.splitlines() if ln.strip()]
    if lines:
        delim = detect_csv_delimiter(txt)
        try:
            reader = csv.DictReader(io.StringIO(txt), delimiter=delim)
            rows = []
            for i, r in enumerate(reader):
                if i >= max_rows:
                    break
                clean = {}
                for k, v in r.items():
                    if k is None:
                        continue
                    kk = str(k).strip().replace("\ufeff", "").replace("\ufffe", "")
                    clean[kk] = v
                if any((v or "").strip() for v in clean.values()):
                    rows.append(clean)
            if rows:
                return rows, raw_text
        except Exception:
            # fallback: attempt manual split
            header = [h.strip().replace("\ufeff", "").replace("\ufffe", "") for h in lines[0].split(delim)]
            rows = []
            for ln in lines[1:1 + max_rows]:
                parts = [p.strip() for p in ln.split(delim)]
                if len(parts) < len(header):
                    parts += [""] * (len(header) - len(parts))
                rec = {h: (parts[i] if i < len(parts) else "") for i, h in enumerate(header)}
                if any((v or "").strip() for v in rec.values()):
                    rows.append(rec)
            if rows:
                return rows, raw_text

    # last resort: each non-empty line as raw_line
    return ([{"raw_line": ln} for ln in lines[:max_rows]], raw_text)

# -------------------------
# Groq API wrapper
# -------------------------
def _extract_json_from_model_text(text: str):
    """
    Attempt to robustly extract the first JSON object/array from freeform model output.
    Returns parsed JSON object.
    """
    # Try direct parse first
    text = text.strip()
    try:
        return json.loads(text)
    except Exception:
        pass

    # Use regex to find a JSON object or array
    # find the first {...} or [...]
    obj_re = re.search(r"(\{(?:.|\n)*\}|\[(?:.|\n)*\])", text)
    if obj_re:
        candidate = obj_re.group(0)
        try:
            return json.loads(candidate)
        except Exception:
            # try to fix trailing commas and common minor issues
            fixed = re.sub(r",\s*}", "}", candidate)
            fixed = re.sub(r",\s*]", "]", fixed)
            try:
                return json.loads(fixed)
            except Exception:
                raise ValueError(f"Failed to parse JSON from model output after cleanup. Raw start: {text[:500]}")
    raise ValueError("Could not locate strict JSON in model output.")

def send_chat_completion_groq(api_key: str, model: str, messages: list, temperature: float = 0.0, max_tokens: int = 2048):
    """
    Send a chat completion request to Groq using the groq SDK if available.
    Returns the model's raw text response.
    """
    try:
        import groq
    except Exception as e:
        raise RuntimeError("Groq SDK not installed or importable in this environment. Install 'groq' Python package.") from e

    # Instantiate client. Adjust according to your groq SDK if different.
    client = None
    try:
        client = groq.Client(api_key=api_key)
    except Exception:
        # older/newer variants might be different; try groq.GroqClient
        try:
            client = groq.GroqClient(api_key=api_key)
        except Exception as e:
            raise RuntimeError("Failed to initialize Groq client. Check SDK version and API usage.") from e

    # Prepare messages as expected by the SDK
    # Many SDKs accept messages list with role/content similar to OpenAI chat
    try:
        resp = client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        # resp likely contains choices with message structure; attempt to retrieve text
        # Support multiple resp shapes
        if hasattr(resp, "choices") and resp.choices:
            choice = resp.choices[0]
            # choice may have .message.content or .text
            if hasattr(choice, "message") and isinstance(choice.message, dict) and "content" in choice.message:
                return choice.message["content"]
            if hasattr(choice, "message") and hasattr(choice.message, "content"):
                return choice.message.content
            if hasattr(choice, "text"):
                return choice.text
        # fallback: string representation
        return str(resp)
    except Exception as e:
        # Surface underlying error message
        raise RuntimeError(f"Groq API request failed: {e}") from e

# -------------------------
# Stage-1: Extract schema (single dynamic prompt)
# -------------------------
STAGE1_PROMPT_TEMPLATE = """
You are an expert at extracting structured field schemas from partner data samples (CSV, JSON, XML, or plain text).
You will be given either an inline sample_records array and/or a file_url path. Use sample_records if provided for concrete examples.
Do NOT produce any commentary — return STRICT valid JSON ONLY that matches the schema below.

Rules & normalization:
1. Detect file format automatically (CSV, JSON, XML, plain text).
   - For JSON: if input is an object containing an array (e.g., {"shipments": [...]}) extract that array as records.
   - For XML: flatten nested tags into dotted paths (e.g., <Receiver><Name> becomes "Receiver.Name").
   - For CSV: use header row as field names.
2. partner_field must be trimmed of BOM / null chars. Replace spaces/underscores with dots.
3. inferred_type must be one of ["string","number","integer","boolean","datetime","object","array","null"].
4. Provide example_value (first non-empty example), short_description (1 short phrase), and confidence 0.0-1.0.
5. Use dotted paths for nested keys.

Return JSON with exact shape:
{
  "partner_name": "<partner_name>",
  "transport": "<transport>",
  "source": {"file_url": "<file_url or empty>"},
  "fields": [
    {
      "partner_field": "<string>",
      "inferred_type": "<string>",
      "example_value": <string|number|null>,
      "short_description": "<string>",
      "confidence": <number between 0.0 and 1.0>
    }, ...
  ],
  "transform_snippet": "<optional python snippet>",
  "notes": "<optional>"
}

Now produce that JSON using at most 10 sample records for examples.
"""

def call_groq_extract_schema(api_key, model_name, partner_name, transport, sample_path, sample_records=None, temperature=0.0):
    """
    Call Stage-1 prompt to extract partner field schema using Groq.
    Returns parsed Python dict as described in STAGE1 output shape.
    """
    # parse sample if not provided
    if sample_records is None:
        sample_records, _ = parse_sample_to_records(sample_path, max_rows=10)

    # flatten records for better field extraction
    flattened_samples = [flatten_record(rec) for rec in sample_records]

    # prepare messages (system + user)
    user_payload = {
        "partner_name": partner_name,
        "transport": transport,
        "file_url": str(sample_path),
        "sample_records": flattened_samples[:10],
    }
    system_msg = {"role": "system", "content": "You are a strict JSON-output generator. Follow instructions exactly."}
    user_msg = {"role": "user", "content": STAGE1_PROMPT_TEMPLATE + "\n\nINPUT_PAYLOAD:\n" + json.dumps(user_payload, ensure_ascii=False, indent=2)}

    # call groq
    if not api_key:
        raise RuntimeError("No Groq API key provided to call_groq_extract_schema. Provide API key or implement fallback.")

    raw_resp_text = send_chat_completion_groq(api_key, model_name, messages=[system_msg, user_msg], temperature=temperature, max_tokens=4096)
    # extract JSON
    parsed = _extract_json_from_model_text(raw_resp_text)
    # Basic normalization on parsed fields
    if "fields" in parsed and isinstance(parsed["fields"], list):
        for f in parsed["fields"]:
            if "partner_field" in f:
                f["partner_field"] = normalize_partner_field_name(f["partner_field"])
            if "confidence" in f:
                try:
                    f["confidence"] = float(f["confidence"])
                except Exception:
                    f["confidence"] = 0.0
    # Attach source if not present
    if "source" not in parsed:
        parsed["source"] = {"file_url": str(sample_path)}
    return parsed

# -------------------------
# Stage-2: Map to canonical (single dynamic prompt)
# -------------------------
STAGE2_PROMPT_TEMPLATE = """
You are an expert in mapping partner fields to a canonical logistics schema.
Input: a JSON list of partner fields (with partner_field, inferred_type, example_value, short_description, confidence)
and a list of canonical leaf paths (dotted).
Task: For each partner_field, choose the best matching canonical leaf path, or null if no reliable match.

Rules:
1. Match by normalized name first (case-insensitive; underscores/spaces -> dots).
2. If no exact normalized match, match by token overlap (e.g., 'receiver.phone' -> 'customer_contact.phone').
3. Prefer the most specific leaf path.
4. If your confidence would be < 0.5, return null for that field.
Return STRICT JSON only in the following shape:
{
  "partner_name": "<partner_name>",
  "canonical_leaves": [ "<dot.path.one>", "<another.path>" ... ],
  "mappings": [
    { "partner_field": "<string>", "mapped_to": "<dot.path|null>", "confidence": <0.0-1.0>, "reason": "<short reason>" },
    ...
  ]
}
Now map the provided partner fields to the canonical_leaves and return only the JSON.
"""

def call_groq_map_to_canonical(api_key, model_name, partner_name, fields, canonical_path=None, temperature=0.0):
    """
    Call Stage-2 prompt to map partner fields to canonical leaves.
    Returns a dict with key 'mappings' which is a list of mapping objects.
    """
    # prepare canonical leaves
    canon_leaves = []
    if canonical_path and Path(canonical_path).exists():
        try:
            canon = json.loads(Path(canonical_path).read_text(encoding="utf-8"))
            # flatten leaves
            def flatten(schema, parent=""):
                props = schema.get("properties", {})
                leaves = []
                for k, v in props.items():
                    path = f"{parent}.{k}" if parent else k
                    if isinstance(v, dict) and v.get("type") == "object" and "properties" in v:
                        leaves.extend(flatten(v, parent=path))
                    else:
                        leaves.append(path)
                return leaves
            canon_leaves = flatten(canon)
        except Exception:
            canon_leaves = []
    # build user payload
    user_payload = {
        "partner_name": partner_name,
        "fields": fields,
        "canonical_leaves": canon_leaves,
    }
    system_msg = {"role": "system", "content": "You are a strict JSON-output generator. Follow instructions exactly."}
    user_msg = {"role": "user", "content": STAGE2_PROMPT_TEMPLATE + "\n\nINPUT_PAYLOAD:\n" + json.dumps(user_payload, ensure_ascii=False, indent=2)}
    if not api_key:
        raise RuntimeError("No Groq API key provided to call_groq_map_to_canonical. Provide API key.")
    raw_resp = send_chat_completion_groq(api_key, model_name, messages=[system_msg, user_msg], temperature=temperature, max_tokens=2048)
    parsed = _extract_json_from_model_text(raw_resp)
    # Normalize mapping shapes: ensure parsed["mappings"] exists and each item has partner_field,mapped_to,confidence
    if isinstance(parsed, dict) and "mappings" in parsed and isinstance(parsed["mappings"], list):
        mappings = []
        for m in parsed["mappings"]:
            pf = m.get("partner_field")
            mt = m.get("mapped_to")
            conf = m.get("confidence", 0.0)
            try:
                conf = float(conf)
            except Exception:
                conf = 0.0
            mappings.append({"partner_field": normalize_partner_field_name(pf), "mapped_to": mt, "confidence": conf, "reason": m.get("reason", "")})
        return {"mappings": mappings, "canonical_leaves": parsed.get("canonical_leaves", canon_leaves)}
    # If model returned a flat dict mapping, normalize
    if isinstance(parsed, dict):
        # If flat mapping (k->v)
        possible = []
        for k, v in parsed.items():
            if isinstance(k, str) and (isinstance(v, str) or v is None):
                possible.append({"partner_field": normalize_partner_field_name(k), "mapped_to": v, "confidence": 0.95 if v else 0.0, "reason": ""})
        return {"mappings": possible, "canonical_leaves": canon_leaves}
    raise ValueError("Unexpected Stage-2 model response shape; expected mapping dict or object with 'mappings' list.")

# -------------------------
# Apply mappings to sample records (used by UI)
# -------------------------
def apply_mappings(records, schema_obj):
    """
    records: list[dict]
    schema_obj: object generated/edited by Stage1 with 'fields' containing partner_field->mapped_to
    Returns list of canonical objects
    """
    canonical_list = []
    mapped_targets = [f.get("mapped_to") for f in schema_obj.get("fields", []) if f.get("mapped_to")]
    needs_dest = any(str(t).startswith("destination.") for t in mapped_targets if t)
    needs_origin = any(str(t).startswith("origin.") for t in mapped_targets if t)

    for rec in records:
        canon = {}
        for f in schema_obj.get("fields", []):
            pf = f.get("partner_field")
            mapped = f.get("mapped_to")
            if not mapped:
                continue
            v = get_nested_value(rec, pf)
            # basic type conversions
            if v is None or v == "":
                continue
            if isinstance(v, str):
                vs = v.strip()
                # integers
                if vs.isdigit():
                    try:
                        v = int(vs)
                    except Exception:
                        pass
                else:
                    # float?
                    try:
                        v = float(vs)
                    except Exception:
                        # keep as string
                        v = vs
            # build nested dict per dotted path
            parts = str(mapped).split(".")
            cur = canon
            for part in parts[:-1]:
                if part not in cur or not isinstance(cur[part], dict):
                    cur[part] = {}
                cur = cur[part]
            cur[parts[-1]] = v

        # ensure certain structures exist if required by canonical
        if needs_dest and "destination" not in canon:
            canon["destination"] = canon.get("destination", {})
        if needs_origin and "origin" not in canon:
            canon["origin"] = canon.get("origin", {})

        # customer_contact.phone to string
        cc = canon.get("customer_contact")
        if isinstance(cc, dict):
            ph = cc.get("phone")
            if ph is not None and not isinstance(ph, str):
                cc["phone"] = str(ph)

        canonical_list.append(canon)
    return canonical_list

# -------------------------
# Validate canonical objects against canonical JSON schema
# -------------------------
def validate_list(canonical_list, schema_file=CANONICAL_SCHEMA_FILE):
    """
    Validates each object with jsonschema against provided schema_file.
    Returns (results_list, valid_count)
    """
    schema = {}
    try:
        schema = json.loads(Path(schema_file).read_text(encoding="utf-8"))
    except Exception as e:
        raise RuntimeError(f"Failed to load canonical schema file {schema_file}: {e}")

    results = []
    valid_count = 0
    for i, obj in enumerate(canonical_list):
        try:
            js_validate(instance=obj, schema=schema)
            results.append({"index": i, "valid": True, "errors": None, "object": obj})
            valid_count += 1
        except ValidationError as e:
            results.append({"index": i, "valid": False, "errors": str(e.message), "object": obj})
    return results, valid_count

# Expose common names for UI import
__all__ = [
    "parse_sample_to_records",
    "normalize_partner_field_name",
    "call_groq_extract_schema",
    "call_groq_map_to_canonical",
    "apply_mappings",
    "validate_list",
    "CANONICAL_SCHEMA_FILE",
]
