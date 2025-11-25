# AI Coding Assistant Instructions

## System Overview
This is a Python-based data ingestion and transformation system for logistics partners. It ingests shipment data via SFTP, REST, or Webhook, uses GenAI (Groq) to extract partner schemas and map to a canonical JSON schema, then transforms and validates data.

## Key Components
- `ingest.py`: Main script for partner onboarding; extracts schema via GenAI.
- `handlers/`: Transport-specific handlers (sftp_handler.py, rest_handler.py, webhook_handler.py) for parsing samples.
- `canonical_schema.json`: Target schema for unified shipment data.
- `partner_schema_*.json`: Extracted schemas with GenAI mappings.
- `streamlit_mapping_ui.py`: Interactive UI for reviewing/editing field mappings.
- `generate_and_run_transform.py`: Applies mappings and validates output.

## Data Flow
1. Upload/parse partner sample data via handlers.
2. Stage 1: GenAI extracts partner fields (no mapping yet).
3. Stage 2: GenAI maps partner fields to canonical leaves (e.g., "origin.city").
4. Apply mappings to transform data; validate against canonical schema.

## Developer Workflows
- **Onboard new partner**: Run `python ingest.py --partner NAME --transport sftp --file sample.csv` to extract schema.
- **Map fields**: Use `streamlit run streamlit_mapping_ui.py` to review/edit mappings interactively.
- **Transform & validate**: Run `python generate_and_run_transform.py` (uses heuristics; update for GenAI mappings).
- **Test LLMs**: Run `python test_groq.py` or `test_openai.py` for API validation.
- **Debug**: Use `python python_debug_csv.py` for CSV issues; inspect schemas with `python inspect_schema.py`.

## Conventions
- Canonical fields use dot notation (e.g., "destination.city", "package.dimensions.length_cm").
- Flatten schema to leaf paths for mapping (see `_flatten_canonical_leaves` in ingest.py).
- Handle multiple encodings (utf-8-sig, cp1252, latin-1) for partner files.
- Validate with jsonschema; ensure required fields: tracking_id, destination, status, status_timestamp, customer_contact.
- Use pandas for CSV parsing; sanitize column names (remove BOMs, controls).
- GenAI prompts specify JSON output; handle JSONDecodeError with salvage logic.

## Examples
- Mapping: Partner "FromCity" → "origin.city"; "ReceiverName" → "customer_contact.name".
- Transform: Use `set_nested(canon, "destination.city", value)` for nested assignment.
- Validation: `validate(instance=obj, schema=canonical_schema)` catches errors.

## Dependencies
- groq: For GenAI extraction/mapping.
- streamlit: For mapping UI.
- pandas: For CSV handling.
- jsonschema: For validation.
- dateutil: For date parsing.

Focus on logistics context (origin/destination, consignee/shipper) when mapping fields.