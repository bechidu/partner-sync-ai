# Logistics Partner Data Ingestion System

A Python-based system for ingesting and transforming shipment data from logistics partners using GenAI for schema extraction and mapping.

## Overview

This system processes shipment data from partners via multiple transport methods (SFTP, REST, Webhook), extracts partner schemas using Generative AI, maps fields to a canonical JSON schema, and validates the transformed data. All operations are performed through an intuitive web interface.

## Features

- **Multi-transport ingestion**: Support for SFTP (CSV/TXT), REST (JSON), and Webhook (XML/JSON)
- **GenAI-powered schema extraction**: Uses Groq or OpenAI LLMs to infer partner field schemas
- **Interactive mapping UI**: Streamlit-based interface for reviewing and editing field mappings
- **Automated transformation**: Applies mappings to transform data to canonical format
- **Validation**: Ensures output conforms to canonical JSON schema
- **Two-stage AI process**: Separate extraction and mapping for better accuracy

## Architecture

### Key Components
- `streamlit_mapping_ui.py`: Main web interface for all operations
- `ingest.py`: Core logic for parsing, AI calls, and transformations
- `handlers/`: Transport-specific parsers (sftp_handler.py, rest_handler.py, webhook_handler.py)
- `canonical_schema.json`: Target unified schema for shipment data
- `partner_schema_*.json`: Extracted partner schemas with mappings

### Data Flow
1. Upload partner sample data via web interface
2. **Stage 1**: GenAI extracts partner fields and creates schema
3. **Stage 2**: GenAI maps partner fields to canonical leaf paths (e.g., "origin.city")
4. Review and edit mappings interactively
5. Apply mappings to transform data
6. Validate against canonical schema

## Quick Start

### Prerequisites
- Python 3.8+
- API key for Groq or OpenAI

### Installation
```bash
pip install -r requirements.txt
```

### Onboard a New Partner
```bash
# Launch the web interface
streamlit run streamlit_mapping_ui.py
```

Then in the browser:
1. Select AI provider (Groq/OpenAI) and enter API key
2. Upload partner sample file (CSV/JSON/XML/TXT)
3. Enter partner name and transport type
4. Click "Stage 1" to extract schema via AI
5. Review/edit AI-suggested mappings
6. Click "Run Transform & Validate" to process sample data

## Dependencies

- groq: GenAI API client
- streamlit: Interactive UI framework
- pandas: Data manipulation
- jsonschema: JSON schema validation
- dateutil: Date parsing

## Project Structure

```
├── handlers/                    # Transport-specific handlers
│   ├── sftp_handler.py
│   ├── rest_handler.py
│   └── webhook_handler.py
├── llm_raw/                     # Raw LLM outputs
├── uploads/                     # Uploaded sample files
├── canonical_schema.json        # Target schema
├── partner_schema_*.json        # Partner schemas
├── canonical_output_*.json      # Transformed outputs
├── streamlit_mapping_ui.py      # Main UI application
├── ingest.py                    # Core processing logic
├── test_groq.py                 # LLM testing
└── .github/copilot-instructions.md # AI assistant guidance
```

## Development

### Testing LLMs
```bash
python test_groq.py
python test_openai.py
```

### Debugging
```bash
python python_debug_csv.py    # CSV parsing issues
python inspect_schema.py      # Schema inspection
```

## Conventions

- Canonical fields use dot notation (e.g., "destination.city", "package.dimensions.length_cm")
- Handle multiple file encodings (UTF-8, UTF-8-BOM, CP1252, Latin-1)
- Validate required fields: tracking_id, destination, status, status_timestamp, customer_contact
- Use logistics context for mappings (origin vs destination, consignee vs shipper)

## License

MIT License