# streamlit_mapping_ui.py
"""
Streamlit UI for GenAI Partner Onboarding (2-stage).
Works with ingest.py which must provide:
 - parse_sample_to_records
 - normalize_partner_field_name
 - call_groq_extract_schema
 - call_groq_map_to_canonical
 - apply_mappings
 - validate_list
 - CANONICAL_SCHEMA_FILE
"""

import json
from pathlib import Path
from datetime import datetime

import pandas as pd
import streamlit as st

# Import ingest helpers (must be available in same directory)
from ingest import (
    parse_sample_to_records,
    normalize_partner_field_name,
    call_groq_extract_schema,
    call_groq_map_to_canonical,
    apply_mappings,
    validate_list,
    CANONICAL_SCHEMA_FILE,
)

# Page config
st.set_page_config(page_title="GenAI Partner Onboarding — 2-stage", layout="wide")

st.title("🚚 GenAI Logistics Partner Onboarding Platform")
st.markdown("Automate partner data integration with AI-powered schema extraction and field mapping")

# Initialize session state keys
for k in (
    "schema_obj",
    "last_sample_records",
    "last_schema_file",
    "llm_logs",
    "model_api_key",
    "selected_provider",
    "selected_model_name",
    "recent_sample_path",
    "stage2_suggestions",
):
    if k not in st.session_state:
        st.session_state[k] = None

# Model options by provider
PROVIDERS = ["groq", "openai"]
MODELS_BY_PROVIDER = {
    "groq": ["llama-3.1-8b-instant", "llama-3.3-70b-versatile"],
    "openai": ["gpt-3.5-turbo", "gpt-4o-mini"],
}

st.markdown("---")

# Layout: model selection and upload side by side
col_left, col_right = st.columns([1, 1])
with col_left:
    # Model selection & API key
    col_prov, col_mod = st.columns([1, 1])
    with col_prov:
        provider = st.selectbox(
            "Choose Gen AI provider",
            options=PROVIDERS,
            index=PROVIDERS.index(st.session_state.get("selected_provider") or "groq"),
        )
    with col_mod:
        available_models = MODELS_BY_PROVIDER.get(provider, [])
        model_name = st.selectbox(
            "Choose model",
            options=available_models,
            index=available_models.index(st.session_state.get("selected_model_name")) if st.session_state.get("selected_model_name") in available_models else 0,
            key=f"model_{provider}",
        )
    st.session_state["selected_provider"] = provider
    st.session_state["selected_model_name"] = model_name

    selected_model = {"provider": provider, "name": model_name, "label": f"{provider}/{model_name}"}

    api_key = st.text_input(
        f"API Key for {provider.upper()}",
        value=st.session_state.get("model_api_key") or "",
        type="password",
    )
    if api_key:
        st.session_state["model_api_key"] = api_key

with col_right:
    upload = st.file_uploader("Upload partner sample (CSV/JSON/XML/TXT)", type=["csv", "json", "xml", "txt"])
    col_name, col_transport = st.columns([1, 1])
    with col_name:
        partner_name = st.text_input("Partner name", value="NEW_PARTNER")
    with col_transport:
        transport = st.selectbox("Transport", options=["sftp", "rest", "webhook", "soap", "other"])

# Stage 1 button below
extract_btn = st.button("Stage 1: Extract Partner Fields via GenAI")

# When user uploads, save to uploads/ and record path in session
if upload is not None:
    uploads_dir = Path("uploads")
    uploads_dir.mkdir(exist_ok=True)
    tmp_path = uploads_dir / f"uploaded_{normalize_partner_field_name(partner_name)}.tmp"
    tmp_path.write_bytes(upload.getvalue())
    st.session_state["recent_sample_path"] = str(tmp_path.resolve())
    st.success(f"Saved uploaded sample to {tmp_path}")

# Resolve sample_path preference: uploaded only
sample_path = st.session_state.get("recent_sample_path")

# LLM log helper
def log_llm(raw_prompt, raw_response, model_name):
    entry = {
        "time": datetime.utcnow().isoformat() + "Z",
        "model": model_name,
        "raw_prompt": raw_prompt,
        "raw_response": raw_response,
    }
    logs = st.session_state.get("llm_logs") or []
    if not isinstance(logs, list):
        logs = []
    logs.insert(0, entry)
    # keep only recent 200
    st.session_state["llm_logs"] = logs[:200]

# Stage 1: Extract via GenAI (or call ingest wrapper)
if extract_btn:
    if not sample_path:
        st.error("No sample provided (upload a file or provide a local sample path).")
    else:
        # parse sample (for preview + to pass inline)
        try:
            recs, raw_text = parse_sample_to_records(sample_path)
            st.session_state["last_sample_records"] = recs
        except Exception as e:
            st.error(f"Failed to parse sample: {e}")
            recs = []
            raw_text = ""

        # RAW prompt payload for logging
        raw_prompt = {
            "stage": "extract_fields",
            "partner_name": partner_name,
            "transport": transport,
            "file_url": sample_path,
            "sample_records_preview": (recs[:10] if recs else []),
        }

        # call ingest wrapper that performs the Groq call
        try:
            schema = call_groq_extract_schema(
                st.session_state.get("model_api_key"),
                selected_model["name"],
                partner_name,
                transport,
                sample_path,
                recs,
            )
            # normalize partner_field names to stable keys
            for f in schema.get("fields", []):
                if "partner_field" in f:
                    f["partner_field"] = normalize_partner_field_name(f["partner_field"])
            st.session_state["schema_obj"] = schema

            # persist schema
            outp = Path(f"partner_schema_{normalize_partner_field_name(partner_name)}.json")
            outp.write_text(json.dumps(schema, indent=2, ensure_ascii=False), encoding="utf-8")
            st.session_state["last_schema_file"] = str(outp.resolve())

            log_llm(raw_prompt, schema, selected_model["name"])
            st.success(f"Stage 1 complete. Partner schema saved to {outp}")
        except Exception as e:
            log_llm(raw_prompt, {"error": str(e)}, selected_model["name"])
            st.error(f"Stage 1 extraction failed: {e}")

st.markdown("### Sample preview (first 10 rows)")
rows = st.session_state.get("last_sample_records") or []
if rows:
    try:
        st.dataframe(pd.DataFrame(rows[:10]), use_container_width=True)
    except Exception:
        st.write(rows[:10])
else:
    st.info("No sample parsed yet. Upload a file or run Stage 1 to parse a local sample.")

st.markdown("---")

# Stage 2 & mapping section (only shown if schema exists)
schema_obj = st.session_state.get("schema_obj")
if schema_obj:
    st.markdown("### Partner schema (current)")
    st.code(json.dumps(schema_obj, indent=2, ensure_ascii=False), language="json", height=400)

    if st.button("Stage 2: Suggest mappings via GenAI"):
        raw_prompt = {"stage": "map_to_canonical", "partner_name": schema_obj.get("partner_name"), "fields_preview": schema_obj.get("fields", [])[:20]}
        try:
            mappings_raw = call_groq_map_to_canonical(
                st.session_state.get("model_api_key"),
                selected_model["name"],
                schema_obj.get("partner_name"),
                schema_obj.get("fields", []),
                canonical_path=CANONICAL_SCHEMA_FILE,
            )

            # Normalize mappings into a dictionary partner_field -> mapped_to
            mapping_by_pf = {}
            if isinstance(mappings_raw, dict) and "mappings" in mappings_raw:
                for m in mappings_raw["mappings"]:
                    pf = m.get("partner_field")
                    mt = m.get("mapped_to")
                    if pf and mt:
                        mapping_by_pf[pf] = mt
            elif isinstance(mappings_raw, dict):
                # fallback flat mapping {pf: mt}
                for k, v in mappings_raw.items():
                    if isinstance(k, str) and (isinstance(v, str) or v is None):
                        mapping_by_pf[k] = v

            # apply mapping into schema_obj fields
            for f in schema_obj.get("fields", []):
                pf = f.get("partner_field")
                if pf in mapping_by_pf:
                    f["mapped_to"] = mapping_by_pf[pf]
            st.session_state["schema_obj"] = schema_obj
            st.session_state["stage2_suggestions"] = mapping_by_pf

            # set widget selectbox keys so UI pre-selects
            for pf, mt in mapping_by_pf.items():
                st.session_state[f"map_{pf}"] = mt

            log_llm(raw_prompt, mappings_raw, selected_model["name"])
            st.success(f"Stage 2: AI suggested mappings for {len(mapping_by_pf)} field(s).")
            st.code(json.dumps(mapping_by_pf, indent=2, ensure_ascii=False), language="json", height=300)
        except Exception as e:
            log_llm(raw_prompt, {"error": str(e)}, selected_model["name"])
            st.error(f"Stage 2 mapping failed: {e}")

    st.markdown("### Edit field mappings (review / override)")
    fields = schema_obj.get("fields", [])

    # load canonical leaves (flatten canonical schema)
    try:
        canonical = json.loads(Path(CANONICAL_SCHEMA_FILE).read_text(encoding="utf-8"))
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
        canon_leaves = flatten(canonical)
    except Exception:
        canon_leaves = []

    # mapping form
    with st.form("mapping_form"):
        st.markdown("**Partner field mappings**")
        # Column headers
        h1, h2, h3, h4, h5 = st.columns([1.2, 2.0, 2.8, 0.7, 3.2])
        h1.markdown("**Partner field**")
        h2.markdown("**Example value**")
        h3.markdown("**Canonical field**")
        h4.markdown("**Conf.**")
        h5.markdown("**Description**")

        with st.container(height=600):
            edited = []
            for f in fields:
                pf = f.get("partner_field")
                ev = f.get("example_value")
                cur = f.get("mapped_to") or ""
                conf = float(f.get("confidence", 0.0) or 0.0)

                c1, c2, c3, c4, c5 = st.columns([1.2, 2.0, 2.8, 0.7, 3.2], vertical_alignment='center')
                with c1:
                    st.markdown(f"**{pf}**")
                with c2:
                    st.code(str(ev), language="text")

                opts = ["--ignore--"] + canon_leaves
                if cur and cur not in canon_leaves:
                    opts.append(cur)

                state_key = f"map_{pf}"
                current_value = st.session_state.get(state_key, cur or "--ignore--")
                if current_value not in opts:
                    current_value = "--ignore--"

                with c3:
                    sel = st.selectbox("", options=opts, index=opts.index(current_value), key=state_key)
                with c4:
                    st.markdown(f"`{conf:.2f}`")
                with c5:
                    desc = st.text_input("", value=f.get("short_description", ""), key=f"desc_{pf}")

                edited.append({
                    "partner_field": pf,
                    "inferred_type": f.get("inferred_type"),
                    "example_value": ev,
                    "short_description": desc,
                    "mapped_to": None if sel == "--ignore--" else sel,
                    "confidence": conf,
                })

        save = st.form_submit_button("Save mappings")
        run = st.form_submit_button("Run Transform & Validate")

        if save:
            st.session_state["schema_obj"]["fields"] = edited
            try:
                outp = Path(st.session_state.get("last_schema_file") or f"partner_schema_{normalize_partner_field_name(schema_obj.get('partner_name','partner'))}.json")
                outp.write_text(json.dumps(st.session_state["schema_obj"], indent=2, ensure_ascii=False), encoding="utf-8")
                st.session_state["last_schema_file"] = str(outp.resolve())
                st.success("Mappings saved to schema file.")
            except Exception as e:
                st.error(f"Failed to write schema file: {e}")

        if run:
            # load records if not already present
            records = st.session_state.get("last_sample_records") or []
            if not records:
                if not sample_path:
                    st.error("No sample available to run transform.")
                    records = []
                else:
                    records, _ = parse_sample_to_records(sample_path)
                    st.session_state["last_sample_records"] = records

            st.write(f"Loaded {len(records)} sample record(s). Showing first:")
            if records:
                st.code(json.dumps(records[0], indent=2, ensure_ascii=False), language="json", height=400)

            # apply mappings (ingest.apply_mappings)
            try:
                canonical_objs = apply_mappings(records, st.session_state["schema_obj"])
            except Exception as e:
                st.error(f"Apply mappings failed: {e}")
                canonical_objs = []

            st.markdown("### Canonical objects (preview, first 5)")
            st.code(json.dumps(canonical_objs[:5], indent=2, ensure_ascii=False), language="json", height=400)

            try:
                results, valid_count = validate_list(canonical_objs, CANONICAL_SCHEMA_FILE)
                st.markdown(f"Validation: {valid_count}/{len(canonical_objs)} valid")
                st.code(json.dumps(results, indent=2, ensure_ascii=False), language="json", height=400)
            except Exception as e:
                st.error(f"Validation failed: {e}")

# LLM logs expander
st.markdown("---")
with st.expander("LLM Prompts & Responses (raw)", expanded=False):
    logs = st.session_state.get("llm_logs") or []
    if not logs:
        st.info("No LLM interactions yet.")
    else:
        with st.container(height=600):
            for e in logs[:60]:
                st.markdown(f"**{e['time']} — model: {e['model']}**")
                try:
                    st.code(json.dumps(e["raw_prompt"], indent=2, ensure_ascii=False), language="json")
                except Exception:
                    st.code(str(e["raw_prompt"]), language="text")
                try:
                    st.code(json.dumps(e["raw_response"], indent=2, ensure_ascii=False), language="json")
                except Exception:
                    st.code(str(e["raw_response"]), language="text")
                st.markdown("---")
