import streamlit as st
import csv
import io
import json
import base64
import time
import random
import threading
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
 
import re
import requests
import anthropic
import streamlit.components.v1 as components
 
st.set_page_config(
    page_title="1stDibs Prompt Tester",
    page_icon="🪑",
    layout="wide"
)
 
# ── Fallback prompts ───────────────────────────────────────────────────────────
 
FALLBACK_PROMPTS = {
    "Furniture": {
        "system": "Fallback: connect a Google Sheet to load your prompts.",
        "bullets": ""
    }
}
 
# ── Smart Suggestions prompt ───────────────────────────────────────────────────
 
SUGGESTIONS_SYSTEM = """You are a luxury product specialist for 1stDibs. \
Review the seller's item data and the generated listing description. \
Identify 2–4 specific facts the seller should add to meaningfully improve their listing. \
Focus on what a buyer needs to make a purchase decision. \
Be concrete and seller-facing — start each point with an action verb \
(e.g. "Add carat weight and cut grade", "Include seat height and seat depth", \
"Specify whether the marble top is original", "Confirm country of origin"). \
Only flag genuinely missing information that would materially help a buyer. \
Return a short bulleted list only — no preamble, no explanation."""
 
# ── Judge prompt ──────────────────────────────────────────────────────────────
 
JUDGE_SYSTEM = """You are an expert evaluator for 1stDibs luxury marketplace listing descriptions.
You will be shown two descriptions (A and B) generated from the same seller item data and product image.
 
Important context: sellers review all generated descriptions before they are published.
Your role is to catch clear fabrications that could mislead buyers, not to penalise
reasonable visual observations that a seller can confirm or correct.
 
## STEP 1 — ACCURACY GATE (mandatory, evaluated first)
 
### Always acceptable — do NOT flag these as failures
Visual observations about what is directly visible in the image are legitimate:
- Form and geometry: shape, angles, silhouette, faceting, panel configuration, leg style
- Visible construction elements: stretchers, cantilever base, slatted back, drawer configuration
- Color and finish: 'dark-stained', 'brass-toned', 'matte lacquer', 'patinated surface'
- Apparent materials based on clear visual evidence: calling something 'wood' when it is
  clearly wood, 'metal frame' when metal is visible, 'upholstered seat' when fabric is visible
- Observable condition: visible wear, patina, surface marks, restoration visible in image
- Structural relationships: 'seat within the frame', 'suspended construction', 'floating shelf'
 
### Flag as accuracy failures — only clear fabrications
Only mark a description as failing accuracy if it:
- Names a specific material species or grade that cannot be confirmed visually and is not
  in the seller text (e.g. 'solid maple', 'Carrara marble', 'hand-blown Murano glass'
  when the seller only says 'marble' or 'glass')
- States a construction method impossible to see (e.g. 'welded', 'dovetail jointed',
  'hand-stitched' without seller confirmation)
- Attributes the piece to a specific designer, maker, period, or origin not stated by the seller
- Makes a claim that directly contradicts the seller's text
- Invents dimensions, quantities, or specifications not provided
 
When in doubt, lean toward pass. A seller can correct an over-described visual detail;
a clearly false material claim is the real risk.
 
## STEP 2 — QUALITY CRITERIA (only if accuracy gate is passed)
- TONE: mirrors seller's language, no added adjectives, superlatives, or filler phrases
- LENGTH: ideally 400–800 characters
- SEO: primary descriptor and key material repeated naturally 2–3 times, strong opening 160 chars
- ATTRIBUTION: exactly one of: By [Name] / Attributed to [Name] / In the style of [Name]
- FORBIDDEN: no 'Oriental', 'Primitive', urgency language, or collector superlatives
- CONTENT: prioritises materials, condition, period/country, functional details
 
## OUTPUT
Return ONLY a valid JSON object — no markdown, no extra text:
{
  "accuracy_a": "pass" or "fail",
  "accuracy_b": "pass" or "fail",
  "accuracy_issue_a": "describe only clear fabrications — specific material species, construction method, or attribution not in seller text. Empty string if pass.",
  "accuracy_issue_b": "describe only clear fabrications — specific material species, construction method, or attribution not in seller text. Empty string if pass.",
  "winner": "A" or "B" or "tie",
  "confidence": "high" or "medium" or "low",
  "reason": "one concise sentence — lead with accuracy only if a clear fabrication decided it",
  "a_notes": "brief strength or weakness of A",
  "b_notes": "brief strength or weakness of B"
}
 
Be decisive. Only use "tie" if descriptions are genuinely equivalent."""
 
# ── Model pricing (per million tokens) ────────────────────────────────────────
# https://www.anthropic.com/pricing
MODEL_PRICING = {
    "claude-opus-4-6":           {"input": 15.00, "output": 75.00},
    "claude-sonnet-4-6":         {"input": 3.00,  "output": 15.00},
    "claude-haiku-4-5-20251001": {"input": 0.80,  "output": 4.00},
}
 
def compute_cost(model, input_tokens, output_tokens):
    p = MODEL_PRICING.get(model, {"input": 3.00, "output": 15.00})
    return (input_tokens * p["input"] + output_tokens * p["output"]) / 1_000_000
 
# ── Google Sheets loaders ──────────────────────────────────────────────────────
 
def _fetch_sheet_csv(url):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.read().decode("utf-8"), None
    except Exception as e:
        return None, str(e)
 
 
def load_prompts_from_sheet(url):
    text, err = _fetch_sheet_csv(url)
    if err:
        return None, err
    reader = csv.DictReader(io.StringIO(text))
    prompts = {}
    for row in reader:
        name = row.get("Name", "").strip()
        if name:
            prompts[name] = {
                "system": row.get("System Prompt", "").strip(),
                "bullets": row.get("Bullet Prompt", "").strip()
            }
    if not prompts:
        return None, "Sheet loaded but no prompts found. Check column headers: Name | System Prompt | Bullet Prompt"
    return prompts, None
 
 
def load_items_from_sheet(url):
    text, err = _fetch_sheet_csv(url)
    if err:
        return None, None, err
    reader = csv.DictReader(io.StringIO(text))
    rows = list(reader)
    if not rows:
        return None, None, "Sheet loaded but contains no rows."
    return rows, list(reader.fieldnames), None
 
 
# ── Secrets helpers ────────────────────────────────────────────────────────────
 
def get_secret(key):
    try:
        return st.secrets[key]
    except Exception:
        return None
 
def get_all_item_sheets():
    try:
        return dict(st.secrets["item_sheets"])
    except Exception:
        return {}
 
secret_api_key   = get_secret("ANTHROPIC_API_KEY")
secret_sheet_url = get_secret("PROMPTS_SHEET_URL")
 
# ── Session state init ─────────────────────────────────────────────────────────
 
if "prompts" not in st.session_state:
    st.session_state.prompts = dict(FALLBACK_PROMPTS)
 
if "active" not in st.session_state:
    st.session_state.active = list(FALLBACK_PROMPTS.keys())[0]
 
if "sheet_loaded" not in st.session_state:
    st.session_state.sheet_loaded = False
 
if "manual_rows" not in st.session_state:
    st.session_state.manual_rows = []
 
if "gsheet_rows" not in st.session_state:
    st.session_state.gsheet_rows = []
 
if "gsheet_fieldnames" not in st.session_state:
    st.session_state.gsheet_fieldnames = []
 
if "gsheet_loaded_name" not in st.session_state:
    st.session_state.gsheet_loaded_name = None
 
if "last_prompt" not in st.session_state:
    st.session_state.last_prompt = None
 
BASIC_FIELDS = ["NATURAL_KEY", "IMAGE_URL", "ITEM_DESCRIPTION"]
IMAGE_COLS   = ("ITEM_IMAGE", "IMAGE_URL")
 
if "last_loaded_name" not in st.session_state:
    st.session_state.last_loaded_name = None
 
if "ab_results" not in st.session_state:
    st.session_state.ab_results = []
 
# Auto-load prompts on first run
if secret_sheet_url and not st.session_state.sheet_loaded:
    loaded, err = load_prompts_from_sheet(secret_sheet_url)
    if loaded:
        st.session_state.prompts = loaded
        st.session_state.active = list(loaded.keys())[0]
        st.session_state.sheet_loaded = True
 
# ── Header ─────────────────────────────────────────────────────────────────────
 
st.title("🪑 1stDibs Prompt Tester")
st.caption("Select a prompt, load item data, and run.")
 
# ── Sidebar ────────────────────────────────────────────────────────────────────
 
with st.sidebar:
    st.header("Configuration")
 
    # Prompt selector at top as dropdown
    st.subheader("Prompt")
    prompt_names = list(st.session_state.prompts.keys())
    # Initialise the widget key from session state (only on first render)
    if "prompt_select" not in st.session_state:
        st.session_state["prompt_select"] = st.session_state.active
    # If current value is stale (e.g. after a prompt sync), realign
    if st.session_state["prompt_select"] not in prompt_names:
        st.session_state["prompt_select"] = prompt_names[0]
    selected = st.selectbox("Select prompt", options=prompt_names,
                            key="prompt_select", label_visibility="collapsed")
    st.session_state.active = selected
 
    st.divider()
 
    # Prompt Library
    st.subheader("Prompt Library")
    if secret_sheet_url:
        st.caption("✓ Sheet connected · Edit prompts in Google Sheets")
        if st.button("🔄 Sync prompts", use_container_width=True):
            loaded, err = load_prompts_from_sheet(secret_sheet_url)
            if loaded:
                st.session_state.prompts = loaded
                if st.session_state.active not in loaded:
                    st.session_state.active = list(loaded.keys())[0]
                    st.session_state["prompt_select"] = st.session_state.active
                st.success(f"Synced {len(loaded)} prompts.")
                st.rerun()
            else:
                st.error(f"Sync failed: {err}")
    else:
        sheet_url_input = st.text_input(
            "Google Sheet CSV URL",
            placeholder="https://docs.google.com/spreadsheets/d/.../pub?output=csv"
        )
        if st.button("Load from sheet", use_container_width=True):
            if sheet_url_input.strip():
                loaded, err = load_prompts_from_sheet(sheet_url_input.strip())
                if loaded:
                    st.session_state.prompts = loaded
                    st.session_state.active = list(loaded.keys())[0]
                    st.session_state["prompt_select"] = st.session_state.active
                    st.success(f"Loaded {len(loaded)} prompts.")
                    st.info("Add PROMPTS_SHEET_URL to Streamlit secrets to make this permanent.")
                    st.rerun()
                else:
                    st.error(f"Could not load: {err}")
            else:
                st.warning("Paste a sheet URL first.")
 
    st.divider()
 
    # Run settings
    st.subheader("Run Settings")
 
    if secret_api_key:
        api_key = secret_api_key
    else:
        api_key = st.text_input("Anthropic API Key", type="password",
                                help="Your sk-ant-... key.")
 
    model = st.selectbox("Model",
        options=["claude-opus-4-6", "claude-sonnet-4-6", "claude-haiku-4-5-20251001"],
        index=0,
        help="Opus = best quality. Sonnet = faster & cheaper."
    )
 
    num_rows    = st.slider("Rows to test", min_value=1, max_value=100, value=5)
    sample_mode = st.radio("Row selection", ["From top", "Random sample"], horizontal=True)
    workers     = st.slider("Parallel workers", min_value=1, max_value=10, value=5)
 
    st.divider()
    st.subheader("Description Filter")
    filter_by_length = st.toggle("Filter by original description length", value=False,
                                 help="Only test items whose existing description is under a character limit.")
    max_desc_chars = None
    desc_col_filter = None
    if filter_by_length:
        max_desc_chars = st.slider("Max original description length (chars)",
                                   min_value=50, max_value=2000, value=500, step=50)
        desc_col_filter = st.text_input("Description column name",
                                        value="ITEM_DESCRIPTION",
                                        help="Column to measure. Use CHARACTER_LENGTH if you pre-computed it.")
 
# ── Active prompt ──────────────────────────────────────────────────────────────
 
active_prompt = st.session_state.prompts.get(st.session_state.active, {})
system_prompt = active_prompt.get("system", "")
bullet_prompt = active_prompt.get("bullets", "")
 
# ── Prompt-change → auto-sync dataset ─────────────────────────────────────────
 
_configured_sheets = get_all_item_sheets()
if st.session_state.active != st.session_state.last_prompt:
    st.session_state.last_prompt = st.session_state.active
    if st.session_state.active in _configured_sheets:
        # Matching sheet exists — auto-load it
        _rows, _fns, _err = load_items_from_sheet(_configured_sheets[st.session_state.active])
        if _rows:
            st.session_state.gsheet_rows = _rows
            st.session_state.gsheet_fieldnames = _fns
            st.session_state.gsheet_loaded_name = st.session_state.active
        else:
            st.session_state.gsheet_rows = []
            st.session_state.gsheet_fieldnames = []
            st.session_state.gsheet_loaded_name = None
    else:
        # No matching sheet — clear loaded data
        st.session_state.gsheet_rows = []
        st.session_state.gsheet_fieldnames = []
        st.session_state.gsheet_loaded_name = None
 
# ── Manual-entry field definitions ────────────────────────────────────────────
 
MANUAL_FIELDNAMES = [
    "NATURAL_KEY", "ITEM_TITLE", "ITEM_DESCRIPTION", "CATEGORY",
    "CREATOR", "MATERIALS", "CONDITION", "PERIOD", "ORIGIN",
    "ITEM_IMAGE", "SOURCE_URL"
]
MANUAL_CONTEXT_COLS = [f for f in MANUAL_FIELDNAMES if f not in ("ITEM_IMAGE", "SOURCE_URL")]
 
# ── Main tabs ──────────────────────────────────────────────────────────────────
 
tab1, tab2, tab3, tab4 = st.tabs(["📝 Prompt Preview", "📊 Item Data", "✍️ Quick Entry", "⚖️ A/B Compare"])
 
with tab1:
    st.subheader(f"{st.session_state.active} — System Prompt")
    st.text_area("system_preview", value=system_prompt, height=300,
                 disabled=True, label_visibility="collapsed")
    st.caption(f"{len(system_prompt)} characters  ·  Edit in Google Sheets")
 
    enable_bullets     = st.toggle("Enable Item Highlights", value=bool(bullet_prompt))
    enable_suggestions = st.toggle("Enable Smart Suggestions", value=True,
                                   help="Adds a seller-facing column flagging missing information that would improve the listing.")
 
    if enable_bullets and bullet_prompt:
        st.subheader("Item Highlights Prompt")
        st.text_area("bullets_preview", value=bullet_prompt, height=150,
                     disabled=True, label_visibility="collapsed")
        st.caption(f"{len(bullet_prompt)} characters  ·  Edit in Google Sheets")
    elif enable_bullets and not bullet_prompt:
        st.info("No Item Highlights prompt found for this vertical. Add a 'Bullet Prompt' column to your sheet.")
 
    if enable_suggestions:
        with st.expander("Smart Suggestions prompt (read-only)"):
            st.text_area("suggestions_preview", value=SUGGESTIONS_SYSTEM, height=160,
                         disabled=True, label_visibility="collapsed")
 
 
with tab2:
    st.subheader("Item Data")
    data_source = st.radio("Source", ["📊 Google Sheet", "📂 Upload CSV"],
                           horizontal=True, label_visibility="collapsed")
 
    rows          = []
    fieldnames    = []
    included_cols = []
    image_col     = "(none)"
 
    if data_source == "📊 Google Sheet":
 
        if st.session_state.gsheet_rows:
            fns = st.session_state.gsheet_fieldnames
 
            # Status + reload button
            col_status, col_reload = st.columns([5, 1])
            with col_status:
                st.success(f"✓ **{st.session_state.gsheet_loaded_name}** — {len(st.session_state.gsheet_rows)} rows, {len(fns)} columns")
            with col_reload:
                reload_url = _configured_sheets.get(st.session_state.gsheet_loaded_name)
                if st.button("🔄", help="Reload sheet", use_container_width=True,
                             disabled=not reload_url):
                    with st.spinner("Reloading..."):
                        _r, _f, _e = load_items_from_sheet(reload_url)
                    if _r:
                        st.session_state.gsheet_rows = _r
                        st.session_state.gsheet_fieldnames = _f
                        st.rerun()
                    else:
                        st.error(_e)
 
            # Reset column selection when dataset changes
            if st.session_state.last_loaded_name != st.session_state.gsheet_loaded_name:
                st.session_state.last_loaded_name = st.session_state.gsheet_loaded_name
                st.session_state["col_sel_gsheet"] = [c for c in fns if c not in IMAGE_COLS]
 
            # Ensure key is initialised
            if "col_sel_gsheet" not in st.session_state:
                st.session_state["col_sel_gsheet"] = [c for c in fns if c not in IMAGE_COLS]
 
            # Quick-select buttons — only affect the context columns multiselect
            qs1, qs2 = st.columns(2)
            with qs1:
                if st.button("All fields", use_container_width=True, key="gs_all"):
                    st.session_state["col_sel_gsheet"] = [c for c in fns if c not in IMAGE_COLS]
            with qs2:
                if st.button("Basic fields only", use_container_width=True, key="gs_basic"):
                    st.session_state["col_sel_gsheet"] = [c for c in BASIC_FIELDS if c in fns]
 
            included_cols = st.multiselect("Columns to pass as context",
                                           options=fns, key="col_sel_gsheet")
            image_col = st.selectbox(
                "Image URL column (optional)",
                options=["(none)"] + fns,
                index=next((fns.index(c) + 1 for c in IMAGE_COLS if c in fns), 0)
            )
            rows       = st.session_state.gsheet_rows
            fieldnames = fns
            with st.expander("Preview first 3 rows"):
                for r in rows[:3]:
                    st.json({k: v for k, v in r.items() if k in included_cols})
 
        else:
            if st.session_state.active in _configured_sheets:
                st.info(f"Switch to **{st.session_state.active}** triggered a load — if this persists, use the URL loader below.")
            else:
                st.warning(f"No dataset configured for **{st.session_state.active}**. Load one from a URL below, or switch to Upload CSV.")
 
        with st.expander("Load from a different URL"):
            manual_sheet_url = st.text_input(
                "Sheet URL",
                placeholder="https://docs.google.com/spreadsheets/d/.../pub?output=csv",
                label_visibility="collapsed"
            )
            if st.button("Load URL", type="secondary", use_container_width=True):
                if manual_sheet_url.strip():
                    with st.spinner("Loading..."):
                        loaded_rows, loaded_fns, err = load_items_from_sheet(manual_sheet_url.strip())
                    if loaded_rows:
                        st.session_state.gsheet_rows = loaded_rows
                        st.session_state.gsheet_fieldnames = loaded_fns
                        st.session_state.gsheet_loaded_name = "Custom URL"
                        st.rerun()
                    else:
                        st.error(f"Could not load: {err}")
                else:
                    st.warning("Paste a sheet URL first.")
 
    else:  # Upload CSV
        uploaded_file = st.file_uploader("Upload CSV", type=["csv"],
                                         label_visibility="collapsed")
        if uploaded_file:
            try:
                content    = uploaded_file.read().decode("utf-8")
                reader     = csv.DictReader(io.StringIO(content))
                rows       = list(reader)
                fieldnames = list(reader.fieldnames)
                st.success(f"✓ {len(rows)} rows — {len(fieldnames)} columns detected")
 
                if "col_sel_csv" not in st.session_state:
                    st.session_state["col_sel_csv"] = [c for c in fieldnames if c not in IMAGE_COLS]
 
                qs1, qs2 = st.columns(2)
                with qs1:
                    if st.button("All fields", use_container_width=True, key="csv_all"):
                        st.session_state["col_sel_csv"] = [c for c in fieldnames if c not in IMAGE_COLS]
                with qs2:
                    if st.button("Basic fields only", use_container_width=True, key="csv_basic"):
                        st.session_state["col_sel_csv"] = [c for c in BASIC_FIELDS if c in fieldnames]
 
                included_cols = st.multiselect("Columns to pass as context",
                                               options=fieldnames, key="col_sel_csv")
                image_col = st.selectbox(
                    "Image URL column (optional)",
                    options=["(none)"] + fieldnames,
                    index=next((fieldnames.index(c) + 1 for c in ("ITEM_IMAGE", "IMAGE_URL") if c in fieldnames), 0)
                )
                with st.expander("Preview first 3 rows"):
                    for r in rows[:3]:
                        st.json({k: v for k, v in r.items() if k in included_cols})
            except Exception as e:
                st.error(f"Error reading CSV: {e}")
 
 
with tab3:
    st.subheader("Quick Entry")
    st.caption("Paste details from a single item to test quickly. Only Description is required.")
 
    with st.form("manual_entry_form"):
        me_title       = st.text_input("Item title")
        me_url         = st.text_input("Item URL (for reference)")
        me_description = st.text_area("Seller description *", height=180,
                                      placeholder="Paste the seller's description from the item page...")
        me_creator     = st.text_input("Designer / Maker")
        me_category    = st.text_input("Category")
        me_materials   = st.text_input("Materials")
        me_condition   = st.selectbox("Condition",
                                      ["", "New", "Excellent", "Good", "Fair", "Distressed"])
        me_period      = st.text_input("Period / Circa")
        me_origin      = st.text_input("Country / Origin")
        me_image       = st.text_input("Image URL (optional)")
        add_clicked    = st.form_submit_button("➕ Add to queue", type="primary")
 
    if add_clicked:
        if not me_description.strip():
            st.warning("Seller description is required.")
        else:
            slug = re.sub(r"[^a-z0-9]", "-", (me_title or me_description[:30]).lower())[:40]
            new_row = {
                "NATURAL_KEY":      slug,
                "ITEM_TITLE":       me_title.strip(),
                "ITEM_DESCRIPTION": me_description.strip(),
                "CATEGORY":         me_category.strip(),
                "CREATOR":          me_creator.strip(),
                "MATERIALS":        me_materials.strip(),
                "CONDITION":        me_condition,
                "PERIOD":           me_period.strip(),
                "ORIGIN":           me_origin.strip(),
                "ITEM_IMAGE":       me_image.strip(),
                "SOURCE_URL":       me_url.strip(),
            }
            st.session_state.manual_rows.append(new_row)
            st.success(f"✓ Added — {len(st.session_state.manual_rows)} item(s) in queue")
 
    if st.session_state.manual_rows:
        st.divider()
        col_info, col_clear = st.columns([4, 1])
        with col_info:
            st.success(f"✓ {len(st.session_state.manual_rows)} item(s) queued")
        with col_clear:
            if st.button("✕ Clear all", key="clear_manual"):
                st.session_state.manual_rows = []
                st.rerun()
        with st.expander("Preview queue"):
            for r in st.session_state.manual_rows:
                label = r.get("ITEM_TITLE") or r.get("NATURAL_KEY")
                st.markdown(f"**{label}**")
                st.json({k: v for k, v in r.items() if v and k != "ITEM_IMAGE"})
 
 
with tab4:
    st.subheader("Prompt Evaluation")
 
    ab_mode = st.radio("Mode", ["⚖️ A vs B", "📊 Prompt vs Original"],
                       horizontal=True, label_visibility="collapsed")
 
    prompt_names_ab = list(st.session_state.prompts.keys())
 
    if ab_mode == "⚖️ A vs B":
        st.caption("Run the same items through two prompts and let the judge pick the winner.")
 
        if len(prompt_names_ab) < 2:
            st.warning("You need at least two prompts loaded to run a comparison.")
        else:
            ab_col1, ab_col2 = st.columns(2)
            with ab_col1:
                prompt_a_name = st.selectbox("Prompt A", options=prompt_names_ab, index=0, key="ab_prompt_a")
            with ab_col2:
                prompt_b_name = st.selectbox("Prompt B", options=prompt_names_ab,
                                             index=min(1, len(prompt_names_ab) - 1), key="ab_prompt_b")
 
            if prompt_a_name == prompt_b_name:
                st.warning("Select two different prompts to compare.")
 
        ab_num_rows = st.slider("Items to compare", min_value=1, max_value=50, value=5, key="ab_num_rows")
        ab_sample   = st.radio("Row selection", ["From top", "Random sample"],
                               horizontal=True, key="ab_sample")
        ab_model    = st.selectbox("Judge model",
                                   options=["claude-opus-4-6", "claude-sonnet-4-6", "claude-haiku-4-5-20251001"],
                                   index=1, key="ab_model",
                                   help="Sonnet is a good balance of quality and speed for judging.")
 
        # Data source info
        if st.session_state.gsheet_rows:
            st.info(f"Will use **{st.session_state.gsheet_loaded_name}** dataset ({len(st.session_state.gsheet_rows)} rows)")
        elif st.session_state.manual_rows:
            st.info(f"Will use Quick Entry queue ({len(st.session_state.manual_rows)} item(s))")
        else:
            st.warning("Load item data in the Item Data tab first.")
 
        ab_can_run = (api_key and prompt_a_name != prompt_b_name and
                      (st.session_state.gsheet_rows or st.session_state.manual_rows))
 
        ab_clicked = st.button("▶ Run A/B Compare", type="primary",
                               disabled=not ab_can_run, use_container_width=True)
 
        if ab_clicked and ab_can_run:
            # Resolve data source
            if st.session_state.gsheet_rows:
                ab_pool      = st.session_state.gsheet_rows
                ab_img_col   = next((c for c in IMAGE_COLS if c in st.session_state.gsheet_fieldnames), "(none)")
                ab_ctx_cols  = [c for c in st.session_state.gsheet_fieldnames if c not in IMAGE_COLS]
            else:
                ab_pool      = st.session_state.manual_rows
                ab_img_col   = "ITEM_IMAGE"
                ab_ctx_cols  = MANUAL_CONTEXT_COLS
 
            if ab_sample == "Random sample" and len(ab_pool) > ab_num_rows:
                ab_rows = random.sample(ab_pool, ab_num_rows)
            else:
                ab_rows = ab_pool[:ab_num_rows]
 
            prompt_a_sys = st.session_state.prompts[prompt_a_name]["system"]
            prompt_b_sys = st.session_state.prompts[prompt_b_name]["system"]
            ab_client    = anthropic.Anthropic(api_key=api_key)
 
            ab_progress = st.progress(0)
            ab_status   = st.empty()
            ab_results_temp = []
 
            for i, row in enumerate(ab_rows):
                label = row.get("ITEM_TITLE") or row.get("NATURAL_KEY") or f"Item {i+1}"
                ab_status.text(f"Processing {i+1}/{len(ab_rows)}: {label}...")
 
                ctx = "\n".join(
                    f"{c}: {row.get(c,'').strip()}"
                    for c in ab_ctx_cols if row.get(c,"").strip()
                )
 
                # Fetch image once, share between both calls
                img_b64, img_meta = None, None
                img_url = row.get(ab_img_col, "") if ab_img_col != "(none)" else ""
                if img_url and img_url.startswith("http"):
                    try:
                        resp = requests.get(img_url, timeout=10)
                        resp.raise_for_status()
                        img_meta = resp.headers.get("Content-Type","image/jpeg").split(";")[0].strip()
                        img_b64  = base64.standard_b64encode(resp.content).decode()
                    except Exception:
                        pass
 
                def _gen(sys_prompt):
                    content = []
                    if img_b64:
                        content.append({"type": "image",
                                        "source": {"type": "base64", "media_type": img_meta, "data": img_b64}})
                    content.append({"type": "text",
                                    "text": f"Write a listing description for this 1stDibs item. "
                                            f"Stay between 400 and 800 characters total.\n\nITEM DATA:\n{ctx}"})
                    try:
                        r = ab_client.messages.create(
                            model=ab_model, max_tokens=220, system=sys_prompt,
                            messages=[{"role": "user", "content": content}]
                        )
                        return r.content[0].text.strip()
                    except Exception as e:
                        return f"[Error: {e}]"
 
                desc_a = _gen(prompt_a_sys)
                desc_b = _gen(prompt_b_sys)
 
                # Randomly blind the judge to reduce position bias
                flipped = random.random() < 0.5
                judge_a = desc_b if flipped else desc_a
                judge_b = desc_a if flipped else desc_b
 
                judge_content = (
                    f"ITEM DATA:\n{ctx}\n\n"
                    f"DESCRIPTION A:\n{judge_a}\n\n"
                    f"DESCRIPTION B:\n{judge_b}"
                )
                try:
                    judge_resp = ab_client.messages.create(
                        model=ab_model, max_tokens=400, system=JUDGE_SYSTEM,
                        messages=[{"role": "user", "content": judge_content}]
                    )
                    raw = judge_resp.content[0].text.strip()
                    # Extract just the JSON object in case there's surrounding text
                    match = re.search(r'\{.*\}', raw, re.DOTALL)
                    if match:
                        verdict = json.loads(match.group())
                    else:
                        raise ValueError(f"No JSON found in response: {raw[:200]}")
                except Exception as e:
                    verdict = {"winner": "error", "confidence": "—", "reason": str(e),
                               "accuracy_a": "", "accuracy_issue_a": "",
                               "accuracy_b": "", "accuracy_issue_b": "",
                               "a_notes": "", "b_notes": ""}
 
                # Un-flip the winner back to real A/B labels
                raw_winner = verdict.get("winner", "tie")
                if flipped and raw_winner == "A":
                    real_winner = "B"
                elif flipped and raw_winner == "B":
                    real_winner = "A"
                else:
                    real_winner = raw_winner
 
                verdict["winner"] = real_winner
                ab_results_temp.append({
                    "label":            label,
                    "desc_a":           desc_a,
                    "desc_b":           desc_b,
                    "img_url":          img_url,
                    "winner":           real_winner,
                    "confidence":       verdict.get("confidence", ""),
                    "reason":           verdict.get("reason", ""),
                    "accuracy_a":       verdict.get("accuracy_a", ""),
                    "accuracy_issue_a": verdict.get("accuracy_issue_a", ""),
                    "accuracy_b":       verdict.get("accuracy_b", ""),
                    "accuracy_issue_b": verdict.get("accuracy_issue_b", ""),
                    "a_notes":          verdict.get("a_notes", ""),
                    "b_notes":          verdict.get("b_notes", ""),
                    "ctx":              ctx,
                })
                ab_progress.progress((i + 1) / len(ab_rows))
 
            st.session_state.ab_results = ab_results_temp
            st.session_state.ab_prompt_a_label = prompt_a_name
            st.session_state.ab_prompt_b_label = prompt_b_name
            ab_status.text("✓ Done!")
 
        # Results display
        if st.session_state.ab_results:
            results    = st.session_state.ab_results
            a_label    = st.session_state.get("ab_prompt_a_label", "A")
            b_label    = st.session_state.get("ab_prompt_b_label", "B")
            a_wins     = sum(1 for r in results if r["winner"] == "A")
            b_wins     = sum(1 for r in results if r["winner"] == "B")
            ties       = sum(1 for r in results if r["winner"] == "tie")
            errors     = sum(1 for r in results if r["winner"] == "error")
            total      = len(results)
            decidable  = total - ties - errors
 
            st.divider()
            st.subheader("Results")
 
            acc_fails_a = sum(1 for r in results if r.get("accuracy_a") == "fail")
            acc_fails_b = sum(1 for r in results if r.get("accuracy_b") == "fail")
 
            m1, m2, m3, m4, m5 = st.columns(5)
            m1.metric(f"✅ {a_label} wins", a_wins)
            m2.metric(f"✅ {b_label} wins", b_wins)
            m3.metric("🤝 Ties", ties)
            if decidable > 0:
                win_pct = round(a_wins / decidable * 100)
                m4.metric(f"{a_label} win rate", f"{win_pct}%", help="Excludes ties and errors")
            m5.metric("❌ Accuracy fails", f"{acc_fails_a}A / {acc_fails_b}B",
                      help="Items where a description invented or inferred facts not in the seller data")
 
            for r in results:
                icon = ("🅰️" if r["winner"] == "A" else
                        "🅱️" if r["winner"] == "B" else
                        "🤝" if r["winner"] == "tie" else "⚠️")
                conf = f" ({r['confidence']} confidence)" if r["confidence"] not in ("", "—") else ""
                with st.expander(f"{icon} **{r['label']}** — {r['winner'].upper()} wins{conf}"):
                    if r.get("img_url"):
                        try:
                            st.image(r["img_url"], width=250)
                        except Exception:
                            pass
 
                    c1, c2 = st.columns(2)
                    with c1:
                        acc_a = r.get("accuracy_a", "")
                        acc_icon_a = "✅ Accurate" if acc_a == "pass" else ("❌ Accuracy fail" if acc_a == "fail" else "")
                        st.markdown(f"**{a_label}** {acc_icon_a}")
                        if r.get("accuracy_issue_a"):
                            st.error(f"⚠️ {r['accuracy_issue_a']}")
                        st.write(r["desc_a"])
                        st.caption(f"{len(r['desc_a'])} chars")
                        if r["a_notes"]:
                            st.caption(f"📝 {r['a_notes']}")
                    with c2:
                        acc_b = r.get("accuracy_b", "")
                        acc_icon_b = "✅ Accurate" if acc_b == "pass" else ("❌ Accuracy fail" if acc_b == "fail" else "")
                        st.markdown(f"**{b_label}** {acc_icon_b}")
                        if r.get("accuracy_issue_b"):
                            st.error(f"⚠️ {r['accuracy_issue_b']}")
                        st.write(r["desc_b"])
                        st.caption(f"{len(r['desc_b'])} chars")
                        if r["b_notes"]:
                            st.caption(f"📝 {r['b_notes']}")
 
                    if r["reason"]:
                        st.info(f"**Judge:** {r['reason']}")
 
            # Download
            ab_fields = ["label", "winner", "confidence", "reason",
                         "accuracy_a", "accuracy_issue_a", "desc_a", "a_notes",
                         "accuracy_b", "accuracy_issue_b", "desc_b", "b_notes"]
            ab_buf = io.StringIO()
            ab_writer = csv.DictWriter(ab_buf, fieldnames=ab_fields, extrasaction="ignore")
            ab_writer.writeheader()
            ab_writer.writerows(results)
            st.download_button("⬇ Download A/B results CSV", data=ab_buf.getvalue(),
                               file_name="ab_compare_results.csv", mime="text/csv",
                               use_container_width=True)
 
    else:  # Prompt vs Original mode
        st.caption("Compare the generated description against the seller's original. "
                   "Measures whether the prompt improves on what the seller wrote.")
 
        pvo_prompt_name = st.selectbox("Prompt to evaluate", options=prompt_names_ab,
                                       index=0, key="pvo_prompt")
        pvo_orig_col    = st.text_input("Original description column", value="ITEM_DESCRIPTION",
                                        key="pvo_orig_col")
        pvo_num_rows    = st.slider("Items to test", min_value=1, max_value=50, value=5, key="pvo_num_rows")
        pvo_sample      = st.radio("Row selection", ["From top", "Random sample"],
                                   horizontal=True, key="pvo_sample")
        pvo_model       = st.selectbox("Judge model",
                                       options=["claude-opus-4-6", "claude-sonnet-4-6", "claude-haiku-4-5-20251001"],
                                       index=1, key="pvo_model")
 
        if st.session_state.gsheet_rows:
            st.info(f"Will use **{st.session_state.gsheet_loaded_name}** dataset ({len(st.session_state.gsheet_rows)} rows)")
        elif st.session_state.manual_rows:
            st.info(f"Will use Quick Entry queue ({len(st.session_state.manual_rows)} item(s))")
        else:
            st.warning("Load item data in the Item Data tab first.")
 
        pvo_pool = st.session_state.gsheet_rows or st.session_state.manual_rows
        pvo_can_run = bool(api_key and pvo_pool)
        pvo_clicked = st.button("▶ Run Prompt vs Original", type="primary",
                                disabled=not pvo_can_run, use_container_width=True, key="pvo_run")
 
        if pvo_clicked and pvo_can_run:
            pvo_sys     = st.session_state.prompts[pvo_prompt_name]["system"]
            pvo_client  = anthropic.Anthropic(api_key=api_key)
 
            if st.session_state.gsheet_rows:
                pvo_img_col  = next((c for c in IMAGE_COLS if c in st.session_state.gsheet_fieldnames), "(none)")
                pvo_ctx_cols = [c for c in st.session_state.gsheet_fieldnames if c not in IMAGE_COLS]
            else:
                pvo_img_col  = "ITEM_IMAGE"
                pvo_ctx_cols = MANUAL_CONTEXT_COLS
 
            if pvo_sample == "Random sample" and len(pvo_pool) > pvo_num_rows:
                pvo_rows = random.sample(pvo_pool, pvo_num_rows)
            else:
                pvo_rows = pvo_pool[:pvo_num_rows]
 
            pvo_progress = st.progress(0)
            pvo_status   = st.empty()
            pvo_results  = []
 
            for i, row in enumerate(pvo_rows):
                label    = row.get("ITEM_TITLE") or row.get("NATURAL_KEY") or f"Item {i+1}"
                original = row.get(pvo_orig_col, "").strip()
                pvo_status.text(f"Processing {i+1}/{len(pvo_rows)}: {label}...")
 
                ctx = "\n".join(
                    f"{c}: {row.get(c,'').strip()}"
                    for c in pvo_ctx_cols if row.get(c,"").strip()
                )
 
                img_b64, img_meta = None, None
                img_url = row.get(pvo_img_col, "") if pvo_img_col != "(none)" else ""
                if img_url and img_url.startswith("http"):
                    try:
                        resp     = requests.get(img_url, timeout=10)
                        img_meta = resp.headers.get("Content-Type","image/jpeg").split(";")[0].strip()
                        img_b64  = base64.standard_b64encode(resp.content).decode()
                    except Exception:
                        pass
 
                # Generate description
                gen_content = []
                if img_b64:
                    gen_content.append({"type": "image",
                                        "source": {"type": "base64", "media_type": img_meta, "data": img_b64}})
                gen_content.append({"type": "text",
                                    "text": f"Write a listing description for this 1stDibs item. "
                                            f"Stay between 400 and 800 characters total.\n\nITEM DATA:\n{ctx}"})
                try:
                    gen_resp = pvo_client.messages.create(
                        model=pvo_model, max_tokens=220, system=pvo_sys,
                        messages=[{"role": "user", "content": gen_content}]
                    )
                    generated = gen_resp.content[0].text.strip()
                except Exception as e:
                    generated = f"[Error: {e}]"
 
                # Blind judge: randomly assign generated/original to A/B positions
                flipped = random.random() < 0.5
                judge_a = original  if flipped else generated
                judge_b = generated if flipped else original
                a_label_judge = "Original" if flipped else "Generated"
                b_label_judge = "Generated" if flipped else "Original"
 
                judge_content = (
                    f"ITEM DATA:\n{ctx}\n\n"
                    f"DESCRIPTION A ({a_label_judge}):\n{judge_a}\n\n"
                    f"DESCRIPTION B ({b_label_judge}):\n{judge_b}"
                )
                try:
                    j_resp = pvo_client.messages.create(
                        model=pvo_model, max_tokens=400, system=JUDGE_SYSTEM,
                        messages=[{"role": "user", "content": judge_content}]
                    )
                    raw   = j_resp.content[0].text.strip()
                    match = re.search(r'\{.*\}', raw, re.DOTALL)
                    vdict = json.loads(match.group()) if match else {}
                except Exception as e:
                    vdict = {"winner": "error", "confidence": "—", "reason": str(e)}
 
                # Translate blinded A/B back to Generated/Original
                raw_w = vdict.get("winner", "tie")
                if raw_w == "A":
                    real_winner = a_label_judge
                elif raw_w == "B":
                    real_winner = b_label_judge
                else:
                    real_winner = raw_w  # "tie" or "error"
 
                pvo_results.append({
                    "label":       label,
                    "original":    original,
                    "generated":   generated,
                    "img_url":     img_url,
                    "winner":      real_winner,
                    "confidence":  vdict.get("confidence", ""),
                    "reason":      vdict.get("reason", ""),
                    "acc_gen":     vdict.get("accuracy_a" if not flipped else "accuracy_b", ""),
                    "acc_issue_gen": vdict.get("accuracy_issue_a" if not flipped else "accuracy_issue_b", ""),
                    "gen_notes":   vdict.get("a_notes" if not flipped else "b_notes", ""),
                    "orig_notes":  vdict.get("b_notes" if not flipped else "a_notes", ""),
                })
                pvo_progress.progress((i + 1) / len(pvo_rows))
 
            st.session_state["pvo_results"]       = pvo_results
            st.session_state["pvo_prompt_label"]  = pvo_prompt_name
            pvo_status.text("✓ Done!")
 
        # Prompt vs Original results
        if st.session_state.get("pvo_results"):
            pvo_res   = st.session_state["pvo_results"]
            pvo_label = st.session_state.get("pvo_prompt_label", "Prompt")
            gen_wins  = sum(1 for r in pvo_res if r["winner"] == "Generated")
            orig_wins = sum(1 for r in pvo_res if r["winner"] == "Original")
            ties      = sum(1 for r in pvo_res if r["winner"] == "tie")
            acc_fails = sum(1 for r in pvo_res if r.get("acc_gen") == "fail")
            decidable = len(pvo_res) - ties - sum(1 for r in pvo_res if r["winner"] == "error")
 
            st.divider()
            st.subheader("Results")
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("✅ Generated wins", gen_wins)
            m2.metric("📄 Original wins", orig_wins)
            m3.metric("🤝 Ties", ties)
            if decidable > 0:
                m4.metric("Generated win rate", f"{round(gen_wins / decidable * 100)}%",
                          help="How often the generated description beats the seller's original")
            if acc_fails:
                st.error(f"⚠️ {acc_fails} generated description(s) failed the accuracy gate.")
 
            for r in pvo_res:
                icon = ("✅" if r["winner"] == "Generated" else
                        "📄" if r["winner"] == "Original" else
                        "🤝" if r["winner"] == "tie" else "⚠️")
                conf = f" ({r['confidence']} confidence)" if r.get("confidence") not in ("", "—", None) else ""
                with st.expander(f"{icon} **{r['label']}** — {r['winner']} wins{conf}"):
                    if r.get("img_url"):
                        try:
                            st.image(r["img_url"], width=250)
                        except Exception:
                            pass
 
                    c1, c2 = st.columns(2)
                    with c1:
                        st.markdown("**Original (seller)**")
                        st.write(r["original"] or "_empty_")
                        st.caption(f"{len(r['original'])} chars")
                        if r.get("orig_notes"):
                            st.caption(f"📝 {r['orig_notes']}")
                    with c2:
                        acc_icon = "✅ Accurate" if r.get("acc_gen") == "pass" else (
                                   "❌ Accuracy fail" if r.get("acc_gen") == "fail" else "")
                        st.markdown(f"**Generated** {acc_icon}")
                        if r.get("acc_issue_gen"):
                            st.error(f"⚠️ {r['acc_issue_gen']}")
                        st.write(r["generated"] or "_empty_")
                        st.caption(f"{len(r['generated'])} chars")
                        if r.get("gen_notes"):
                            st.caption(f"📝 {r['gen_notes']}")
 
                    if r.get("reason"):
                        st.info(f"**Judge:** {r['reason']}")
 
            pvo_fields = ["label", "winner", "confidence", "reason",
                          "original", "generated", "acc_gen", "acc_issue_gen",
                          "gen_notes", "orig_notes"]
            pvo_buf = io.StringIO()
            pvo_writer = csv.DictWriter(pvo_buf, fieldnames=pvo_fields, extrasaction="ignore")
            pvo_writer.writeheader()
            pvo_writer.writerows(pvo_res)
            st.download_button("⬇ Download results CSV", data=pvo_buf.getvalue(),
                               file_name="prompt_vs_original.csv", mime="text/csv",
                               use_container_width=True)
 
# ── Helpers ────────────────────────────────────────────────────────────────────
 
BOILERPLATE_TRIGGERS = [
    "message us with your zip", "shipping", "contact us", "please note",
    "delivery", "white glove", "local pickup", "inquire", "call us"
]
 
 
def fetch_image_as_base64(url, timeout=10):
    if not url or not url.startswith("http"):
        return None, "No image URL"
    try:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        ct  = resp.headers.get("Content-Type", "image/jpeg").split(";")[0].strip()
        b64 = base64.standard_b64encode(resp.content).decode("utf-8")
        return b64, ct
    except requests.exceptions.Timeout:
        return None, "Image fetch timed out"
    except requests.exceptions.HTTPError as e:
        return None, f"HTTP {e.response.status_code}"
    except Exception as e:
        return None, str(e)
 
 
def build_item_context(row, cols):
    return "\n".join(
        f"{col}: {row.get(col, '').strip()}"
        for col in cols if row.get(col, "").strip()
    )
 
 
def generate_row(client, row, sys_prompt, bul_prompt, use_bullets,
                 use_suggestions, inc_cols, img_col, mdl):
    notes = []
 
    raw_context = build_item_context(row, inc_cols)
    cleaned_context = raw_context
    for trigger in BOILERPLATE_TRIGGERS:
        if trigger.lower() in raw_context.lower():
            lines = raw_context.split("\n")
            cleaned_context = "\n".join(l for l in lines if trigger.lower() not in l.lower())
            notes.append(f"Boilerplate stripped ('{trigger}')")
            break
 
    if not cleaned_context.strip():
        notes.append("No usable item data in selected columns")
 
    img_url_for_display = row.get(img_col, "") if img_col and img_col != "(none)" else ""
    img_b64, img_meta = None, None
    if img_url_for_display:
        img_b64, img_meta = fetch_image_as_base64(img_url_for_display)
        if img_b64 is None:
            notes.append(f"Image unavailable: {img_meta}")
 
    def build_content(user_text, extra_context=""):
        content = []
        if img_b64:
            content.append({"type": "image",
                             "source": {"type": "base64", "media_type": img_meta, "data": img_b64}})
        body = f"{user_text}\n\nITEM DATA:\n{cleaned_context or '(no data provided)'}"
        if extra_context:
            body += f"\n\n{extra_context}"
        content.append({"type": "text", "text": body})
        return content
 
    total_input_tokens  = 0
    total_output_tokens = 0
 
    def call_api(sys, user_content, max_tok):
        nonlocal total_input_tokens, total_output_tokens
        for attempt in range(3):
            try:
                resp = client.messages.create(
                    model=mdl, max_tokens=max_tok, system=sys,
                    messages=[{"role": "user", "content": user_content}]
                )
                total_input_tokens  += resp.usage.input_tokens
                total_output_tokens += resp.usage.output_tokens
                return resp.content[0].text.strip()
            except anthropic.RateLimitError:
                wait = 20 * (attempt + 1)
                notes.append(f"Rate limited, retried after {wait}s")
                time.sleep(wait)
            except Exception as e:
                notes.append(f"API error: {e}")
                return ""
        return ""
 
    # Call 1 — description
    new_desc = call_api(
        sys_prompt,
        build_content("Write a listing description for this 1stDibs item. Stay between 400 and 800 characters total."),
        max_tok=220
    )
 
    char_len = len(new_desc)
    if new_desc and char_len < 400:
        notes.append(f"Below 400-char minimum ({char_len} chars)")
    if char_len > 800:
        notes.append(f"Exceeds 800-char maximum ({char_len} chars)")
 
    # Call 2 — bullets (optional)
    new_bullets = ""
    if use_bullets and bul_prompt.strip():
        new_bullets = call_api(
            "You are a luxury product copywriter for 1stDibs. Output only the bullet points requested.",
            build_content(bul_prompt),
            max_tok=150
        )
 
    # Call 3 — smart suggestions (optional)
    new_suggestions = ""
    if use_suggestions:
        new_suggestions = call_api(
            SUGGESTIONS_SYSTEM,
            build_content(
                "Review this item and its generated description. What specific information should the seller add?",
                extra_context=f"GENERATED DESCRIPTION:\n{new_desc}" if new_desc else ""
            ),
            max_tok=200
        )
 
    item_cost = compute_cost(mdl, total_input_tokens, total_output_tokens)
 
    result = dict(row)
    result["NEW_DESCRIPTION"]   = new_desc
    result["CHAR_COUNT"]        = char_len
    result["ITEM_HIGHLIGHTS"]   = new_bullets
    result["SMART_SUGGESTIONS"] = new_suggestions
    result["NOTES"]             = " | ".join(notes) if notes else ""
    result["_IMG_URL"]          = img_url_for_display
    result["_INPUT_TOKENS"]     = total_input_tokens
    result["_OUTPUT_TOKENS"]    = total_output_tokens
    result["_COST_USD"]         = item_cost
    return result
 
 
# ── Run ────────────────────────────────────────────────────────────────────────
 
st.divider()
 
has_sheet_data = bool(rows and included_cols)
has_manual     = bool(st.session_state.manual_rows)
 
if has_sheet_data:
    run_rows       = rows
    run_cols       = included_cols
    run_img_col    = image_col
    run_fieldnames = fieldnames
elif has_manual:
    run_rows       = st.session_state.manual_rows
    run_cols       = MANUAL_CONTEXT_COLS
    run_img_col    = "ITEM_IMAGE"
    run_fieldnames = MANUAL_FIELDNAMES
else:
    run_rows       = []
    run_cols       = []
    run_img_col    = "(none)"
    run_fieldnames = []
 
can_run = bool(api_key and system_prompt and run_rows and run_cols)
run_clicked = st.button("▶ Run Prompt", type="primary",
                        disabled=not can_run, use_container_width=True)
 
if not api_key:
    st.warning("Add your Anthropic API key in the sidebar.")
elif not system_prompt or system_prompt.startswith("Fallback:"):
    st.warning("Connect a Google Sheet to load your prompts.")
elif not run_rows:
    st.warning("Load item data in the Item Data tab, or add items in Quick Entry.")
elif not run_cols:
    st.warning("Select at least one column to include as context.")
 
if run_clicked and can_run:
    use_bullets     = enable_bullets and bool(bullet_prompt)
    use_suggestions = enable_suggestions
    client          = anthropic.Anthropic(api_key=api_key)
 
    # Apply description length filter
    filtered_rows = run_rows
    if filter_by_length and max_desc_chars and desc_col_filter:
        def _desc_len(r):
            val = r.get(desc_col_filter, "")
            try:
                return int(val) if str(val).isdigit() else len(str(val))
            except Exception:
                return 0
        filtered_rows = [r for r in run_rows if _desc_len(r) <= max_desc_chars]
        if len(filtered_rows) < len(run_rows):
            st.info(f"Filter applied: {len(filtered_rows)} of {len(run_rows)} items have "
                    f"original descriptions ≤ {max_desc_chars} chars.")
 
    # Sample selection
    if sample_mode == "Random sample" and len(filtered_rows) > num_rows:
        test_rows = random.sample(filtered_rows, num_rows)
    else:
        test_rows = filtered_rows[:num_rows]
 
    if not test_rows:
        st.warning("No items passed the description length filter. Try raising the limit.")
        st.stop()
 
    st.subheader(f"Running **{st.session_state.active}** on {len(test_rows)} item(s)...")
    progress_bar = st.progress(0)
    status_text  = st.empty()
 
    results = [None] * len(test_rows)
    lock    = threading.Lock()
    done    = [0]
 
    def process(idx_row):
        idx, row = idx_row
        return idx, generate_row(
            client, row, system_prompt, bullet_prompt,
            use_bullets, use_suggestions, run_cols, run_img_col, model
        )
 
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(process, (i, r)): i for i, r in enumerate(test_rows)}
        for future in as_completed(futures):
            idx, result = future.result()
            results[idx] = result
            with lock:
                done[0] += 1
                progress_bar.progress(done[0] / len(test_rows))
                status_text.text(
                    f"Processed {done[0]} of {len(test_rows)}: "
                    f"{result.get('ITEM_TITLE', result.get('NATURAL_KEY', ''))} "
                    f"({result['CHAR_COUNT']} chars)"
                )
 
    progress_bar.progress(1.0)
    status_text.text("✓ Done!")
 
    # Metrics
    st.subheader("Results")
    avg_len    = sum(r["CHAR_COUNT"] for r in results) / len(results)
    total_cost = sum(r.get("_COST_USD", 0) for r in results)
    total_in   = sum(r.get("_INPUT_TOKENS", 0) for r in results)
    total_out  = sum(r.get("_OUTPUT_TOKENS", 0) for r in results)
 
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Avg length", f"{avg_len:.0f} chars")
    m2.metric("Below 400",  sum(1 for r in results if 0 < r["CHAR_COUNT"] < 400))
    m3.metric("Over 800",   sum(1 for r in results if r["CHAR_COUNT"] > 800))
    m4.metric("With notes", sum(1 for r in results if r["NOTES"]))
    m5.metric("Total cost", f"${total_cost:.4f}",
              help=f"{total_in:,} input + {total_out:,} output tokens ({model})")
 
    for r in results:
        label     = r.get("ITEM_TITLE") or r.get("NATURAL_KEY") or list(r.values())[0]
        item_cost = r.get("_COST_USD", 0)
        with st.expander(f"**{label}** — {r['CHAR_COUNT']} chars · ${item_cost:.4f}"):
 
            # Image at top if available
            if r.get("_IMG_URL"):
                try:
                    st.image(r["_IMG_URL"], width=300)
                except Exception:
                    pass
 
            c1, c2 = st.columns(2)
            with c1:
                st.markdown("**Original**")
                original = r.get("ITEM_DESCRIPTION", "")
                if not original:
                    for col in run_cols:
                        if r.get(col):
                            original = f"_{col}_: {r[col]}"
                            break
                st.write(original or "_empty_")
                if r.get("SOURCE_URL"):
                    st.caption(f"[View on 1stDibs]({r['SOURCE_URL']})")
            with c2:
                st.markdown("**New description**")
                st.write(r.get("NEW_DESCRIPTION", "") or "_empty_")
 
            if use_bullets and r.get("ITEM_HIGHLIGHTS"):
                st.markdown("**✨ Item Highlights**")
                st.info(r["ITEM_HIGHLIGHTS"])
 
            if use_suggestions and r.get("SMART_SUGGESTIONS"):
                st.markdown("**💡 Smart Suggestions**")
                st.info(r["SMART_SUGGESTIONS"])
 
            if r["NOTES"]:
                st.caption(f"📝 {r['NOTES']}")
 
    # Download — only context cols sent to prompt + generated outputs
    out_fields = list(run_cols) + ["NEW_DESCRIPTION", "CHAR_COUNT"]
    if use_bullets and bool(bullet_prompt):
        out_fields.append("ITEM_HIGHLIGHTS")
    if use_suggestions:
        out_fields.append("SMART_SUGGESTIONS")
    out_fields.append("NOTES")
 
    out_buffer = io.StringIO()
    writer = csv.DictWriter(out_buffer, fieldnames=out_fields, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(results)
 
    st.download_button("⬇ Download results CSV", data=out_buffer.getvalue(),
                       file_name="prompt_test_results.csv", mime="text/csv",
                       use_container_width=True)
