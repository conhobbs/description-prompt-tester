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
    idx = prompt_names.index(st.session_state.active) if st.session_state.active in prompt_names else 0
    selected = st.selectbox("Select prompt", options=prompt_names, index=idx,
                            label_visibility="collapsed")
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

tab1, tab2, tab3 = st.tabs(["📝 Prompt Preview", "📊 Item Data", "✍️ Quick Entry"])

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

    def call_api(sys, user_content, max_tok):
        for attempt in range(3):
            try:
                resp = client.messages.create(
                    model=mdl, max_tokens=max_tok, system=sys,
                    messages=[{"role": "user", "content": user_content}]
                )
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

    result = dict(row)
    result["NEW_DESCRIPTION"]   = new_desc
    result["CHAR_COUNT"]        = char_len
    result["ITEM_HIGHLIGHTS"]     = new_bullets
    result["SMART_SUGGESTIONS"] = new_suggestions
    result["NOTES"]             = " | ".join(notes) if notes else ""
    result["_IMG_URL"]          = img_url_for_display
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

    # Sample selection
    if sample_mode == "Random sample" and len(run_rows) > num_rows:
        test_rows = random.sample(run_rows, num_rows)
    else:
        test_rows = run_rows[:num_rows]

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
    avg_len = sum(r["CHAR_COUNT"] for r in results) / len(results)
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Avg length", f"{avg_len:.0f} chars")
    m2.metric("Below 400",  sum(1 for r in results if 0 < r["CHAR_COUNT"] < 400))
    m3.metric("Over 800",   sum(1 for r in results if r["CHAR_COUNT"] > 800))
    m4.metric("With notes", sum(1 for r in results if r["NOTES"]))

    for r in results:
        label = r.get("ITEM_TITLE") or r.get("NATURAL_KEY") or list(r.values())[0]
        with st.expander(f"**{label}** — {r['CHAR_COUNT']} chars"):

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

    # Download — exclude _IMG_URL display field
    out_fields = list(run_fieldnames) + ["NEW_DESCRIPTION", "CHAR_COUNT"]
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
