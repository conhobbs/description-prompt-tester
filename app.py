import streamlit as st
import csv
import io
import json
import base64
import time
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

# ── Smart Suggestions prompt (baked in, works across all verticals) ────────────

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
    """Fetch a published Google Sheet as CSV text. Returns (text, error)."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.read().decode("utf-8"), None
    except Exception as e:
        return None, str(e)


def load_prompts_from_sheet(url):
    """
    Fetch published Google Sheet CSV.
    Expected columns: Name | System Prompt | Bullet Prompt
    Returns (dict, error_string).
    """
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
    """
    Fetch published Google Sheet CSV as item rows.
    Returns (rows, fieldnames, error_string).
    """
    text, err = _fetch_sheet_csv(url)
    if err:
        return None, None, err
    reader = csv.DictReader(io.StringIO(text))
    rows = list(reader)
    if not rows:
        return None, None, "Sheet loaded but contains no rows."
    return rows, list(reader.fieldnames), None


# ── Secrets ────────────────────────────────────────────────────────────────────

def get_secret(key, subkey=None):
    try:
        val = st.secrets[key]
        return val[subkey] if subkey else val
    except Exception:
        return None


secret_api_key   = get_secret("ANTHROPIC_API_KEY")
secret_sheet_url = get_secret("PROMPTS_SHEET_URL")

def get_item_sheet_url(vertical):
    """Return configured item sheet URL for a vertical, or None."""
    try:
        return st.secrets["item_sheets"][vertical]
    except Exception:
        return None

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

if "gsheet_loaded_vertical" not in st.session_state:
    st.session_state.gsheet_loaded_vertical = None

# Auto-load prompts from sheet secret on first run
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

    if secret_api_key:
        api_key = secret_api_key
        st.success("✓ API key loaded")
    else:
        api_key = st.text_input("Anthropic API Key", type="password",
                                help="Your sk-ant-... key.")

    model = st.selectbox("Model",
        options=["claude-opus-4-6", "claude-sonnet-4-6", "claude-haiku-4-5-20251001"],
        index=0,
        help="Opus = best quality. Sonnet = faster & cheaper."
    )

    num_rows = st.slider("Rows to test", min_value=1, max_value=100, value=5)
    workers  = st.slider("Parallel workers", min_value=1, max_value=10, value=5)

    st.divider()

    # ── Prompt Library ─────────────────────────────────────────────────────────
    st.subheader("Prompt Library")

    if secret_sheet_url:
        st.success("✓ Sheet connected")
        st.caption("Edit prompts in your Google Sheet, then sync.")
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

    # ── Prompt selector ────────────────────────────────────────────────────────
    st.subheader("Select Prompt")
    prompt_names = list(st.session_state.prompts.keys())
    idx = prompt_names.index(st.session_state.active) if st.session_state.active in prompt_names else 0
    selected = st.radio("Vertical", options=prompt_names, index=idx,
                        label_visibility="collapsed")
    st.session_state.active = selected

# ── Active prompt ──────────────────────────────────────────────────────────────

active_prompt = st.session_state.prompts.get(st.session_state.active, {})
system_prompt = active_prompt.get("system", "")
bullet_prompt = active_prompt.get("bullets", "")

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

    enable_bullets     = st.toggle("Enable bullet point prompt", value=bool(bullet_prompt))
    enable_suggestions = st.toggle("Enable Smart Suggestions", value=True,
                                   help="Adds a seller-facing column flagging missing information that would improve the listing.")

    if enable_bullets and bullet_prompt:
        st.subheader("Bullet Point Prompt")
        st.text_area("bullets_preview", value=bullet_prompt, height=150,
                     disabled=True, label_visibility="collapsed")
        st.caption(f"{len(bullet_prompt)} characters  ·  Edit in Google Sheets")
    elif enable_bullets and not bullet_prompt:
        st.info("No bullet prompt found for this vertical. Add a 'Bullet Prompt' column to your sheet.")

    if enable_suggestions:
        with st.expander("Smart Suggestions prompt (read-only)"):
            st.text_area("suggestions_preview", value=SUGGESTIONS_SYSTEM, height=160,
                         disabled=True, label_visibility="collapsed")


with tab2:
    st.subheader("Item Data")
    data_source = st.radio("Source", ["📊 Google Sheet", "📂 Upload CSV"],
                           horizontal=True, label_visibility="collapsed")

    rows        = []
    fieldnames  = []
    included_cols = []
    image_col   = "(none)"

    if data_source == "📊 Google Sheet":
        # Check if this vertical has a configured sheet URL in secrets
        auto_url = get_item_sheet_url(st.session_state.active)

        # Auto-reload when vertical changes and a secret URL is configured
        if auto_url and st.session_state.gsheet_loaded_vertical != st.session_state.active:
            with st.spinner(f"Loading {st.session_state.active} item sheet..."):
                loaded_rows, loaded_fns, err = load_items_from_sheet(auto_url)
            if loaded_rows:
                st.session_state.gsheet_rows = loaded_rows
                st.session_state.gsheet_fieldnames = loaded_fns
                st.session_state.gsheet_loaded_vertical = st.session_state.active
            else:
                st.error(f"Could not auto-load sheet: {err}")

        if auto_url:
            st.success(f"✓ Auto-configured for **{st.session_state.active}**")
            st.caption("Sheet URL is set in Streamlit secrets. Switch verticals in the sidebar to load a different sheet.")
            if st.button("🔄 Reload sheet", use_container_width=False):
                loaded_rows, loaded_fns, err = load_items_from_sheet(auto_url)
                if loaded_rows:
                    st.session_state.gsheet_rows = loaded_rows
                    st.session_state.gsheet_fieldnames = loaded_fns
                    st.session_state.gsheet_loaded_vertical = st.session_state.active
                    st.rerun()
                else:
                    st.error(f"Reload failed: {err}")
        else:
            st.caption("Paste a published Google Sheet CSV URL. To auto-load per vertical, add URLs to Streamlit secrets under `[item_sheets]`.")
            manual_sheet_url = st.text_input(
                "Sheet URL",
                placeholder="https://docs.google.com/spreadsheets/d/.../pub?output=csv",
                label_visibility="collapsed"
            )
            if st.button("Load sheet", type="secondary", use_container_width=True):
                if manual_sheet_url.strip():
                    with st.spinner("Loading..."):
                        loaded_rows, loaded_fns, err = load_items_from_sheet(manual_sheet_url.strip())
                    if loaded_rows:
                        st.session_state.gsheet_rows = loaded_rows
                        st.session_state.gsheet_fieldnames = loaded_fns
                        st.session_state.gsheet_loaded_vertical = st.session_state.active
                        st.rerun()
                    else:
                        st.error(f"Could not load: {err}")
                else:
                    st.warning("Paste a sheet URL first.")

        if st.session_state.gsheet_rows:
            fns = st.session_state.gsheet_fieldnames
            st.success(f"✓ {len(st.session_state.gsheet_rows)} rows — {len(fns)} columns")
            default_include = [c for c in fns if c not in ("ITEM_IMAGE",)]
            included_cols = st.multiselect("Columns to pass as context",
                                           options=fns, default=default_include)
            image_col = st.selectbox(
                "Image URL column (optional)",
                options=["(none)"] + fns,
                index=fns.index("ITEM_IMAGE") + 1 if "ITEM_IMAGE" in fns else 0
            )
            rows      = st.session_state.gsheet_rows
            fieldnames = fns
            with st.expander("Preview first 3 rows"):
                for r in rows[:3]:
                    st.json({k: v for k, v in r.items() if k in included_cols})

    else:  # Upload CSV
        uploaded_file = st.file_uploader("Upload CSV", type=["csv"],
                                         label_visibility="collapsed")
        if uploaded_file:
            try:
                content  = uploaded_file.read().decode("utf-8")
                reader   = csv.DictReader(io.StringIO(content))
                rows     = list(reader)
                fieldnames = list(reader.fieldnames)
                st.success(f"✓ {len(rows)} rows — {len(fieldnames)} columns detected")
                default_include = [c for c in fieldnames if c not in ("ITEM_IMAGE",)]
                included_cols = st.multiselect("Columns to pass as context",
                                               options=fieldnames, default=default_include)
                image_col = st.selectbox(
                    "Image URL column (optional)",
                    options=["(none)"] + fieldnames,
                    index=fieldnames.index("ITEM_IMAGE") + 1 if "ITEM_IMAGE" in fieldnames else 0
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

    img_b64, img_meta = None, None
    if img_col and img_col != "(none)":
        img_b64, img_meta = fetch_image_as_base64(row.get(img_col, ""))
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
        suggestions_user = build_content(
            "Review this item and its generated description. What specific information should the seller add?",
            extra_context=f"GENERATED DESCRIPTION:\n{new_desc}" if new_desc else ""
        )
        new_suggestions = call_api(SUGGESTIONS_SYSTEM, suggestions_user, max_tok=200)

    result = dict(row)
    result["NEW_DESCRIPTION"]   = new_desc
    result["CHAR_COUNT"]        = char_len
    result["BULLET_POINTS"]     = new_bullets
    result["SMART_SUGGESTIONS"] = new_suggestions
    result["NOTES"]             = " | ".join(notes) if notes else ""
    return result


# ── Run ────────────────────────────────────────────────────────────────────────

st.divider()

# Determine active data source (CSV/gSheet takes priority over Quick Entry)
has_sheet_data = bool(rows and included_cols)
has_manual     = bool(st.session_state.manual_rows)

if has_sheet_data:
    run_rows       = rows
    run_cols       = included_cols
    run_img_col    = image_col
    run_fieldnames = fieldnames
    data_source_label = f"{len(rows)} rows"
elif has_manual:
    run_rows       = st.session_state.manual_rows
    run_cols       = MANUAL_CONTEXT_COLS
    run_img_col    = "ITEM_IMAGE"
    run_fieldnames = MANUAL_FIELDNAMES
    data_source_label = f"{len(st.session_state.manual_rows)} quick-entry item(s)"
else:
    run_rows = run_cols = run_fieldnames = []
    run_img_col = "(none)"
    data_source_label = ""

can_run = api_key and system_prompt and run_rows and run_cols
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
    test_rows       = run_rows[:num_rows]

    st.subheader(f"Running {st.session_state.active} prompt on {len(test_rows)} {data_source_label}...")
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
                    f"{result.get('ITEM_TITLE', result.get('NATURAL_KEY', ''))} ({result['CHAR_COUNT']} chars)"
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

    # Side-by-side results
    for r in results:
        label = r.get("ITEM_TITLE") or r.get("NATURAL_KEY") or list(r.values())[0]
        with st.expander(f"**{label}** — {r['CHAR_COUNT']} chars"):
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
            if use_bullets and r.get("BULLET_POINTS"):
                st.markdown("**Bullet points**")
                st.write(r["BULLET_POINTS"])
            if use_suggestions and r.get("SMART_SUGGESTIONS"):
                st.markdown("**💡 Smart Suggestions**")
                st.info(r["SMART_SUGGESTIONS"])
            if r["NOTES"]:
                st.caption(f"📝 {r['NOTES']}")

    # Download
    out_fields = list(run_fieldnames) + ["NEW_DESCRIPTION", "CHAR_COUNT"]
    if use_bullets:
        out_fields.append("BULLET_POINTS")
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
