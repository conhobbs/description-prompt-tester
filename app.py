import streamlit as st
import csv
import io
import json
import base64
import time
import threading
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import anthropic

st.set_page_config(
    page_title="1stDibs Prompt Tester",
    page_icon="🪑",
    layout="wide"
)

# ── Fallback prompts (used only if no sheet is connected) ──────────────────────

FALLBACK_PROMPTS = {
    "Furniture": {
        "system": "Fallback: connect a Google Sheet to load your prompts.",
        "bullets": ""
    }
}

# ── Google Sheets loader ───────────────────────────────────────────────────────

def load_prompts_from_sheet(url):
    """
    Fetch a published Google Sheet CSV.
    Expected columns: Name | System Prompt | Bullet Prompt
    Returns (dict, error_string).
    """
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            content = resp.read().decode("utf-8")
        reader = csv.DictReader(io.StringIO(content))
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
    except Exception as e:
        return None, str(e)


# ── Secrets ────────────────────────────────────────────────────────────────────

def get_secret(key):
    try:
        return st.secrets[key]
    except Exception:
        return None

secret_api_key = get_secret("ANTHROPIC_API_KEY")
secret_sheet_url = get_secret("PROMPTS_SHEET_URL")

# ── Session state init ─────────────────────────────────────────────────────────

if "prompts" not in st.session_state:
    st.session_state.prompts = dict(FALLBACK_PROMPTS)

if "active" not in st.session_state:
    st.session_state.active = list(FALLBACK_PROMPTS.keys())[0]

if "sheet_loaded" not in st.session_state:
    st.session_state.sheet_loaded = False

# Auto-load from sheet secret on first run
if secret_sheet_url and not st.session_state.sheet_loaded:
    loaded, err = load_prompts_from_sheet(secret_sheet_url)
    if loaded:
        st.session_state.prompts = loaded
        st.session_state.active = list(loaded.keys())[0]
        st.session_state.sheet_loaded = True

# ── Header ─────────────────────────────────────────────────────────────────────

st.title("🪑 1stDibs Prompt Tester")
st.caption("Select a prompt, upload item data, and run.")

# ── Sidebar ────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.header("Configuration")

    # API key
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
    workers = st.slider("Parallel workers", min_value=1, max_value=10, value=5)

    st.divider()

    # ── Google Sheet ───────────────────────────────────────────────────────────
    st.subheader("Prompt Library")

    if secret_sheet_url:
        st.success("✓ Sheet connected")
        st.caption("Edit prompts in your Google Sheet, then sync.")
        if st.button("🔄 Sync from sheet", use_container_width=True):
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
                    st.info("To make this permanent, add PROMPTS_SHEET_URL to Streamlit secrets.")
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

    selected = st.radio(
        "Vertical",
        options=prompt_names,
        index=idx,
        label_visibility="collapsed"
    )
    st.session_state.active = selected

# ── Active prompt ──────────────────────────────────────────────────────────────

active_prompt = st.session_state.prompts.get(st.session_state.active, {})
system_prompt = active_prompt.get("system", "")
bullet_prompt = active_prompt.get("bullets", "")

# ── Main tabs ──────────────────────────────────────────────────────────────────

tab1, tab2 = st.tabs(["📝 Prompt Preview", "📂 Data"])

with tab1:
    st.subheader(f"{st.session_state.active} — System Prompt")
    st.text_area(
        "system_preview",
        value=system_prompt,
        height=300,
        disabled=True,
        label_visibility="collapsed"
    )
    st.caption(f"{len(system_prompt)} characters  ·  Edit in Google Sheets")

    enable_bullets = st.toggle("Enable bullet point prompt", value=bool(bullet_prompt))

    if enable_bullets and bullet_prompt:
        st.subheader("Bullet Point Prompt")
        st.text_area(
            "bullets_preview",
            value=bullet_prompt,
            height=200,
            disabled=True,
            label_visibility="collapsed"
        )
        st.caption(f"{len(bullet_prompt)} characters  ·  Edit in Google Sheets")
    elif enable_bullets and not bullet_prompt:
        st.info("No bullet prompt found for this vertical in the sheet. Add a 'Bullet Prompt' column.")

with tab2:
    st.subheader("Upload Item Data")
    uploaded_file = st.file_uploader("Upload CSV", type=["csv"],
                                     label_visibility="collapsed")

    rows = []
    fieldnames = []
    included_cols = []
    image_col = "(none)"

    if uploaded_file:
        try:
            content = uploaded_file.read().decode("utf-8")
            reader = csv.DictReader(io.StringIO(content))
            rows = list(reader)
            fieldnames = list(reader.fieldnames)
            st.success(f"✓ {len(rows)} rows — {len(fieldnames)} columns detected")

            st.markdown("**Select columns to pass as context to the prompt:**")
            default_exclude = ["ITEM_IMAGE"]
            default_include = [c for c in fieldnames if c not in default_exclude]
            included_cols = st.multiselect(
                "Columns", options=fieldnames, default=default_include,
                label_visibility="collapsed"
            )

            image_col = st.selectbox(
                "Image URL column (optional)",
                options=["(none)"] + fieldnames,
                index=fieldnames.index("ITEM_IMAGE") + 1 if "ITEM_IMAGE" in fieldnames else 0
            )

            with st.expander("Preview first 3 rows"):
                for row in rows[:3]:
                    st.json({k: v for k, v in row.items() if k in included_cols})
        except Exception as e:
            st.error(f"Error reading CSV: {e}")

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
        ct = resp.headers.get("Content-Type", "image/jpeg").split(";")[0].strip()
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
                 inc_cols, img_col, mdl):
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

    def build_content(user_text):
        content = []
        if img_b64:
            content.append({"type": "image",
                             "source": {"type": "base64", "media_type": img_meta, "data": img_b64}})
        content.append({"type": "text",
                         "text": f"{user_text}\n\nITEM DATA:\n{cleaned_context or '(no data provided)'}"})
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

    new_bullets = ""
    if use_bullets and bul_prompt.strip():
        new_bullets = call_api(
            "You are a luxury product copywriter for 1stDibs. Output only the bullet points requested.",
            build_content(bul_prompt),
            max_tok=150
        )

    result = dict(row)
    result["NEW_DESCRIPTION"] = new_desc
    result["CHAR_COUNT"] = char_len
    result["BULLET_POINTS"] = new_bullets
    result["NOTES"] = " | ".join(notes) if notes else ""
    return result


# ── Run ────────────────────────────────────────────────────────────────────────

st.divider()

can_run = api_key and system_prompt and rows and included_cols
run_clicked = st.button("▶ Run Prompt", type="primary",
                        disabled=not can_run, use_container_width=True)

if not api_key:
    st.warning("Add your Anthropic API key in the sidebar.")
elif not system_prompt or system_prompt.startswith("Fallback:"):
    st.warning("Connect a Google Sheet to load your prompts.")
elif not rows:
    st.warning("Upload a CSV in the Data tab.")
elif not included_cols:
    st.warning("Select at least one column to include as context.")

if run_clicked and can_run:
    use_bullets = enable_bullets and bool(bullet_prompt)
    client = anthropic.Anthropic(api_key=api_key)
    test_rows = rows[:num_rows]

    st.subheader(f"Running {st.session_state.active} prompt on {len(test_rows)} rows...")
    progress_bar = st.progress(0)
    status_text = st.empty()

    results = [None] * len(test_rows)
    lock = threading.Lock()
    done = [0]

    def process(idx_row):
        idx, row = idx_row
        return idx, generate_row(
            client, row, system_prompt, bullet_prompt,
            use_bullets, included_cols, image_col, model
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
                    f"{result.get('NATURAL_KEY', '')} ({result['CHAR_COUNT']} chars)"
                )

    progress_bar.progress(1.0)
    status_text.text("✓ Done!")

    # Metrics
    st.subheader("Results")
    avg_len = sum(r["CHAR_COUNT"] for r in results) / len(results)
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Avg length", f"{avg_len:.0f} chars")
    m2.metric("Below 400", sum(1 for r in results if 0 < r["CHAR_COUNT"] < 400))
    m3.metric("Over 800", sum(1 for r in results if r["CHAR_COUNT"] > 800))
    m4.metric("With notes", sum(1 for r in results if r["NOTES"]))

    # Side-by-side
    for r in results:
        label = r.get("NATURAL_KEY", "") or list(r.values())[0]
        with st.expander(f"**{label}** — {r['CHAR_COUNT']} chars"):
            c1, c2 = st.columns(2)
            with c1:
                st.markdown("**Original**")
                original = r.get("ITEM_DESCRIPTION", "")
                if not original:
                    for col in included_cols:
                        if r.get(col):
                            original = f"_{col}_: {r[col]}"
                            break
                st.write(original or "_empty_")
            with c2:
                st.markdown("**New description**")
                st.write(r.get("NEW_DESCRIPTION", "") or "_empty_")
            if use_bullets and r.get("BULLET_POINTS"):
                st.markdown("**Bullet points**")
                st.write(r["BULLET_POINTS"])
            if r["NOTES"]:
                st.info(f"📝 {r['NOTES']}")

    # Download
    out_fields = list(fieldnames) + ["NEW_DESCRIPTION", "CHAR_COUNT"]
    if use_bullets:
        out_fields.append("BULLET_POINTS")
    out_fields.append("NOTES")

    out_buffer = io.StringIO()
    writer = csv.DictWriter(out_buffer, fieldnames=out_fields, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(results)

    st.download_button("⬇ Download results CSV", data=out_buffer.getvalue(),
                       file_name="prompt_test_results.csv", mime="text/csv",
                       use_container_width=True)
