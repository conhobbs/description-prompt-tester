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
from bs4 import BeautifulSoup
 
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
 
if "url_rows" not in st.session_state:
    st.session_state.url_rows = []
 
if "url_scrape_errors" not in st.session_state:
    st.session_state.url_scrape_errors = []
 
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
 
# ── URL scraper (defined here so tab3 can call it) ─────────────────────────────
 
URL_FIELDNAMES = [
    "NATURAL_KEY", "ITEM_TITLE", "ITEM_DESCRIPTION", "CATEGORY",
    "CREATOR", "MATERIALS", "CONDITION", "PERIOD", "ORIGIN", "PRICE",
    "ITEM_IMAGE", "SOURCE_URL"
]
URL_CONTEXT_COLS = [f for f in URL_FIELDNAMES if f not in ("ITEM_IMAGE", "SOURCE_URL")]
 
 
def scrape_1stdibs_url(url):
    """
    Fetch a 1stDibs item page and extract key fields via __NEXT_DATA__.
    Returns (dict, error_string).
    """
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
        }
        resp = requests.get(url.strip(), headers=headers, timeout=15)
        resp.raise_for_status()
 
        soup = BeautifulSoup(resp.text, "html.parser")
        tag = soup.find("script", {"id": "__NEXT_DATA__"})
        if not tag:
            return None, "Page loaded but __NEXT_DATA__ not found — may be blocked or JS-rendered."
 
        data = json.loads(tag.string)
        props = data.get("props", {}).get("pageProps", {})
 
        item = None
        for key in ("item", "pdpData", "product"):
            candidate = props.get(key)
            if isinstance(candidate, dict):
                item = candidate.get("item", candidate)
                break
        if item is None:
            return None, "Found page data but could not locate item object."
 
        def _str(val):
            if isinstance(val, dict):
                return val.get("displayName", "") or val.get("name", "") or ""
            return str(val) if val else ""
 
        natural_key = _str(item.get("id", "")) or re.sub(r"[^a-z0-9-]", "", url.split("/")[-1].split("?")[0])
        title = _str(item.get("title") or item.get("name"))
        description = (
            item.get("description") or
            item.get("sellerDescription") or
            item.get("publicDescription") or ""
        )
        if isinstance(description, dict):
            description = description.get("raw", "") or description.get("html", "")
        description = re.sub(r"<[^>]+>", " ", str(description)).strip()
 
        category = _str(item.get("category") or (item.get("categoryPath", [{}])[-1] if item.get("categoryPath") else {}))
 
        mats = item.get("materials") or item.get("materialsTechniques", [])
        if isinstance(mats, list):
            materials = ", ".join(_str(m) for m in mats if _str(m))
        else:
            materials = _str(mats)
 
        condition = _str(item.get("condition") or item.get("conditionDetails"))
 
        creator = ""
        for ck in ("creator", "designer", "seller", "manufacturer"):
            c = item.get(ck)
            if c:
                creator = _str(c)
                break
 
        period = _str(item.get("period") or item.get("style") or item.get("circa"))
        origin = _str(item.get("origin") or item.get("countryOfOrigin") or item.get("productionLocation"))
 
        price = ""
        pd_raw = item.get("price") or item.get("priceAmount")
        if isinstance(pd_raw, dict):
            amt = pd_raw.get("amount") or pd_raw.get("value") or ""
            curr = pd_raw.get("currency", "USD")
            if amt:
                price = f"{curr} {amt}"
        elif pd_raw:
            price = str(pd_raw)
 
        image_url = ""
        for media_key in ("media", "images", "photos", "photoList"):
            media = item.get(media_key)
            if isinstance(media, list) and media:
                first = media[0]
                if isinstance(first, dict):
                    image_url = (
                        first.get("masterOrZoomUrl") or
                        first.get("src") or first.get("url") or
                        first.get("imageUrl") or ""
                    )
                elif isinstance(first, str):
                    image_url = first
                if image_url:
                    break
 
        row = {
            "NATURAL_KEY": natural_key,
            "ITEM_TITLE": title,
            "ITEM_DESCRIPTION": description,
            "CATEGORY": category,
            "CREATOR": creator,
            "MATERIALS": materials,
            "CONDITION": condition,
            "PERIOD": period,
            "ORIGIN": origin,
            "PRICE": price,
            "ITEM_IMAGE": image_url,
            "SOURCE_URL": url.strip(),
        }
        return row, None
 
    except requests.exceptions.Timeout:
        return None, "Request timed out (15s)"
    except requests.exceptions.HTTPError as e:
        return None, f"HTTP {e.response.status_code}"
    except json.JSONDecodeError:
        return None, "Failed to parse JSON from page"
    except Exception as e:
        return None, str(e)
 
 
# ── Main tabs ──────────────────────────────────────────────────────────────────
 
tab1, tab2, tab3 = st.tabs(["📝 Prompt Preview", "📂 CSV Upload", "🔗 1stDibs URLs"])
 
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
 
with tab3:
    st.subheader("Scrape from 1stDibs URLs")
    st.caption("Paste one URL per line. The app will fetch item data directly from each page.")
 
    url_input = st.text_area(
        "Item URLs",
        placeholder="https://www.1stdibs.com/furniture/seating/lounge-chairs/...\nhttps://www.1stdibs.com/furniture/tables/...",
        height=160,
        label_visibility="collapsed"
    )
 
    col_fetch, col_clear = st.columns([3, 1])
    with col_fetch:
        fetch_clicked = st.button("🔍 Fetch Items", type="secondary", use_container_width=True,
                                  disabled=not url_input.strip())
    with col_clear:
        if st.button("✕ Clear", use_container_width=True):
            st.session_state.url_rows = []
            st.session_state.url_scrape_errors = []
            st.rerun()
 
    if fetch_clicked and url_input.strip():
        urls = [u.strip() for u in url_input.strip().splitlines() if u.strip()]
        st.session_state.url_rows = []
        st.session_state.url_scrape_errors = []
        fetch_bar = st.progress(0)
        fetch_status = st.empty()
        for i, u in enumerate(urls):
            fetch_status.text(f"Fetching {i + 1}/{len(urls)}: {u[:80]}...")
            row, err = scrape_1stdibs_url(u)
            if row:
                st.session_state.url_rows.append(row)
            else:
                st.session_state.url_scrape_errors.append((u, err))
            fetch_bar.progress((i + 1) / len(urls))
        fetch_status.text(
            f"✓ Fetched {len(st.session_state.url_rows)} item(s)"
            + (f" — {len(st.session_state.url_scrape_errors)} failed" if st.session_state.url_scrape_errors else "")
        )
 
    if st.session_state.url_scrape_errors:
        with st.expander(f"⚠ {len(st.session_state.url_scrape_errors)} URL(s) failed"):
            for u, err in st.session_state.url_scrape_errors:
                st.markdown(f"**{u}**  \n`{err}`")
 
    if st.session_state.url_rows:
        st.success(f"✓ {len(st.session_state.url_rows)} item(s) ready to run")
        with st.expander("Preview scraped items"):
            for r in st.session_state.url_rows:
                label = r.get("ITEM_TITLE") or r.get("NATURAL_KEY") or r.get("SOURCE_URL")
                st.markdown(f"**{label}**")
                preview = {k: v for k, v in r.items() if v and k not in ("ITEM_IMAGE",)}
                st.json(preview)
 
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
 
# Determine active data source
has_csv = bool(rows and included_cols)
has_urls = bool(st.session_state.url_rows)
 
if has_csv:
    run_rows = rows
    run_cols = included_cols
    run_img_col = image_col
    run_fieldnames = fieldnames
    data_source_label = f"{len(rows)} CSV rows"
elif has_urls:
    run_rows = st.session_state.url_rows
    run_cols = URL_CONTEXT_COLS
    run_img_col = "ITEM_IMAGE"
    run_fieldnames = URL_FIELDNAMES
    data_source_label = f"{len(st.session_state.url_rows)} scraped item(s)"
else:
    run_rows = []
    run_cols = []
    run_img_col = "(none)"
    run_fieldnames = []
    data_source_label = ""
 
can_run = api_key and system_prompt and run_rows and run_cols
run_clicked = st.button("▶ Run Prompt", type="primary",
                        disabled=not can_run, use_container_width=True)
 
if not api_key:
    st.warning("Add your Anthropic API key in the sidebar.")
elif not system_prompt or system_prompt.startswith("Fallback:"):
    st.warning("Connect a Google Sheet to load your prompts.")
elif not run_rows:
    st.warning("Upload a CSV in the Data tab, or scrape URLs in the 1stDibs URLs tab.")
elif not run_cols:
    st.warning("Select at least one column to include as context.")
 
if run_clicked and can_run:
    use_bullets = enable_bullets and bool(bullet_prompt)
    client = anthropic.Anthropic(api_key=api_key)
    test_rows = run_rows[:num_rows]
 
    st.subheader(f"Running {st.session_state.active} prompt on {len(test_rows)} {data_source_label}...")
    progress_bar = st.progress(0)
    status_text = st.empty()
 
    results = [None] * len(test_rows)
    lock = threading.Lock()
    done = [0]
 
    def process(idx_row):
        idx, row = idx_row
        return idx, generate_row(
            client, row, system_prompt, bullet_prompt,
            use_bullets, run_cols, run_img_col, model
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
                    f"{result.get('NATURAL_KEY', result.get('ITEM_TITLE', ''))} ({result['CHAR_COUNT']} chars)"
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
            if r["NOTES"]:
                st.info(f"📝 {r['NOTES']}")
 
    # Download
    out_fields = list(run_fieldnames) + ["NEW_DESCRIPTION", "CHAR_COUNT"]
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
