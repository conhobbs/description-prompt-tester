import streamlit as st
import csv
import io
import base64
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import anthropic

st.set_page_config(
    page_title="1stDibs Prompt Tester",
    page_icon="🪑",
    layout="wide"
)

st.title("🪑 1stDibs Prompt Tester")
st.caption("Upload a prompt and CSV of items to test AI-generated descriptions.")

# ── Sidebar config ─────────────────────────────────────────────────────────────

with st.sidebar:
    st.header("Configuration")

    api_key = st.text_input(
        "Anthropic API Key",
        type="password",
        help="Your sk-ant-... key. Never stored or logged."
    )

    model = st.selectbox(
        "Model",
        options=["claude-opus-4-6", "claude-sonnet-4-6", "claude-haiku-4-5-20251001"],
        index=0,
        help="Opus = best quality. Sonnet = faster & cheaper. Haiku = fastest."
    )

    num_rows = st.slider(
        "Rows to test",
        min_value=1,
        max_value=100,
        value=5,
        help="How many rows from the CSV to process."
    )

    workers = st.slider(
        "Parallel workers",
        min_value=1,
        max_value=10,
        value=5,
        help="Higher = faster, but may hit rate limits."
    )

    st.divider()
    st.markdown("**CSV Requirements**")
    st.markdown("""
Your CSV must include these columns:
- `NATURAL_KEY` — unique item ID
- `ITEM_IMAGE` — product image URL
- `ITEM_DESCRIPTION` — seller's original description

All other columns are preserved in the output.
    """)

# ── Main layout ────────────────────────────────────────────────────────────────

col1, col2 = st.columns([1, 1])

with col1:
    st.subheader("System Prompt")
    system_prompt = st.text_area(
        label="Paste your system prompt here",
        height=400,
        placeholder="You are a copywriter for 1stDibs...",
        label_visibility="collapsed"
    )
    st.caption(f"{len(system_prompt)} characters")

with col2:
    st.subheader("Item Data")
    uploaded_file = st.file_uploader(
        "Upload CSV",
        type=["csv"],
        label_visibility="collapsed"
    )

    if uploaded_file:
        try:
            content = uploaded_file.read().decode("utf-8")
            reader = csv.DictReader(io.StringIO(content))
            rows = list(reader)
            fieldnames = reader.fieldnames
            st.success(f"✓ {len(rows)} rows loaded — columns: {', '.join(fieldnames)}")
            with st.expander("Preview first 3 rows"):
                for row in rows[:3]:
                    st.json({k: v for k, v in row.items() if k in ["NATURAL_KEY", "ITEM_DESCRIPTION"]})
        except Exception as e:
            st.error(f"Error reading CSV: {e}")
            rows = []
            fieldnames = []
    else:
        rows = []
        fieldnames = []


# ── Image fetching ─────────────────────────────────────────────────────────────

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


# ── Description generation ─────────────────────────────────────────────────────

BOILERPLATE_TRIGGERS = [
    "message us with your zip", "shipping", "contact us", "please note",
    "delivery", "white glove", "local pickup", "inquire", "call us"
]

USER_PROMPT_TEMPLATE = """Write a listing description for this 1stDibs item. Stay between 400 and 800 characters total.

SELLER'S ORIGINAL DESCRIPTION:
{description}"""


def generate_description(client, row, sys_prompt, mdl):
    key = row.get("NATURAL_KEY", "unknown")
    image_url = row.get("ITEM_IMAGE", "")
    original_desc = row.get("ITEM_DESCRIPTION", "").strip()
    notes = []

    # Strip boilerplate
    cleaned_desc = original_desc
    for trigger in BOILERPLATE_TRIGGERS:
        if trigger.lower() in original_desc.lower():
            sentences = original_desc.split(". ")
            filtered = [s for s in sentences if trigger.lower() not in s.lower()]
            cleaned_desc = ". ".join(filtered).strip()
            notes.append(f"Boilerplate stripped ('{trigger}')")
            break

    if not cleaned_desc:
        notes.append("No usable seller description")

    # Fetch image
    img_b64, img_meta = fetch_image_as_base64(image_url)
    if img_b64 is None:
        notes.append(f"Image unavailable: {img_meta}")

    # Build content
    content = []
    if img_b64:
        content.append({
            "type": "image",
            "source": {"type": "base64", "media_type": img_meta, "data": img_b64}
        })
    content.append({
        "type": "text",
        "text": USER_PROMPT_TEMPLATE.format(description=cleaned_desc or "(no description provided)")
    })

    # Call API
    new_description = ""
    for attempt in range(3):
        try:
            response = client.messages.create(
                model=mdl,
                max_tokens=220,
                system=sys_prompt,
                messages=[{"role": "user", "content": content}]
            )
            new_description = response.content[0].text.strip()
            break
        except anthropic.RateLimitError:
            wait = 20 * (attempt + 1)
            notes.append(f"Rate limited, retried after {wait}s")
            time.sleep(wait)
        except Exception as e:
            notes.append(f"API error: {e}")
            break

    char_len = len(new_description)
    if new_description and char_len < 400:
        notes.append(f"Below 400-char minimum ({char_len} chars)")
    if char_len > 800:
        notes.append(f"Exceeds 800-char maximum ({char_len} chars)")

    result = dict(row)
    result["NEW_DESCRIPTION"] = new_description
    result["CHAR_COUNT"] = char_len
    result["NOTES"] = " | ".join(notes) if notes else ""
    return result


# ── Run button ─────────────────────────────────────────────────────────────────

st.divider()

can_run = api_key and system_prompt and rows
run_clicked = st.button(
    "▶ Run Prompt",
    type="primary",
    disabled=not can_run,
    use_container_width=True
)

if not api_key:
    st.warning("Add your Anthropic API key in the sidebar to run.")
elif not system_prompt:
    st.warning("Paste a system prompt on the left.")
elif not rows:
    st.warning("Upload a CSV file on the right.")

if run_clicked and can_run:
    client = anthropic.Anthropic(api_key=api_key)
    test_rows = rows[:num_rows]

    st.subheader(f"Running on {len(test_rows)} rows...")
    progress_bar = st.progress(0)
    status_text = st.empty()

    results = [None] * len(test_rows)
    lock = threading.Lock()
    completed_count = [0]

    def process(idx_row):
        idx, row = idx_row
        return idx, generate_description(client, row, system_prompt, model)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(process, (i, row)): i for i, row in enumerate(test_rows)}
        for future in as_completed(futures):
            idx, result = future.result()
            results[idx] = result
            with lock:
                completed_count[0] += 1
                pct = completed_count[0] / len(test_rows)
                progress_bar.progress(pct)
                status_text.text(f"Processed {completed_count[0]} of {len(test_rows)}: {result['NATURAL_KEY']} ({result['CHAR_COUNT']} chars)")

    progress_bar.progress(1.0)
    status_text.text("✓ Done!")

    # ── Results ────────────────────────────────────────────────────────────────

    st.subheader("Results")

    avg_len = sum(r["CHAR_COUNT"] for r in results) / len(results)
    below_min = sum(1 for r in results if 0 < r["CHAR_COUNT"] < 400)
    over_max = sum(1 for r in results if r["CHAR_COUNT"] > 800)
    with_notes = sum(1 for r in results if r["NOTES"])

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Avg length", f"{avg_len:.0f} chars")
    m2.metric("Below 400", below_min)
    m3.metric("Over 800", over_max)
    m4.metric("With notes", with_notes)

    # Side-by-side view
    for r in results:
        with st.expander(f"**{r['NATURAL_KEY']}** — {r['CHAR_COUNT']} chars"):
            c1, c2 = st.columns(2)
            with c1:
                st.markdown("**Original**")
                st.write(r.get("ITEM_DESCRIPTION", "") or "_empty_")
            with c2:
                st.markdown("**New description**")
                st.write(r.get("NEW_DESCRIPTION", "") or "_empty_")
            if r["NOTES"]:
                st.info(f"📝 {r['NOTES']}")

    # ── Download ───────────────────────────────────────────────────────────────

    out_fields = list(fieldnames) + ["NEW_DESCRIPTION", "CHAR_COUNT", "NOTES"]
    out_buffer = io.StringIO()
    writer = csv.DictWriter(out_buffer, fieldnames=out_fields, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(results)

    st.download_button(
        label="⬇ Download results CSV",
        data=out_buffer.getvalue(),
        file_name="prompt_test_results.csv",
        mime="text/csv",
        use_container_width=True
    )
