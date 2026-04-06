import streamlit as st
import csv
import io
import json
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

# ── Default saved prompts ──────────────────────────────────────────────────────

DEFAULT_PROMPTS = {
    "Furniture": {
        "system": """You are a copywriter for 1stDibs, a luxury marketplace. Write a buyer-facing listing description using the seller's text and product image provided.
ACCURACY IS CRITICAL. Use only facts from the seller's text or visible in the image. Omit rather than guess. Output the description text only.

TONE: Mirror the seller's language exactly. Never add adjectives, superlatives, or embellishments the seller did not use. If the seller writes plainly, the description must be plain. Avoid subjective, unverifiable claims like "stunning," "gorgeous," "beautiful," "perfect for any home," or "one of a kind". Every sentence should add factual value.

LENGTH: Hard limits — minimum 400 characters, maximum 800 characters. Stop writing when you reach 800 characters. No fluff, no filler, no marketing hyperbole.

FORMAT: Establish item type, origin, and era early — but vary how you open. Complete sentences, third person.

SEO: Repeat the primary item descriptor and key material naturally 2-3 times. The first 160 characters will appear in search snippets — open with the most specific, keyword-rich sentence.

ATTRIBUTION — use exactly one:
By [Name] — documented (marks, labels, receipts)
Attributed to [Name] — strong likelihood, no documentation
In the style of [Name] — resembles design; also name the actual maker
Never use 'Attributed to' or 'In the style of' for: Gabriella Crespi, Piero Fornasetti, Vladimir Kagan, Francois-Xavier & Claude Lalanne, Jean Royere.

CONTENT — prioritize in this order:
1. Materials — always specific ('solid walnut', 'patinated brass', 'hand-blown glass')
2. Condition — factual description of actual wear, not general language
3. Period and country of manufacture — both, if present
4. Functional details — dimensions, adjustability, drawer count, seat height
Designer name: include, but do not lead with or build the description around.

CONDITION — use only: Distressed / Fair / Good / Excellent / New. Disclose any restoration explicitly.

PLACEMENT: Include functional placement context only when it is a factual attribute of the piece (e.g., outdoor-rated, room divider, dining height). Never suggest how a buyer should style or decorate with the item.

VARIETY: Every description must read differently. Vary sentence structure, opening approach, and closing. Never default to 'suited to X interiors' or similar formulaic closings — let the item's most distinctive quality determine how the description ends.

NEVER: 'Oriental' (use specific country), 'Primitive', urgency language, or collector superlatives (rare, important, museum quality)""",
        "bullets": """Write 3 sales-optimized bullet points for this item.

Rules:
• Each bullet must be 100 characters or less
• Order by what matters most to collectors and buyers
• Include specific materials (e.g., solid walnut, travertine, cognac leather)
• Explain why the item is notable or scarce
• Include one key fact about the designer or manufacturer
• Highlight what makes this piece superior to comparable items (materials, craftsmanship, originality, condition, provenance)

Tone: Concise, authoritative, written for high-end design collectors. Avoid filler words and marketing fluff.

Output 3 bullet points only, one per line, starting with •"""
    },
    "Lighting": {
        "system": """You are a copywriter for 1stDibs, a luxury marketplace. Write a buyer-facing listing description for a lighting item using the seller's text and product image provided.
ACCURACY IS CRITICAL. Use only facts from the seller's text or visible in the image. Omit rather than guess. Output the description text only.

TONE: Mirror the seller's language exactly. Never add adjectives, superlatives, or embellishments the seller did not use. Avoid subjective claims like "warm glow," "creates ambiance," "statement piece." Every sentence should add factual value.

LENGTH: Hard limits — minimum 400 characters, maximum 800 characters. Stop writing when you reach 800 characters. No fluff, no filler, no marketing hyperbole.

FORMAT: Establish fixture type, origin, and era early — but vary how you open. Complete sentences, third person.

SEO: Repeat the primary fixture type and key material naturally 2-3 times. The first 160 characters appear in search snippets — open with the most specific, keyword-rich sentence.

ATTRIBUTION — use exactly one:
By [Name] — documented (marks, labels, receipts)
Attributed to [Name] — strong likelihood, no documentation
In the style of [Name] — resembles design; also name the actual maker
Never use 'Attributed to' or 'In the style of' for: Gabriella Crespi, Piero Fornasetti, Vladimir Kagan, Serge Mouille, Gino Sarfatti.
For studio pieces: By [Designer] for [Manufacturer] (e.g., "By Achille Castiglioni for Flos").

CONTENT — prioritize in this order:
1. Materials — always specific ('patinated brass', 'mouth-blown opaline glass', 'spun aluminum', 'fabric shade')
2. Electrical — whether rewired, socket type, wattage rating, cord length
3. Condition — factual; note if original shade, hardware, or canopy is present or replaced
4. Period and country of manufacture — both, if present
5. Functional details — fixture type, number of bulbs, adjustability, mounting requirements, overall height, shade diameter
Designer/manufacturer: include, but do not lead with or build the description around.

CONDITION — use only: Distressed / Fair / Good / Excellent / New. Disclose any rewiring or restoration explicitly.

PLACEMENT: Include functional placement context only when factual (e.g., hardwired vs. plug-in, ceiling height required for pendant, UL-listed for damp locations). Never suggest mood or atmosphere.

VARIETY: Every description must read differently. Vary sentence structure, opening approach, and closing.

NEVER: 'Oriental' (use specific country), urgency language, collector superlatives (rare, important, museum quality), or lighting atmosphere claims ('warm,' 'cozy,' 'dramatic').""",
        "bullets": """Write 3 sales-optimized bullet points for this lighting item.

Rules:
• Each bullet must be 100 characters or less
• Lead with the most important material or maker fact
• Include electrical details (rewired, wattage, plug-in vs hardwired) if available
• Note if original shade or hardware is present
• Avoid superlatives and unverifiable claims

Tone: Concise, authoritative, written for high-end design collectors.

Output 3 bullet points only, one per line, starting with •"""
    },
    "Rugs": {
        "system": """You are a copywriter for 1stDibs, a luxury marketplace. Write a buyer-facing listing description for a rug item using the seller's text and product image provided.
ACCURACY IS CRITICAL. Use only facts from the seller's text or visible in the image. Omit rather than guess. Output the description text only.

TONE: Mirror the seller's language exactly. Never add adjectives, superlatives, or embellishments the seller did not use. Avoid subjective claims like "stunning," "vibrant," "rich color." Every sentence should add factual value.

LENGTH: Hard limits — minimum 400 characters, maximum 800 characters. Stop writing when you reach 800 characters. No fluff, no filler, no marketing hyperbole.

FORMAT: Establish rug type, origin, and era early — but vary how you open. Complete sentences, third person.

SEO: Repeat the primary rug type and region of origin naturally 2-3 times. The first 160 characters appear in search snippets — open with the most specific, keyword-rich sentence.

ATTRIBUTION: Rugs are attributed to region, workshop, or tribal group — not individual makers unless documented. Format as: [Region/Tribe] [type] (e.g., 'Tabriz carpet', 'Beni Ourain rug', 'Oushak runner'). If a workshop or designer is documented, use: By [Name].
Never use 'Oriental' — always use the specific country or region.

CONTENT — prioritize in this order:
1. Construction — always specific: hand-knotted, hand-woven, flat-weave, hooked, tufted; wool pile, silk pile, wool on cotton, etc.
2. Dimensions — always include if provided (width × length, pile height if noted)
3. Condition — factual: pile wear level and location, fringe condition, any repairs, reweaving, color restoration, or moth damage
4. Region and period — both, if present; note antique (100+ years) or vintage (20–99 years) where applicable
5. Design — pattern type (geometric, floral, medallion, tribal, pictorial), field color, border description
Knot count: include if provided — do not estimate.

CONDITION — use only: Distressed / Fair / Good / Excellent / New. Disclose any repairs, reweaving, or cleaning explicitly.

PLACEMENT: Include functional placement context only when factual (e.g., runner format, outdoor-rated, specific dimensions). Never suggest room styling.

VARIETY: Every description must read differently. Let the rug's most distinctive quality — construction, age, or provenance — determine how the description ends.

NEVER: 'Oriental' (use specific country or region), 'Primitive', urgency language, collector superlatives (rare, important, museum quality), or color superlatives ('vibrant,' 'rich,' 'jewel-toned').""",
        "bullets": """Write 3 sales-optimized bullet points for this rug.

Rules:
• Each bullet must be 100 characters or less
• Lead with construction type and region of origin
• Include dimensions if available
• Note condition specifics (pile wear, repairs, fringe)
• Avoid superlatives and color embellishments

Tone: Concise, authoritative, written for high-end design collectors.

Output 3 bullet points only, one per line, starting with •"""
    },
    "Jewelry": {
        "system": """You are a copywriter for 1stDibs, a luxury marketplace. Write a buyer-facing listing description for a jewelry item using the seller's text and product image provided.
ACCURACY IS CRITICAL. Use only facts from the seller's text or visible in the image. Omit rather than guess. Output the description text only.

TONE: Mirror the seller's language exactly. Avoid subjective claims. Every sentence should add factual value.

LENGTH: Hard limits — minimum 400 characters, maximum 800 characters. Stop writing when you reach 800 characters.

FORMAT: Establish item type, metal, and era early — but vary how you open. Complete sentences, third person.

SEO: Repeat the primary item descriptor and key material naturally 2-3 times. The first 160 characters appear in search snippets — open with the most specific, keyword-rich sentence.

ATTRIBUTION — use exactly one:
By [Name] — documented (hallmarks, receipts, labels)
Attributed to [Name] — strong likelihood, no documentation
In the style of [Name] — resembles design; also name the actual maker

CONTENT — prioritize in this order:
1. Metal and gemstones — always specific (18k yellow gold, VS1 diamond, natural Burma ruby)
2. Hallmarks, maker's marks, assay marks — if present
3. Condition — factual
4. Period and country of manufacture
5. Weight, dimensions, ring size if provided
6. Provenance or collection history if available

CONDITION — use only: Distressed / Fair / Good / Excellent / New.

NEVER: urgency language, collector superlatives (rare, important, museum quality), unverifiable claims about stone quality.""",
        "bullets": """Write 3 sales-optimized bullet points for this jewelry item.

Rules:
• Each bullet must be 100 characters or less
• Lead with the most important material or gemstone fact
• Include metal purity and gemstone specifics where available
• Note any hallmarks, maker's marks, or provenance
• Avoid superlatives and unverifiable claims

Tone: Concise, authoritative, written for high-end design collectors.

Output 3 bullet points only, one per line, starting with •"""
    },
    "Fine Art": {
        "system": """You are a copywriter for 1stDibs, a luxury marketplace. Write a buyer-facing listing description for a fine art item using the seller's text and product image provided.
ACCURACY IS CRITICAL. Use only facts from the seller's text or visible in the image. Omit rather than guess. Output the description text only.

LENGTH: Min 400 characters. For complex works with provenance, condition, and attribution all present, target 1,000+ characters. No filler.

FORMAT: Establish work type, medium, and attribution early — but vary how you open. Complete sentences, third person.

SEO: Repeat the primary descriptor and medium naturally 2-3 times. The first 160 characters appear in search snippets.

ATTRIBUTION — use exactly one:
By [Name] — documented
Attributed to [Name] — strong likelihood, research-based
Circle of [Name] — artist's influence, associated but not a student
In the Style of [Name] — stylistic resemblance, within 50 years
After [Name] — authorized or posthumous reproduction

CONTENT priority: 1. Provenance 2. Medium 3. Condition 4. Artist 5. Dimensions/framing 6. Period/century

CONDITION — use only: Distressed / Fair / Good / Excellent / New.

NEVER: urgency language, collector superlatives (rare, important, museum quality)""",
        "bullets": """Write 3 sales-optimized bullet points for this artwork.

Rules:
• Each bullet must be 100 characters or less
• Lead with the most compelling provenance or attribution fact
• Include medium and dimensions if available
• Note exhibition history or publication if present
• Avoid superlatives and unverifiable claims

Tone: Concise, authoritative, written for high-end design collectors.

Output 3 bullet points only, one per line, starting with •"""
    },
    "Fashion": {
        "system": """You are a copywriter for 1stDibs, a luxury marketplace. Write a buyer-facing listing description for a fashion item using the seller's text and product image provided.
ACCURACY IS CRITICAL. Use only facts from the seller's text or visible in the image. Omit rather than guess. Output the description text only.

LENGTH: Hard limits — minimum 400 characters, maximum 800 characters. Stop writing when you reach 800 characters.

FORMAT: Establish item type, brand, and era early — but vary how you open. Complete sentences, third person.

SEO: Repeat the primary item descriptor and brand naturally 2-3 times. The first 160 characters appear in search snippets.

ATTRIBUTION: Fashion uses only 'By [Brand]' format. For designer-era pieces: 'By [Brand], designed by [Designer]' (e.g., 'By Gucci, designed by Tom Ford'). No 'Attributed to' or 'In the Style of'.

CONTENT — prioritize in this order:
1. Materials — always specific (100% cashmere, vegetable-tanned leather, silk charmeuse)
2. Condition — factual description of actual wear
3. Brand and designer era
4. Size and measurements
5. Hardware, lining, closures

CONDITION — use only: New / Excellent / Good / Fair (no Distressed for fashion).

NEVER: urgency language, collector superlatives, unverifiable condition claims.""",
        "bullets": """Write 3 sales-optimized bullet points for this fashion item.

Rules:
• Each bullet must be 100 characters or less
• Lead with the most important material or brand fact
• Include size, measurements, or fit notes if available
• Note hardware, lining, or construction details
• Avoid superlatives and unverifiable claims

Tone: Concise, authoritative, written for high-end design collectors.

Output 3 bullet points only, one per line, starting with •"""
    }
}

# ── Session state init ─────────────────────────────────────────────────────────

if "saved_prompts" not in st.session_state:
    st.session_state.saved_prompts = dict(DEFAULT_PROMPTS)

if "active_prompt_name" not in st.session_state:
    st.session_state.active_prompt_name = "Furniture"

if "system_prompt" not in st.session_state:
    st.session_state.system_prompt = DEFAULT_PROMPTS["Furniture"]["system"]

if "bullet_prompt" not in st.session_state:
    st.session_state.bullet_prompt = DEFAULT_PROMPTS["Furniture"]["bullets"]

# ── API key: prefer Streamlit secret, fall back to sidebar input ───────────────

def get_api_key():
    """Return API key from Streamlit secrets if set, otherwise None."""
    try:
        return st.secrets["ANTHROPIC_API_KEY"]
    except Exception:
        return None

secret_api_key = get_api_key()

# ── Header ─────────────────────────────────────────────────────────────────────

st.title("🪑 1stDibs Prompt Tester")
st.caption("Test AI description prompts against real item data.")

# ── Sidebar ────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.header("Configuration")

    if secret_api_key:
        api_key = secret_api_key
        st.success("✓ API key loaded from settings")
    else:
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

    num_rows = st.slider("Rows to test", min_value=1, max_value=100, value=5)
    workers = st.slider("Parallel workers", min_value=1, max_value=10, value=5)

    st.divider()

    # ── Saved prompts ──────────────────────────────────────────────────────────
    st.subheader("Saved Prompts")

    prompt_names = list(st.session_state.saved_prompts.keys())
    selected = st.selectbox(
        "Load a prompt",
        options=prompt_names,
        index=prompt_names.index(st.session_state.active_prompt_name)
        if st.session_state.active_prompt_name in prompt_names else 0
    )

    if st.button("Load", use_container_width=True):
        st.session_state.active_prompt_name = selected
        st.session_state.system_prompt = st.session_state.saved_prompts[selected]["system"]
        st.session_state.bullet_prompt = st.session_state.saved_prompts[selected].get("bullets", "")
        st.rerun()

    st.divider()

    new_prompt_name = st.text_input("Save current prompt as...",
                                    placeholder="e.g. Lighting, Rugs, Seating")
    if st.button("Save prompt", use_container_width=True):
        if new_prompt_name.strip():
            st.session_state.saved_prompts[new_prompt_name.strip()] = {
                "system": st.session_state.system_prompt,
                "bullets": st.session_state.bullet_prompt
            }
            st.session_state.active_prompt_name = new_prompt_name.strip()
            st.success(f"Saved '{new_prompt_name.strip()}'")
        else:
            st.warning("Enter a name first.")

    st.divider()

    st.caption("Export / import prompts")
    prompts_json = json.dumps(st.session_state.saved_prompts, indent=2)
    st.download_button(
        "⬇ Export prompts JSON",
        data=prompts_json,
        file_name="prompts.json",
        mime="application/json",
        use_container_width=True
    )

    uploaded_prompts = st.file_uploader("Import prompts JSON", type=["json"],
                                        label_visibility="collapsed")
    if uploaded_prompts:
        try:
            imported = json.loads(uploaded_prompts.read().decode("utf-8"))
            st.session_state.saved_prompts.update(imported)
            st.success(f"Imported {len(imported)} prompts.")
        except Exception as e:
            st.error(f"Failed to import: {e}")

# ── Main tabs ──────────────────────────────────────────────────────────────────

tab1, tab2 = st.tabs(["📝 Prompts", "📂 Data"])

with tab1:
    st.subheader(f"System Prompt — {st.session_state.active_prompt_name}")
    system_prompt = st.text_area(
        "System prompt",
        value=st.session_state.system_prompt,
        height=350,
        label_visibility="collapsed",
        key="system_prompt_input"
    )
    st.session_state.system_prompt = system_prompt
    st.caption(f"{len(system_prompt)} characters")

    st.divider()

    enable_bullets = st.toggle("Enable bullet point prompt", value=True)

    if enable_bullets:
        st.subheader("Bullet Point Prompt")
        bullet_prompt = st.text_area(
            "Bullet prompt",
            value=st.session_state.bullet_prompt,
            height=250,
            label_visibility="collapsed",
            key="bullet_prompt_input"
        )
        st.session_state.bullet_prompt = bullet_prompt
        st.caption(f"{len(bullet_prompt)} characters")

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

            st.markdown("**Select columns to include as context in the prompt:**")
            default_exclude = ["ITEM_IMAGE"]
            default_include = [c for c in fieldnames if c not in default_exclude]
            included_cols = st.multiselect(
                "Columns",
                options=fieldnames,
                default=default_include,
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
    lines = []
    for col in cols:
        val = row.get(col, "").strip()
        if val:
            lines.append(f"{col}: {val}")
    return "\n".join(lines)


def generate_row(client, row, sys_prompt, bullet_prompt_text, use_bullets,
                 inc_cols, img_col, mdl):
    notes = []
    key = row.get("NATURAL_KEY", list(row.values())[0] if row else "unknown")

    raw_context = build_item_context(row, inc_cols)

    cleaned_context = raw_context
    for trigger in BOILERPLATE_TRIGGERS:
        if trigger.lower() in raw_context.lower():
            lines = raw_context.split("\n")
            filtered = [l for l in lines if trigger.lower() not in l.lower()]
            cleaned_context = "\n".join(filtered)
            notes.append(f"Boilerplate stripped ('{trigger}')")
            break

    if not cleaned_context.strip():
        notes.append("No usable item data in selected columns")

    img_b64, img_meta = None, None
    if img_col and img_col != "(none)":
        img_url = row.get(img_col, "")
        img_b64, img_meta = fetch_image_as_base64(img_url)
        if img_b64 is None:
            notes.append(f"Image unavailable: {img_meta}")

    def build_content(prompt_text):
        content = []
        if img_b64:
            content.append({
                "type": "image",
                "source": {"type": "base64", "media_type": img_meta, "data": img_b64}
            })
        content.append({
            "type": "text",
            "text": f"{prompt_text}\n\nITEM DATA:\n{cleaned_context or '(no data provided)'}"
        })
        return content

    def call_api(sys, user_content, max_tok=220):
        for attempt in range(3):
            try:
                resp = client.messages.create(
                    model=mdl,
                    max_tokens=max_tok,
                    system=sys,
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

    new_description = call_api(
        sys_prompt,
        build_content("Write a listing description for this 1stDibs item. Stay between 400 and 800 characters total."),
        max_tok=220
    )

    char_len = len(new_description)
    if new_description and char_len < 400:
        notes.append(f"Below 400-char minimum ({char_len} chars)")
    if char_len > 800:
        notes.append(f"Exceeds 800-char maximum ({char_len} chars)")

    new_bullets = ""
    if use_bullets and bullet_prompt_text.strip():
        new_bullets = call_api(
            "You are a luxury product copywriter for 1stDibs. Output only the bullet points requested.",
            build_content(bullet_prompt_text),
            max_tok=150
        )

    result = dict(row)
    result["NEW_DESCRIPTION"] = new_description
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
elif not system_prompt:
    st.warning("Add a system prompt in the Prompts tab.")
elif not rows:
    st.warning("Upload a CSV in the Data tab.")
elif not included_cols:
    st.warning("Select at least one column to include as context.")

if run_clicked and can_run:
    client = anthropic.Anthropic(api_key=api_key)
    test_rows = rows[:num_rows]
    use_bullets = enable_bullets and st.session_state.bullet_prompt.strip()

    st.subheader(f"Running on {len(test_rows)} rows...")
    progress_bar = st.progress(0)
    status_text = st.empty()

    results = [None] * len(test_rows)
    lock = threading.Lock()
    done = [0]

    def process(idx_row):
        idx, row = idx_row
        return idx, generate_row(
            client, row,
            st.session_state.system_prompt,
            st.session_state.bullet_prompt,
            use_bullets,
            included_cols,
            image_col,
            model
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

    out_fields = list(fieldnames) + ["NEW_DESCRIPTION", "CHAR_COUNT"]
    if use_bullets:
        out_fields.append("BULLET_POINTS")
    out_fields.append("NOTES")

    out_buffer = io.StringIO()
    writer = csv.DictWriter(out_buffer, fieldnames=out_fields, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(results)

    st.download_button(
        "⬇ Download results CSV",
        data=out_buffer.getvalue(),
        file_name="prompt_test_results.csv",
        mime="text/csv",
        use_container_width=True
    )
