# 1stDibs Prompt Tester

A simple web app for testing AI description prompts against real item data.

## Deploy to Streamlit Community Cloud (free)

### Step 1 — Push to GitHub
1. Create a new **private** GitHub repo (e.g. `1stdibs-prompt-tester`)
2. Upload the three files in this folder: `app.py`, `requirements.txt`, `README.md`

### Step 2 — Deploy on Streamlit Cloud
1. Go to [share.streamlit.io](https://share.streamlit.io) and sign in with GitHub
2. Click **New app**
3. Select your repo → branch `main` → main file `app.py`
4. Click **Deploy**

### Step 3 — Add your API key as a secret
1. In your deployed app, click **⋮ → Settings → Secrets**
2. Add:
```
ANTHROPIC_API_KEY = "sk-ant-your-key-here"
```
3. The app will read it automatically (you won't need to paste it in the UI each time)

That's it — you'll get a shareable URL like `https://your-app.streamlit.app` that anyone on your team can use.

## How to use the app

1. Paste your system prompt in the left panel
2. Upload a CSV with columns: `NATURAL_KEY`, `ITEM_IMAGE`, `ITEM_DESCRIPTION`
3. Set the number of rows to test and model in the sidebar
4. Click **Run Prompt**
5. Review results side-by-side, then download the output CSV
