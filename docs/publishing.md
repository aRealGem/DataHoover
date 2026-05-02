# Publishing dashboards and PDFs (ExpressionPi)

This repo can publish **static artifacts** to a web root on your LAN (for example **ExpressionPi** at `expressionpi.home.arpa`). Two PDF paths matter:

| Output | Tool | Notes |
|--------|------|--------|
| Cursor **`.canvas.tsx`** → PDF | [`scripts/canvas-pdf/`](../scripts/canvas-pdf/) (`canvas-pdf`) | Uses Cursor’s bundled canvas runtime + Playwright. |
| **Sentiment HTML** (`build_sentiment_dashboard.py`) → PDF | [`scripts/canvas-pdf/bin/html-pdf.mjs`](../scripts/canvas-pdf/bin/html-pdf.mjs) (`html-pdf`) | Headless Chromium; waits for Plotly charts from CDN. |
| End-to-end sentiment → Pi | [`scripts/publish_sentiment_to_expressionpi.py`](../scripts/publish_sentiment_to_expressionpi.py) | Builds HTML + PDF + merged index + `rsync`. |

Planned **`hoover publish`** (canvas manifest + lane badges) is described elsewhere in kanban; sentiment publishing shipped first as the script above.

---

## One-time: Playwright (canvas-pdf and html-pdf)

```bash
cd scripts/canvas-pdf
npm install
npx playwright install chromium
```

---

## Sentiment dashboard PDF

Build HTML from DuckDB (output defaults to `data/dashboard/sentiment.html`, gitignored):

```bash
python scripts/build_sentiment_dashboard.py
```

Render PDF only:

```bash
node scripts/canvas-pdf/bin/html-pdf.mjs \
  data/dashboard/sentiment.html \
  -o data/dashboard/sentiment.pdf \
  --width 1280
```

---

## Publish sentiment to ExpressionPi

From repo root (adjust remote user/host/path):

```bash
python scripts/publish_sentiment_to_expressionpi.py \
  --remote jackie@expressionpi.home.arpa \
  --remote-path /var/www/datahoover/
```

- Writes **`data/published/<UTC-date>/sentiment-dashboard.pdf`** and a small **`index.html`** in that folder.
- Writes **`data/published/index.html`** rollup listing:
  - **Canvas & analytics PDFs** — from repo-root **`published_rollup.toml`** (`[[manual]]` rows: `title` + `href`).
  - **Sentiment dashboards** — auto-listed dated folders under `data/published/`.

**Dry-run** (no `rsync`):

```bash
python scripts/publish_sentiment_to_expressionpi.py --dry-run
```

**Flags:**

| Flag | Meaning |
|------|---------|
| `--rollup-config PATH` | Alternate TOML for manual PDF links (default `published_rollup.toml`). |
| `--no-sync-root-index` | Do not upload `data/published/index.html`; only sync the dated folder. |
| `--delete-remote-day` | Pass **`rsync --delete`** for the dated folder only — makes the remote day folder an exact mirror of local files (**removes** extra PDFs there). **Default is off** so sibling canvas PDFs are not deleted. |

---

## `published_rollup.toml`

Keep **`[[manual]]`** entries for canvas exports so the rollup index continues to link **Iran**, **sharp run-up**, or any other PDFs whose paths on the server are stable:

```toml
[[manual]]
title = "Sharp run-up: bull market signal"
href = "./2026-05-02/sharp-runup-bull-market-signal.pdf"

[[manual]]
title = "Iran war: market impact"
href = "./2026-05-02/iran-war-market-impact.pdf"
```

Paths are **relative to** the rollup `index.html` on the server (same origin as `/var/www/datahoover/index.html`). Edit titles/hrefs after you run `find /var/www/datahoover -name '*.pdf'` on the Pi if needed.

---

## Recovering from accidental deletes

If the dated-folder sync previously used **`rsync --delete`**, extra PDFs in that remote folder could have been removed. Restore copies from backup or re-run **`canvas-pdf`** on the original `.canvas.tsx` files, upload them to the matching dated directory, then re-sync **`index.html`** (or run the publish script again).

---

## Gitignored output

- `data/dashboard/`
- `data/published/`

Do not commit generated PDFs or rollup trees; **`published_rollup.toml`** (manual links) **is** committed.
