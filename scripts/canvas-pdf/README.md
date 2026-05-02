# canvas-pdf

Render any Cursor `.canvas.tsx` file to a PDF that matches what the IDE shows in
its Canvas pane. This works by reusing Cursor's own canvas runtime (the same
`canvas-runtime.esm.js` shipped inside `Cursor.app`), so output is byte-for-byte
the same renderer the IDE uses — no reimplementation of the SDK.

## Install

```bash
cd scripts/canvas-pdf
npm install
npx playwright install chromium
```

Node 20+ is required.

## Usage

```bash
# Defaults: writes <basename>.pdf next to the .canvas.tsx file
npx canvas-pdf path/to/foo.canvas.tsx

# Custom output path
npx canvas-pdf path/to/foo.canvas.tsx -o ~/Desktop/foo.pdf

# A4 paper instead of Letter
npx canvas-pdf path/to/foo.canvas.tsx --paper A4

# Render in dark theme (default is light, easier to print/share)
npx canvas-pdf path/to/foo.canvas.tsx --theme dark

# Wider viewport (default 1024px) — affects layout that depends on width
npx canvas-pdf path/to/foo.canvas.tsx --width 1280

# Single tall page (avoids cross-page section breaks; good for sharing)
npx canvas-pdf path/to/foo.canvas.tsx --single-page

# Override the Cursor runtime location (Linux/Windows, or non-default install)
npx canvas-pdf foo.canvas.tsx --runtime /path/to/canvas-runtime.esm.js
# or via env:
CURSOR_CANVAS_RUNTIME=/path/to/canvas-runtime.esm.js npx canvas-pdf foo.canvas.tsx
```

## html-pdf (static HTML dashboards)

The companion `html-pdf` command renders an arbitrary `.html` file to a **single tall PDF**
(headless Chromium). It targets dashboards that pull Plotly from a CDN (same Playwright
install as above).

```bash
cd scripts/canvas-pdf
node bin/html-pdf.mjs ../../docs/preview/sentiment-dashboard-preview.html -o /tmp/sentiment.pdf --width 1280
```

To build **live** sentiment HTML from DuckDB, render PDF, and rsync to ExpressionPi (same
layout as other `data/published/<YYYY-MM-DD>/` drops):

```bash
python scripts/publish_sentiment_to_expressionpi.py \
  --remote pi@expressionpi.home.arpa \
  --remote-path /var/www/datahoover/
```

Use `--dry-run` to generate `data/published/<date>/` locally without `rsync`.

**Roll-up index:** add your canvas PDF links to repo-root `published_rollup.toml` as `[[manual]]`
entries (`title` + `href`). They are merged into `data/published/index.html` next to sentiment dates so a sentiment publish does not remove Iran / run-up PDFs from the listing.

Use `--no-sync-root-index` if you want to rsync only the dated sentiment folder and leave the Pi's existing `index.html` unchanged.

The dated-folder rsync **does not** pass `--delete` by default (so canvas PDFs sitting next to `sentiment-dashboard.pdf` on the Pi are not removed). Only use `--delete-remote-day` if you want an exact mirror.

## How it works

1. esbuild bundles your `.canvas.tsx` with the **classic** JSX transform
   (`React.createElement`), externalizing `cursor/canvas`, `react`, and
   `react/jsx-runtime`.
2. A tiny localhost HTTP server serves four assets in-memory:
   - `/runtime.js` — Cursor's `canvas-runtime.esm.js`, served verbatim
   - `/sdk-bridge.mjs` — re-exports every `cursor/canvas` name from
     `globalThis` (the runtime puts the entire SDK there before importing
     your module)
   - `/canvas.bundle.mjs` — the esbuild output
   - `/index.html` — a harness with an import map redirecting
     `cursor/canvas` to `/sdk-bridge.mjs`
3. Headless Chromium (Playwright) loads the harness, waits for the canvas to
   mount, and calls `page.pdf()`.

## Caveats

- **macOS-only by default.** Runtime path is auto-detected from
  `/Applications/Cursor.app`; override with `--runtime` or
  `CURSOR_CANVAS_RUNTIME` on other platforms.
- **Tied to Cursor's bundle layout.** A future Cursor release could move the
  runtime or change `mountCanvas`. The tool fails fast with a clear error if
  the runtime file is missing.
- **`useCanvasState` does not persist.** There is no host to write back the
  `.canvas.data.json` sidecar — that's fine for one-shot PDF rendering, but
  any state changes in the canvas at render time are lost.
- **`useCanvasAction` is a no-op.** Buttons that dispatch IDE actions like
  `openAgent` will render but not do anything when clicked in the PDF
  (PDFs are static anyway).
- **Paginated output may split sections.** Use `--single-page` for a single
  tall page that won't break a Card across pages.
