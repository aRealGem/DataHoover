# canvases

Point-in-time snapshots of the Cursor `.canvas.tsx` dashboards that get rendered to PDF and published to ExpressionPi via [scripts/canvas-pdf/](../scripts/canvas-pdf/).

## What lives here

| File | Purpose | First published |
|------|---------|-----------------|
| [iran-war-market-impact.canvas.tsx](iran-war-market-impact.canvas.tsx) | Original Iran war market-impact dashboard | 2026-05-02 |
| [iran-war-market-impact-2026-06-28.canvas.tsx](iran-war-market-impact-2026-06-28.canvas.tsx) | Refresh with post-Hormuz-MoU data, IEA-OMR-anchored cover rows, "Changes since 2026-04-29" callout | 2026-06-28 |
| [sharp-runup-bull-market-signal.canvas.tsx](sharp-runup-bull-market-signal.canvas.tsx) | Sharp-runup / bull-market-signal canvas | 2026-05-02 |
| [tsconfig.json](tsconfig.json) | Per-canvas-dir TS config (used by Cursor's canvas runtime). Sets `jsx: "react-jsx"`. |  |

## Cursor vs. repo

The interactive canvases that Cursor renders in the Canvas pane live in `~/.cursor/projects/Users-jackiemartindale-CodeProjects-DataHoover/canvases/`. **Those** are the source-of-truth for live editing.

The copies here are **point-in-time snapshots** for version control, diff review, and non-Cursor tooling (e.g. re-rendering the PDF from a headless environment). If you edit a canvas interactively in Cursor and want the snapshot updated, copy the file back here and commit.

## Rendering to PDF

From the repo root:

```bash
node scripts/canvas-pdf/bin/canvas-pdf.mjs \
  canvases/iran-war-market-impact-2026-06-28.canvas.tsx \
  -o data/published/2026-06-28/iran-war-market-impact-2026-06-28.pdf \
  --width 1280 --single-page
```

[scripts/canvas-pdf/src/compileCanvas.ts](../scripts/canvas-pdf/src/compileCanvas.ts) passes `tsconfigRaw: "{}"` so esbuild ignores the sibling `tsconfig.json` and honors the script's explicit classic-JSX settings; the tsconfig here exists purely so the Cursor IDE renders the canvases correctly when opened.

## Publishing

Full publish workflow (canvas -> PDF -> ExpressionPi rsync -> rollup `index.html`) is documented in [docs/publishing.md](../docs/publishing.md). The manual PDF entries in [published_rollup.toml](../published_rollup.toml) are what gets listed on the ExpressionPi index.
