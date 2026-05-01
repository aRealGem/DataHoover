# Publishing pipeline

`hoover publish` renders Cursor `.canvas.tsx` dashboards to dated PDFs,
generates a static index page that splits them into commercial-safe and
personal-use lanes, and rsyncs the bundle to ExpressionPi as a peer site
beside the existing DokuWiki.

The MVP target is a public **PDFs + index** site. A first-class HTML React
canvas SPA (live interactive dashboards) is out of scope here — see the
[backlog](kanban/backlog.md) card for Phase 5.

## Pipeline at a glance

```
publications.toml          ┐
sources.toml + catalogs    ┤→ datahoover.publish.run_publish ──→ data/published/<DATE>/*.pdf
.canvas.tsx files          ┘                                  ──→ data/published/index.html
                                                              ──→ rsync → expressionpi.home.arpa
```

Stage by stage:

1. **Load `publications.toml`.** Each block declares one publication: a
   name, a canvas filename, a title, a one-line summary, and the list of
   `[[sources]]` it draws from.
2. **Load `sources.toml` (+ auto-merged `catalogs.toml`).** Every source has
   a `license` and `redistribute` tag (enforced by
   [`tests/test_sources_contract.py`](../tests/test_sources_contract.py)).
3. **Validate.** Every source name listed in `publications.toml` must be a
   real source. Unknown names abort with a non-zero exit (no half-publish).
4. **Render PDFs.** For each publication, invoke
   [`scripts/canvas-pdf/bin/canvas-pdf.mjs`](../scripts/canvas-pdf/) on the
   canvas file with `--single-page`, writing to
   `data/published/<DATE>/<name>.pdf`.
5. **Compute lane.** [`datahoover.lane.lane_for_publication`](../src/datahoover/lane.py)
   takes the worst-case `redistribute` value across all sources used by the
   publication. See "Lane semantics" below.
6. **Render attribution.** [`datahoover.lane.attribution_block`](../src/datahoover/lane.py)
   builds the per-source license + redistribute list shown under each entry
   in the index.
7. **Write `index.html`.** Plain HTML5, no JS, no framework — sectioned by
   lane (commercial-safe first, then personal-use).
8. **Upload.** `rsync -av --delete data/published/ <remote>:<remote-path>/`.
   Skipped on `--dry-run`.

## Quickstart

```bash
# Local development & verification (no upload)
hoover publish --dry-run

# Live publish (run from a regular Terminal so rsync can reach the LAN)
hoover publish --remote pi@expressionpi.home.arpa \
               --remote-path /var/www/datahoover/
```

`hoover publish --help` lists every flag.

## Lane semantics

Lanes mirror the rules in [`docs/licensing.md`](licensing.md). A publication's
lane is the **worst case** across the sources it draws from:

| `redistribute` value | Lane | Reasoning |
|----------------------|------|-----------|
| `public-domain` | commercial-safe | No restrictions. |
| `with-attribution` | commercial-safe | CC-BY style; we credit sources in the attribution footer. |
| `share-alike` | commercial-safe | CC-BY-SA / ODbL; derived data may inherit the licence. |
| `non-commercial` | personal-use | CC-BY-NC explicitly excludes commercial redistribution. |
| `display-only` | personal-use | Vendor permits showing values but not bulk redistribution (Twelve Data, etc.). |
| `per-package` | personal-use | Conservative default for catalog endpoints whose member datasets each carry their own licence. |
| `no` | personal-use | Explicit prohibition. |
| `None` / unknown | personal-use | Conservative default. |

So a publication that mixes `twelvedata_watchlist_daily` (`display-only`)
with `eia_petroleum_wpsr_weekly` (`public-domain`) lands in **personal-use**
— one display-only source contaminates the lane.

If you want a commercial-safe demo, build a canvas that draws **only** from
sources where `redistribute ∈ {public-domain, with-attribution, share-alike}`.
Good candidates: USGS, FEMA, NWS, EIA, BLS, Census, World Bank, Eurostat.

## Adding a new publication

1. Create the canvas in Cursor — it'll land at
   `~/.cursor/projects/<dashed-repo-path>/canvases/<name>.canvas.tsx`.
2. Append a block to [`publications.toml`](../publications.toml):
   ```toml
   [[publications]]
   name = "my-new-canvas"
   canvas = "my-new-canvas.canvas.tsx"
   title = "Display title for the index page"
   summary = "One-line description of the dashboard."
   sources = ["fred_macro_watchlist", "twelvedata_watchlist_daily"]
   ```
3. Run `hoover publish --dry-run` to verify the canvas renders and the lane
   is what you expect, then run live without `--dry-run`.

The `name` becomes the PDF filename and the link slug. Keep it kebab-case
and stable (it forms part of public URLs).

## Where canvases live

Cursor stores per-project canvases under
`~/.cursor/projects/<dashed-absolute-path>/canvases/`. The dashed path is the
absolute project path with `/` replaced by `-` and the leading `/` stripped.
For DataHoover that's
`~/.cursor/projects/Users-jackiemartindale-CodeProjects-DataHoover/canvases/`.

[`datahoover.publish.default_canvases_dir`](../src/datahoover/publish.py)
computes this automatically and walks out of `.claude/worktrees/<name>` paths
when invoked from a Claude Code worktree, so you can run `hoover publish`
from either the main checkout or a worktree without thinking about it.

Override with `--canvases-dir` if you keep canvases elsewhere.

## Watermarking — current state

The MVP attribution lives **in the index page**, not stamped onto each PDF.
This was a deliberate scope decision: per-PDF watermarking via `pypdf`/`pikepdf`
doesn't change the lane logic and would require either modifying every canvas
template or adding a Python PDF-manipulation dependency. Tracked as a kanban
card.

## ExpressionPi prerequisites

Out of scope for the publish PR but blocking the first live run:

- **SSH key.** Currently password-only on `pi@expressionpi.home.arpa`. Run
  `ssh-copy-id pi@expressionpi.home.arpa` once so rsync (and any future
  cron) can run non-interactively.
- **Webserver docroot.** The Pi has nginx or Apache running alongside
  DokuWiki. Pick a directory for the publish target (e.g.
  `/var/www/datahoover/`) and pass it as `--remote-path`. There is no default.
- **DNS.** `expressionpi.home.arpa` resolves to `192.168.7.57` on the LAN.
  From off-LAN you'll need a Cloudflare tunnel or VPN — also a kanban card.

## Cron / scheduling

The MVP is **on-demand only**. The user runs `hoover publish` when they want
to push fresh content. Once SSH keys are set up, a `systemd --user` timer or
launchd plist on the Mac (or a cron on the Pi pulling from this repo) becomes
trivial to add — see the kanban card for "Cron/systemd timer for `hoover
publish`".

## Failure modes & exit codes

| Code | Meaning |
|------|---------|
| `0` | Success. |
| `1` | `publications.toml` is empty or has no `[[publications]]`. |
| `2` | A publication references a source name not in `sources.toml` / `catalogs.toml`. |
| `3` | A canvas file referenced in `publications.toml` was not found in `--canvases-dir`. |
| `4` | Not `--dry-run`, but `--remote` and/or `--remote-path` were not provided. |

`canvas-pdf` and `rsync` are invoked with `check=True`, so their non-zero
exits propagate as `subprocess.CalledProcessError` (which surfaces as a
non-zero `hoover` exit code).

## What does **not** happen

- **The publish script does not refresh data.** Per design (Q8 in the
  planning conversation), warehouse data is the user's prerequisite — run
  `hoover ingest-* && hoover compute-signals` first if your canvases pull
  fresh metrics via [`scripts/canvas_market_snapshot.py`](../scripts/canvas_market_snapshot.py).
- **The publish script does not modify canvas files.** Whatever values
  appear in the `.canvas.tsx` at render time appear in the PDF.
- **The publish script does not touch DokuWiki.** It writes to a peer path,
  not to the wiki tree. A backlog card tracks adding a one-line wiki page
  that backlinks to the index.

## Related

- [`src/datahoover/lane.py`](../src/datahoover/lane.py) — lane resolver + attribution.
- [`src/datahoover/publish.py`](../src/datahoover/publish.py) — pipeline.
- [`scripts/publish_to_expressionpi.py`](../scripts/publish_to_expressionpi.py) — thin shim, callable without `pip install -e .`.
- [`scripts/canvas-pdf/`](../scripts/canvas-pdf/) — Node renderer (Cursor canvas-runtime + Playwright).
- [`docs/licensing.md`](licensing.md) — full license + redistribute tag schema.
- [`docs/kanban/backlog.md`](kanban/backlog.md) — deferred follow-ups.
