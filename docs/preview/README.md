# Preview HTML — committed for fast iteration

The file [`sentiment-dashboard-preview.html`](sentiment-dashboard-preview.html)
is a **rendered output** of [`scripts/build_sentiment_dashboard.py`](../../scripts/build_sentiment_dashboard.py),
generated against an empty warehouse so every panel uses its **synthetic seed**.
It exists so you can preview the dashboard layout in Cursor / a browser
without first running the ingest pipeline. Open it directly:

```
docs/preview/sentiment-dashboard-preview.html
```

Each panel header carries a chip indicating whether the data is `live: <source>`
or `SYNTHETIC SEED` so it's never ambiguous what you're looking at. In this
checked-in copy every chip says SYNTHETIC SEED.

## Refresh from real data

On a box with network egress (the API endpoints for alt.me, CNN, Reddit,
GDELT, etc. require it):

```bash
# 1) Ingest at least one no-auth source you care about. Examples:
hoover ingest-altme-fng --source alternative_me_fng_daily
hoover ingest-cnn-fg     --source cnn_fear_greed_daily
hoover ingest-reddit     --source reddit_sentiment_subs
hoover ingest-stocktwits --source stocktwits_watchlist
hoover ingest-gdelt      --source gdelt_democracy_24h
hoover compute-signals   --since 7d   # populates the sentiment_tone signals panel

# 2) Regenerate the dashboard. The script reads `data/warehouse.duckdb` by default;
#    panels with real rows render real values, the rest fall back to synthetic.
python scripts/build_sentiment_dashboard.py

# 3) Open the live output (gitignored, regenerated each run):
open data/dashboard/sentiment.html      # macOS
xdg-open data/dashboard/sentiment.html  # Linux
```

## Why this file is committed when `data/dashboard/` is gitignored

`data/dashboard/` is correctly ignored — its contents are derived artifacts
that vary per machine and per run. This `docs/preview/` copy is a deliberate
exception: a single, intentionally-synthetic snapshot used as a UI screenshot.
It's regenerated only when the dashboard layout itself changes, and any
synthetic data it contains is clearly marked as such.
