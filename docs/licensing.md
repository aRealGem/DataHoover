# Licensing & redistribution

Every `[[sources]]` block in [`sources.toml`](../sources.toml) and
[`catalogs.toml`](../catalogs.toml) carries two required fields:

- **`license`** — short SPDX-style identifier from
  [`LICENSE_TAGS`](../src/datahoover/sources.py).
- **`redistribute`** — operational answer to "can the raw rows or signals
  derived from them be republished?", from
  [`REDISTRIBUTE_TAGS`](../src/datahoover/sources.py).

The contract test
[`tests/test_sources_contract.py::test_every_source_declares_license_and_redistribute`](../tests/test_sources_contract.py)
fails if either is missing or uses an unknown value, so a new connector cannot
be added without choosing a tag.

## `redistribute` value semantics

| Value | Meaning |
|-------|---------|
| `public-domain` | No restrictions. Republish freely (no attribution required). |
| `with-attribution` | CC-BY-style. Republish freely if the source is credited. |
| `share-alike` | CC-BY-SA / ODbL. Derived data inherits the same licence. |
| `non-commercial` | CC-BY-NC / CC-BY-NC-SA. Not eligible for commercial products. |
| `display-only` | Vendor permits showing values to users but not bulk redistribution of the raw series (typical of paid market-data feeds). |
| `per-package` | Depends on the underlying record (CKAN-style catalogs whose member datasets each carry their own licence). |
| `no` | Explicit prohibition. Ingest for internal use only. |

## Lanes for downstream consumers

When building a commercial product (paid API, monetised dashboard, etc.), only
include sources whose `redistribute` is one of:

- `public-domain`
- `with-attribution`
- `share-alike` *(if the product itself can adopt the same licence)*

Treat **everything else as personal / research-only**. The dashboard, canvas
PDFs, and any signal feeds that mix lanes should annotate which lane each
piece came from so the contamination is visible.

## Per-source table

The table below reflects the values currently in `sources.toml` and
`catalogs.toml`. **Verify before publishing** — vendor terms change, and a
few entries below are conservative interpretations of ambiguous wording
(noted in the "Notes" column).

| Source | License | Redistribute | Notes |
|--------|---------|--------------|-------|
| `usgs_all_day` | `PD-USGov` | `public-domain` | US federal work (17 USC §105). |
| `usgs_catalog_m45_day` | `PD-USGov` | `public-domain` | US federal work. |
| `eurostat_gdp` | `eu-commission-reuse` | `with-attribution` | Commission Decision 2011/833/EU; commercial reuse permitted with attribution. |
| `openfema_disaster_declarations` | `PD-USGov` | `public-domain` | FEMA. |
| `nws_alerts_active` | `PD-USGov` | `public-domain` | NOAA / NWS. |
| `gdacs_alerts` | `proprietary-gdacs` | `with-attribution` | UN/EC joint initiative; site invites reuse but no formal CC licence — treat as attribution-required. |
| `worldbank_gdp_usa` | `CC-BY-4.0` | `with-attribution` | World Bank Open Data Terms of Use. |
| `worldbank_macro_fiscal` | `CC-BY-4.0` | `with-attribution` | Same. |
| `gdelt_democracy_24h` | `CC-BY-NC-SA-4.0` | `non-commercial` | GDELT 2.0 explicit terms. **Excludes commercial reuse**. Feeds `_gdelt_tone_signals` (pipeline #9). |
| `gdelt_gkg_15min` | `CC-BY-NC-SA-4.0` | `non-commercial` | GDELT 2.0 GKG (V2Tone, V2Themes, V2Persons/Locations/Organizations). **Excludes commercial reuse**. Feeds `_gdelt_tone_signals`. |
| `ooni_us_recent` | `CC-BY-NC-SA-4.0` | `non-commercial` | OONI metadata. **Verify** — OONI's site has carried both NC-SA and SA in different revisions; conservative call. |
| `caida_ioda_recent` | `proprietary-caida` | `non-commercial` | CAIDA AUP: research use; redistribution requires permission. |
| `ripe_ris_live_10s` | `CC-BY-SA-4.0` | `share-alike` | RIPE NCC measurement data licence. |
| `ripe_atlas_probes` | `CC-BY-SA-4.0` | `share-alike` | RIPE NCC measurement data licence. |
| `twelvedata_watchlist_daily` | `proprietary-twelvedata` | `display-only` | Paid vendor; ToS prohibits redistribution of raw bars. Derived signals (e.g. `market_move`) need careful treatment if commercialised. |
| `fred_macro_watchlist` | `mixed-fred` | `with-attribution` | FRED redistributes from many primary sources. **Most macro series are PD**, but `SP500`/`DJIA` (S&P / Dow Jones) carry the index providers' own restrictions. Treat the watchlist as attribution-required at the aggregate level and audit per-series before commercial use. |
| `fred_commodity_monthly` | `mixed-fred` | `with-attribution` | IMF + BLS PPI; mostly attribution-friendly but verify per-series. |
| `fred_crypto_fx` | `mixed-fred` | `with-attribution` | Coinbase reference rates have Coinbase ToS; treat as display-only for any commercial product. |
| `eia_petroleum_wpsr_weekly` | `PD-USGov` | `public-domain` | DOE / EIA. |
| `bls_truthbot_watchlist` | `PD-USGov` | `public-domain` | BLS. |
| `census_acs_state_basic` | `PD-USGov` | `public-domain` | Census. |
| `fred_sentiment_indicators` | `mixed-fred` | `with-attribution` | STLFSI4 (St. Louis Fed Financial Stress), NFCI (Chicago Fed National Financial Conditions), CSCICP03USM665S (OECD US Consumer Confidence), BSCICP03USM665S (OECD US Business Confidence), CIVPART (BLS Labor Force Participation). All ultimately PD US gov't or attribution-friendly OECD; safe for the commercial lane with attribution. |
| `alternative_me_fng_daily` | `proprietary-altme` | `with-attribution` | Alternative.me publishes the index publicly without explicit redistribution terms. **Verify** before commercial use; treat as attribution-required. |
| `cnn_fear_greed_daily` | `proprietary-cnn` | `non-commercial` | Backing endpoint for CNN's public dataviz; no published reuse terms. **Treat as personal/research-only**. |
| `reddit_sentiment_subs` | `proprietary-reddit` | `display-only` | Reddit ToS permits display of post data with attribution but restricts redistribution of raw posts. Derived signals (post-volume, score-weighted sentiment) face fewer restrictions. **Personal/research lane** until you've reviewed the Reddit User Agreement for your specific publication context. |
| `stocktwits_watchlist` | `proprietary-stocktwits` | `display-only` | StockTwits ToS allows display with attribution; bulk redistribution restricted. Pre-labeled Bullish/Bearish sentiment is the headline value here. **Personal/research lane**. |
| `fed_press_releases_rss` | `PD-USGov` | `public-domain` | Federal Reserve press releases. Per-feed license — other RSS sources added via the `generic_rss` connector each carry their own license tag. |
| `datagov_catalog_climate` | `per-package` | `per-package` | Catalog metadata; each member dataset has its own licence (use the `license_id` column on `ckan_packages`). |
| `hdx_catalog_cholera` | `per-package` | `per-package` | OCHA HDX; mostly CC-BY-IGO 4.0 but verify per-package. |
| `socrata_example` | `nyc-open-data` | `with-attribution` | NYC Open Data Terms of Use. |
| `opendatasoft_example` | `CC-BY-4.0` | `with-attribution` | Geonames. |

## When you add a new source

1. Look up the publisher's terms of use / data licence page.
2. Pick the closest tag from `LICENSE_TAGS`. If none fits, add a new
   `proprietary-<vendor>` or open-licence tag to the frozenset in
   `src/datahoover/sources.py` and document it here.
3. Pick the most restrictive `redistribute` value that still matches the terms
   — this is the safe default.
4. If the licence is uncertain, add a row to this file with a "**Verify**"
   note, the same way the OONI and FRED rows are marked.

## Known FRED series that are *not* fully redistributable

These live inside `mixed-fred` watchlists and need per-series care if you
publish a commercial derivative:

- `SP500`, `DJIA` — S&P Dow Jones Indices terms.
- `CBBTCUSD`, `CBETHUSD`, `CBXMRUSD` — Coinbase reference rates.

The rest of the FRED series in `fred_macro_watchlist` (FX cross-rates from the
Federal Reserve Board, VIX, treasury spreads, oil/gas, U. Michigan consumer
sentiment, trade-weighted dollar) are PD or attribution-only.
