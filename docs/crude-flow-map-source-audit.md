# Crude Flow + Refining Margin Map — Source Audit & Implementation Plan

**Scope:** free/public sources only.
**Verification sweep:** 2026-09-15 (~30 live fetches against source documentation).
**Corrections + computed coverage:** 2026-09-16.
**Rule applied throughout:** every factual claim about a source was checked against that
source's current documentation or queried directly. Nothing below is from recall unless
it is explicitly labelled as such.

---

## Corrections log

This report supersedes two claims made earlier in the research session. Both were wrong
and both are corrected in place below.

1. **"No free global refinery database exists."** Wrong. Generalised from one checked
   dataset (Global Energy Monitor's Global Oil Infrastructure Tracker, which really is
   midstream pipelines only) to the whole category. **Climate TRACE publishes 728
   refineries across 110 countries with per-asset capacity, throughput and coordinates,
   free, CC BY 4.0.** Re-verified by direct API query 2026-09-16. This flips Layer 2 from
   "not buildable" to "buildable globally at asset level".

2. **"India's PPAC publishes Petrol (FOB) and Diesel (FOB) international prices in $/bbl,
   giving a complete free Arab Gulf crack spread."** Not supported. Verified 2026-09-16:
   the PPAC international-prices page is *titled* "International Prices of Crude Oil
   (Indian Basket), Petrol and Diesel", but its data endpoint and its anonymous workbook
   serve **the crude leg only**, and **only the current fiscal year**. The historical
   report is behind a login. Detail in Data Need 3. This moves India out of the
   "rigorous free crack" tier and materially lowers the Layer 3 coverage number.

---

## Could not verify — stated as unknown, not guessed

- **Energy Institute licence terms.** `energyinst.org/statistical-review/about` and
  `/resources-and-data-downloads` both return **HTTP 403** to automated fetch. Files
  download fine in a browser; the licence text is unconfirmed. Open the workbook and
  read its own copyright page before shipping.
- **JODI terms of use.** `jodidata.org/about-jodi/terms-of-use.aspx` returns **404**;
  `/about-jodi/` returns **403**. No licence statement located anywhere on the site.
- **OPEC ASB bulk file inventory.** `publications.opec.org/asb/Download` requires account
  registration; the inventory behind the login was not inspected.
- **EIA international API product/activity facet IDs.** Route metadata confirms the facet
  *names*; the numeric IDs were not enumerated. **Treat any specific product ID as
  UNVERIFIED and derive it from live metadata at build time.**
- **PPAC petrol/diesel international series.** Named in the page title, absent from the
  anonymous data endpoint and workbook. Whether the login-gated historical report
  contains them is **untested** — it needs a free PPAC account, which is a decision for
  the project owner, not something to create unasked.

**One retirement confirmed:** the EIA **APIv1 query browser is retired** (notice on
`eia.gov/opendata/browser/`). v2 only.

---

## Part 1 — Source Availability Audit

### Data Need 1: Crude flows, origin → destination

| Publisher / Dataset | Access | Geo granularity | Temporal + lag | Auth | Format |
|---|---|---|---|---|---|
| **UN Comtrade** — HS 2709 / 2710 | Documented API + premium bulk | Country → **partner country** (true bilateral) | Annual + monthly; 3–12 mo lag, uneven | Free key, 500 calls/day | JSON, CSV |
| **JODI Oil** — World (secondary) + Primary DB | Bulk file | Country totals **only** (no partner) | Monthly; ~2 mo lag | None | CSV, .ivt |
| **Eurostat** `nrg_ti_oilm` | Bulk + API | EU reporter → **partner country** | Monthly; ~3 mo lag | None | TSV, CSV, JSON-stat |
| **EIA** Company Level Imports (EIA-814) | Bulk XLSX | Origin country → **US port → US refinery**, by grade | Monthly; ~2 mo lag | None | XLSX / XLS |
| **EIA** International (API v2 `/international/`) | API | Country totals **only** | Annual + monthly, 1949– | Free key | JSON, XML |
| **IEA** MODS Trade | **Paid** | OECD importer → 90+ origins | Monthly | Subscription | — |
| **IMF PortWatch** | ArcGIS REST API + CSV | **Per port**, not OD pairs | Daily; weekly refresh | None | CSV, GeoJSON, JSON |
| **Canada CER** crude exports by destination | Bulk CSV | Canada → destination | Monthly + annual | None | CSV |
| **China GACC** | HTML / query portal | China ← origin country | Monthly | None | HTML, XLS |
| **Brazil ANP** imports/exports | Bulk CSV | Brazil ↔ partner | Monthly; ~1 mo lag | None | CSV |
| **India PPAC** | **PDF-only** | India ← origin country | Monthly | None | PDF |
| **OPEC ASB** | PDF + gated interactive | Country, exports by destination region | Annual | Registration | PDF, XLSX |
| **Kpler / Vortexa / ClipperData** | **Commercial** | Vessel-level, true OD, incl. STS | Real-time | Paid | API, SDK |

**URLs, licences, and gaps**

- **UN Comtrade** — API portal `https://comtradedeveloper.un.org/`; data
  `https://comtradeplus.un.org/`. Free tier: **500 calls/day**. Premium raises per-batch
  records to 250,000 and unlocks bulk/async download.
  **Licensing is the sharp edge:** per the
  [re-dissemination FAQ](https://uncomtrade.org/docs/faqs-on-use-and-re-dissemination/),
  redistributing *original* Comtrade records to non-subscribers above **100,000 records**
  triggers a "license to distribute" fee, minimum 1× Premium Pro subscription — with
  **no profit/non-profit distinction**. *Transformed* data (new indicators, cleaning,
  estimation, gap-filling, aggregation, joins to external sources) is explicitly **not**
  subject to copyright restriction, and visualisation-only display is free.
  **An aggregated flow map is transformed data and lands on the free side — but do not
  ship a raw-record export endpoint.**
  Gaps: China has withheld partner detail in some periods; Iran / Venezuela / Russia
  flows are systematically under-reported by the reporter and must be recovered from
  mirror (importer-side) statistics.
- **JODI** — downloads at `https://www.jodidata.org/oil/database/data-downloads.aspx`.
  `world_ext.zip` covers **Jan 2009 onward**; annual CSVs 2002–2026 under
  `annual-csv/primary/[year].csv` and `annual-csv/secondary/[year].csv`. Latest cycle:
  **20 Aug 2026, 52 countries reporting June 2026**. Carries a colour-code column
  (1=blue, 2=yellow, 3=white) encoding assessment status — **use it as a quality filter,
  don't drop it**. Gaps: coverage is voluntary and incomplete; several major exporters
  report late or not at all; **no partner dimension at all** — JODI tells you *how much*
  a country exported, never *to whom*.
- **Eurostat `nrg_ti_oilm`** —
  [databrowser](https://ec.europa.eu/eurostat/databrowser/view/nrg_ti_oilm/default/table).
  Coverage **2008-01 → 2026-03**, 23,370,404 cells, last updated **2026-06-11**
  (≈3-month lag). Annual sibling `nrg_ti_oil`; supply/transformation `nrg_cb_oilm`.
  Bulk download + REST API. Reuse permitted under the Commission's standard reuse policy
  with attribution. **The single best free bilateral monthly source — but only where one
  side of the pair is an EU reporter.**
- **EIA Company Level Imports** — `https://www.eia.gov/petroleum/imports/companylevel/`.
  Form EIA-814, **1987 → Jun 2026**. June 2026 released **31 Aug 2026**; next
  **30 Sep 2026** → ~2-month lag. Example file:
  `/petroleum/imports/companylevel/archive/2026/2026_06/data/import.xlsx`.
  US Government work, public domain. **The only free source giving origin country →
  named destination refinery with grade / API / sulfur.** Gap: US-inbound only.
- **EIA API v2** — `https://api.eia.gov/v2/`, version **2.1.12 (March 2026)**, docs
  `https://www.eia.gov/opendata/documentation.php`. Free key, **must be in the URL, not
  headers**. Hard cap **5,000 rows JSON / 300 rows XML** per request. Throttling exists
  but thresholds are undocumented; exceeding it temporarily auto-suspends the key.
  The `/international/` route's facets are **Product, Activity, Country/Region,
  Country/Region Type, Data Flag ID, Unit** — **there is no partner facet**, which is the
  structural proof that EIA international cannot give you bilateral flows.
- **IMF PortWatch** — `https://portwatch.imf.org/`. Source: **UN Global Platform AIS**,
  ~90,000 ships, **2,065 ports** + 28 chokepoints, from **2019-01-01**, refreshed
  **weekly, Tuesdays 09:00 ET**. ArcGIS endpoint:
  `https://services9.arcgis.com/weJ1QsnbMYJlCHdG/ArcGIS/rest/services/Daily_Ports_Data/FeatureServer/0/query`.
  Fields include `portcalls_tanker`, `import_tanker`, `export_tanker`, `capacity_tanker`
  (metric tons). Site content **CC BY-SA 4.0**, code MIT.
  **Critical limitation: per-port totals, not origin-destination pairs, and "tanker" is
  not split crude vs. product vs. chemical.** It can corroborate a port's throughput
  trend; it can never draw an arrow.
- **Canada CER** —
  `https://www.cer-rec.gc.ca/open/imports-exports/crude-oil-exports-by-destination-monthly.csv`
  (and `-annual.csv`); catalogued at
  [open.canada.ca](https://open.canada.ca/data/en/dataset/0b7bf4b3-423a-45d0-a92b-e69be0b81ce4)
  under the Open Government Licence – Canada. CER's own
  [reconciliation page](https://www.cer-rec.gc.ca/en/data-analysis/energy-commodities/crude-oil-petroleum-products/report/canadian-crude-oil-exports-30-year-review/reconciling-data.html)
  documents that CER and StatCan export figures disagree — expect a real discrepancy.
- **India PPAC** — `https://ppac.gov.in/`. Country-wise crude import data appears in the
  monthly "Snapshot of India's Oil & Gas Data" **PDF**. No machine-readable country-wise
  import file was locatable. For the world's #3 crude importer this means **PDF
  extraction, or substitution by Comtrade mirror data.**
- **Free AIS alternatives** — NOAA/BOEM MarineCadastre
  `https://marinecadastre.gov/accessais/` is genuinely free raw AIS from **2009**, but is
  limited to the **US EEZ and ~40–50 miles from coast**, with no coverage of foreign
  waters. Useless for global flows.

---

### Data Need 2: Refinery markets — capacity & utilization

| Publisher / Dataset | Access | Geo granularity | Temporal + lag | Auth | Format |
|---|---|---|---|---|---|
| **Climate TRACE** — oil-and-gas-refining assets | Documented REST API | **Individual refinery, global**, with lat/lon | Annual (activity year); rolling | None | JSON |
| **EIA** Refinery Capacity Report (EIA-820) | Bulk XLSX + PDF | **Individual refinery**, state, PADD (US only) | Annual as-of 1 Jan; ~6 mo lag | None | XLSX, PDF |
| **EIA** Weekly Petroleum Status Report | Bulk CSV/XLS | US + PADD | Weekly; **6-day lag** | None | CSV, XLS, PDF |
| **EIA** International (API v2) | API | Country | Annual, mostly 1980– | Free key | JSON |
| **Energy Institute** Statistical Review | Bulk XLSX | Country / region | Annual; ~6 mo lag | None | XLSX |
| **EDF OGIM** (Zenodo v2.7) | Bulk download | Individual refinery (location only) | Irregular | None | GeoPackage / CSV |
| **Eurostat** `nrg_cb_oilm` | Bulk + API | EU country | Monthly; ~3 mo lag | None | TSV, JSON-stat |
| **JODI** refinery intake | Bulk CSV | Country | Monthly; ~2 mo lag | None | CSV |
| **Wikipedia** refinery lists | HTML scrape | Individual refinery + coords | Irregular | None | HTML |

**URLs, licences, and gaps**

- **Climate TRACE** — `https://api.climatetrace.org/v6/assets?subsectors=oil-and-gas-refining`.
  **CC BY 4.0**, no key, no registration. **Re-verified by direct query 2026-09-16:**

  - **728 refinery assets across 110 countries**
  - **Capacity populated on all 728** (units `BBL per day`); **35 are zero-valued**, so
    treat 693 as the usable capacity population
  - **Activity populated on 679** (units `BBL`, annual throughput)
  - **`Centroid` lat/lon present on all 728**
  - Also carries `Owners` (company name + ID), `AssetType` (e.g. "Deep Conversion"),
    `Confidence` flags per field per year, and `SectorRanks`

  **Global capacity sum: 104.64 mb/d.** Cross-check against OPEC ASB's published
  ~103.80 mb/d world total for 2024 → a **0.8% overshoot**, which is definitional noise
  (condensate splitters, vintage differences), not a coverage gap. Effectively complete.

  Spot checks reproduced exactly: Jamnagar **1,369,000 bbl/d**, Zhejiang Rongsheng
  **800,000 bbl/d**. Utilization falls straight out of the two fields —
  Jamnagar `480,590,314 BBL ÷ (1,369,000 × 365)` = **96.2%**.

  Two caveats to carry into the UI:
  - **Per-field confidence is often "low".** Jamnagar's own `capacity` confidence is
    flagged `low`. Surface the flag; don't present these as survey-grade.
  - **Nameplate ≠ operable.** PDVSA Paraguaná appears at 955,000 bbl/d nameplate; actual
    Venezuelan throughput is a fraction of that. The `Activity`-derived utilization is
    what carries the truth, and where `Activity` is null you have capacity with no
    reality check.

  **Sector-specific licence restriction:** the same API **redacts** `Capacity` and
  `Activity` as `"license restricted"` for oil-and-gas **production** assets. Refining is
  open; upstream is not. Don't assume the whole sector is free.
- **EIA Refinery Capacity Report** — `https://www.eia.gov/petroleum/refinerycapacity/`.
  Current edition **as-of 2026-01-01, published 2026-06-26**; next June 2027. File
  `refcap26.xlsx` plus `table1.pdf`–`table13.pdf`. Covers operating *and idle* refineries
  plus those under construction; fields include **atmospheric crude distillation
  capacity**, downstream charge/production capacities, fuel/electricity/steam
  consumption, and crude receipt method by transport mode. Public domain.
  **Gap: United States only, and the ~6-month lag means mid-year closures are invisible
  until the next June.** Use it as the high-resolution US overlay on Climate TRACE, and
  as the reconciliation benchmark for the US subtotal.
- **EIA WPSR** — `https://www.eia.gov/petroleum/supply/weekly/`. Table 2 "U.S. Inputs and
  Production by PAD District", Table 9 weekly estimates; CSV/XLS/PDF via
  `ir.eia.gov/wpsr/`. Released **2026-09-10 for week ending 2026-09-04** → **6-day lag**.
  Gross inputs, operable capacity and **percent utilization** at PADD level, weekly — the
  freshest utilization signal available free anywhere. *The WPSR page does not itself
  confirm API exposure; verify the specific route in v2 metadata before wiring it.*
- **EIA International** — `https://www.eia.gov/international/data/world`; API route
  `/v2/international/`. Country-level capacity, most series back to **1980**, time range
  Jan 1949 → May 2026. **UNVERIFIED: the exact product/activity IDs for refinery
  capacity — enumerate from live route metadata, do not hardcode.**
- **Energy Institute Statistical Review** —
  `https://www.energyinst.org/statistical-review/resources-and-data-downloads`.
  **2026 = 75th edition**; historical series from **1965**; both a multi-sheet
  human-readable XLSX and a consolidated panel-format dataset. Free to access
  ("global public good"). **Licence text unverified — 403 on fetch.** Note OWID's ETL
  flags data-quality issues in the consolidated panel file; prefer the main workbook
  where they disagree.
- **EDF OGIM** (Zenodo, CC BY 4.0, v2.7) — has a `Crude Oil Refineries` category with
  name / operator / status / country. Confirmed from its schema that it carries **no
  capacity field**. Good for cross-checking Climate TRACE's *asset list*; useless as a
  capacity source.
- **Wikipedia refinery lists** — e.g. `https://en.wikipedia.org/wiki/List_of_oil_refineries`.
  **CC BY-SA 4.0 — viral.** Incorporating it into your dataset arguably obliges you to
  share-alike the dataset. Use strictly as a **cross-check and geocoding hint, never as a
  value source.**
- **Verified negative:** Global Energy Monitor's
  [Global Oil Infrastructure Tracker](https://globalenergymonitor.org/projects/global-oil-infrastructure-tracker)
  covers **midstream pipelines only**. There is no GEM refinery tracker.

---

### Data Need 3: Crack spreads & price propagation

| Publisher / Dataset | Access | Geo granularity | Temporal + lag | Auth | Format |
|---|---|---|---|---|---|
| **EIA** Spot Prices | Bulk XLS + API v2 | US hubs + Brent | Daily/wkly/mo/ann; 3–5 day | None / free key | XLS, JSON |
| **FRED** (mirrors EIA) | Documented API | Same hubs | Daily | Free key | JSON, CSV |
| **EC** Weekly Oil Bulletin | Bulk XLSX | EU member states | Weekly; **1-day lag** | None | XLSX |
| **EIA** retail gasoline/diesel | API v2 | US + PADD + select cities | Weekly (Mon) | Free key | JSON |
| **India PPAC** international crude | JSON endpoint + XLSX | India (Indian Basket FOB) | Monthly; **current FY only** anonymously | None (history needs login) | JSON, XLSX |
| **ICE / CME** settlements | Web / licensed API | Global benchmarks | Daily | **Licensed** | — |
| **Argus / Platts** (Rotterdam, Singapore) | **Commercial** | Global product hubs | Daily | Paid | — |

**URLs, licences, and gaps**

- **EIA Spot Prices** — `https://www.eia.gov/dnav/pet/pet_pri_spt_s1_d.htm`. Verified
  series and start years: WTI Cushing **1986**, Brent Europe **1987**, conventional
  gasoline NY Harbor & US Gulf Coast **1986**, RBOB Los Angeles **2003**, No. 2 heating
  oil NYH **1986**, ULSD NYH & USGC **2006**, ULSD Los Angeles **1996**, kerosene jet
  USGC **1990**, propane Mont Belvieu **1992**. Daily/weekly/monthly/annual; weekly+ are
  **unweighted averages of daily closes** (matters for reconciliation). Public domain.
  **This one source carries the entire rigorous free crack-spread layer.**
- **FRED** — `https://fred.stlouisfed.org/`. Verified series: `DCOILWTICO`,
  `DCOILBRENTEU`, `DGASNYH`, `DDFUELNYH`. These are **EIA data re-served**, not
  independent. Convenient API ergonomics, zero extra information —
  **don't treat FRED/EIA agreement as validation.**
- **EC Weekly Oil Bulletin** —
  `https://energy.ec.europa.eu/data-and-analysis/weekly-oil-bulletin_en`. Euro-super 95
  and diesel, **with and without tax**, all EU member states, XLSX, from **2005**,
  submitted Wednesday / published Thursday — a **1-day lag**, the fastest series in this
  audit. **Critical caveat: these are consumer/retail prices, not refinery-gate or
  wholesale barge quotes.** They cannot form a European crack spread; they are usable
  only as the *downstream* leg of a pass-through analysis.
- **India PPAC — corrected 2026-09-16.** The page
  [International Prices of Crude Oil](https://ppac.gov.in/prices/international-prices-of-crude-oil)
  is titled "International Prices of Crude Oil (Indian Basket), Petrol and Diesel".
  What is actually served anonymously:
  - JSON endpoint `POST https://ppac.gov.in/AjaxController/getInternationalPricesCrudeOil`
    with `financialYear=2026-2027&reportBy=4&pageId=30` returns **the crude leg only** —
    Indian Basket FOB in $/bbl, monthly, current fiscal year. Probing `pageId` 28–33
    returns empty; there is no sibling endpoint serving products.
  - The "Download Current Report" button (no login) yields
    `Crude_PP_1_a_InternationalPrice(C)_<date>.xlsx`. Its live sheet is
    `CRUDE PRICE (CURRENT)` — crude only, current FY. The other four sheets
    (`RSP(Current)1`, `DC(History)`, `UR(FAQ)`, `Subsidy(History)1`) are **stale template
    boilerplate** — retail prices as-of 2010, under-recoveries 2002-03 to 2009-10. Do not
    wire them to anything.
  - The **"Download Historical Report" button is `reportDownloadWithLogin`** and opens a
    sign-in modal. **PPAC history requires a free account.** Anonymous depth is the
    current fiscal year only — as of today, six monthly points (Apr–Sep 2026).
  - `getReportBy` offers exactly one option: `($/bbl.)`.

  **Net:** PPAC gives you a free, government-published **crude** benchmark for the Indian
  basket. It does **not** anonymously give you the refined-product FOB legs, and without
  those there is no crack spread. Whether the login-gated historical report contains the
  product series is untested and would need an account.
- **ICE / CME** — CME publishes a
  [Derived Data License Fee schedule effective 1 Jan 2026](https://www.cmegroup.com/market-data/files/2026-derived-data-fees.pdf)
  and requires a distribution agreement to redistribute.
  **Displaying CME/ICE settlement values in a public web app is a licensed activity.
  Do not scrape it.**
- **Counter-example worth knowing:** the Australian Institute of Petroleum publishes
  Singapore MOGAS95 and Gasoil 10ppm benchmarks — but **charts only, Argus data under
  licence, attribution required**. Look-don't-touch. The rule is not "transparency makes
  everything free"; it is **government republication is generally reusable,
  industry-body republication generally is not** — and each channel's terms need reading
  individually.
- **Hard gap:** Rotterdam barge and Singapore cargo product assessments — the inputs to
  European and Asian crack spreads — are **Argus and Platts proprietary assessments with
  no free equivalent.** This is not an access inconvenience; the prices do not exist in
  public form.

---

## Part 2 — Feasibility Verdict

### Layer 1 — Crude flows origin → destination: **PARTIALLY BUILDABLE**

The bilateral matrix is buildable at **annual resolution from UN Comtrade HS 2709**, and
at **monthly resolution only for EU-reporter pairs via Eurostat `nrg_ti_oilm`** and for
**US-inbound pairs via EIA Company Level Imports**. Everything else is country totals:
JODI gives monthly export volumes with no partner, EIA's international route has no
partner facet at all, and IEA's bilateral product (MODS Trade) is paywalled. What is
**not buildable free** is a current, global, monthly OD matrix — and the specific pairs
that matter most to an economic-signal dashboard (Russian crude to India and China,
Iranian crude to China, Venezuelan flows) are exactly the ones where reporter-side
customs data is absent, falsified, or laundered through ship-to-ship transfers that only
vessel-level tracking resolves.

**Coverage, sized rather than asserted.** Against a denominator of **~42 mb/d of seaborne
crude exports** (Bloomberg tanker tracking, September 2025; EIA's chokepoints analysis
independently puts total seaborne *petroleum* trade at **79.8 mb/d in 1H2025, 76% of
104.4 mb/d global supply**), the sanctioned tranche is roughly: Russia ~4.7 mb/d, Iran
~1.6 mb/d (of which ~1.38 mb/d to China in 2025), Venezuela ~0.9 mb/d — **~7.2 mb/d, or
about 17%.** Kpler independently reports China alone imported **≥2.6 mb/d of sanctioned
crude in 2025, 22% of its total imports.** So **~83% of seaborne crude trade is
attributable from free bilateral sources at annual resolution**, which corroborates the
earlier 80–85% estimate — but note these tonnages come from commercial vessel-tracking
journalism, not from a free statistical source, so the denominator itself is a
second-hand number.

The honest approximation is **Comtrade annual, gap-filled with importer-side mirror
statistics where the exporter is silent, at a 6–14 month lag.** The error is substantial
and asymmetric: mirror reconstruction recovers *volume* but attributes it to the last
declared shipper rather than the wellhead, so sanctioned-origin barrels get systematically
mis-assigned to intermediary countries. Vessel-level truth is **strictly commercial** —
Kpler and Vortexa build cargo-by-cargo from AIS plus port agent and bill-of-lading data.
IMF PortWatch is genuinely free and AIS-derived but gives per-port tanker tonnage with
**no origin-destination pairing and no crude/product split**.

### Layer 2 — Refining markets as sized nodes: **BUILDABLE FROM PUBLIC DATA**

This is the strongest layer of the three and it is buildable **globally, at individual
asset level, for free, today, from a single API call.** Climate TRACE gives 728
refineries across 110 countries with nameplate capacity in bbl/d, annual throughput in
bbl, and lat/lon centroids, under CC BY 4.0 with no key and no registration. The capacity
sum — **104.64 mb/d against OPEC's ~103.80 mb/d world total, a 0.8% overshoot** — means
coverage is effectively complete, not sampled. Utilization is a direct quotient of two
supplied fields, so the "inbound crude vs. local throughput capacity" comparison the view
is built around works out of the box.

The United States then gets a high-resolution overlay for free on top: EIA-820 for
refinery-level detail beyond distillation capacity, and WPSR for **weekly** PADD-level
utilization at a 6-day lag, versus Climate TRACE's annual vintage. No commercial feed is
needed anywhere in this layer.

Two honest limits. **Per-field confidence flags are frequently "low"** — even Jamnagar's
capacity is flagged low — so this is a good global dataset, not a survey-grade one, and
the UI should surface the flag. And **nameplate is not operable capacity**: 35 assets
carry zero capacity, 49 carry no throughput at all, and idle or sanctioned plants
(Paraguaná at 955,000 bbl/d nameplate) will overstate a market unless the
`Activity`-derived utilization is shown alongside.

### Layer 3 — Crack spreads & price propagation: **PARTIALLY BUILDABLE — and thinner than it first looked**

A rigorous, free, redistributable crack spread exists for **the United States only**. EIA
spot prices give daily crude and refined-product quotes at NY Harbor, US Gulf Coast and
Los Angeles with 20–40 years of history, public domain, so a true 3:2:1 is computable at
three US hubs. Everywhere else you are approximating, and the approximation degrades as
you move east.

The reason is structural rather than bureaucratic, and it is worth being precise about it,
because the intuition that market democracies must publish their prices is half right.
Platts and Argus sell the *assessment business* — methodology, real-time feed, licensing —
and governments that regulate fuel pricing really do republish numbers to justify domestic
price decisions. But what they republish is almost always **retail or crude**, not the
**wholesale refined-product quote** that the crack spread actually needs. The EC Weekly
Oil Bulletin is the cleanest case: superb coverage, 1-day lag, all EU states, with and
without tax — and useless as a crack numerator because it is a pump price. India's PPAC
is the case that looked like an exception and, on verification, is not: it publishes the
Indian Basket **crude** FOB freely, and the product legs named in its own page title are
not served anonymously. Rotterdam barge and Singapore cargo assessments have no free
equivalent at all.

**Coverage, computed against the Climate TRACE capacity base (104.64 mb/d):**

| Tier | Basis | Capacity | Share |
|---|---|---|---|
| **A — rigorous free crack** | Free daily/weekly *wholesale* quotes for crude **and** products → US only (EIA spot; NYH / USGC / LA 3:2:1) | 18.55 mb/d | **17.7%** |
| **B — defensible approximation** | Some free public price leg exists (gov ex-tax retail, official crude basket, public rack) → EU-27 12.24, India 5.18, Korea 3.29, Japan 3.23, Brazil 2.23, Mexico 1.98, Canada 1.95, UK 1.22, Thailand 1.24, Taiwan 1.14, Singapore 1.12, others | 39.53 mb/d | **37.8%** |
| **C — no free crack** | Administered prices, sanctioned, or no public price publication → China 18.14, Russia 6.81, Saudi 3.27, Iran 2.24, Kuwait 1.42, Iraq 1.31, Venezuela 1.29, UAE 1.25, Nigeria 1.14, … | 46.56 mb/d | **44.5%** |
| | **A+B cumulative** | 58.08 mb/d | **55.5%** |

So: **~18% rigorous, ~56% cumulative with stated error bars, ~44% dark.** This is
materially worse than the ~23% / ~64% / ~36% split estimated before the PPAC correction,
for two reasons — India moved from tier A to tier B once its product legs proved
unavailable, and the tier rule here is applied explicitly by country rather than
eyeballed. **China alone is 17.3% of world refining capacity and its product prices are
administered by the NDRC, not market-cleared — it is not a data-access problem, and no
amount of source-hunting will fix it.**

---

## Part 3 — Implementation Plan

### Build sequence — cheapest useful layer first

| # | Deliverable | Sources | Effort | Why this order |
|---|---|---|---|---|
| **1** | **Refinery nodes, global** | Climate TRACE (1 API call) | **~0.5 day** | One unauthenticated call yields 728 geocoded assets with capacity + utilization. Highest value per unit of work in the entire project; it is a complete map layer on its own. |
| **2** | **US crack spreads** | EIA spot via API v2 | **~1 day** | One keyed source, four to six series, a three-line formula. Gives a genuinely rigorous margin layer for 17.7% of world capacity. |
| **3** | **US weekly utilization overlay** | EIA WPSR + EIA-820 | **~1 day** | Upgrades US nodes from annual to 6-day-lag weekly, and gives the EIA-820 vs Climate TRACE reconciliation check. |
| **4** | **Flows — annual global** | Comtrade HS 2709 + mirror fill | **~3–4 days** | The expensive one: rate limits, mirror reconciliation, M49↔ISO3 mapping, and the licensing care described above. |
| **5** | **Flows — monthly partial** | Eurostat `nrg_ti_oilm` + EIA-814 | **~2 days** | Monthly resolution for EU-reporter and US-inbound pairs only. Layer it *over* the annual matrix; do not blend the two into one number. |
| **6** | **Tier-B approximated margins** | EC Weekly Oil Bulletin, PPAC crude | **~2 days** | Do this last and label it loudly. It is a pass-through indicator, not a crack spread. |

Total ≈ **10–12 working days** to all six. Steps 1–3 alone (≈2.5 days) produce a
genuinely useful dashboard view.

### Pipeline: fetch → normalize → join → cache

**Fetch.** One module per source, each emitting raw payloads to an append-only store with
the retrieval timestamp and the request that produced it. Never transform on fetch — this
mirrors the L1/L2/L3 discipline already used by the fiscal-sustainability collector in
[architecture.md](architecture.md) pipeline #10, and it means a definition change is a
re-derive, not a re-fetch.

**Normalize.** Two hard conversions, both of which must happen exactly once and be
recorded in provenance:
- **Mass → volume.** JODI and PortWatch report metric tons; everything else is barrels.
  Conversion is **grade-dependent** (roughly 7.0–7.6 bbl/t for crude). Do not use a single
  global constant silently — pick 7.33 bbl/t as the default, record it as a named
  assumption per value, and flag any series where it materially moves the result.
- **$/gal → $/bbl.** EIA product spot prices are per gallon. Multiply by **42**.

**Join keys.**
- **Countries: ISO 3166-1 alpha-3** as the canonical key. Climate TRACE already emits
  alpha-3 (`IND`, `USA`, `CHN`) — verified. **Comtrade uses M49 numeric** and needs a
  mapping table. **EIA uses its own country codes** and needs another. Eurostat uses
  ISO-2 with EU-specific extensions (`EU27_2020`, `XK` for Kosovo). Budget real time for
  this; it is the single most common source of silent wrong answers.
- **Refineries: no global standard ID exists.** Use Climate TRACE `Id` as the primary key
  and carry `NativeId` alongside. EIA-820 has its own refinery ID that does **not** join
  cleanly — match US assets on name + state + operator with a manual override table, and
  expect to hand-resolve a few dozen. **Flag this as the weakest join in the system.**
- **Ports: IMF PortWatch `portid`.** Self-consistent, but does not join to refineries
  except geographically. If you need port→refinery association, do it by distance with a
  manual override list, and treat it as a hint, not a fact.

**Cache.** Cache by `(source, endpoint, params, retrieved_at)`. TTL should match the
source's own publication cadence, never shorter — there is no point re-fetching Climate
TRACE hourly when it updates annually.

### Data contract

Three top-level collections. Every numeric value carries its own provenance envelope so
the UI can show lineage on hover.

**Values in the example below:** the `refinery_markets` record is a **real, verified**
Climate TRACE record (asset 3143912, queried 2026-09-16). The `flows` and `crack_spreads`
records are **illustrative shapes with plausible but unverified numbers** — they show the
contract, not measured values.

**Centroid gotcha:** Climate TRACE returns `"Centroid": {"Geometry": [lon, lat], "SRID": 4326}`
— **longitude first**, GeoJSON-style. Normalise to explicit `lat`/`lon` keys at ingest;
silently passing the array through is how a refinery ends up in the wrong hemisphere.

```json
{
  "schema_version": "1.0.0",
  "generated_at": "2026-09-16T12:00:00Z",

  "provenance_defs": {
    "ct-v6-2026-09-16": {
      "source": "Climate TRACE v6 assets API",
      "url": "https://api.climatetrace.org/v6/assets?subsectors=oil-and-gas-refining",
      "licence": "CC BY 4.0",
      "retrieved_at": "2026-09-16T12:41:00Z",
      "original_units": "BBL per day",
      "transformation": "none"
    },
    "eia-spot-2026-09-16": {
      "source": "EIA Petroleum Spot Prices",
      "url": "https://api.eia.gov/v2/petroleum/pri/spt/data/",
      "licence": "public domain (US Government work)",
      "retrieved_at": "2026-09-16T12:42:00Z",
      "original_units": "USD per gallon",
      "transformation": "x42 -> USD per barrel"
    }
  },

  "refinery_markets": [
    {
      "asset_id": "ct:3143912",
      "native_id": "406",
      "name": "RPL (Reliance Petroleum Limited) Jamnagar Refinery",
      "country_iso3": "IND",
      "centroid": { "lat": 22.335477, "lon": 69.854513 },
      "asset_type": "Deep Conversion",
      "operator": "Reliance Industries Ltd",
      "capacity_bbl_per_day": { "value": 1369000, "prov": "ct-v6-2026-09-16", "confidence": "low" },
      "throughput_bbl_per_year": { "value": 480590313.8, "prov": "ct-v6-2026-09-16", "confidence": "very low" },
      "utilization_pct": { "value": 96.2, "derived": "throughput_bbl_per_year / (capacity_bbl_per_day * 365) * 100" },
      "staleness": { "vintage": "2021", "as_of": "annual", "stale": true }
    }
  ],

  "flows": [
    {
      "flow_id": "2024:SAU->IND:2709",
      "origin_iso3": "SAU",
      "destination_iso3": "IND",
      "commodity": "HS2709",
      "period": { "year": 2024, "resolution": "annual" },
      "volume_bbl_per_day": { "value": 755000, "prov": "comtrade-2026-09-16" },
      "share_of_origin_exports_pct": { "value": 11.4, "derived": "volume_bbl_per_day / origin_total_exports_bbl_per_day * 100" },
      "basis": "reported",
      "mirror_fill": false,
      "confidence": "high"
    },
    {
      "flow_id": "2024:RUS->IND:2709",
      "origin_iso3": "RUS",
      "destination_iso3": "IND",
      "commodity": "HS2709",
      "period": { "year": 2024, "resolution": "annual" },
      "volume_bbl_per_day": { "value": 1750000, "prov": "comtrade-mirror-2026-09-16" },
      "share_of_origin_exports_pct": { "value": 37.2, "derived": "see above" },
      "basis": "mirror",
      "mirror_fill": true,
      "confidence": "low",
      "caveat": "Reporter silent; reconstructed from importer declarations. Origin may be last declared shipper rather than wellhead."
    }
  ],

  "crack_spreads": [
    {
      "market_id": "USGC",
      "label": "US Gulf Coast",
      "countries_iso3": ["USA"],
      "tier": "A",
      "formula": "3:2:1",
      "crude_usd_per_bbl": { "value": 78.42, "series": "RWTC", "prov": "eia-spot-2026-09-16" },
      "gasoline_usd_per_bbl": { "value": 92.61, "series": "EER_EPMRU_PF4_RGC_DPG", "prov": "eia-spot-2026-09-16" },
      "distillate_usd_per_bbl": { "value": 105.84, "series": "EER_EPD2DXL0_PF4_RGC_DPG", "prov": "eia-spot-2026-09-16" },
      "crack_usd_per_bbl": { "value": 19.14, "derived": "(2*gasoline + 1*distillate)/3 - crude" },
      "as_of": "2026-09-12",
      "staleness": { "lag_days": 4, "stale": false }
    },
    {
      "market_id": "EU27",
      "label": "European Union (approximated)",
      "countries_iso3": ["DEU", "NLD", "ITA"],
      "tier": "B",
      "formula": "pass-through-proxy",
      "crack_usd_per_bbl": null,
      "proxy_margin_usd_per_bbl": { "value": null, "prov": "ec-wob-2026-09-16" },
      "caveat": "EC Weekly Oil Bulletin publishes RETAIL ex-tax prices, not wholesale barge quotes. This is a downstream pass-through indicator, NOT a crack spread. Do not compare its level against tier A markets."
    }
  ]
}
```

### Derivation formulas

**Crack spread, 3:2:1.** Three barrels of crude in, two of gasoline and one of distillate
out:

```
crack_$/bbl = (2 × gasoline_$/bbl + 1 × distillate_$/bbl) / 3  −  crude_$/bbl
```

with EIA product prices converted `$/gal × 42 = $/bbl` first. Recommended pairings:

| Market | Crude leg | Gasoline leg | Distillate leg |
|---|---|---|---|
| **USGC** | WTI Cushing (`RWTC`) | Conventional regular, Gulf Coast | ULSD, Gulf Coast |
| **NYH** | Brent (`RBRTE`) | Conventional regular, NY Harbor | ULSD, NY Harbor |
| **USWC (LA)** | WTI Cushing | RBOB, Los Angeles | ULSD, Los Angeles |

Use **Brent for NY Harbor**, not WTI — the East Coast prices off waterborne imports and a
WTI-based NYH crack embeds the Brent-WTI spread as a spurious signal. The **5:3:2** variant
is also conventional for the Gulf Coast; compute both and let the UI choose, but never
average them.

**Utilization.**
```
utilization_pct = throughput_bbl_per_year / (capacity_bbl_per_day × 365) × 100
```
For US assets prefer the WPSR figure (`gross inputs / operable capacity`, weekly) where
available; it is two orders of magnitude fresher. Where both exist, show WPSR and keep the
Climate TRACE value as the reconciliation check.

**Export share.**
```
share_pct = flow_bbl_per_day / origin_total_exports_bbl_per_day × 100
```
The denominator should come from **JODI** where the country reports (monthly, ~2-month
lag, and genuinely a total rather than a sum of observed partners), falling back to the
sum of Comtrade partner flows. **Never compute the denominator as the sum of the flows you
happen to have** — that forces the shares to sum to 100% and hides exactly the missing
volume the map should be disclosing.

### Refresh cadence & staleness to surface

| Source | Publishes | Poll | UI should say |
|---|---|---|---|
| Climate TRACE | Annual | Monthly | "Refinery capacity: 2021 vintage" — **the vintage is the story here, surface it prominently** |
| EIA spot prices | Daily (3–5 day lag) | Daily | "Prices as of \<date\>" |
| EIA WPSR | Weekly, Wed (6-day lag) | Weekly Thu | "US utilization week ending \<date\>" |
| EIA-820 | Annual, each June | Annually | "US capacity as-of 1 Jan \<year\>" |
| EIA-814 | Monthly (~2-month lag) | Monthly | "US imports through \<month\>" |
| Eurostat `nrg_ti_oilm` | Monthly (~3-month lag) | Monthly | "EU flows through \<month\>" |
| Comtrade | Annual (6–14-month lag) | Quarterly | "Global flows: \<year\>" |
| JODI | Monthly (~2-month lag) | Monthly | "Export totals through \<month\>" |
| EC Weekly Oil Bulletin | Weekly, Thu (1-day lag) | Weekly | "EU pump prices week of \<date\>" |
| PPAC | Monthly | Monthly | "Indian basket crude, \<month\>" |

The staleness spread across layers is the single biggest UX hazard in this view: a map
showing **daily** crack spreads on top of **annual, 2021-vintage** capacity and
**14-month-lagged** flows will be read as one coherent snapshot unless each layer carries
its own visible as-of date. Put the date on the layer, not in a footnote.

### Sanity checks & reconciliation

| # | Check | Tolerance | Action on breach |
|---|---|---|---|
| R1 | Climate TRACE global capacity sum vs OPEC ASB / Energy Institute world total | **±3%** (observed: +0.8% vs OPEC 2024) | Investigate; likely a vintage or condensate-splitter definitional difference |
| R2 | Climate TRACE US subtotal (18.55 mb/d) vs EIA-820 US operable capacity | **±5%** | EIA-820 is authoritative for the US; prefer it and log the delta. *Not yet run — EIA-820's current headline number was not verified this session.* |
| R3 | Comtrade reported export vs partner-declared import (mirror), per pair | **±10%** for OECD pairs | Above tolerance, prefer importer side and mark `basis: "mirror"` |
| R4 | Sum of Comtrade partner flows vs JODI country export total | **±15%** | Gap = unattributed volume. **Show it as an explicit "unattributed" bucket rather than silently normalising it away.** |
| R5 | Per-asset utilization within 0–105% | hard bound | >105% means a capacity/throughput vintage mismatch — suppress the value, keep the asset |
| R6 | Flow sum into a market vs that market's refining capacity | advisory | Persistent excess implies transit/re-export (Singapore, Netherlands, UAE) rather than error — label those markets as hubs |

R4 and R6 are the two that earn their keep. R4 is the mechanism by which the sanctioned-flow
blind spot becomes *visible* in the UI instead of invisible, and R6 catches the
Rotterdam/Singapore re-export problem that otherwise makes those markets look like they
refine several times what they can.

### Provenance metadata carried per value

Every numeric leaf carries `{ value, prov, confidence?, derived? }` where `prov` indexes
into `provenance_defs` holding **source, URL, licence, retrieved_at, original_units,
transformation**. Derived values carry the formula string rather than a source, so lineage
is walkable from any displayed number back to a fetched payload. Climate TRACE's own
per-field confidence flags pass straight through into `confidence` — they are already
per-field and per-year, and discarding them would be throwing away the dataset's own
honesty.

### Licensing posture for the finished app

- **Comtrade** — ship aggregated/derived flows only. No raw-record export endpoint. Stay
  under the 100,000-original-record redistribution threshold.
- **Climate TRACE** — CC BY 4.0: attribute visibly.
- **EIA** — public domain, no constraint.
- **Eurostat** — attribution.
- **IMF PortWatch** — CC BY-SA 4.0: **share-alike is viral.** If PortWatch values are
  incorporated into a published dataset, that dataset inherits SA. Prefer using it as an
  on-screen corroboration layer rather than merging it into the flow store.
- **Wikipedia** — CC BY-SA 4.0, same viral problem. Cross-check only, never a value source.
- **ICE / CME / Argus / Platts** — do not scrape, do not display. Licensed activity.

---

## Open items

1. **R2 not yet run** — EIA-820's current US operable-capacity headline was not verified
   this session, so the Climate TRACE US subtotal has no benchmark yet.
2. **PPAC login-gated history** — untested. Would need a free PPAC account; that is
   the project owner's call. If it turns out to carry the petrol/diesel FOB legs, India moves back to
   tier A and Layer 3 rigorous coverage rises from 17.7% to ~22.6%.
3. **Layer 1 denominator is second-hand** — the ~42 mb/d seaborne crude figure and the
   ~7.2 mb/d sanctioned tranche come from commercial vessel-tracking journalism
   (Bloomberg, Kpler), not from a free statistical source. Every other coverage number in
   this report was computed directly; this one was not.
4. **Energy Institute and JODI licence terms** remain unconfirmed (403 / 404).
5. **EIA international facet IDs** must be enumerated from live metadata at build time.
