import {
  Callout,
  Code,
  Divider,
  Grid,
  H1,
  H2,
  H3,
  Pill,
  Row,
  Stack,
  Stat,
  Table,
  Text,
} from "cursor/canvas";

export default function SharpRunupBullMarketSignal() {
  return (
    <Stack gap={24}>
      <Stack gap={8}>
        <H1>Does a hard 15-day run-up signal a new bull market?</H1>
        <Text tone="secondary">
          Evaluating the claim: "sharp upward thrusts historically presage near-term bull markets — so why is this time different?"
        </Text>
      </Stack>

      <Callout tone="info" title="Short answer: it's a valid question, but the premise is half-right.">
        Sharp run-ups <Text as="span" weight="semibold">with confirming breadth</Text> have an exceptional historical track record (Zweig / Whaley breadth thrusts: ~14–16 signals since 1945, every one higher 6 and 12 months later in most data sets). But sharp run-ups <Text as="span" weight="semibold">without</Text> breadth confirmation are exactly what bear-market rallies look like — and there are many of those too. The discriminator isn't the size of the rally; it's its <Text as="span" italic>character</Text>.
      </Callout>

      <Stack gap={12}>
        <H2>The bullish case for "rallies presage bulls"</H2>
        <Text>
          The strongest version of the claim relies on <Text as="span" weight="semibold">breadth thrusts</Text> — sharp moves where almost every stock rallies, not just a few mega-caps. The most-cited is the Zweig Breadth Thrust (ZBT): the 10-day average ratio of advancing issues moves from oversold (under ~0.40) to euphoric (over ~0.615) in 10 trading days or fewer. It's rare and almost mechanically bullish.
        </Text>
        <Grid columns={4} gap={16}>
          <Stat value="~14–16" label="ZBT signals since 1945" />
          <Stat value="100%" label="Higher 6 & 12 mo later" tone="success" />
          <Stat value="~24%" label="Avg 12-month return" tone="success" />
          <Stat value="3–4 mo" label="Median time to new high" />
        </Grid>
        <Text tone="secondary" size="small">
          Approximate figures from Ned Davis Research, Wayne Whaley, and Ryan Detrick studies; specifics vary by data set and the exact threshold used.
        </Text>
      </Stack>

      <Stack gap={12}>
        <H2>The bearish counter-case: violent rallies live inside bear markets</H2>
        <Text>
          The single largest one-day S&P 500 gains in history almost all happened during bear markets, not new bulls. The 2000–2002 and 2007–2009 bears each contained multiple 10–20% multi-week rallies that completely failed. So a sharp 15-day move is, on its own, <Text as="span" italic>more</Text> common in bear markets than in fresh bulls — what's rare is the <Text as="span" weight="semibold">kind</Text> of sharp move.
        </Text>
        <Row gap={8} wrap>
          <Pill tone="danger">Oct 1929 → +35% rally → new low</Pill>
          <Pill tone="danger">Apr 2001 → +19% → new low</Pill>
          <Pill tone="danger">Mar 2008 (Bear Stearns) → +12% → new low</Pill>
          <Pill tone="danger">Nov 2008 → +21% → Mar 2009 low</Pill>
          <Pill tone="danger">Jun 2022 → +12% → Oct 2022 low</Pill>
          <Pill tone="danger">Aug 2022 → +18% → Oct 2022 low</Pill>
        </Row>
      </Stack>

      <Stack gap={12}>
        <H2>Side-by-side: what real thrusts vs. failed bounces looked like</H2>
        <Table
          headers={[
            "Episode",
            "Type",
            "Breadth confirmed?",
            "Catalyst",
            "12 months later",
          ]}
          rows={[
            ["Aug 1982", "ZBT", "Yes", "Volcker pivot, recession ending", "+44%"],
            ["Jan 1987", "ZBT", "Yes", "Earnings momentum", "+22% (pre-crash)"],
            ["Jan 1991", "ZBT", "Yes", "Gulf War resolution, Fed easing", "+30%"],
            ["Mar–Apr 2009", "ZBT", "Yes", "TARP/Fed backstop, capitulation low", "+45%"],
            ["Jun 2020", "ZBT", "Yes", "Fed liquidity flood, reopen", "+38%"],
            ["Nov 2023", "ZBT", "Yes", "Dovish Fed pivot, soft-landing data", "+32%"],
            ["Oct 1929", "Sharp rally only", "No", "Reflexive bounce after crash", "−54%"],
            ["Apr 2001", "Sharp rally only", "No", "Fed cuts, dot-com hopium", "−18%"],
            ["Nov 2008", "Sharp rally only", "Partial", "Bailout headlines", "+8% (after deeper low)"],
            ["Jun & Aug 2022", "Sharp rally only", "No", "Peak-inflation hopes", "−4% to flat"],
          ]}
          rowTone={[
            "success",
            "success",
            "success",
            "success",
            "success",
            "success",
            "danger",
            "danger",
            "warning",
            "danger",
          ]}
          columnAlign={["left", "left", "left", "left", "right"]}
        />
        <Text tone="secondary" size="small">
          "Breadth confirmed" means a real ZBT or Whaley up-volume thrust fired at the time. Returns rounded; figures from S&P 500 price index.
        </Text>
      </Stack>

      <Divider />

      <Stack gap={12}>
        <H2>Local warehouse snapshot (DataHoover)</H2>
        <Text tone="secondary">
          Numbers below come from ingested Twelve Data ETF daily closes and FRED index levels stored in DuckDB (<Text as="span" weight="semibold">not</Text> NYSE internals). Useful for grounding <Text as="span" italic>recent index-level thrust size</Text> and mega-cap versus small-cap behavior — not for declaring a Zweig breadth thrust.
        </Text>
        <Text tone="secondary" size="small">
          Snapshot refreshed from DuckDB · generated <Text as="span" weight="semibold">2026-04-30 12:24 UTC</Text>. Regenerate with <Code>python scripts/canvas_market_snapshot.py</Code> after ingest. ETF window ends <Text as="span" weight="semibold">2026-04-28</Text> close (15 sessions); latest FRED index rows through <Text as="span" weight="semibold">2026-04-29</Text> where shown.
        </Text>
        <Grid columns={5} gap={16}>
          <Stat value="+5.09%" label="SPY trailing 15d (TD)" tone="success" />
          <Stat value="+8.93%" label="QQQ trailing 15d (TD)" tone="success" />
          <Stat value="+4.18%" label="IWM trailing 15d (TD)" tone="success" />
          <Stat value="+1.24%" label="RSP trailing 15d (equal-wt)" />
          <Stat value="+4.7 pp" label="QQQ − IWM (gap)" tone="info" />
        </Grid>
        <Grid columns={5} gap={16}>
          <Stat value="+1.61%" label="FRED SP500 (15 obs)" />
          <Stat value="17.83" label="VIX (FRED, latest)" />
          <Stat value="+0.50 pp" label="10Y − 2Y (FRED)" />
          <Stat value="2.85%" label="HY OAS (FRED, %)" tone="warning" />
          <Stat value="−3.8 pp" label="RSP − SPY (15d gap)" tone="warning" />
        </Grid>
        <Text tone="secondary" size="small">
          Equal-weight versus cap-weight gap is a rough participation read only: positive means broad S&P outperformance; here RSP − SPY over the same window is <Text as="span" weight="semibold">negative</Text> while QQQ materially beats IWM → growth/large-factor leadership dominates, not tape-wide euphoria. That is <Text as="span" italic>consistent with</Text> checklist item 1 (narrow rallies) — not a substitute for NYSE internals or a Zweig thrust.
        </Text>
        <Text tone="secondary" size="small">
          FRED SP500 uses contiguous daily index observations from the warehouse, so its 15-observation window differs slightly from Twelve Data calendar sessions on ETFs.
        </Text>
        <H3>Recent DataHoover <Code>market_move</Code> signals</H3>
        <Text tone="secondary" size="small">
          Pipeline flags when the prior day’s absolute return exceeds the configured threshold (default 2%). Empty here means none stored for tracked symbols until you run <Code>hoover compute-signals</Code>.
        </Text>
        <Table
          headers={["Ts", "Symbol", "Summary", "Severity"]}
          rows={[
            [
              "(none)",
              "—",
              "Run hoover ingest + compute-signals to populate.",
              "—",
            ],
          ]}
          striped
          columnAlign={["left", "left", "left", "right"]}
        />
        <Callout tone="warning" title="What DuckDB still cannot answer here">
          You now have ETF prices, SP500/VIX/yield curve/HY spreads from this warehouse snapshot — useful for framing magnitude, volatility regime, curve, credit, and a crude equal-weight-vs-cap cue. Still missing from DuckDB are NYSE advance/decline and up/down volume (ZBT style), thrust counts, percentage above MA, AAII/VIX-derived sentiment beyond the quoted VIX level, and geopolitical overlays. Interpret the grids as one layer of diagnostics, not a thrust alarm.
        </Callout>
      </Stack>

      <Divider />

      <Stack gap={12}>
        <H2>So what would tell you "this time is (or isn't) different"?</H2>
        <Text>
          The honest answer to the user's question is: it's not "is this time different" — it's "which kind of run-up is this?" Six concrete diagnostics separate real thrusts from bear-market rallies:
        </Text>
        <Grid columns={2} gap={16}>
          <Stack gap={6}>
            <H3>1. Breadth participation</H3>
            <Text>
              Did a ZBT or Whaley thrust actually trigger? Are 80%+ of stocks above their 50-day MA? If only the top 7 mega-caps moved while the equal-weight index lagged, it's narrow — that's the bear-market-rally fingerprint.
            </Text>
          </Stack>
          <Stack gap={6}>
            <H3>2. Volume character</H3>
            <Text>
              Up-volume vs. down-volume on a 5- and 10-day basis. Real thrusts show up-volume swamping down-volume (Whaley's classic 9-to-1 up days clustered together). Short-cover-only rallies often don't.
            </Text>
          </Stack>
          <Stack gap={6}>
            <H3>3. Where it started from</H3>
            <Text>
              Real bottoms come out of <Text as="span" italic>capitulation</Text> — VIX spikes above 35–40, AAII bulls below 25%, credit spreads blowing out. A run-up off a mild dip with sentiment never having truly puked is suspicious.
            </Text>
          </Stack>
          <Stack gap={6}>
            <H3>4. The catalyst's durability</H3>
            <Text>
              A rally on a Fed pivot or earnings inflection is more durable than one on a reversible policy headline (tariff pause, geopolitical de-escalation). If the news that caused the rally can be undone by a tweet, so can the rally.
            </Text>
          </Stack>
          <Stack gap={6}>
            <H3>5. Confirmation from credit & rates</H3>
            <Text>
              Bull markets are usually confirmed by tightening high-yield spreads and a steepening curve out of inversion. If equities rip but credit spreads stay wide or 2s/10s re-inverts, equities are out over their skis.
            </Text>
          </Stack>
          <Stack gap={6}>
            <H3>6. Follow-through 1–3 months out</H3>
            <Text>
              The clearest tell is time. Real thrusts make a higher high within 1–3 months and don't retest the low. Failed rallies stall, roll over, and break the prior support within ~6–10 weeks.
            </Text>
          </Stack>
        </Grid>
      </Stack>

      <Callout tone="warning" title="What I can't tell you from here">
        The narrative framework is still the main source; the DataHoover snapshot only adds what is in your local DuckDB after ingest. Without NYSE breadth ingested, even a live warehouse cannot say whether a Zweig thrust fired, what true participation looks like, or whether credit is confirming (unless you added VIX/HY/curve to FRED and refreshed). Use the six diagnostics above plus any series you actually store — the question stays a checklist, not a vibe.
      </Callout>

      <Stack gap={8}>
        <H2>Bottom line for the original question</H2>
        <Text>
          The premise — "sharp run-ups historically presage bull markets" — is <Text as="span" weight="semibold">conditionally</Text> true. It's true for breadth-confirmed thrusts (an exceptional record). It's not true for sharp rallies in general; those are at least as common inside bear markets as at the start of bulls. So the right reframing of the question isn't "why is this time different?" — it's "is this run-up the rare breadth-thrust kind, or the common bear-market-rally kind?" That's an empirically answerable question, not a narrative one.
        </Text>
      </Stack>
    </Stack>
  );
}
