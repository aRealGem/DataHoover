import {
  BarChart,
  Callout,
  Divider,
  Grid,
  H1,
  H2,
  H3,
  LineChart,
  Pill,
  Row,
  Stack,
  Stat,
  Table,
  Text,
} from "cursor/canvas";

const PULL_TS = "2026-06-28 21:30 UTC";
const WAR_START = "2026-02-27";

// Headline tile data: [label, latest_display, delta_pct, sub]
type HeadlineTone = "danger" | "warning" | "success" | "info" | undefined;

const headline: Array<{
  label: string;
  value: string;
  pct: string;
  tone: HeadlineTone;
}> = [
  { label: "Brent crude (USD/bbl)",      value: "$76.49",   pct: "+7.2%",   tone: "info"    },
  { label: "WTI crude (USD/bbl)",        value: "$78.94",   pct: "+17.9%",  tone: "warning" },
  { label: "US gasoline (USD/gal, retail)", value: "$3.77", pct: "+34.9%",  tone: "danger"  },
  { label: "Henry Hub gas (USD/MMBtu)",  value: "$3.16",    pct: "+5.7%",   tone: "warning" },
  { label: "Spot gold (XAU/USD)",        value: "$4,081",   pct: "-22.7%",  tone: "danger"  },
  { label: "USD broad index (DTWEXBGS)", value: "120.40",   pct: "+2.2%",   tone: "info"    },
  { label: "Yuan onshore CNY",           value: "6.769",    pct: "-1.30%",  tone: "info"    },
  { label: "Yuan offshore CNH",          value: "6.801",    pct: "-1.04%",  tone: "info"    },
  { label: "Lockheed Martin (LMT)",      value: "$507.40",  pct: "-22.9%",  tone: "danger"  },
  { label: "Elbit Systems (ESLT)",       value: "$715.72",  pct: "-6.9%",   tone: "warning" },
  { label: "US SPR (Mbbl)",              value: "331.2",    pct: "-20.28%", tone: "danger"  },
  { label: "S&P 500",                    value: "7,354",    pct: "+6.9%",   tone: "success" },
  { label: "Bitcoin (BTC/USD)",          value: "$59,578",  pct: "-11.0%",  tone: "warning" },
];

// War timeline: chronological, factual
const timeline: Array<{ date: string; text: string }> = [
  { date: "Feb 27, 2026", text: "Pre-war close. Brent $71.32, WTI $66.96, US retail gasoline $2.80/gal, S&P 500 6,879. Used as the baseline for every delta on this canvas." },
  { date: "Feb 28, 2026", text: "Joint US/Israeli air-and-missile campaign on Iran begins; Iran begins mining the Strait of Hormuz." },
  { date: "Mar  3, 2026", text: "Iran declares Strait of Hormuz closed; US announces naval blockade of Iranian ports." },
  { date: "Mar 27, 2026", text: "Brent first closes above $120 (data: $121.47)." },
  { date: "Apr  3, 2026", text: "Reported peak: Brent ~$147 intraday (FRED close $138.21 on Apr 7); WTI close hits $138 area." },
  { date: "Apr  7, 2026", text: "Trump announces a ceasefire after Kushner/Witkoff/Vance meet Iranian counterparts in Islamabad. US blockade and Hormuz closure remain in effect." },
  { date: "Apr 17, 2026", text: "Iran briefly reopens Hormuz tied to a separate Israel/Lebanon ceasefire; Trump says US blockade continues until a final deal." },
  { date: "Apr 18, 2026", text: "Iran shuts Hormuz again (\"breaches of trust\")." },
  { date: "Apr 21, 2026", text: "Trump extends ceasefire pending an Iranian end-state proposal." },
  { date: "Apr 27, 2026", text: "Iran tables proposal: mutual reopen of Hormuz, defer nuclear talks. CENTCOM: 38 ships stopped/turned by US blockade." },
  { date: "Apr 28, 2026", text: "UAE announces it will quit OPEC. Trump publicly says he is unlikely to accept Iran's offer; Brent tops $112." },
  { date: "Apr 29, 2026", text: "End of first publication snapshot. Ceasefire holds, Hormuz remains effectively closed, US-Iran talks stalled. Brent ~$114; Iranian rial ~1.05M/USD." },
  { date: "May  4, 2026", text: "Iranian rial hits an all-time low of ~1.93M IRR/USD in Tehran free market (Pashizi historical archive)." },
  { date: "Jun 15, 2026", text: "US and Iran sign a 14-point interim agreement (MoU) in Oman. Iran commits to safe passage of commercial vessels through the Strait of Hormuz for 60 days; both sides set a 60-day window to negotiate a final deal." },
  { date: "Jun 17, 2026", text: "MoU in force. Hundreds of blockaded tankers begin transiting Hormuz; oil prices slide toward pre-war levels (Globe & Mail)." },
  { date: "Jun 22, 2026", text: "Brent close $76.49 — war premium effectively erased. Iranian rial at ~1.60M/USD (Bonbast/Pashizi)." },
  { date: "Jun 25, 2026", text: "Iran attacks a Singapore-flagged container ship near the Strait of Hormuz. Trump calls it a \"foolish violation\" of the MoU (CNN, Al Jazeera)." },
  { date: "Jun 26, 2026", text: "US strikes Iranian missile/drone storage and coastal radar around Hormuz. CENTCOM says strikes were \"in direct response to continued Iranian aggression against commercial shipping\" (CNN)." },
  { date: "Jun 27, 2026", text: "Another tanker hit by drone in Hormuz; US strikes Sirik, Bandar-e Lengeh, and Qeshm Island (Al Jazeera, Globe & Mail)." },
  { date: "Jun 28, 2026", text: "Day 121. IRGC fires ballistic missiles and drones at Ali Al Salem Air Base (Kuwait) and US Fifth Fleet HQ (Bahrain). Iran declares sole control of Hormuz for 30 days; Trump threatens to \"complete the job\" (Al Jazeera)." },
];

// Energy & gold table
const energyRows: Array<{
  label: string; pre: string; latest: string; pct: string; src: string; tone?: "danger" | "warning" | "success";
}> = [
  { label: "Brent crude (USD/bbl)",                pre: "71.32",  latest: "76.49",  pct: "+7.2%",   src: "FRED DCOILBRENTEU (06-22)", tone: "warning" },
  { label: "WTI crude (USD/bbl)",                  pre: "66.96",  latest: "78.94",  pct: "+17.9%",  src: "FRED DCOILWTICO (06-22)",   tone: "danger"  },
  { label: "US gasoline retail (USD/gal)",         pre: "2.80",   latest: "3.77",   pct: "+34.9%",  src: "FRED GASREGCOVW (06-22)",   tone: "danger"  },
  { label: "Brent ETF (BNO)",                      pre: "34.81",  latest: "40.31",  pct: "+15.8%",  src: "TwelveData (06-25)",        tone: "danger"  },
  { label: "US Oil ETF (USO)",                     pre: "81.95",  latest: "105.48", pct: "+28.7%",  src: "TwelveData (06-25)",        tone: "danger"  },
  { label: "Henry Hub natural gas (USD/MMBtu)",    pre: "2.99",   latest: "3.16",   pct: "+5.7%",   src: "FRED DHHNGSP (06-22)",      tone: "warning" },
  { label: "US Nat Gas ETF (UNG)",                 pre: "11.52",  latest: "11.87",  pct: "+3.0%",   src: "TwelveData (06-25)" },
  { label: "Spot gold (XAU/USD)",                  pre: "5,277.88", latest: "4,080.91", pct: "-22.7%", src: "TwelveData (06-28)",     tone: "danger"  },
  { label: "Gold ETF (GLD)",                       pre: "483.75", latest: "373.63", pct: "-22.8%",  src: "TwelveData (06-25)",        tone: "danger"  },
];

// Dollar / FX table. Convention: "Δ% (USD)" = how much USD strengthened vs that currency.
const fxRows: Array<{
  label: string; pre: string; latest: string; pctUsd: string; note: string; tone?: "danger" | "warning" | "success" | "info";
}> = [
  { label: "USD broad index (DTWEXBGS)",   pre: "117.82",  latest: "120.40",  pctUsd: "+2.19%",  note: "FRED, vs broad basket" },
  { label: "USD vs advanced FX (DTWEXAFEGS)", pre: "110.16", latest: "113.70", pctUsd: "+3.21%", note: "FRED" },
  { label: "USD vs emerging FX (DTWEXEMEGS)", pre: "127.40", latest: "128.92", pctUsd: "+1.19%", note: "FRED" },
  { label: "USD Index ETF (UUP)",          pre: "27.08",   latest: "28.46",   pctUsd: "+5.10%",  note: "TwelveData", tone: "warning" },
  { label: "EUR/USD",                       pre: "1.1816",  latest: "1.1386",  pctUsd: "+3.64%",  note: "USD/EUR rose \u2192 dollar stronger", tone: "info" },
  { label: "GBP/USD",                       pre: "1.3480",  latest: "1.3206",  pctUsd: "+2.07%",  note: "Sterling weaker on energy shock fade" },
  { label: "AUD/USD",                       pre: "0.7118",  latest: "0.6893",  pctUsd: "+3.16%",  note: "AUD weaker (commodity FX gave back gains)" },
  { label: "USD/JPY",                       pre: "156.05",  latest: "161.37",  pctUsd: "+3.41%",  note: "Yen weaker, energy import hit",  tone: "info" },
  { label: "USD/MXN",                       pre: "17.23",   latest: "17.53",   pctUsd: "+1.74%",  note: "Peso stable" },
  { label: "USD/INR",                       pre: "91.07",   latest: "94.38",   pctUsd: "+3.63%",  note: "Rupee still hit \u2014 India ~80% crude via Hormuz", tone: "info" },
  { label: "USD/CNY",                       pre: "6.8579",  latest: "6.7686",  pctUsd: "-1.30%",  note: "PBoC kept floor; CNY actually stronger thru war + post-MoU", tone: "success" },
  { label: "USD/KRW",                       pre: "1,439.82", latest: "1,540.64", pctUsd: "+7.00%", note: "Won materially weaker, energy importer", tone: "warning" },
  { label: "USD/SGD",                       pre: "1.2643",  latest: "1.2898",  pctUsd: "+2.02%",  note: "Singapore dollar weaker" },
  { label: "USD/ZAR",                       pre: "15.89",   latest: "16.46",   pctUsd: "+3.59%",  note: "Rand weak (EM risk-off)" },
  { label: "USD/THB",                       pre: "31.04",   latest: "32.79",   pctUsd: "+5.64%",  note: "Baht weak (Thailand oil importer)",   tone: "warning" },
  { label: "USD/SEK",                       pre: "9.0199",  latest: "9.5709",  pctUsd: "+6.11%",  note: "Krona materially weaker",            tone: "warning" },
  { label: "USD/TWD",                       pre: "31.35",   latest: "31.58",   pctUsd: "+0.73%",  note: "Taiwan dollar barely moved" },
  { label: "USD/BRL",                       pre: "5.1357",  latest: "5.1730",  pctUsd: "+0.73%",  note: "Real gave back its oil-exporter premium as Brent fell" },
  { label: "USD/ILS",                       pre: "3.1295",  latest: "3.0023",  pctUsd: "-4.07%",  note: "Shekel STRONGER through war and post-MoU \u2014 Iran-disadvantage premium", tone: "success" },
  { label: "Iranian rial (IRR, free-market)", pre: "~720,000 / USD", latest: "~1,600,500 / USD", pctUsd: "~+122%", note: "Bonbast/Pashizi free-market; peaked ~1.93M on May 4. Not from primary feed.", tone: "danger" },
];

// Ag / fertilizer
const agRows: Array<{
  label: string; pre: string; latest: string; pct: string; src: string; tone?: "danger" | "warning" | "success";
}> = [
  { label: "Wheat (IMF, USD/MT)",           pre: "174.75", latest: "220.88", pct: "+26.4%", src: "FRED PWHEAMTUSDM (monthly, May)", tone: "danger" },
  { label: "Wheat ETF (WEAT)",              pre: "22.57",  latest: "22.17",  pct: "-1.8%",  src: "TwelveData (06-25)" },
  { label: "Maize/corn (IMF, USD/MT)",      pre: "210.64", latest: "215.62", pct: "+2.4%",  src: "FRED PMAIZMTUSDM (monthly, May)" },
  { label: "Corn ETF (CORN)",               pre: "17.89",  latest: "16.86",  pct: "-5.8%",  src: "TwelveData (06-25)", tone: "warning" },
  { label: "Soybeans (IMF, USD/MT)",        pre: "409.48", latest: "439.15", pct: "+7.2%",  src: "FRED PSOYBUSDM (monthly, May)",  tone: "warning" },
  { label: "Soybean ETF (SOYB)",            pre: "23.82",  latest: "24.50",  pct: "+2.9%",  src: "TwelveData (06-25)" },
  { label: "US PPI mixed fertilizer",       pre: "141.10", latest: "172.06", pct: "+21.9%", src: "FRED WPU0652013A (monthly, May)", tone: "danger" },
  { label: "Agribusiness ETF (MOO)",        pre: "85.59",  latest: "79.22",  pct: "-7.4%",  src: "TwelveData (06-25)", tone: "warning" },
];

// Weekly-aligned, indexed-to-100 (Jan 2 = 100) chart series.
// Pulled live from the warehouse on 2026-04-29.
const energyChartCats = [
  "Jan 2", "Jan 9", "Jan 16", "Jan 23", "Jan 30",
  "Feb 6", "Feb 13", "Feb 20", "Feb 27 (war)",
  "Mar 6", "Mar 13", "Mar 20", "Mar 27",
  "Apr 3", "Apr 10", "Apr 17", "Apr 24",
  "May 1", "May 8", "May 15", "May 22", "May 29",
  "Jun 5", "Jun 12", "Jun 19", "Jun 26",
];

// Brent / WTI / gasoline rebased to Feb 27 = 100 to make the war-era trajectory legible.
function rebaseToFeb27(values: Array<number | null>): Array<number | null> {
  const baseIdx = 8; // Feb 27 in the categories array
  const base = values[baseIdx];
  if (base == null || base === 0) return values;
  return values.map((v) => (v == null ? null : Math.round((v / base) * 1000) / 10));
}

const brentRaw: Array<number | null> = [61.98, 65.11, 66.97, 68.16, 72.25, 70.45, 69.96, 72.75, 71.32, 95.74, 103.23, 118.42, 121.47, 127.61, 119.07, 98.63, 111.86, 118.26, 103.48, 113.96, 106.90, 92.88, 97.29, 88.64, 80.46, 76.49];
const wtiRaw: Array<number | null> = [57.21, 58.96, 59.40, 60.70, 64.50, 63.77, 63.05, 66.69, 66.96, 90.77, 98.48, 98.71, 101.26, 113.23, 98.34, 85.91, 98.42, 105.38, 98.87, 108.99, 100.35, 91.16, 94.32, 88.62, 80.35, 78.94];
const gasRaw: Array<number | null> = [2.690, 2.681, 2.665, 2.700, 2.747, 2.747, 2.770, 2.790, 2.796, 2.884, 3.364, 3.566, 3.788, 3.814, 3.947, 3.962, 3.885, 3.948, 4.305, 4.353, 4.334, 4.327, 4.138, 3.986, 3.908, 3.771];
const goldRaw: Array<number | null> = [null, null, null, null, 4865.46, 4959.11, 5043.16, 5098.61, 5277.88, 5171.10, 5019.79, 4497.52, 4507.69, 4676.51, 4750.53, 4834.10, 4708.60, 4613.27, 4715.49, 4537.73, 4505.66, 4542.53, 4330.15, 4215.39, 4156.51, 4080.81];

const energyIndexedSeries = [
  { name: "Brent",       data: rebaseToFeb27(brentRaw).map((v) => (v == null ? 0 : v)), tone: "danger" as const },
  { name: "WTI",         data: rebaseToFeb27(wtiRaw).map((v) => (v == null ? 0 : v)),   tone: "warning" as const },
  { name: "US gasoline", data: rebaseToFeb27(gasRaw).map((v) => (v == null ? 0 : v)),   tone: "info" as const },
  { name: "Spot gold",   data: rebaseToFeb27(goldRaw).map((v) => (v == null ? 0 : v)),  tone: "neutral" as const },
];

// FX vs USD: indexed to Feb 27 = 100. Higher value = USD STRONGER vs that currency
// (rises in USD/JPY, USD/INR, USD/ZAR, falls in USD/BRL = real stronger).
const usdJpyRaw: Array<number | null> = [156.72, 158.07, 158.02, 157.57, 154.34, 157.10, 152.77, 154.99, 156.05, 157.64, 159.54, 159.26, 160.16, 159.64, 159.22, 158.10, 159.35, 156.76, 156.64, 158.69, 159.20, 159.23, 160.26, 160.24, 161.37, 161.37];
const usdInrRaw: Array<number | null> = [null, null, null, null, 91.68, 90.55, 90.56, 90.73, 91.07, 91.95, 92.53, 93.72, 94.84, 92.84, 93.10, 92.73, 94.11, 94.84, 94.47, 96.04, 95.72, 95.00, 94.95, 95.21, 94.42, 94.44];
const usdZarRaw: Array<number | null> = [null, null, null, null, 16.16, 16.03, 15.97, 16.04, 15.89, 16.56, 16.88, 17.00, 17.09, 16.98, 16.41, 16.32, 16.53, 16.64, 16.40, 16.69, 16.45, 16.24, 16.56, 16.29, 16.44, 16.47];
const usdBrlRaw: Array<number | null> = [null, null, null, null, 5.2333, 5.2226, 5.2126, 5.1833, 5.1357, 5.2575, 5.2809, 5.2947, 5.2383, 5.1879, 5.0214, 4.9803, 4.9811, 4.9841, 4.9079, 5.0293, 5.0154, 5.0479, 5.1713, 5.1119, 5.1554, 5.1821];

const fxIndexedSeries = [
  { name: "USD/JPY", data: rebaseToFeb27(usdJpyRaw).map((v) => (v == null ? 0 : v)), tone: "warning" as const },
  { name: "USD/INR", data: rebaseToFeb27(usdInrRaw).map((v) => (v == null ? 0 : v)), tone: "danger"  as const },
  { name: "USD/ZAR", data: rebaseToFeb27(usdZarRaw).map((v) => (v == null ? 0 : v)), tone: "info"    as const },
  { name: "USD/BRL", data: rebaseToFeb27(usdBrlRaw).map((v) => (v == null ? 0 : v)), tone: "success" as const },
];

// --- Yuan (CNY onshore via FRED DEXCHUS, CNH offshore via TwelveData USD/CNH) ---
const cnyRaw: Array<number | null> = [6.9877, 6.9772, 6.9681, 6.9631, 6.9510, 6.9388, 6.9080, 6.9031, 6.8579, 6.8965, 6.8961, 6.8857, 6.9116, 6.8824, 6.8278, 6.8170, 6.8359, 6.8276, 6.8005, 6.8092, 6.7945, 6.7662, 6.7655, 6.7626, 6.7686, 6.7686];
const cnhRaw: Array<number | null> = [null, null, null, null, 6.9569, 6.9374, 6.9013, 6.9005, 6.8727, 6.9105, 6.9065, 6.9067, 6.9026, 6.8902, 6.8277, 6.8134, 6.8361, 6.8325, 6.7975, 6.8113, 6.7985, 6.7343, 6.8192, 6.7617, 6.7905, 6.8031];

const yuanIndexedSeries = [
  { name: "CNY (onshore)",   data: rebaseToFeb27(cnyRaw).map((v) => (v == null ? 0 : v)), tone: "info"    as const },
  { name: "CNH (offshore)",  data: rebaseToFeb27(cnhRaw).map((v) => (v == null ? 0 : v)), tone: "warning" as const },
];

// --- Defense stocks: weekly-aligned, rebased to Feb 27 = 100 ---
const lmtRaw: Array<number | null> = [497.07, 542.92, 582.43, 590.82, 634.22, 623.58, 652.58, 658.26, 658.08, 671.77, 646.00, 627.43, 615.84, 622.79, 613.72, 592.19, 513.45, 512.77, 506.51, 516.01, 533.24, 530.45, 523.76, 540.33, 510.95, 507.40];
const nocRaw: Array<number | null> = [585.66, 618.82, 666.90, 672.95, 692.26, 709.11, 702.57, 723.56, 724.38, 756.13, 733.71, 706.95, 679.00, 702.50, 673.73, 665.26, 575.11, 568.14, 549.52, 540.69, 555.58, 563.68, 544.40, 550.33, 521.50, 500.03];
const rnmbyRaw: Array<number | null> = [376.03, 442.18, 442.81, 433.47, 421.32, 379.00, 383.51, 409.51, 394.79, 369.11, 364.50, 344.90, 315.70, 361.60, 341.86, 354.00, 314.09, 319.71, 284.75, 260.40, 283.60, 300.25, 276.20, 277.91, 270.25, 215.68];
const esltRaw: Array<number | null> = [591.96, 683.36, 730.84, 718.99, 702.57, 665.00, 676.43, 724.73, 769.04, 936.14, 871.11, 920.75, 869.82, 888.97, 925.24, 872.58, 821.96, 831.22, 782.21, 750.01, 767.82, 880.89, 823.36, 854.06, 788.23, 715.72];
const itaRaw: Array<number | null> = [222.01, 232.97, 243.77, 235.07, 232.38, 233.93, 234.87, 243.65, 243.72, 242.20, 229.34, 222.56, 216.04, 221.91, 229.64, 231.94, 215.80, 216.27, 223.49, 217.27, 225.37, 235.44, 229.45, 233.79, 238.99, 236.78];

const defenseIndexedSeries = [
  { name: "LMT (US)",         data: rebaseToFeb27(lmtRaw).map((v)   => (v == null ? 0 : v)), tone: "danger"  as const },
  { name: "NOC (US)",         data: rebaseToFeb27(nocRaw).map((v)   => (v == null ? 0 : v)), tone: "warning" as const },
  { name: "Rheinmetall (DE)", data: rebaseToFeb27(rnmbyRaw).map((v) => (v == null ? 0 : v)), tone: "info"    as const },
  { name: "Elbit (IL)",       data: rebaseToFeb27(esltRaw).map((v)  => (v == null ? 0 : v)), tone: "success" as const },
  { name: "ITA (US ETF)",     data: rebaseToFeb27(itaRaw).map((v)   => (v == null ? 0 : v)), tone: "neutral" as const },
];

const defenseRows: Array<{
  ticker: string; name: string; country: string; pre: string; latest: string; pct: string; tone?: "success" | "danger" | "warning";
}> = [
  { ticker: "ITA", name: "iShares US Aerospace ETF", country: "US ETF", pre: "243.72", latest: "236.78", pct: "-2.85%" },
  { ticker: "GD", name: "General Dynamics", country: "United States", pre: "357.05", latest: "346.71", pct: "-2.90%" },
  { ticker: "ESLT", name: "Elbit Systems", country: "Israel (Tel Aviv / NASDAQ)", pre: "769.04", latest: "715.72", pct: "-6.93%", tone: "warning" },
  { ticker: "RTX", name: "RTX (Raytheon)", country: "United States", pre: "202.62", latest: "187.99", pct: "-7.22%", tone: "warning" },
  { ticker: "BAESY", name: "BAE Systems (ADR)", country: "United Kingdom", pre: "116.00", latest: " 94.91", pct: "-18.18%", tone: "danger" },
  { ticker: "THLLY", name: "Thales (ADR)", country: "France", pre: " 60.97", latest: " 49.07", pct: "-19.52%", tone: "danger" },
  { ticker: "LHX", name: "L3Harris", country: "United States", pre: "364.54", latest: "291.25", pct: "-20.10%", tone: "danger" },
  { ticker: "FINMY", name: "Leonardo (ADR)", country: "Italy", pre: " 33.49", latest: " 26.23", pct: "-21.68%", tone: "danger" },
  { ticker: "LMT", name: "Lockheed Martin", country: "United States", pre: "658.08", latest: "507.40", pct: "-22.90%", tone: "danger" },
  { ticker: "MHVYF", name: "Mitsubishi Heavy (PNK)", country: "Japan", pre: " 31.73", latest: " 22.15", pct: "-30.19%", tone: "danger" },
  { ticker: "NOC", name: "Northrop Grumman", country: "United States", pre: "724.38", latest: "500.03", pct: "-30.97%", tone: "danger" },
  { ticker: "SAABY", name: "Saab AB (ADR)", country: "Sweden", pre: " 36.24", latest: " 24.97", pct: "-31.10%", tone: "danger" },
  { ticker: "HII", name: "Huntington Ingalls", country: "United States", pre: "444.52", latest: "281.99", pct: "-36.56%", tone: "danger" },
  { ticker: "RNMBY", name: "Rheinmetall (ADR)", country: "Germany", pre: "394.79", latest: "215.68", pct: "-45.37%", tone: "danger" },
];

// --- Energy reserves snapshot (Energy Institute Statistical Review 2024 + EIA / OPEC) ---
const oilReserves: Array<{ country: string; bbl: number; pct: number; hormuz: boolean; sanctioned: boolean; tone?: "warning" | "danger" | "info" }> = [
  { country: "Venezuela",     bbl: 303.2, pct: 17.3, hormuz: false, sanctioned: true,  tone: "info" },
  { country: "Saudi Arabia",  bbl: 267.2, pct: 15.3, hormuz: true,  sanctioned: false, tone: "warning" },
  { country: "Iran",          bbl: 208.6, pct: 11.9, hormuz: true,  sanctioned: true,  tone: "danger" },
  { country: "Canada",        bbl: 163.6, pct:  9.3, hormuz: false, sanctioned: false },
  { country: "Iraq",          bbl: 145.0, pct:  8.3, hormuz: true,  sanctioned: false, tone: "warning" },
  { country: "UAE",           bbl: 111.0, pct:  6.3, hormuz: true,  sanctioned: false, tone: "warning" },
  { country: "Kuwait",        bbl: 101.5, pct:  5.8, hormuz: true,  sanctioned: false, tone: "warning" },
  { country: "Russia",        bbl:  80.0, pct:  4.6, hormuz: false, sanctioned: true,  tone: "info" },
  { country: "Libya",         bbl:  48.4, pct:  2.8, hormuz: false, sanctioned: false },
  { country: "United States", bbl:  47.1, pct:  2.7, hormuz: false, sanctioned: false },
  { country: "Nigeria",       bbl:  36.9, pct:  2.1, hormuz: false, sanctioned: false },
  { country: "Kazakhstan",    bbl:  30.0, pct:  1.7, hormuz: false, sanctioned: false },
  { country: "China",         bbl:  26.0, pct:  1.5, hormuz: false, sanctioned: false },
  { country: "Qatar",         bbl:  25.2, pct:  1.4, hormuz: true,  sanctioned: false, tone: "warning" },
  { country: "Brazil",        bbl:  14.9, pct:  0.9, hormuz: false, sanctioned: false },
];

const gasReserves: Array<{ country: string; tcf: number; pct: number }> = [
  { country: "Russia",        tcf: 1320, pct: 18.2 },
  { country: "Iran",          tcf: 1200, pct: 16.5 },
  { country: "Qatar",         tcf:  870, pct: 12.0 },
  { country: "Turkmenistan",  tcf:  500, pct:  6.9 },
  { country: "United States", tcf:  446, pct:  6.1 },
  { country: "Saudi Arabia",  tcf:  319, pct:  4.4 },
  { country: "China",         tcf:  233, pct:  3.2 },
  { country: "UAE",           tcf:  209, pct:  2.9 },
  { country: "Nigeria",       tcf:  207, pct:  2.9 },
  { country: "Venezuela",     tcf:  198, pct:  2.7 },
  { country: "Algeria",       tcf:  159, pct:  2.2 },
  { country: "Iraq",          tcf:  124, pct:  1.7 },
];

// --- US Strategic Petroleum Reserve weekly time series (EIA WPSR / WCSSTUS1) ---
// Source: data/raw/eia_spr_snapshot/2026-04-29.json (hand-transcribed; FRED does
// not host EIA WPSR series — see methodology). Chart cadence is quarterly through
// 2025 and weekly from late January 2026 onward to make the war-era drawdown legible.
const sprChartCats = [
  "Q1'20", "Q2'20", "Q3'20", "Q4'20",
  "Q1'21", "Q2'21", "Q3'21", "Q4'21",
  "Q1'22", "Q2'22", "Q3'22", "Q4'22",
  "Q1'23", "Q2'23", "Q3'23", "Q4'23",
  "Q1'24", "Q2'24", "Q3'24", "Q4'24",
  "Q1'25", "Q2'25", "Q3'25", "Q4'25",
  "Jan 30 '26", "Feb 6", "Feb 13", "Feb 20",
  "Feb 27 (war)", "Mar 6", "Mar 13", "Mar 20",
  "Mar 27", "Apr 3", "Apr 10", "Apr 17", "Apr 24",
  "May 1", "May 8", "May 15", "May 22", "May 29",
  "Jun 5", "Jun 12", "Jun 19",
];

// US SPR weekly stocks in Mbbl (EIA WCSSTUS1 / 1000).
const sprMbblRaw: number[] = [
  634.967, 634.967, 656.023, 642.006,
  637.773, 637.773, 621.304, 617.770,
  593.682, 564.580, 492.028, 408.699,
  371.175, 347.159, 351.280, 354.388,
  363.641, 372.595, 382.553, 393.570,
  396.434, 402.765, 406.700, 413.219,
  415.213, 415.212, 415.441, 415.441,
  415.441, 415.442, 415.442, 415.442,
  415.064, 413.325, 409.181, 405.045, 397.924,
  392.700, 384.095, 374.175, 365.112, 357.119,
  349.192, 340.251, 331.191,
];

const sprSeries = [
  { name: "US SPR (Mbbl)", data: sprMbblRaw, tone: "warning" as const },
];

// --- Strategic reserves & days of import cover snapshot (IEA / OECD) ---
// Source: data/raw/iea_emergency_stocks_snapshot/2026-04-29.json
// `days` is days of net-import cover (per IEA methodology). Net exporters
// have `days = null` — IEA metric is undefined for them.
type CoverRow = {
  country: string;
  days: number | null;
  totalMbbl: number | null;
  ieaMember: boolean;
  netImporter: boolean;
  asOf: string;
  note?: string;
  tone?: "danger" | "warning" | "success" | "info";
};

const coverRows: CoverRow[] = [
  { country: "Japan",         days: 175,  totalMbbl: 200,   ieaMember: true,  netImporter: true,  asOf: "2026-06", note: "Released 80 Mbbl from national + industry reserves starting Mar 16 (PM Takaichi acted ahead of formal IEA approval). Crude imports also cut by ~1.9 Mbpd Feb \u2192 Apr per IEA OMR, so days-of-cover only fell ~25 days. Stocks ~263 \u2192 200 Mbbl.", tone: "warning" },
  { country: "South Korea",   days: 185,  totalMbbl: 70,    ieaMember: true,  netImporter: true,  asOf: "2026-06", note: "Imports cut ~1 Mbpd Feb \u2192 Apr (IEA OMR); estimated ~9 Mbbl drawn as part of IEA release. Cover ~208 \u2192 185 days.", tone: "warning" },
  { country: "Netherlands",   days: 152,  totalMbbl: null,  ieaMember: true,  netImporter: true,  asOf: "2026-06", note: "Logistical hub; drew alongside IEA release (~165 \u2192 152 days estimated)." },
  { country: "Spain",         days: 130,  totalMbbl: null,  ieaMember: true,  netImporter: true,  asOf: "2026-06", note: "Drew alongside IEA release (~145 \u2192 130 days estimated)." },
  { country: "Germany",       days: 125,  totalMbbl: null,  ieaMember: true,  netImporter: true,  asOf: "2026-06", note: "Drew ~10-15 Mbbl as part of the IEA release; cover ~140 \u2192 125 days estimated." },
  { country: "China",         days: 115,  totalMbbl: 1240,  ieaMember: false, netImporter: true,  asOf: "2026-06", note: "Drew ~1 Mbpd from May through Jun on Vortexa/Kpler estimates as imports hit decade-low. Combined SPR+commercial ~1,390 \u2192 1,240 Mbbl (~150 Mbbl draw). Crude imports cut ~3.6 Mbpd Feb \u2192 Apr (IEA OMR); May refinery throughput 12.66 Mbpd \u2014 lowest since Aug 2022. Beijing has NOT yet authorized strategic-reserve releases; only commercial draws.", tone: "warning" },
  { country: "France",        days: 108,  totalMbbl: null,  ieaMember: true,  netImporter: true,  asOf: "2026-06", note: "Drew alongside IEA release; cover ~120 \u2192 108 days estimated." },
  { country: "Italy",         days: 102,  totalMbbl: null,  ieaMember: true,  netImporter: true,  asOf: "2026-06", note: "Cover ~115 \u2192 102 days estimated; still above the 90-day IEA obligation." },
  { country: "United Kingdom", days: 85,  totalMbbl: null,  ieaMember: true,  netImporter: true,  asOf: "2026-06", note: "Cover ~95 \u2192 85 days estimated; just below the 90-day IEA obligation after coordinated release.", tone: "warning" },
  { country: "India",         days:  70,  totalMbbl: 25,    ieaMember: false, netImporter: true,  asOf: "2026-06", note: "Did NOT join the IEA coordinated release (not a full IEA member). Imports cut ~0.76 Mbpd Feb \u2192 Apr (IEA OMR), buffering stock draws. SPR still ~3.37 MMT crude (~24.6 Mbbl, 9.5 days strategic at full capacity). Govt ordered ONGC to build a new 1.75 MMT (~13 Mbbl) reserve in Mangaluru. Total cover incl. commercial OMC stocks ~74 \u2192 70 days, below 90-day IEA standard. ~2.5-2.7 Mbpd of crude imports (half of total) historically transit Hormuz.", tone: "danger" },
  { country: "Australia",     days:  42,  totalMbbl: null,  ieaMember: true,  netImporter: true,  asOf: "2026-06", note: "Cover ~49 \u2192 42 days. Mar 13: gov released 20% of Minimum Stockholding Obligation (~5 Mbbl, ~7 days worth) into the domestic market. May: $10B Australian Fuel Security Reserve package committed for ~1 billion litres (~6.3 Mbbl) new gov-owned reserve + raised MSO by ~10 days for every fuel. Still deepest breach of the 90-day IEA obligation; in breach since 2012, but the net move was smaller than feared because consumption fell alongside.", tone: "danger" },
  { country: "United States", days: null, totalMbbl: 331.2, ieaMember: true,  netImporter: false, asOf: "2026-06-19", note: "Net exporter since 2020; IEA days metric n/a. SPR shown for context (415.4 Mbbl on Feb 27 → 331.2 Mbbl on Jun 19, an 84.3 Mbbl drawdown over 16 weeks, fastest pace since 2022).", tone: "danger" },
  { country: "Saudi Arabia",  days: null, totalMbbl: 82,    ieaMember: false, netImporter: false, asOf: "2026-04",    note: "Net exporter; on-land oil (excludes leased storage in JP/KR)." },
  { country: "Iran",          days: null, totalMbbl: 71,    ieaMember: false, netImporter: false, asOf: "2026-04",    note: "Net exporter (sanctioned); on-land oil only." },
  { country: "UAE",           days: null, totalMbbl: 34,    ieaMember: false, netImporter: false, asOf: "2026-04",    note: "Net exporter; excludes Fujairah underground + leased storage abroad." },
  { country: "Brazil",        days: null, totalMbbl: null,  ieaMember: false, netImporter: false, asOf: "2026-01",    note: "Net exporter; no formal SPR. Commercial industry stocks ~30 days." },
  { country: "Russia",        days: null, totalMbbl: null,  ieaMember: false, netImporter: false, asOf: "2026-01",    note: "Net exporter." },
];

// BarChart shows only the IEA-defined "days of net-import cover" — net exporters
// are undefined and are intentionally omitted from the bar (kept in the table).
const coverChartRows = coverRows
  .filter((r) => r.days != null)
  .sort((a, b) => (b.days as number) - (a.days as number));

const sprPreWar = 415.441;
const sprLatest = 331.191;
const sprDeltaMbbl = +(sprLatest - sprPreWar).toFixed(1); // -84.3
const sprDeltaPct = +(((sprLatest - sprPreWar) / sprPreWar) * 100).toFixed(2); // -20.28
const sprWeeksElapsed = 16;
const sprWeeklyAvgMbbl = +(sprDeltaMbbl / sprWeeksElapsed).toFixed(2); // -5.27

export default function IranWarMarketImpact() {
  return (
    <Stack gap={28}>
      <Stack gap={6}>
        <H1>Trump's war on Iran: market impact</H1>
        <Text tone="secondary">
          Day 121. A 14-point US-Iran interim agreement (Jun 15) briefly reopened the Strait of Hormuz and crushed the
          war premium in oil, but the ceasefire is collapsing again: drone strikes on commercial vessels Jun 25 → 27, US
          retaliatory strikes around Hormuz, and Iranian missile/drone attacks on US bases in Bahrain and Kuwait Jun 28.
          Every delta below still anchors on the Feb 27, 2026 close (last business day before the strikes began).
        </Text>
        <Row gap={8} wrap>
          <Pill tone="info" active>Brent +7.2% (war premium evaporated)</Pill>
          <Pill tone="warning" active>WTI +17.9%</Pill>
          <Pill tone="danger" active>US gasoline +34.9%</Pill>
          <Pill tone="warning" active>Henry Hub gas +5.7%</Pill>
          <Pill tone="danger" active>Spot gold -22.7%</Pill>
          <Pill tone="info" active>USD broad +2.2%</Pill>
          <Pill tone="success" active>CNY -1.30% / CNH -1.04% (yuan stronger)</Pill>
          <Pill tone="danger" active>Defense primes -19 to -45%</Pill>
          <Pill tone="warning" active>Elbit (Israel) -6.9%</Pill>
          <Pill tone="danger" active>US SPR -84.3 Mbbl since Feb 27 (-20.3%)</Pill>
          <Pill tone="success" active>S&P +6.9% / QQQ +16.3%</Pill>
          <Pill tone="warning" active>BTC -11.0%</Pill>
          <Pill tone="success">Israeli shekel +4.1%</Pill>
          <Pill tone="danger">Iranian rial ~-55% (free-market, news-cited)</Pill>
        </Row>
      </Stack>

      <Grid columns={4} gap={12}>
        {headline.map((h) => (
          <Stat
            key={h.label}
            value={h.value}
            label={`${h.label}  ${h.pct}`}
            tone={h.tone}
          />
        ))}
      </Grid>

      <Callout tone="warning" title="Changes since the 2026-04-29 publication">
        Two macro stories rewrote the dashboard between the prior pub and today.
        First, the war premium evaporated: the Jun 15 14-point US-Iran interim agreement reopened Hormuz for 60 days, hundreds of blockaded
        tankers transited, and <Text as="span" weight="semibold">Brent fell 38% from $124 → $76</Text> (WTI -28%, USO -28%, BNO -31%) while
        <Text as="span" weight="semibold"> Henry Hub gas actually flipped +22%</Text> as the storage build moderated. US gasoline
        retail is still elevated (+34.9% vs Feb 27) but has eased 4.5% off the April peak. Second, the defense-sector unwind kept going:
        <Text as="span" weight="semibold"> Rheinmetall -32%, Brent ETF -31%, MHVYF -24%, HII -23%, SAABY -18%, Northrop -14%, Elbit -15%</Text>
        — a sell-the-event move that overshot even after the ceasefire. SPR drew an additional <Text as="span" weight="semibold">66.7 Mbbl</Text> over the 8 weeks
        Apr 24 → Jun 19, accelerating the pace from -2.2 to <Text as="span" weight="semibold">-5.3 Mbbl/wk</Text> (fastest since the 2022 Biden release).
        Spot gold sold off another 10.5% as the safety bid faded; BTC gave back 21% as risk-on rotated into equities (S&P +3.1%, QQQ +5.8%).
        Iranian rial continued to crater on the free market: 1.05M → 1.60M IRR/USD (-52% rial purchasing power vs prior pub).
        Late-June re-escalation (Jun 25-28 strikes on US bases + commercial shipping) is not yet in the FRED Brent print (last close Jun 22 at $76.49).
      </Callout>

      <Divider />

      <Stack gap={10}>
        <H2>Energy: war premium gone, but gasoline still elevated</H2>
        <Text tone="secondary">
          The shock has fully unwound in crude. Brent peaked at ~$138 (Apr 7 close), was at ~$114 in the prior pub, and now
          sits at $76.49 (Jun 22 close, FRED) — within 7% of the pre-war anchor. The Jun 15 MoU and Hormuz reopening let
          hundreds of blockaded tankers exit the Gulf, crushing the war premium. US gasoline retail has only partially
          followed (still +34.9% vs pre-war), and Henry Hub gas — which actually fell during the war — has flipped
          positive (+5.7%). Gold remains off its early-March anticipation peak (-22.7%), the safety bid having unwound
          even faster than the energy premium. Late-June re-escalation (Jun 25-28 strikes around Hormuz, IRGC attacks
          on US bases in Bahrain/Kuwait) is not yet reflected in this print.
        </Text>
        <LineChart
          categories={energyChartCats}
          series={energyIndexedSeries}
          height={280}
          valueSuffix=" (Feb 27 = 100)"
        />
        <Table
          headers={["Series", "Pre-war (Feb 27)", "Latest", "Δ%", "Source"]}
          rows={energyRows.map((r) => [r.label, r.pre, r.latest, r.pct, r.src])}
          rowTone={energyRows.map((r) => r.tone)}
          columnAlign={["left", "right", "right", "right", "left"]}
        />
      </Stack>

      <Divider />

      <Stack gap={10}>
        <H2>Dollar &amp; FX: a modest broad-USD bid, with painful exceptions</H2>
        <Text tone="secondary">
          The USD broad index is up only 2.2%. The asymmetry inside the basket is the story: Asian and European oil
          importers (USD/SEK +6.1%, USD/THB +5.6%, USD/KRW +7.0%, USD/INR +3.6%, USD/JPY +3.4%) absorbed the war's
          energy bill in a weaker currency, while the Israeli shekel STRENGTHENED 4.1% — the markets continue to price
          Iran as the bigger loser of the war and ceasefire-then-MoU cycle. The Chinese yuan (DEXCHUS) actually firmed
          1.3% versus the dollar, an extraordinary outcome through a 60% oil shock and a 122% rial blowout next door.
          The Brazilian real gave back the oil-exporter premium it carried at the April peak as Brent collapsed.
          Convention: positive Δ% (USD) means the dollar strengthened vs that currency.
        </Text>
        <LineChart
          categories={energyChartCats}
          series={fxIndexedSeries}
          height={240}
          valueSuffix=" (Feb 27 = 100)"
        />
        <Table
          headers={["Pair", "Pre-war (Feb 27)", "Latest", "Δ% (USD)", "Note"]}
          rows={fxRows.map((r) => [r.label, r.pre, r.latest, r.pctUsd, r.note])}
          rowTone={fxRows.map((r) => r.tone)}
          columnAlign={["left", "right", "right", "right", "left"]}
        />
        <Callout tone="info" title="Iranian rial caveat">
          Iran has had no realistic single official USD/IRR for years; the figure shown is the unofficial Tehran
          free-market rate (Bonbast / Pashizi / news cites). It moved from ~720,000 IRR/USD on Feb 27 to ~1.05M by the
          Apr 29 prior publication, peaked at ~1.93M on May 4 (Pashizi historical extreme), and is at ~1.60M as of
          Jun 22. That is a ~55% loss of rial purchasing power against the dollar over the 4-month war window. We
          deliberately do not put it on the chart because it is not from the same primary-source pipeline as everything
          else on this canvas.
        </Callout>
      </Stack>

      <Divider />

      <Stack gap={10}>
        <H2>Chinese yuan: the dog that didn't bark</H2>
        <Text tone="secondary">
          Even through a 60%+ oil shock at peak and a 2.2% USD broad bid, the yuan held. Onshore CNY (FRED DEXCHUS) is
          at 6.7686 vs 6.8579 pre-war — <Text as="span" weight="semibold">stronger by 1.30%</Text>. Offshore CNH (TwelveData USD/CNH)
          sits at 6.8014 vs 6.8727 pre-war, stronger by 1.04%. The onshore/offshore gap is now ~33 pips with CNY firmer
          than CNH — PBoC fixings have continued to set a floor, and there is no speculative wedge. Beijing is
          choosing to absorb the entire energy import bill via FX strength rather than let the yuan signal weakness.
        </Text>
        <Grid columns={4} gap={12}>
          <Stat value="6.8579" label="CNY pre-war (Feb 27)" />
          <Stat value="6.7686" label="CNY latest (Jun 18)  -1.30%"  tone="success" />
          <Stat value="6.8727" label="CNH pre-war (Feb 27)" />
          <Stat value="6.8014" label="CNH latest (Jun 28)  -1.04%"  tone="success" />
        </Grid>
        <LineChart
          categories={energyChartCats}
          series={yuanIndexedSeries}
          height={200}
          valueSuffix=" (Feb 27 = 100)"
        />
        <Text tone="tertiary" size="small">
          Read: when CNH trades materially weaker than CNY, the offshore market is testing PBoC's defense of the floor
          (last seen ~Aug 2023). When CNH trades right on top of CNY through a global energy shock, that is the PBoC
          telling you it has the firepower and the political will to absorb the move. Beijing is choosing to eat the
          import-cost inflation rather than let the yuan signal weakness. China has its own thesis on this war.
        </Text>
      </Stack>

      <Divider />

      <Stack gap={10}>
        <H2>Agriculture &amp; fertilizer: wheat the standout</H2>
        <Text tone="secondary">
          The post-war monthly prints have landed and they confirm the directional read: wheat IMF is up 26% vs pre-war,
          fertilizer PPI is up 22%, and soybeans are up 7%. The corresponding daily ETFs (WEAT, CORN, SOYB) have already
          mostly faded the move as crude rolled over and Hormuz reopened, but the monthly cash benchmarks lag and the
          stored cost of inputs (fertilizer, bunker fuel, freight) is the inflation that will be paid through next
          harvest. Agribusiness equities (MOO) are down 7.4% as margin compression continues.
        </Text>
        <Table
          headers={["Series", "Pre-war (Feb 27)", "Latest", "Δ%", "Source"]}
          rows={agRows.map((r) => [r.label, r.pre, r.latest, r.pct, r.src])}
          rowTone={agRows.map((r) => r.tone)}
          columnAlign={["left", "right", "right", "right", "left"]}
        />
        <Text tone="tertiary" size="small">
          Cadence caveat: the four FRED commodity series (PWHEAMTUSDM, PMAIZMTUSDM, PSOYBUSDM, WPU0652013A) are
          monthly. Latest observation date is May 1, 2026 — three post-war prints now. Direction is settled; the next
          one (June print, due July) will be the first read after the Jun 15 Hormuz reopening.
        </Text>
      </Stack>

      <Divider />

      <Stack gap={10}>
        <H2>Equities &amp; risk assets</H2>
        <Grid columns={3} gap={12}>
          <Stat value="+6.91%"  label="S&P 500 (FRED SP500)"          tone="success" />
          <Stat value="+11.60%" label="NASDAQ Composite"              tone="success" />
          <Stat value="+5.92%"  label="Dow Jones Industrial Average"  tone="success" />
          <Stat value="+6.27%"  label="SPY (TwelveData)"              tone="success" />
          <Stat value="+16.34%" label="QQQ (TwelveData)"              tone="success" />
          <Stat value="-11.04%" label="Bitcoin (BTC/USD)"             tone="warning" />
        </Grid>
        <Text tone="secondary">
          Risk-on. With the war premium gone in crude, breadth has actually broadened: Dow industrials went from flat to
          +5.9%, IWM (small caps) +14.7%, QQQ +16.3%. Bitcoin gave back its digital-gold premium (-11.0%) as the safety
          bid unwound across asset classes. The composite read flipped from "mild stagflation" in the prior pub to
          "soft-landing with re-escalation tail risk": real-economy inputs (gasoline, wheat, fertilizer) are still
          elevated, but financial assets are pricing the ceasefire holding rather than the late-June flare-up.
        </Text>
      </Stack>

      <Divider />

      <Stack gap={10}>
        <H2>Defense stocks: the war was already priced in</H2>
        <Text tone="secondary">
          The sell-the-event move kept going. Every prime contractor on the planet is now in the red versus the Feb 27
          pre-war anchor: US primes are down 7-37% (HII -37%, NOC -31%, LMT -23%, LHX -20%, RTX -7%); European primes
          are down 18-45% (Rheinmetall -45%, Saab -31%, Leonardo -22%, Thales -20%, BAE -18%). Even
          <Text as="span" weight="semibold">Elbit Systems</Text>, the lone counter-trend through April, has rolled over
          (-6.9% vs pre-war from +6.4% in the prior pub). The Jun 15 ceasefire/MoU accelerated the unwind on the read
          that the rearmament cycle priced in late 2025 was the high. The Jun 25-28 re-escalation has not yet shown up
          in tape.
        </Text>
        <Grid columns={4} gap={12}>
          <Stat value="-22.9%" label="LMT (Lockheed)"      tone="danger"  />
          <Stat value="-31.0%" label="NOC (Northrop)"      tone="danger"  />
          <Stat value="-45.4%" label="RNMBY (Rheinmetall)" tone="danger"  />
          <Stat value="-2.8%"  label="ITA (US def. ETF)"   />
          <Stat value="-7.2%"  label="RTX"                 tone="warning" />
          <Stat value="-31.1%" label="SAABY (Saab)"        tone="danger"  />
          <Stat value="-6.9%"  label="ESLT (Elbit, Israel)" tone="warning" />
          <Stat value="-36.6%" label="HII (Huntington Ingalls)" tone="danger" />
        </Grid>
        <LineChart
          categories={energyChartCats}
          series={defenseIndexedSeries}
          height={260}
          valueSuffix=" (Feb 27 = 100)"
        />
        <Table
          headers={["Ticker", "Company", "Country", "Pre-war (Feb 27)", "Latest", "Δ%"]}
          rows={defenseRows.map((r) => [r.ticker, r.name, r.country, r.pre, r.latest, r.pct])}
          rowTone={defenseRows.map((r) => r.tone)}
          columnAlign={["left", "left", "left", "right", "right", "right"]}
          striped
        />
        <Callout tone="info" title="Coverage gap (free-tier API limits)">
          Russian (Almaz-Antey, Tactical Missiles Corp), Chinese (AVIC, NORINCO), and Korean (Hanwha Aerospace,
          KAI) defense names are either unlisted, sanctioned, or unavailable on TwelveData's free tier. Mitsubishi
          Heavy is on a US pink-sheet (MHVYF) so liquidity is thin. The European primes are pulled via ADRs (RNMBY,
          FINMY, THLLY, BAESY, SAABY) because RHM.DE / LDO.MI / HO.PA / SAAB-B.ST require a paid TwelveData plan; ADR
          tracking error vs the home listing is small but non-zero.
        </Callout>
      </Stack>

      <Divider />

      <Stack gap={10}>
        <H2>War timeline (data context only)</H2>
        <Stack gap={4}>
          {timeline.map((t) => (
            <Row key={t.date} gap={12} align="start">
              <Text tone="tertiary" weight="semibold" style={{ minWidth: 110 }}>
                {t.date}
              </Text>
              <Text>{t.text}</Text>
            </Row>
          ))}
        </Stack>
      </Stack>

      <Divider />

      <Stack gap={10}>
        <H2>Proved geological reserves: who has the long-term leverage</H2>
        <Text tone="secondary">
          This section is structural, not strategic — it answers "who holds the world's oil and gas in the ground" but
          says nothing about who can actually weather a six-month supply shock today. For that, see the next section
          on strategic reserves and days of import cover. The reserves table is the structural reason this war moves
          Brent 60% on a six-week shock: six countries ringing the Strait of Hormuz —
          <Text as="span" weight="semibold"> Saudi Arabia, Iran, Iraq, UAE, Kuwait, Qatar</Text> — together hold
          <Text as="span" weight="semibold"> ~49% of world proved oil reserves</Text> and ship roughly 20% of world
          crude through that single 39 km waterway. Add Iran's gas position (1,200 Tcf, #2 globally) and Qatar's LNG
          (870 Tcf, #3) and Hormuz also gates ~20% of world LNG. This is not a tactical war over a target list; it is
          a war over the chokepoint to the global energy supply.
        </Text>
        <Grid columns={3} gap={12}>
          <Stat value="49%"   label="Hormuz littoral states' share of world oil reserves"    tone="danger" />
          <Stat value="11.9%" label="Iran's share of world oil reserves (#3 globally)"       tone="warning" />
          <Stat value="16.5%" label="Iran's share of world gas reserves (#2 globally)"       tone="warning" />
        </Grid>
        <H3>Top 15 countries — proved crude oil reserves (billion barrels, end-2023)</H3>
        <BarChart
          categories={oilReserves.map((r) => r.country)}
          series={[{ name: "Proved oil reserves", data: oilReserves.map((r) => r.bbl) }]}
          horizontal
          height={420}
          valueSuffix=" Bbbl"
        />
        <Table
          headers={["Country", "Oil (Bbbl)", "% of world", "Nat gas (Tcf)", "Hormuz transit", "US-sanctioned"]}
          rows={oilReserves.map((r) => {
            const gas = gasReserves.find((g) => g.country === r.country);
            return [
              r.country,
              r.bbl.toFixed(1),
              r.pct.toFixed(1) + "%",
              gas ? gas.tcf.toString() : "—",
              r.hormuz ? "Yes" : "—",
              r.sanctioned ? "Yes" : "—",
            ];
          })}
          rowTone={oilReserves.map((r) => r.tone)}
          columnAlign={["left", "right", "right", "right", "left", "left"]}
          striped
        />
        <H3>Top 12 countries — proved natural gas reserves (Tcf, end-2023)</H3>
        <BarChart
          categories={gasReserves.map((r) => r.country)}
          series={[{ name: "Proved gas reserves", data: gasReserves.map((r) => r.tcf) }]}
          horizontal
          height={360}
          valueSuffix=" Tcf"
        />
        <Callout tone="info" title="Snapshot, not feed">
          Reserves are reported with a one-year lag and change ~1% annually. Values are end-2023 from the Energy
          Institute Statistical Review of World Energy 2024, cross-checked against OPEC ASB 2024 and EIA International
          Energy Statistics web tables. The EIA bulk download (`INTL.zip`, no key required) ships only coal-reserve
          series; crude-oil and dry-gas reserves require the EIA v2 API key. We saved a JSON snapshot at
          `data/raw/eia_reserves_snapshot/2026-04-29.json` for provenance. Venezuela's 303 Bbbl reflects officially
          reported "proved" reserves including extra-heavy Orinoco belt crude that requires upgrading to be
          competitive at scale — the practical export-grade reserve is much smaller, which is why Saudi Arabia is
          the world's #1 in operating terms despite being #2 on this paper ranking.
        </Callout>
      </Stack>

      <Divider />

      <Stack gap={10}>
        <H2>Strategic reserves &amp; days of import cover: who can ride this out</H2>
        <Text tone="secondary">
          Proved reserves measure the oil that's still in the ground; strategic reserves measure the oil you can
          actually deliver to a refinery tomorrow. IEA member countries are obligated to hold at least
          <Text as="span" weight="semibold"> 90 days of net-import cover</Text> in emergency stocks (public + industry +
          bilateral). On <Text as="span" weight="semibold">March 11, 2026</Text>, IEA members agreed to a coordinated
          release of 400 Mbbl — the largest in the agency's history. The bar chart below shows where each major
          economy actually stands; the line chart shows what the war has done to the world's largest single stockpile,
          the US Strategic Petroleum Reserve.
        </Text>
        <Grid columns={4} gap={12}>
          <Stat value={sprLatest.toFixed(1)} label="US SPR latest (Mbbl, Jun 19)" tone="danger" />
          <Stat value={sprDeltaMbbl.toFixed(1) + " Mbbl"} label={`Δ since Feb 27 war start (${sprDeltaPct.toFixed(2)}%)`} tone="danger" />
          <Stat value={sprWeeklyAvgMbbl.toFixed(1) + " Mbbl/wk"} label="Avg weekly draw, Mar 27 → Jun 19 (16 wks)" tone="danger" />
          <Stat value="252 / 400 Mbbl" label="IEA coordinated release — 252 Mbbl delivered by Jun 12 (IEA OMR Jun)" tone="warning" />
        </Grid>
        <H3>US Strategic Petroleum Reserve: from Biden drawdown to Trump war draw</H3>
        <LineChart
          categories={sprChartCats}
          series={sprSeries}
          height={260}
          valueSuffix=" Mbbl"
        />
        <Text tone="tertiary" size="small">
          The 2022 Biden drawdown took the SPR from ~594 to ~372 Mbbl (a 220 Mbbl release over the year). A slow refill
          followed through 2023-2025, returning the SPR to ~415 Mbbl just before the war. The post-war draw has now
          accelerated dramatically: <Text as="span" weight="semibold">84.3 Mbbl over 16 weeks (~5.3 Mbbl/wk)</Text>,
          eclipsing even the 2022 pace. At Jun 19's level of 331.2 Mbbl, the SPR is back to mid-2023 levels and would
          hit the 2023 trough (~347 Mbbl) within weeks at the current pace. The Jun 15 MoU and Hormuz reopening should
          slow the draw — the next 2-3 weekly WPSR prints will tell whether the war premium is genuinely gone or just
          paused.
        </Text>
        <H3>Days of net-import oil cover, by country</H3>
        <BarChart
          categories={coverChartRows.map((r) => r.country)}
          series={[{ name: "Days of net-import cover", data: coverChartRows.map((r) => r.days as number) }]}
          horizontal
          height={360}
          valueSuffix=" days"
        />
        <Text tone="tertiary" size="small">
          90-day line: the IEA minimum obligation. Net oil exporters (US, Saudi Arabia, Russia, Brazil, UAE, Iran) are
          excluded from this chart — the metric is undefined for them — but appear in the table below with their
          on-land stocks.
        </Text>
        <Table
          headers={["Country", "Days of cover", "Total stocks (Mbbl)", "IEA member", "Net importer", "As of", "Notes"]}
          rows={coverRows.map((r) => [
            r.country,
            r.days != null ? r.days.toString() + " days" : "n/a",
            r.totalMbbl != null ? r.totalMbbl.toFixed(1) : "—",
            r.ieaMember ? "Yes" : "—",
            r.netImporter ? "Yes" : "Net exporter",
            r.asOf,
            r.note ?? "",
          ])}
          rowTone={coverRows.map((r) => r.tone)}
          columnAlign={["left", "right", "right", "left", "left", "left", "left"]}
          striped
        />
        <Stack gap={8}>
          <Text>
            <Text as="span" weight="semibold">The OECD blanket has been tapped, but less than you'd expect for a 4-month Hormuz closure.</Text>
            The IEA Oil Market Report (Jun 2026) confirms <Text as="span" weight="semibold">252 of the 400 Mbbl</Text> coordinated
            release have hit the market through Jun 12, with the flow expected to decelerate Jun-Jul after the MoU.
            <Text as="span" weight="semibold"> OECD government stocks fell 163 Mbbl Mar → May</Text> (lowest absolute level
            since Dec 1990), and aggregate OECD days-of-cover is on a trajectory from 90 pre-war to a forecast
            <Text as="span" weight="semibold"> 64 days by end-2026</Text> per Oxford OIES — about 13-15 days of cover
            burned mid-year. Japan led the country contributions at 80 Mbbl, US at 172 Mbbl total (84.3 Mbbl of which
            is the SPR draw, rest is industry stocks held under government obligation). The reason cover didn't fall
            more is the demand side: <Text as="span" weight="semibold">Asian importers slashed imports rather than chase
            war-priced barrels</Text> — China -3.6 Mbpd, Japan -1.9 Mbpd, Korea -1 Mbpd, India -0.76 Mbpd Feb → Apr
            (IEA OMR May), and refinery throughput is down ~5 Mbpd y-o-y in Q2. The shock was absorbed mostly by
            import substitution and demand destruction, not by burning down strategic stocks.
          </Text>
          <Text>
            <Text as="span" weight="semibold">The wildcards: China shielded itself with imports + commercial stocks; India still exposed.</Text>
            China cut seaborne crude imports by <Text as="span" weight="semibold">~3.6 Mbpd Feb → Apr</Text> (IEA OMR), the
            single biggest country-level demand response of the war, then drew commercial stocks at ~1 Mbpd from May to
            backfill what couldn't be sourced. May refinery throughput hit <Text as="span" weight="semibold">12.66 Mbpd</Text>
            — lowest since Aug 2022. Combined SPR + commercial fell from ~1.39 Bbbl pre-war to ~1.24 Bbbl (~150 Mbbl
            drawn). Beijing has NOT authorized strategic-reserve releases — the entire Chinese move is commercial
            stockdraw + a buyer strike. India remains the cautionary tale: ISPRL caverns still hold ~3.37 MMT crude
            (~9.5 days strategic full capacity, ~6 days currently) and India did NOT join the IEA coordinated release
            (not a full member). India cut imports ~0.76 Mbpd Feb → Apr but the cushion is shallow. New Delhi has
            ordered ONGC to build a 1.75 MMT (~13 Mbbl) new cavern in Mangaluru; the Phase II 6.5 MMT plan from 2021
            still has nothing online. ~2.5-2.7 Mbpd of India's crude imports (half of total) historically transit
            Hormuz.
          </Text>
          <Text>
            <Text as="span" weight="semibold">What the war has actually cost (per IEA OMR Jun 2026):</Text>{" "}
            Global observed oil stocks have drawn down at an average pace of <Text as="span" weight="semibold">3.8 Mbpd</Text> since
            Feb 28, with the pace accelerating to 4.6 Mbpd in May (143 Mbbl that month alone). Cumulative global stock
            draw through May is roughly <Text as="span" weight="semibold">470 Mbbl</Text>, of which 163 Mbbl came out
            of OECD government stocks (lowest absolute level since Dec 1990). The US SPR alone took 84.3 Mbbl (415.4
            → 331.2 Mbbl); Japan released 80 Mbbl; China drew ~150 Mbbl from commercial inventories at ~1 Mbpd from
            May; European IEA members collectively delivered the rest of the 252 Mbbl IEA-confirmed release. But the
            bigger story is the <Text as="span" weight="semibold">demand-side adjustment</Text>: refinery throughput
            fell ~5 Mbpd y-o-y in Q2 across China/Japan/Korea/Middle East, and global oil demand is now forecast to
            <Text as="span" weight="semibold"> contract 1.1 Mbpd in 2026</Text> (revised down 700 kbpd vs the May OMR).
            The strategic-reserves layer + the Jun 15 MoU is one reason Brent is at $76; the bigger reason is that
            the global consumer is buying less oil. Oxford OIES projects OECD days-of-cover finishing 2026 at ~64
            days (vs 90 pre-war), with a partial rebuild to ~80 days by end-2027.
          </Text>
        </Stack>
        <Callout tone="info" title="Snapshot, not feed">
          The US SPR weekly time series is hand-transcribed from the EIA Weekly Petroleum Status Report
          (<Text as="span" italic> data/raw/eia_spr_snapshot/2026-04-29.json</Text> + manual extension from
          eia.gov/dnav/pet for May 1 → Jun 19, 2026 weekly prints). FRED does not host the EIA WPSR series IDs
          (<Text as="span" italic>WCSSTUS1, WCESTUS1, WTTSTUS1</Text> all return HTTP 400 against the FRED API —
          confirmed at pull time). There is no EIA_API_KEY in the current .env so the live WPSR connector was not
          used this refresh. The cross-country days-of-cover figures are anchored to the
          <Text as="span" italic>IEA Oil Market Report, Jun 2026</Text> (OECD govt stocks down 163 Mbbl Mar→May,
          252 of 400 Mbbl release delivered by Jun 12) and <Text as="span" italic>Oxford Institute for Energy Studies
          Oil Monthly Issue 54</Text> (OECD aggregate path 90 → 64 days by end-2026). Country-level confirmed numbers
          (Japan 80 Mbbl release / -1.9 Mbpd imports, Korea -1 Mbpd, China -3.6 Mbpd / ~1 Mbpd commercial draw, India
          3.37 MMT ISPRL stock / -0.76 Mbpd imports, US 172 Mbbl contribution, Australia 20% MSO release + $10B Fuel
          Security Reserve) come from IEA OMR, Reuters, CNBC, Bloomberg, BusinessToday, ABC News, SBS News, The
          National, Energy News Beat reporting Mar→Jun 2026. Where a specific country has no published days-of-cover
          print for Jun 2026 (Netherlands, Spain, Germany, France, Italy, UK), the figure is a pro-rata estimate
          consistent with the OECD aggregate trajectory — each estimated row is flagged as such in the notes. A
          live EIA WPSR connector and a live IEA emergency-stocks connector are both parked on the DataHoover
          backlog.
        </Callout>
      </Stack>

      <Divider />

      <Stack gap={6}>
        <H3>Methodology &amp; sources</H3>
        <Text tone="secondary" size="small">
          Pre-war anchor: last available close on or before 2026-02-27. Latest: most recent observation in the warehouse
          as of {PULL_TS} (FRED daily series through 2026-06-22; TwelveData through 2026-06-25/28; FRED monthly
          through May 1; SPR through 2026-06-19). All "Δ% (USD)" values use the convention that a positive number means
          the dollar strengthened vs that currency. "Spot gold" and ETFs (USO, BNO, UNG, GLD, WEAT, CORN, SOYB, MOO,
          UUP, BTC/USD, USD/ILS) come from TwelveData; everything else from FRED. A handful of TwelveData pre-war
          anchors drift 0.1-0.5% vs the prior publication because TwelveData's free-tier 90-day rolling window has
          slid forward; we kept the fresh anchors for internal consistency.
        </Text>
        <Text tone="tertiary" size="small">
          Primary feeds: FRED series DCOILBRENTEU, DCOILWTICO, DHHNGSP, GASREGCOVW, DTWEXBGS, DTWEXAFEGS, DTWEXEMEGS,
          DEXUSEU, DEXUSUK, DEXUSAL, DEXJPUS, DEXMXUS, DEXBZUS, DEXINUS, DEXCHUS, DEXKOUS, DEXSDUS, DEXSFUS, DEXTHUS,
          DEXSIUS, DEXTAUS, SP500, DJIA, NASDAQCOM, PWHEAMTUSDM, PMAIZMTUSDM, PSOYBUSDM, WPU0652013A. TwelveData
          symbols: GLD, USO, BNO, UNG, WEAT, CORN, SOYB, MOO, UUP, XAU/USD, BTC/USD, USD/ILS, USD/CNH, SPY, QQQ, DIA,
          IWM, plus defense names LMT, RTX, NOC, GD, LHX, HII, ITA, BAESY, SAABY, RNMBY, FINMY, THLLY, ESLT, MHVYF.
        </Text>
        <Text tone="tertiary" size="small">
          Proved reserves snapshot: Energy Institute Statistical Review of World Energy 2024 (end-2023 vintage),
          cross-checked against OPEC ASB 2024 and EIA International Energy Statistics. Saved as
          <Text as="span" italic> data/raw/eia_reserves_snapshot/2026-04-29.json</Text> for provenance. Reserves
          change ~1% per year and are not pulled live; treat the table as structural context, not a market quote.
        </Text>
        <Text tone="tertiary" size="small">
          Strategic reserves: US SPR weekly stocks from EIA WPSR / WCSSTUS1, hand-transcribed to
          <Text as="span" italic> data/raw/eia_spr_snapshot/2026-04-29.json</Text> (FRED returns HTTP 400 for the
          WPSR series IDs). Cross-country days-of-cover from IEA Oil Stocks of IEA Countries data tool (Jan 2026
          reporting month) plus government filings for non-IEA China and India, saved as
          <Text as="span" italic> data/raw/iea_emergency_stocks_snapshot/2026-04-29.json</Text>. Live EIA WPSR and
          IEA emergency-stocks connectors are parked on the DataHoover backlog.
        </Text>
        <Text tone="tertiary" size="small">
          Iranian rial figures from Bonbast / Pashizi / Iran International free-market reporting (no clean primary
          feed exists). War timeline cross-checked against CNN, Al Jazeera, The Globe and Mail, Gulf News live blogs
          through Jun 28, 2026 (Day 121).
        </Text>
        <Text tone="quaternary" size="small">
          Data warehouse: data/warehouse.duckdb (DataHoover). Raw JSON snapshots in data/raw/&lt;source&gt;/.
        </Text>
      </Stack>
    </Stack>
  );
}
