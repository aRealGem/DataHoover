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

const PULL_TS = "2026-04-29 23:50 UTC";
const WAR_START = "2026-02-27";

// Headline tile data: [label, latest_display, delta_pct, sub]
type HeadlineTone = "danger" | "warning" | "success" | "info" | undefined;

const headline: Array<{
  label: string;
  value: string;
  pct: string;
  tone: HeadlineTone;
}> = [
  { label: "Brent crude (USD/bbl)",      value: "$113.89",  pct: "+59.7%",  tone: "danger"  },
  { label: "WTI crude (USD/bbl)",        value: "$99.89",   pct: "+49.2%",  tone: "danger"  },
  { label: "US gasoline (USD/gal, retail)", value: "$3.95", pct: "+41.2%",  tone: "danger"  },
  { label: "Henry Hub gas (USD/MMBtu)",  value: "$2.72",    pct: "-9.0%",   tone: "success" },
  { label: "Spot gold (XAU/USD)",        value: "$4,547",   pct: "-13.4%",  tone: "warning" },
  { label: "USD broad index (DTWEXBGS)", value: "118.73",   pct: "+0.8%",   tone: "info"    },
  { label: "Yuan onshore CNY",           value: "6.836",    pct: "-0.32%",  tone: "info"    },
  { label: "Yuan offshore CNH",          value: "6.846",    pct: "-0.20%",  tone: "info"    },
  { label: "Lockheed Martin (LMT)",      value: "$509.81",  pct: "-22.5%",  tone: "warning" },
  { label: "Elbit Systems (ESLT)",       value: "$818.55",  pct: "+6.4%",   tone: "success" },
  { label: "US SPR (Mbbl)",              value: "397.9",    pct: "-4.21%",  tone: "warning" },
  { label: "S&P 500",                    value: "7,138",    pct: "+3.8%",   tone: "success" },
  { label: "Bitcoin (BTC/USD)",          value: "$75,536",  pct: "+14.7%",  tone: "success" },
];

// War timeline: chronological, factual
const timeline: Array<{ date: string; text: string }> = [
  { date: "Feb 27, 2026", text: "Pre-war close. Brent $71.32, WTI $66.96, US retail gasoline $2.80/gal, S&P 500 6,879. Used as the baseline for every delta on this canvas." },
  { date: "Feb 28, 2026", text: "Joint US/Israeli air-and-missile campaign on Iran begins; Iran begins mining the Strait of Hormuz." },
  { date: "Mar  3, 2026", text: "Iran declares Strait of Hormuz closed; US announces naval blockade of Iranian ports." },
  { date: "Mar 27, 2026", text: "Brent first closes above $120 (data: $121.47)." },
  { date: "Apr  3, 2026", text: "Reported peak: Brent ~$147 intraday (FRED close $138.21 on Apr 7); WTI close hits $138 area." },
  { date: "Apr  7, 2026", text: "Trump announces a ceasefire after Kushner/Witkoff/Vance meet Iranian counterparts in Islamabad (21h, no deal). US blockade and Hormuz closure remain in effect." },
  { date: "Apr 17, 2026", text: "Iran briefly reopens Hormuz tied to a separate Israel/Lebanon ceasefire; Trump says US blockade continues until a final deal." },
  { date: "Apr 18, 2026", text: "Iran shuts Hormuz again (\"breaches of trust\")." },
  { date: "Apr 21, 2026", text: "Trump extends ceasefire pending an Iranian end-state proposal." },
  { date: "Apr 27, 2026", text: "Iran tables proposal: mutual reopen of Hormuz, defer nuclear talks. CENTCOM: 38 ships stopped/turned by US blockade." },
  { date: "Apr 28, 2026", text: "UAE announces it will quit OPEC. Trump publicly says he is unlikely to accept Iran's offer; Brent tops $112." },
  { date: "Apr 29, 2026", text: "Today. Ceasefire holds, Hormuz remains effectively closed; US-Iran talks stalled." },
];

// Energy & gold table
const energyRows: Array<{
  label: string; pre: string; latest: string; pct: string; src: string; tone?: "danger" | "warning" | "success";
}> = [
  { label: "Brent crude (USD/bbl)",                pre: "71.32",  latest: "113.89", pct: "+59.7%",  src: "FRED DCOILBRENTEU", tone: "danger" },
  { label: "WTI crude (USD/bbl)",                  pre: "66.96",  latest: "99.89",  pct: "+49.2%",  src: "FRED DCOILWTICO",   tone: "danger" },
  { label: "US gasoline retail (USD/gal)",         pre: "2.80",   latest: "3.95",   pct: "+41.2%",  src: "FRED GASREGCOVW",   tone: "danger" },
  { label: "Brent ETF (BNO)",                      pre: "34.81",  latest: "58.80",  pct: "+68.9%",  src: "TwelveData",        tone: "danger" },
  { label: "US Oil ETF (USO)",                     pre: "81.95",  latest: "150.67", pct: "+83.9%",  src: "TwelveData",        tone: "danger" },
  { label: "Henry Hub natural gas (USD/MMBtu)",    pre: "2.99",   latest: "2.72",   pct: "-9.0%",   src: "FRED DHHNGSP",      tone: "success" },
  { label: "US Nat Gas ETF (UNG)",                 pre: "11.52",  latest: "10.16",  pct: "-11.8%",  src: "TwelveData",        tone: "success" },
  { label: "Spot gold (XAU/USD)",                  pre: "5,251.02", latest: "4,546.98", pct: "-13.4%", src: "TwelveData",     tone: "warning" },
  { label: "Gold ETF (GLD)",                       pre: "483.75", latest: "417.43", pct: "-13.7%",  src: "TwelveData",        tone: "warning" },
];

// Dollar / FX table. Convention: "Δ% (USD)" = how much USD strengthened vs that currency.
const fxRows: Array<{
  label: string; pre: string; latest: string; pctUsd: string; note: string; tone?: "danger" | "warning" | "success" | "info";
}> = [
  { label: "USD broad index (DTWEXBGS)",   pre: "117.82",  latest: "118.73",  pctUsd: "+0.77%",  note: "FRED, vs broad basket" },
  { label: "USD vs advanced FX (DTWEXAFEGS)", pre: "110.16", latest: "111.01", pctUsd: "+0.77%", note: "FRED" },
  { label: "USD vs emerging FX (DTWEXEMEGS)", pre: "127.40", latest: "128.38", pctUsd: "+0.77%", note: "FRED" },
  { label: "USD Index ETF (UUP)",          pre: "27.08",   latest: "27.61",   pctUsd: "+1.94%",  note: "TwelveData" },
  { label: "EUR/USD",                       pre: "1.1822",  latest: "1.1718",  pctUsd: "+0.88%",  note: "USD/EUR fell -0.88% \u2192 dollar stronger" },
  { label: "GBP/USD",                       pre: "1.3455",  latest: "1.3518",  pctUsd: "-0.47%",  note: "USD slightly weaker vs sterling" },
  { label: "AUD/USD",                       pre: "0.7121",  latest: "0.7154",  pctUsd: "-0.46%",  note: "AUD stronger (commodity FX)" },
  { label: "USD/JPY",                       pre: "156.05",  latest: "159.35",  pctUsd: "+2.11%",  note: "Yen weaker, energy import hit",  tone: "warning" },
  { label: "USD/MXN",                       pre: "17.22",   latest: "17.39",   pctUsd: "+0.96%",  note: "Peso stable" },
  { label: "USD/INR",                       pre: "91.03",   latest: "94.25",   pctUsd: "+3.54%",  note: "Rupee hit hard \u2014 India imports ~80% of crude via Hormuz", tone: "warning" },
  { label: "USD/CNY",                       pre: "6.86",    latest: "6.84",    pctUsd: "-0.32%",  note: "PBoC managed; barely moved" },
  { label: "USD/KRW",                       pre: "1,439.82", latest: "1,476.47", pctUsd: "+2.55%", note: "Won weaker, energy importer" },
  { label: "USD/SGD",                       pre: "9.02",    latest: "9.22",    pctUsd: "+2.25%",  note: "Singapore dollar weaker" },
  { label: "USD/ZAR",                       pre: "15.92",   latest: "16.49",   pctUsd: "+3.62%",  note: "Rand weak (EM risk-off)",       tone: "warning" },
  { label: "USD/THB",                       pre: "31.04",   latest: "32.32",   pctUsd: "+4.12%",  note: "Baht weakest mover in basket",   tone: "warning" },
  { label: "USD/SEK",                       pre: "1.2643",  latest: "1.2755",  pctUsd: "+0.89%",  note: "Krona slightly weaker" },
  { label: "USD/TWD",                       pre: "31.35",   latest: "31.51",   pctUsd: "+0.51%",  note: "Taiwan dollar barely moved" },
  { label: "USD/BRL",                       pre: "5.1369",  latest: "4.9997",  pctUsd: "-2.67%",  note: "Real STRONGER \u2014 Brazil benefits as oil exporter", tone: "success" },
  { label: "USD/ILS",                       pre: "3.1404",  latest: "2.9834",  pctUsd: "-5.00%",  note: "Shekel STRONGER despite shooting war \u2014 markets price Iran weakening more than Israel risk", tone: "success" },
  { label: "Iranian rial (IRR, free-market)", pre: "~720,000 / USD", latest: "~1,050,000 / USD", pctUsd: "~+45%", note: "No clean public daily feed; figures from Bonbast/news cites \u2014 directional only", tone: "danger" },
];

// Ag / fertilizer
const agRows: Array<{
  label: string; pre: string; latest: string; pct: string; src: string; tone?: "danger" | "warning" | "success";
}> = [
  { label: "Wheat (IMF, USD/MT)",           pre: "174.75", latest: "193.88", pct: "+10.9%", src: "FRED PWHEAMTUSDM (monthly)", tone: "warning" },
  { label: "Wheat ETF (WEAT)",              pre: "22.57",  latest: "24.60",  pct: "+9.0%",  src: "TwelveData (daily)" },
  { label: "Maize/corn (IMF, USD/MT)",      pre: "210.64", latest: "213.30", pct: "+1.3%",  src: "FRED PMAIZMTUSDM (monthly)" },
  { label: "Corn ETF (CORN)",               pre: "17.89",  latest: "18.84",  pct: "+5.3%",  src: "TwelveData (daily)" },
  { label: "Soybeans (IMF, USD/MT)",        pre: "409.48", latest: "426.60", pct: "+4.2%",  src: "FRED PSOYBUSDM (monthly)" },
  { label: "Soybean ETF (SOYB)",            pre: "23.82",  latest: "24.76",  pct: "+3.9%",  src: "TwelveData (daily)" },
  { label: "US PPI mixed fertilizer",       pre: "141.10", latest: "143.32", pct: "+1.6%",  src: "FRED WPU0652013A (monthly)" },
  { label: "Agribusiness ETF (MOO)",        pre: "85.59",  latest: "81.97",  pct: "-4.2%",  src: "TwelveData (daily)" },
];

// Weekly-aligned, indexed-to-100 (Jan 2 = 100) chart series.
// Pulled live from the warehouse on 2026-04-29.
const energyChartCats = [
  "Jan 2", "Jan 9", "Jan 16", "Jan 23", "Jan 30",
  "Feb 6", "Feb 13", "Feb 20", "Feb 27 (war)",
  "Mar 6", "Mar 13", "Mar 20", "Mar 27",
  "Apr 3", "Apr 10", "Apr 17", "Apr 24",
];

// Brent / WTI / gasoline rebased to Feb 27 = 100 to make the war-era trajectory legible.
function rebaseToFeb27(values: Array<number | null>): Array<number | null> {
  const baseIdx = 8; // Feb 27 in the categories array
  const base = values[baseIdx];
  if (base == null || base === 0) return values;
  return values.map((v) => (v == null ? null : Math.round((v / base) * 1000) / 10));
}

const brentRaw: Array<number | null> = [61.98, 65.11, 66.97, 68.16, 72.25, 70.45, 69.96, 72.75, 71.32, 95.74, 103.23, 118.42, 121.47, 127.61, 119.07, 98.63, 111.86];
const wtiRaw:   Array<number | null> = [57.21, 58.96, 59.40, 60.70, 64.50, 63.77, 63.05, 66.69, 66.96, 90.77, 98.48, 98.71, 101.26, 113.23, 98.34, 85.91, 98.42];
const gasRaw:   Array<number | null> = [2.690, 2.681, 2.665, 2.700, 2.747, 2.747, 2.770, 2.790, 2.796, 2.884, 3.364, 3.566, 3.788, 3.814, 3.947, 3.962, 3.885];
const goldRaw:  Array<number | null> = [null, null, null, null, 4940.67, 4957.75, 5029.90, 5079.40, 5251.02, 5161.44, 5020.32, 4500.65, 4514.84, 4676.49, 4749.58, 4830.61, 4709.66];

const energyIndexedSeries = [
  { name: "Brent",       data: rebaseToFeb27(brentRaw).map((v) => (v == null ? 0 : v)), tone: "danger" as const },
  { name: "WTI",         data: rebaseToFeb27(wtiRaw).map((v) => (v == null ? 0 : v)),   tone: "warning" as const },
  { name: "US gasoline", data: rebaseToFeb27(gasRaw).map((v) => (v == null ? 0 : v)),   tone: "info" as const },
  { name: "Spot gold",   data: rebaseToFeb27(goldRaw).map((v) => (v == null ? 0 : v)),  tone: "neutral" as const },
];

// FX vs USD: indexed to Feb 27 = 100. Higher value = USD STRONGER vs that currency
// (rises in USD/JPY, USD/INR, USD/ZAR, falls in USD/BRL = real stronger).
const usdJpyRaw: Array<number | null> = [156.72, 158.07, 158.02, 157.57, 154.34, 157.10, 152.77, 154.99, 156.05, 157.64, 159.54, 159.26, 160.16, 159.64, 159.22, 158.10, 159.35];
const usdInrRaw: Array<number | null> = [90.19, 90.16, 90.87, 91.95, 91.98, 90.65, 90.64, 90.98, 91.03, 91.73, 92.47, 93.73, 94.83, 93.09, 92.72, 92.90, 94.25];
const usdZarRaw: Array<number | null> = [16.48, 16.48, 16.41, 16.15, 16.04, 16.02, 15.98, 16.04, 15.92, 16.58, 16.94, 17.04, 17.08, 17.00, 16.43, 16.21, 16.49];
const usdBrlRaw: Array<number | null> = [5.4224, 5.3543, 5.3719, 5.2951, 5.2459, 5.2183, 5.2297, 5.1860, 5.1369, 5.2698, 5.2914, 5.2896, 5.2312, 5.1568, 5.0254, 4.9810, 4.9997];

const fxIndexedSeries = [
  { name: "USD/JPY", data: rebaseToFeb27(usdJpyRaw).map((v) => (v == null ? 0 : v)), tone: "warning" as const },
  { name: "USD/INR", data: rebaseToFeb27(usdInrRaw).map((v) => (v == null ? 0 : v)), tone: "danger"  as const },
  { name: "USD/ZAR", data: rebaseToFeb27(usdZarRaw).map((v) => (v == null ? 0 : v)), tone: "info"    as const },
  { name: "USD/BRL", data: rebaseToFeb27(usdBrlRaw).map((v) => (v == null ? 0 : v)), tone: "success" as const },
];

// --- Yuan (CNY onshore via FRED DEXCHUS, CNH offshore via TwelveData USD/CNH) ---
const cnyRaw: Array<number | null> = [6.9877, 6.9772, 6.9681, 6.9631, 6.9510, 6.9388, 6.9080, 6.9031, 6.8579, 6.8965, 6.8961, 6.8857, 6.9116, 6.8824, 6.8278, 6.8170, 6.8359];
const cnhRaw: Array<number | null> = [null, null, null, null, 6.9569, 6.9510, 6.9100, 6.9020, 6.8604, 6.9020, 6.9050, 6.8910, 6.9180, 6.8870, 6.8350, 6.8240, 6.8464];

const yuanIndexedSeries = [
  { name: "CNY (onshore)",   data: rebaseToFeb27(cnyRaw).map((v) => (v == null ? 0 : v)), tone: "info"    as const },
  { name: "CNH (offshore)",  data: rebaseToFeb27(cnhRaw).map((v) => (v == null ? 0 : v)), tone: "warning" as const },
];

// --- Defense stocks: weekly-aligned, rebased to Feb 27 = 100 ---
const lmtRaw:    Array<number | null> = [497.07, 542.92, 582.43, 590.82, 634.22, 623.58, 652.58, 658.26, 658.08, 671.77, 646.00, 627.43, 615.84, 622.79, 613.72, 592.19, 513.45];
const nocRaw:    Array<number | null> = [585.66, 618.82, 666.90, 672.95, 692.26, 709.11, 702.57, 723.56, 724.38, 756.13, 733.71, 706.95, 679.00, 702.50, 673.73, 665.26, 575.11];
const rnmbyRaw:  Array<number | null> = [376.03, 442.18, 442.81, 433.47, 421.32, 379.00, 383.51, 409.51, 394.79, 369.11, 364.50, 344.90, 315.70, 361.60, 341.86, 354.00, 314.09];
const esltRaw:   Array<number | null> = [591.96, 683.36, 730.84, 718.99, 702.57, 665.00, 676.43, 724.73, 769.04, 936.14, 871.11, 920.75, 869.82, 888.97, 925.24, 872.58, 821.96];
const itaRaw:    Array<number | null> = [222.01, 232.97, 243.77, 235.07, 232.38, 233.93, 234.87, 243.65, 243.72, 242.20, 229.34, 222.56, 216.04, 221.91, 229.64, 231.94, 215.80];

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
  { ticker: "ESLT",   name: "Elbit Systems",            country: "Israel (Tel Aviv / NASDAQ)", pre: "769.04", latest: "818.55", pct: "+6.44%",  tone: "success" },
  { ticker: "GD",     name: "General Dynamics",         country: "United States",              pre: "357.05", latest: "338.90", pct: "-5.08%",  tone: "warning" },
  { ticker: "BAESY",  name: "BAE Systems (ADR)",        country: "United Kingdom",             pre: "116.00", latest: "108.55", pct: "-6.42%",  tone: "warning" },
  { ticker: "FINMY",  name: "Leonardo (ADR)",           country: "Italy",                      pre: " 33.49", latest: " 30.30", pct: "-9.53%",  tone: "warning" },
  { ticker: "MHVYF",  name: "Mitsubishi Heavy (PNK)",   country: "Japan",                      pre: " 31.73", latest: " 28.30", pct: "-10.81%", tone: "warning" },
  { ticker: "LHX",    name: "L3Harris",                 country: "United States",              pre: "364.54", latest: "321.38", pct: "-11.84%", tone: "warning" },
  { ticker: "THLLY",  name: "Thales (ADR)",             country: "France",                     pre: " 60.97", latest: " 53.49", pct: "-12.27%", tone: "warning" },
  { ticker: "ITA",    name: "iShares US Aerospace ETF", country: "US ETF",                     pre: "243.72", latest: "213.09", pct: "-12.57%", tone: "warning" },
  { ticker: "RTX",    name: "RTX (Raytheon)",           country: "United States",              pre: "202.62", latest: "172.75", pct: "-14.74%", tone: "danger"  },
  { ticker: "SAABY",  name: "Saab AB (ADR)",            country: "Sweden",                     pre: " 36.24", latest: " 29.93", pct: "-17.41%", tone: "danger"  },
  { ticker: "HII",    name: "Huntington Ingalls",       country: "United States",              pre: "444.52", latest: "362.13", pct: "-18.53%", tone: "danger"  },
  { ticker: "NOC",    name: "Northrop Grumman",         country: "United States",              pre: "724.38", latest: "572.41", pct: "-20.98%", tone: "danger"  },
  { ticker: "RNMBY",  name: "Rheinmetall (ADR)",        country: "Germany",                    pre: "394.79", latest: "311.20", pct: "-21.17%", tone: "danger"  },
  { ticker: "LMT",    name: "Lockheed Martin",          country: "United States",              pre: "658.08", latest: "509.81", pct: "-22.53%", tone: "danger"  },
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
  { country: "South Korea",   days: 208,  totalMbbl: 79,    ieaMember: true,  netImporter: true,  asOf: "2026-01" },
  { country: "Japan",         days: 200,  totalMbbl: 263,   ieaMember: true,  netImporter: true,  asOf: "2026-01" },
  { country: "Netherlands",   days: 165,  totalMbbl: null,  ieaMember: true,  netImporter: true,  asOf: "2026-01", note: "Logistical hub; high stocks per net imports" },
  { country: "Spain",         days: 145,  totalMbbl: null,  ieaMember: true,  netImporter: true,  asOf: "2026-01" },
  { country: "Germany",       days: 140,  totalMbbl: null,  ieaMember: true,  netImporter: true,  asOf: "2026-01" },
  { country: "China",         days: 120,  totalMbbl: 1390,  ieaMember: false, netImporter: true,  asOf: "2026-03", note: "Estimated; SPR holdings undisclosed. Combined SPR + commercial.", tone: "warning" },
  { country: "France",        days: 120,  totalMbbl: null,  ieaMember: true,  netImporter: true,  asOf: "2026-01" },
  { country: "Italy",         days: 115,  totalMbbl: null,  ieaMember: true,  netImporter: true,  asOf: "2026-01" },
  { country: "United Kingdom", days: 95,  totalMbbl: null,  ieaMember: true,  netImporter: true,  asOf: "2026-01" },
  { country: "India",         days:  74,  totalMbbl: 25,    ieaMember: false, netImporter: true,  asOf: "2026-03", note: "SPR alone is ~9.5 days at full capacity (currently 64% full = ~6 days). Total petroleum cover incl. commercial OMC stocks ~74 days. Below 90-day IEA benchmark.", tone: "danger" },
  { country: "Australia",     days:  49,  totalMbbl: null,  ieaMember: true,  netImporter: true,  asOf: "2026-01", note: "Below 90-day IEA obligation; in breach since 2012.", tone: "danger" },
  { country: "United States", days: null, totalMbbl: 397.9, ieaMember: true,  netImporter: false, asOf: "2026-04-24", note: "Net exporter since 2020; IEA days metric n/a. SPR shown for context (415.4 Mbbl on Feb 27 → 397.9 Mbbl on Apr 24, a 17.5 Mbbl drawdown over 8 weeks).", tone: "info" },
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
const sprLatest = 397.924;
const sprDeltaMbbl = +(sprLatest - sprPreWar).toFixed(1); // -17.5
const sprDeltaPct = +(((sprLatest - sprPreWar) / sprPreWar) * 100).toFixed(2); // -4.21
const sprWeeksElapsed = 8;
const sprWeeklyAvgMbbl = +(sprDeltaMbbl / sprWeeksElapsed).toFixed(2); // -2.19

export default function IranWarMarketImpact() {
  return (
    <Stack gap={28}>
      <Stack gap={6}>
        <H1>Trump's war on Iran: market impact</H1>
        <Text tone="secondary">
          Day 60 of the US/Israel air-and-naval campaign against Iran. Ceasefire holds for a third week, but the Strait of
          Hormuz is still effectively closed and the US blockade of Iranian ports remains in force. Every delta below
          anchors on the Feb 27, 2026 close (last business day before the strikes began).
        </Text>
        <Row gap={8} wrap>
          <Pill tone="danger" active>Brent +59.7%</Pill>
          <Pill tone="danger" active>WTI +49.2%</Pill>
          <Pill tone="danger" active>US gasoline +41.2%</Pill>
          <Pill tone="warning" active>Gold spot -13.4%</Pill>
          <Pill tone="info" active>USD broad +0.77%</Pill>
          <Pill tone="info" active>CNY -0.32% / CNH -0.20%</Pill>
          <Pill tone="warning" active>US defense primes -10 to -22%</Pill>
          <Pill tone="success" active>Elbit (Israel) +6.4%</Pill>
          <Pill tone="warning" active>US SPR -17.5 Mbbl since Feb 27 (-4.2%)</Pill>
          <Pill tone="success" active>S&P +3.8%</Pill>
          <Pill tone="success" active>BTC +14.7%</Pill>
          <Pill tone="success">Israeli shekel +5%</Pill>
          <Pill tone="danger">Iranian rial ~-31% (free-market, news-cited)</Pill>
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

      <Divider />

      <Stack gap={10}>
        <H2>Energy: oil up, gas down, gold off-peak</H2>
        <Text tone="secondary">
          The shock is concentrated in oil and refined products. Brent peaked at ~$138 (Apr 7 close) and is back at
          ~$114; gasoline lags but has already added more than $1.15/gal at the US pump. Henry Hub natural gas has
          actually fallen since the war began — it is a domestic North American market and the Hormuz disruption barely
          touches it. Gold spot is unusually negative on this clock because the Feb 27 baseline already included weeks
          of war-anticipation hedging; gold has sold off from that pre-war fear peak as the war moved toward stalemate.
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
          The USD broad index is up only 0.77%. The interesting story is below the surface: Asian and South African oil
          importers (USD/INR +3.5%, USD/THB +4.1%, USD/ZAR +3.6%, USD/JPY +2.1%) are paying the war's energy bill in a
          weaker currency, while net oil exporters' currencies (BRL +2.7%, IRR-adjacent ILS +5.0%) are stronger.
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
          Iran has had no realistic single official USD/IRR for years; the figure shown is the unofficial Tehran free-market
          rate (Bonbast / news cites). It moved from roughly 720,000 IRR/USD on Feb 27 to ~1.05M IRR/USD by late April.
          That's a ~31% loss of purchasing power against the dollar over 60 days. We deliberately do not put it on the
          chart because it is not from the same primary-source pipeline as everything else on this canvas.
        </Callout>
      </Stack>

      <Divider />

      <Stack gap={10}>
        <H2>Chinese yuan: the dog that didn't bark</H2>
        <Text tone="secondary">
          A 60% Brent shock plus a 0.77% USD broad-index bid would normally pull the yuan weaker by 2-4%. It hasn't.
          Onshore CNY (FRED DEXCHUS) is at 6.836 vs 6.858 pre-war — actually <Text as="span" weight="semibold">stronger</Text> by
          0.32%. Offshore CNH (TwelveData USD/CNH) is essentially flat at 6.846 vs 6.860, and the CNY-CNH gap is just
          ~10 pips — there is no speculative wedge. PBoC daily fixings have absorbed the entire shock; this is a managed
          float being managed.
        </Text>
        <Grid columns={4} gap={12}>
          <Stat value="6.8579" label="CNY pre-war (Feb 27)" />
          <Stat value="6.8359" label="CNY latest (Apr 24)  -0.32%"  tone="success" />
          <Stat value="6.8604" label="CNH pre-war (Feb 27)" />
          <Stat value="6.8464" label="CNH latest (Apr 30)  -0.20%"  tone="success" />
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
          Wheat is the main casualty here — it depends heavily on Black Sea / Russian / Indian export logistics that get
          disrupted by Hormuz spillover and elevated bunker fuel costs. Corn and soybeans are up modestly. Mixed
          fertilizer is barely up (+1.6%) on the official PPI, but that index lags by a month and the next print
          (May data, due June) is the one to watch. Agribusiness equities (MOO) are down -4.2%, reflecting margin
          compression from input-cost inflation rather than benefit from higher crop prices.
        </Text>
        <Table
          headers={["Series", "Pre-war (Feb 27)", "Latest", "Δ%", "Source"]}
          rows={agRows.map((r) => [r.label, r.pre, r.latest, r.pct, r.src])}
          rowTone={agRows.map((r) => r.tone)}
          columnAlign={["left", "right", "right", "right", "left"]}
        />
        <Text tone="tertiary" size="small">
          Cadence caveat: the four FRED commodity series (PWHEAMTUSDM, PMAIZMTUSDM, PSOYBUSDM, WPU0652013A) are
          monthly. Latest observation date is March 1, 2026 — only one post-war print. Treat as directional, not
          settled.
        </Text>
      </Stack>

      <Divider />

      <Stack gap={10}>
        <H2>Equities &amp; risk assets</H2>
        <Grid columns={3} gap={12}>
          <Stat value="+3.78%"  label="S&P 500 (FRED SP500)"          tone="success" />
          <Stat value="+8.80%"  label="NASDAQ Composite"              tone="success" />
          <Stat value="+0.33%"  label="Dow Jones Industrial Average"  />
          <Stat value="+3.11%"  label="SPY (TwelveData)"              tone="success" />
          <Stat value="-1.56%"  label="DIA (TwelveData)"              tone="danger"  />
          <Stat value="+14.69%" label="Bitcoin (BTC/USD)"             tone="success" />
        </Grid>
        <Text tone="secondary">
          The cap-weighted indices have been remarkably resilient — but the breadth is bad. NASDAQ-heavy mega-cap tech
          carries the S&P; Dow industrials (energy users, transports, materials) are flat to down. Bitcoin's +14.7% fits
          a digital-gold narrative; conventional gold's optical drop is a baseline artifact (see energy section). The
          composite read is mild stagflation: real economy inputs (oil, gasoline, wheat, EM FX) are stressed; financial
          assets levered to AI/tech are not.
        </Text>
      </Stack>

      <Divider />

      <Stack gap={10}>
        <H2>Defense stocks: the war was already priced in</H2>
        <Text tone="secondary">
          Conventional wisdom says shooting wars rip defense stocks higher. The data says the opposite — for this war,
          on this clock. The Feb 27 baseline already sat at multi-year highs after months of US/Israeli rhetoric and
          European rearmament repricing through 2025. From that peak, the actual war + a partial ceasefire was a
          sell-the-news event for almost every prime contractor on the planet. US primes are down 11-23%. European
          primes are down 6-21%, with Rheinmetall (the year's prior darling) leading the sector lower. The single
          counter-trend name is <Text as="span" weight="semibold">Elbit Systems</Text> — which gets actual emergency
          restocking orders from the IDF inside a 2-month war window — and even that is well off its early-March peak.
        </Text>
        <Grid columns={4} gap={12}>
          <Stat value="-22.5%" label="LMT (Lockheed)"      tone="danger"  />
          <Stat value="-21.0%" label="NOC (Northrop)"      tone="danger"  />
          <Stat value="-21.2%" label="RNMBY (Rheinmetall)" tone="danger"  />
          <Stat value="-12.6%" label="ITA (US def. ETF)"   tone="warning" />
          <Stat value="-14.7%" label="RTX"                 tone="warning" />
          <Stat value="-17.4%" label="SAABY (Saab)"        tone="danger"  />
          <Stat value="+6.4%"  label="ESLT (Elbit, Israel)" tone="success" />
          <Stat value="-5.1%"  label="GD (Gen Dynamics)"   tone="warning" />
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
          <Stat value={sprLatest.toFixed(1)} label="US SPR latest (Mbbl, Apr 24)" />
          <Stat value={sprDeltaMbbl.toFixed(1) + " Mbbl"} label={`Δ since Feb 27 war start (${sprDeltaPct.toFixed(2)}%)`} tone="warning" />
          <Stat value={sprWeeklyAvgMbbl.toFixed(1) + " Mbbl/wk"} label="Avg weekly draw, Mar 27 → Apr 24" tone="warning" />
          <Stat value="400 Mbbl" label="IEA coordinated release authorized Mar 11" tone="info" />
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
          followed through 2023-2025, returning the SPR to ~415 Mbbl just before the war. The post-war draw is small
          in absolute terms (~17.5 Mbbl, four weeks at ~$120 Brent) but it is the fastest weekly pace since 2022, and
          unlike 2022 the buyer of last resort is not yet active in the market.
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
            <Text as="span" weight="semibold">The OECD blanket: ~90 days minimum, by treaty.</Text> Every IEA member
            but Australia clears the 90-day bar, and most clear it by a wide margin. Japan and South Korea — both
            ~100% import-dependent and both gated through the same Hormuz/Malacca corridor — sit at 200 and 208 days
            respectively. European IEA members cluster between 95 and 165 days. This is the single biggest reason the
            developed world has not panicked at $114 Brent: the buffer is real and it is not being touched yet.
          </Text>
          <Text>
            <Text as="span" weight="semibold">The wildcards: China opaque, India exposed.</Text> China sits outside
            the IEA system; its strategic petroleum reserve is undisclosed but US EIA and Columbia CGEP both peg the
            combined SPR + commercial stockpile at ~1.39 Bbbl, equivalent to roughly 120 days of net imports at
            ~11.5 Mbpd. That is a strategic asset on a different scale than anyone else's. India is the cautionary
            tale on the other side: ISPRL's caverns are 64% full, holding 3.37 MMT of crude — about
            <Text as="span" weight="semibold"> 6 days of strategic cover</Text> at current consumption. Total
            petroleum cover including commercial OMC stocks is ~74 days, still below the IEA's 90-day benchmark, with
            88% of crude imported and a third of that historically transiting Hormuz. India's Phase II (6.5 MMT
            additional) was approved in 2021; nothing has come online.
          </Text>
          <Text>
            <Text as="span" weight="semibold">What the war has actually cost: 17.5 Mbbl off the US SPR in 8 weeks.</Text>{" "}
            The SPR sat flat at 415.4 Mbbl through the first month of the war (Feb 27 → Mar 20), then drew at an
            average pace of 2.2 Mbbl/wk for four straight weeks (Mar 27 through Apr 24). At that pace, full depletion
            would take ~3.5 years — but the relevant question is the ceasefire, not exhaustion. If the war reignites
            and Hormuz stays closed, you would expect (a) the IEA's authorized 400 Mbbl release to start hitting tape
            (it has not yet), (b) a parallel China stockdraw if Iranian flows stay cut, and (c) European industry
            stocks (~600 Mbbl OECD-aggregate) to start visibly drawing. None of those are happening yet at scale.
            The strategic-reserves layer is the reason Brent is at $114 and not $200.
          </Text>
        </Stack>
        <Callout tone="info" title="Snapshot, not feed">
          The US SPR weekly time series is hand-transcribed from the EIA Weekly Petroleum Status Report
          (<Text as="span" italic> data/raw/eia_spr_snapshot/2026-04-29.json</Text>). FRED does not host the EIA WPSR
          series IDs (<Text as="span" italic>WCSSTUS1, WCESTUS1, WTTSTUS1</Text> all return HTTP 400 against the
          FRED API — confirmed at pull time). The cross-country days-of-cover figures come from the IEA Oil Stocks of
          IEA Countries data tool plus government filings for non-IEA countries (China, India), saved as
          <Text as="span" italic> data/raw/iea_emergency_stocks_snapshot/2026-04-29.json</Text>. A live EIA WPSR
          connector and a live IEA emergency-stocks connector are both parked on the DataHoover backlog.
        </Callout>
      </Stack>

      <Divider />

      <Stack gap={6}>
        <H3>Methodology &amp; sources</H3>
        <Text tone="secondary" size="small">
          Pre-war anchor: last available close on or before 2026-02-27. Latest: most recent observation in the warehouse
          as of {PULL_TS}. FRED daily series can lag one business day relative to TwelveData. All "Δ% (USD)" values use
          the convention that a positive number means the dollar strengthened vs that currency. "Spot gold" and ETFs
          (USO, BNO, UNG, GLD, WEAT, CORN, SOYB, MOO, UUP, BTC/USD, USD/ILS) come from TwelveData; everything else from
          FRED.
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
          Iranian rial figures from Bonbast / Reuters / AP free-market reporting (no clean primary feed exists).
          War timeline cross-checked against CNN, CBS News, CNBC, Bloomberg live blogs as of Apr 28-29, 2026.
        </Text>
        <Text tone="quaternary" size="small">
          Data warehouse: data/warehouse.duckdb (DataHoover). Raw JSON snapshots in data/raw/&lt;source&gt;/.
        </Text>
      </Stack>
    </Stack>
  );
}
