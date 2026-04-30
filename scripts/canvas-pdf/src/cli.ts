import { existsSync } from "node:fs";
import { basename, dirname, isAbsolute, resolve } from "node:path";
import { Command } from "commander";

import { compileCanvas } from "./compileCanvas.js";
import { locateRuntime } from "./locateRuntime.js";
import { renderCanvasToPdf, type PaperFormat, type ThemeKind } from "./render.js";

const PAPER_FORMATS: ReadonlySet<PaperFormat> = new Set([
  "Letter",
  "Legal",
  "A4",
  "A5",
]);

const THEMES: ReadonlySet<ThemeKind> = new Set([
  "light",
  "dark",
  "hc-light",
  "hc",
]);

function defaultOutputPath(input: string): string {
  const dir = dirname(input);
  const base = basename(input);
  const stripped = base.replace(/\.canvas\.tsx$/i, "").replace(/\.tsx$/i, "");
  return resolve(dir, `${stripped}.pdf`);
}

async function main(argv: string[]): Promise<void> {
  const program = new Command();

  program
    .name("canvas-pdf")
    .description(
      "Render a Cursor .canvas.tsx file to PDF using Cursor's bundled canvas runtime.",
    )
    .argument("<input>", "Path to a .canvas.tsx file")
    .option("-o, --output <path>", "Output PDF path (default: <basename>.pdf next to input)")
    .option("--paper <format>", "Paper format: Letter | Legal | A4 | A5", "Letter")
    .option("--width <px>", "Viewport width in CSS pixels (default 1024)", (v) => {
      const n = Number(v);
      if (!Number.isFinite(n) || n <= 0) throw new Error(`Invalid --width: ${v}`);
      return n;
    })
    .option("--margin <css>", "Page margin (CSS length, default '0.5in')")
    .option(
      "--theme <kind>",
      "Theme: light | dark | hc-light | hc (default light, easier to print)",
      "light",
    )
    .option(
      "--single-page",
      "Emit one tall page sized to rendered content (avoids cross-page splits).",
    )
    .option("--runtime <path>", "Override path to canvas-runtime.esm.js")
    .option("-v, --verbose", "Verbose logging to stderr")
    .helpOption("-h, --help", "Show help");

  program.parse(argv);
  const opts = program.opts<{
    output?: string;
    paper: string;
    width?: number;
    margin?: string;
    theme: string;
    singlePage?: boolean;
    runtime?: string;
    verbose?: boolean;
  }>();

  const [inputArg] = program.args;
  if (!inputArg) {
    program.help({ error: true });
    return;
  }

  const input = isAbsolute(inputArg) ? inputArg : resolve(process.cwd(), inputArg);
  if (!existsSync(input)) {
    throw new Error(`Input file not found: ${input}`);
  }
  if (!/\.canvas\.tsx$/i.test(input) && !/\.tsx$/i.test(input)) {
    console.error(
      `Warning: ${input} doesn't end in .canvas.tsx — proceeding anyway.`,
    );
  }

  const paper = opts.paper as PaperFormat;
  if (!PAPER_FORMATS.has(paper)) {
    throw new Error(
      `Invalid --paper: ${opts.paper}. Must be one of: ${[...PAPER_FORMATS].join(", ")}`,
    );
  }

  const theme = opts.theme as ThemeKind;
  if (!THEMES.has(theme)) {
    throw new Error(
      `Invalid --theme: ${opts.theme}. Must be one of: ${[...THEMES].join(", ")}`,
    );
  }

  const runtimePath = locateRuntime({ flag: opts.runtime });
  const outPath = opts.output
    ? isAbsolute(opts.output)
      ? opts.output
      : resolve(process.cwd(), opts.output)
    : defaultOutputPath(input);

  if (opts.verbose) {
    console.error("[canvas-pdf] input:    ", input);
    console.error("[canvas-pdf] runtime:  ", runtimePath);
    console.error("[canvas-pdf] output:   ", outPath);
    console.error("[canvas-pdf] paper:    ", paper);
    console.error("[canvas-pdf] theme:    ", theme);
    console.error("[canvas-pdf] width:    ", opts.width ?? 1024);
    console.error("[canvas-pdf] singlePage:", !!opts.singlePage);
  }

  const compiled = await compileCanvas(input);
  for (const w of compiled.warnings) {
    console.error(`[esbuild warning] ${w.text}`);
  }

  await renderCanvasToPdf({
    canvasBundle: compiled.code,
    runtimePath,
    outPath,
    width: opts.width,
    paper,
    theme,
    singlePage: opts.singlePage,
    margin: opts.margin,
    verbose: opts.verbose,
  });

  console.log(outPath);
}

main(process.argv).catch((err) => {
  console.error(err instanceof Error ? err.message : String(err));
  process.exit(1);
});
