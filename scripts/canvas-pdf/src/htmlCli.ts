import { existsSync } from "node:fs";
import { basename, dirname, extname, isAbsolute, resolve } from "node:path";
import { Command } from "commander";

import { renderHtmlFileToPdf } from "./htmlRender.js";

function defaultOutputPath(input: string): string {
  const dir = dirname(input);
  const base = basename(input, extname(input));
  return resolve(dir, `${base}.pdf`);
}

async function main(argv: string[]): Promise<void> {
  const program = new Command();

  program
    .name("html-pdf")
    .description("Render a static HTML file to a single-page PDF (Playwright Chromium).")
    .argument("<input>", "Path to an .html file")
    .option("-o, --output <path>", "Output PDF path (default: <basename>.pdf next to input)")
    .option(
      "--width <px>",
      "Viewport width in CSS pixels (default 1280)",
      (v) => {
        const n = Number(v);
        if (!Number.isFinite(n) || n <= 0) throw new Error(`Invalid --width: ${v}`);
        return n;
      },
    )
    .option(
      "--settle-ms <ms>",
      "Extra wait after charts render (default 400)",
      (v) => {
        const n = Number(v);
        if (!Number.isFinite(n) || n < 0) throw new Error(`Invalid --settle-ms: ${v}`);
        return n;
      },
    )
    .option("-v, --verbose", "Verbose logging to stderr")
    .helpOption("-h, --help", "Show help");

  program.parse(argv);
  const opts = program.opts<{
    output?: string;
    width?: number;
    settleMs?: number;
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

  const outPath = opts.output
    ? isAbsolute(opts.output)
      ? opts.output
      : resolve(process.cwd(), opts.output)
    : defaultOutputPath(input);

  if (opts.verbose) {
    console.error("[html-pdf] input:     ", input);
    console.error("[html-pdf] output:    ", outPath);
    console.error("[html-pdf] width:     ", opts.width ?? 1280);
    console.error("[html-pdf] settleMs:  ", opts.settleMs ?? 400);
  }

  await renderHtmlFileToPdf({
    htmlPath: input,
    outPath,
    width: opts.width,
    settleMs: opts.settleMs ?? undefined,
    verbose: opts.verbose,
  });

  console.log(outPath);
}

main(process.argv).catch((err) => {
  console.error(err instanceof Error ? err.message : String(err));
  process.exit(1);
});
