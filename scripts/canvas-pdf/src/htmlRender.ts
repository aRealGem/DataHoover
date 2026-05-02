import { readFile } from "node:fs/promises";
import { createServer, type Server } from "node:http";
import { AddressInfo } from "node:net";
import { isAbsolute, resolve } from "node:path";
import { chromium, type Browser } from "playwright";

/** Echoes one HTML document for every path (some Chromium builds probe URLs that would 404 on a path-keyed static server). */
function startHtmlEchoServer(htmlBody: string): Promise<{ server: Server; port: number }> {
  const body = Buffer.from(htmlBody, "utf8");
  return new Promise((resolvePromise, rejectPromise) => {
    const server = createServer((req, res) => {
      res.statusCode = 200;
      res.setHeader("Content-Type", "text/html; charset=utf-8");
      res.setHeader("Cache-Control", "no-store");
      res.end(body);
    });
    server.on("error", rejectPromise);
    server.listen(0, "127.0.0.1", () => {
      const addr = server.address() as AddressInfo;
      resolvePromise({ server, port: addr.port });
    });
  });
}

export interface HtmlPdfOptions {
  /** Path to an HTML file on disk. */
  htmlPath: string;
  /** PDF output path. */
  outPath: string;
  /** Viewport width in CSS pixels (default 1280). */
  width?: number;
  /** Extra milliseconds to wait after plots render (default 400). */
  settleMs?: number;
  verbose?: boolean;
}

/**
 * Render a static HTML file to a single tall PDF via headless Chromium.
 * Intended for dashboards that fetch CDN JS (e.g. Plotly): localhost HTTP avoids file:// quirks.
 */
export async function renderHtmlFileToPdf(opts: HtmlPdfOptions): Promise<void> {
  const width = opts.width ?? 1280;
  const settleMs = opts.settleMs ?? 400;
  const verbose = opts.verbose ?? false;
  const log = (...args: unknown[]) => {
    if (verbose) console.error("[html-pdf]", ...args);
  };

  const htmlPath = isAbsolute(opts.htmlPath)
    ? opts.htmlPath
    : resolve(process.cwd(), opts.htmlPath);
  const html = await readFile(htmlPath, "utf8");

  const { server, port } = await startHtmlEchoServer(html);
  log("static server listening on", port);

  let browser: Browser | undefined;
  try {
    browser = await chromium.launch({ headless: true });
    const context = await browser.newContext({
      viewport: { width, height: 900 },
      deviceScaleFactor: 2,
    });
    const page = await context.newPage();

    page.on("console", (msg) => {
      if (verbose || msg.type() === "error" || msg.type() === "warning") {
        console.error(`[browser:${msg.type()}]`, msg.text());
      }
    });
    page.on("pageerror", (err) => {
      console.error("[browser:pageerror]", err.message);
    });

    const url = `http://127.0.0.1:${port}/`;
    log("navigating to", url);
    await page.goto(url, { waitUntil: "networkidle", timeout: 120_000 });

    // Sentiment dashboard: four Plotly charts in known divs (CDN-loaded Plotly).
    try {
      await page.waitForFunction(
        () => {
          const ids = ["plot-altme", "plot-cnn", "plot-st", "plot-reddit"];
          for (const id of ids) {
            const el = document.querySelector("#" + id);
            if (!el) return false;
            if (!el.querySelector(".plotly") && !el.querySelector("svg")) return false;
          }
          return true;
        },
        undefined,
        { timeout: 120_000 },
      );
    } catch {
      throw new Error(
        "Timed out waiting for Plotly charts. Check network (cdn.plot.ly), or increase timeout.",
      );
    }

    await page.evaluate(() => document.fonts?.ready);
    await page.waitForTimeout(settleMs);

    const heightPx = await page.evaluate(() => document.documentElement.scrollHeight);
    await page.addStyleTag({
      content: `@page { size: ${width}px ${heightPx}px; margin: 0 !important; }`,
    });
    log("single-page height:", heightPx, "px");

    await page.pdf({
      path: opts.outPath,
      printBackground: true,
      width: `${width}px`,
      height: `${heightPx}px`,
      margin: { top: "0", right: "0", bottom: "0", left: "0" },
      preferCSSPageSize: true,
    });

    log("wrote", opts.outPath);
  } finally {
    await browser?.close().catch(() => {});
    server.close();
  }
}
