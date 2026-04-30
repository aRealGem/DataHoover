import { readFile } from "node:fs/promises";
import { createServer, type Server } from "node:http";
import { AddressInfo } from "node:net";
import { chromium, type Browser } from "playwright";

import { SDK_EXPORT_NAMES } from "./sdkExports.js";

export type PaperFormat = "Letter" | "Legal" | "A4" | "A5";
export type ThemeKind = "light" | "dark" | "hc-light" | "hc";

export interface RenderOptions {
  /** Bundled canvas ESM source (output of compileCanvas). */
  canvasBundle: string;
  /** Absolute path to Cursor's canvas-runtime.esm.js. */
  runtimePath: string;
  /** Where the PDF should be written. */
  outPath: string;
  /** Viewport width in CSS pixels. Default 1024. */
  width?: number;
  /** Standard paper format (ignored when singlePage is true). Default 'Letter'. */
  paper?: PaperFormat;
  /** Emit one tall page sized to the rendered content. */
  singlePage?: boolean;
  /** Page margin (CSS length). Default '0.5in'. */
  margin?: string;
  /**
   * Theme kind passed to the runtime via __cursorCanvas.state.get("theme").
   * Defaults to "light" because PDFs are usually shared/printed and dark
   * editor backgrounds waste ink and read poorly on paper.
   */
  theme?: ThemeKind;
  /** Verbose logging to stderr. */
  verbose?: boolean;
}

const HARNESS_HTML = (
  paper: PaperFormat,
  margin: string,
  theme: ThemeKind,
): string => `<!doctype html>
<html><head>
  <meta charset="utf-8"/>
  <title>canvas-pdf</title>
  <style>
    @page { size: ${paper}; margin: ${margin}; }
    html, body { margin: 0; }
  </style>
  <script type="importmap">
    { "imports": { "cursor/canvas": "/sdk-bridge.mjs" } }
  </script>
  <script>
    // Host bridge: must exist BEFORE runtime.js evaluates so useHostTheme()
    // can read state.get("theme"). Runtime expects:
    //   state: Map<string, any>  — channel -> opaque state object
    //                              "theme" channel returns { kind: "light"|"dark"|... }
    //   data:  Map<string, any>  — used by useCanvasState (no-op persistence here)
    //   reportError(e), canvasId, tokenParam — optional
    window.__cursorCanvas = {
      state: new Map([["theme", { kind: ${JSON.stringify(theme)} }]]),
      data: new Map(),
      reportError: (e) => {
        console.error("[canvas-pdf]", e);
        const el = document.createElement("pre");
        el.id = "canvas-pdf-error";
        el.textContent = String(e);
        document.body.appendChild(el);
      },
    };
  </script>
</head><body>
  <div id="root"></div>
  <script type="module">
    import { mountCanvas } from "/runtime.js";
    try {
      await mountCanvas("/canvas.bundle.mjs");
      document.title = "ready";
    } catch (e) {
      window.__cursorCanvas.reportError(e?.stack ?? String(e));
      document.title = "error";
    }
  </script>
</body></html>`;

function buildSdkBridge(): string {
  const lines = [
    "// Auto-generated bridge: re-exports each cursor/canvas SDK symbol from",
    "// globalThis. Cursor's runtime attaches them to globalThis right before",
    "// importing the canvas bundle, so by the time this module is evaluated",
    "// (during the bundle's import phase), every name below is already defined.",
    "const g = globalThis;",
  ];
  for (const name of SDK_EXPORT_NAMES) {
    lines.push(`export const ${name} = g[${JSON.stringify(name)}];`);
  }
  return lines.join("\n") + "\n";
}

interface FileMap {
  [path: string]: { body: Buffer; type: string };
}

function startStaticServer(files: FileMap): Promise<{ server: Server; port: number }> {
  return new Promise((resolvePromise, rejectPromise) => {
    const server = createServer((req, res) => {
      const path = (req.url ?? "/").split("?")[0] ?? "/";
      const file = files[path];
      if (!file) {
        res.statusCode = 404;
        res.end(`Not found: ${path}`);
        return;
      }
      res.statusCode = 200;
      res.setHeader("Content-Type", file.type);
      res.setHeader("Cache-Control", "no-store");
      res.end(file.body);
    });
    server.on("error", rejectPromise);
    server.listen(0, "127.0.0.1", () => {
      const addr = server.address() as AddressInfo;
      resolvePromise({ server, port: addr.port });
    });
  });
}

export async function renderCanvasToPdf(opts: RenderOptions): Promise<void> {
  const paper = opts.paper ?? "Letter";
  const margin = opts.margin ?? "0.5in";
  const width = opts.width ?? 1024;
  const theme = opts.theme ?? "light";
  const verbose = opts.verbose ?? false;

  const log = (...args: unknown[]) => {
    if (verbose) console.error("[canvas-pdf]", ...args);
  };

  const runtimeSource = await readFile(opts.runtimePath);

  const harness = HARNESS_HTML(paper, margin, theme);
  const files: FileMap = {
    "/": {
      body: Buffer.from(harness, "utf8"),
      type: "text/html; charset=utf-8",
    },
    "/index.html": {
      body: Buffer.from(harness, "utf8"),
      type: "text/html; charset=utf-8",
    },
    "/runtime.js": {
      body: runtimeSource,
      type: "text/javascript; charset=utf-8",
    },
    "/sdk-bridge.mjs": {
      body: Buffer.from(buildSdkBridge(), "utf8"),
      type: "text/javascript; charset=utf-8",
    },
    "/canvas.bundle.mjs": {
      body: Buffer.from(opts.canvasBundle, "utf8"),
      type: "text/javascript; charset=utf-8",
    },
  };

  const { server, port } = await startStaticServer(files);
  log("static server listening on", port);

  let browser: Browser | undefined;
  try {
    browser = await chromium.launch({ headless: true });
    const context = await browser.newContext({
      viewport: { width, height: 800 },
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
    await page.goto(url, { waitUntil: "domcontentloaded" });

    await page.waitForFunction(
      () => document.title === "ready" || document.title === "error",
      undefined,
      { timeout: 30_000 },
    );

    const title = await page.title();
    if (title === "error") {
      const err = await page
        .locator("#canvas-pdf-error")
        .innerText()
        .catch(() => "(unknown error)");
      throw new Error(`Canvas failed to mount:\n${err}`);
    }

    await page.waitForFunction(
      () => !!document.querySelector("#root")?.firstElementChild,
      undefined,
      { timeout: 10_000 },
    );

    // Give web fonts and any layout-after-paint a beat to settle.
    await page.waitForLoadState("networkidle").catch(() => {});
    await page.evaluate(() => document.fonts?.ready);
    await page.waitForTimeout(150);

    if (opts.singlePage) {
      const heightPx = await page.evaluate(
        () => document.documentElement.scrollHeight,
      );
      // Override the @page Letter rule we put in the harness so Chromium
      // actually treats the printable area as one tall page.
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
    } else {
      await page.pdf({
        path: opts.outPath,
        printBackground: true,
        format: paper,
        preferCSSPageSize: true,
      });
    }

    log("wrote", opts.outPath);
  } finally {
    await browser?.close().catch(() => {});
    server.close();
  }
}
