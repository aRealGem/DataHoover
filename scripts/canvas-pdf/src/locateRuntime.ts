import { existsSync } from "node:fs";
import { resolve } from "node:path";

const MACOS_DEFAULT =
  "/Applications/Cursor.app/Contents/Resources/app/extensions/cursor-agent-exec/dist/canvas-runtime/canvas-runtime.esm.js";

const ENV_VAR = "CURSOR_CANVAS_RUNTIME";

export interface LocateRuntimeOptions {
  flag?: string | undefined;
}

/**
 * Resolve the path to Cursor's canvas-runtime.esm.js.
 *
 * Precedence: --runtime flag > $CURSOR_CANVAS_RUNTIME env > macOS default.
 * Throws a clear error if the file does not exist.
 */
export function locateRuntime({ flag }: LocateRuntimeOptions = {}): string {
  const candidates: Array<{ source: string; path: string }> = [];

  if (flag) {
    candidates.push({ source: "--runtime flag", path: resolve(flag) });
  }

  const fromEnv = process.env[ENV_VAR];
  if (fromEnv) {
    candidates.push({ source: `$${ENV_VAR} env var`, path: resolve(fromEnv) });
  }

  candidates.push({ source: "macOS default", path: MACOS_DEFAULT });

  for (const c of candidates) {
    if (existsSync(c.path)) return c.path;
  }

  const tried = candidates.map((c) => `  - ${c.source}: ${c.path}`).join("\n");
  throw new Error(
    `Could not find Cursor's canvas-runtime.esm.js. Tried:\n${tried}\n\n` +
      `Set $${ENV_VAR} or pass --runtime <path> to point at it. ` +
      `On macOS the file normally ships inside Cursor.app under ` +
      `Contents/Resources/app/extensions/cursor-agent-exec/dist/canvas-runtime/.`,
  );
}
