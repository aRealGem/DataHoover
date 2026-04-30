import { build, type BuildOptions, type Message } from "esbuild";

export interface CompiledCanvas {
  /** ESM bundle bytes, suitable for `import("/canvas.bundle.mjs")`. */
  code: string;
  /** Non-fatal esbuild warnings, if any. */
  warnings: Message[];
}

/**
 * Bundle a `.canvas.tsx` file with the **classic** JSX transform.
 *
 * - `React.createElement` / `React.Fragment` are emitted as bare references.
 *   The runtime sets `globalThis.React` *and* attaches every React export
 *   (including `createElement`, `Fragment`, hooks) onto `globalThis` before
 *   importing the bundle, so these references resolve at runtime without
 *   needing to bundle React.
 * - `cursor/canvas`, `react`, and `react/jsx-runtime` are externalized so
 *   the import map in the harness can redirect `cursor/canvas` to a bridge
 *   module. The other two never get imported in classic-JSX mode but are
 *   listed defensively in case the canvas itself imports them.
 */
export async function compileCanvas(entry: string): Promise<CompiledCanvas> {
  const opts: BuildOptions = {
    entryPoints: [entry],
    bundle: true,
    format: "esm",
    platform: "browser",
    target: "es2022",
    jsx: "transform",
    jsxFactory: "React.createElement",
    jsxFragment: "React.Fragment",
    external: ["cursor/canvas", "react", "react/jsx-runtime", "react-dom"],
    write: false,
    sourcemap: "inline",
    logLevel: "silent",
  };

  const result = await build(opts);
  const out = result.outputFiles?.[0];
  if (!out) {
    throw new Error("esbuild produced no output for entry: " + entry);
  }
  return { code: out.text, warnings: result.warnings };
}
