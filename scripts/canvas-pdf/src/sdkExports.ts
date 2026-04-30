/**
 * The complete set of names that Cursor's canvas-runtime.esm.js attaches to
 * `globalThis` (via `Object.assign(globalThis, dist_exports)`) right before
 * dynamically importing the canvas module.
 *
 * Source of truth: extracted from the `__export(dist_exports, { ... })` block
 * in `canvas-runtime.esm.js` itself. Any name on this list is something a
 * `.canvas.tsx` file is allowed to import from `cursor/canvas`.
 *
 * Keep this in sync if Cursor adds/removes SDK exports.
 */
export const SDK_EXPORT_NAMES = [
  "Badge",
  "BarChart",
  "Button",
  "Callout",
  "Card",
  "CardBody",
  "CardHeader",
  "Checkbox",
  "Chip",
  "Code",
  "DiffStats",
  "DiffView",
  "Divider",
  "Grid",
  "H1",
  "H2",
  "H3",
  "IconButton",
  "LineChart",
  "Link",
  "PieChart",
  "Pill",
  "Row",
  "Select",
  "Spacer",
  "Stack",
  "Stat",
  "Table",
  "Tag",
  "Text",
  "TextArea",
  "TextInput",
  "TodoList",
  "TodoListCard",
  "Toggle",
  "canvasPaletteDark",
  "canvasPaletteLight",
  "canvasTokens",
  "canvasTokensLight",
  "computeDAGLayout",
  "mergeStyle",
  "useCanvasAction",
  "useCanvasState",
  "useHostTheme",
] as const;

export type SdkExportName = (typeof SDK_EXPORT_NAMES)[number];
