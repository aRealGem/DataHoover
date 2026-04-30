#!/usr/bin/env node
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

const here = dirname(fileURLToPath(import.meta.url));
const cliEntry = resolve(here, "..", "src", "cli.ts");

const require = createRequire(import.meta.url);
const { register } = require("tsx/esm/api");
register();

await import(`file://${cliEntry}`);
