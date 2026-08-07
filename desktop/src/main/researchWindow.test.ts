import test from "node:test";
import assert from "node:assert/strict";
import path from "node:path";

import { researchControlCenterHashRoute, researchControlCenterWindowOptions } from "./researchWindow";

test("Research Control Center window is separate and read-only by construction", () => {
  const preloadPath = path.join("dist", "main", "researchPreload.js");
  const options = researchControlCenterWindowOptions(preloadPath);
  assert.equal(options.title, "Research Control Center");
  assert.equal(options.webPreferences?.preload, preloadPath);
  assert.equal(options.webPreferences?.contextIsolation, true);
  assert.equal(options.webPreferences?.nodeIntegration, false);
});

test("Research Control Center route is explicit and renderer-local", () => {
  assert.equal(researchControlCenterHashRoute(), "#/research-control-center");
});
