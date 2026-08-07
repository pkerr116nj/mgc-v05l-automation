import test from "node:test";
import assert from "node:assert/strict";
import path from "node:path";

import {
  researchControlCenterHashRoute,
  researchControlCenterMenuItem,
  researchControlCenterWindowOptions,
} from "./researchWindow";

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

test("Research Control Center menu item is operator-facing", () => {
  const item = researchControlCenterMenuItem(() => undefined);
  assert.equal(item.label, "Research Control Center");
  assert.equal(typeof item.click, "function");
});

test("Research Control Center menu item opens the supplied window helper", () => {
  let calls = 0;
  const item = researchControlCenterMenuItem(() => {
    calls += 1;
  });
  const click = item.click as () => void;

  click();

  assert.equal(calls, 1);
});

test("Research Control Center menu item delegates repeated opens to the singleton helper", () => {
  const calls: string[] = [];
  const item = researchControlCenterMenuItem(() => {
    calls.push("open");
  });
  const click = item.click as () => void;

  click();
  click();

  assert.deepEqual(calls, ["open", "open"]);
});
