import React from "react";
import ReactDOM from "react-dom/client";
import { App } from "./App";
import {
  BootstrapErrorBoundary,
  BootstrapReadyMarker,
  FatalBootstrapScreen,
  installBootstrapGuards,
  MissingBridgeScreen,
  reportBootstrapEvent,
  setBootstrapShellState,
} from "./bootstrap";
import "./styles.css";

installBootstrapGuards();

const rootElement = document.getElementById("root");

if (!rootElement) {
  setBootstrapShellState({
    state: "ROOT_ELEMENT_MISSING",
    title: "Renderer root container is missing.",
    detail: "The packaged HTML loaded, but the #root mount target was not found.",
    tone: "fatal",
  });
  reportBootstrapEvent("renderer:root-missing", {});
  throw new Error("Renderer root element #root is missing.");
}

reportBootstrapEvent("renderer:root-mount-start", {
  bridge_available: Boolean(window.operatorDesktop),
  href: window.location.href,
});

const root = ReactDOM.createRoot(rootElement);
const bootstrapScenario = window.location.hash.replace(/^#\/?/, "").trim().toLowerCase();

if (bootstrapScenario === "bootstrap-fatal") {
  setBootstrapShellState({
    state: "FORCED_BOOTSTRAP_FATAL",
    title: "Forced renderer bootstrap fatal scenario.",
    detail: "This packaged launch intentionally rendered the fatal bootstrap fallback for validation.",
    tone: "fatal",
  });
  reportBootstrapEvent("renderer:forced-bootstrap-fatal", {
    href: window.location.href,
  });
  root.render(
    <React.StrictMode>
      <FatalBootstrapScreen
        title="Forced renderer bootstrap fatal scenario."
        detail="This packaged launch intentionally rendered the fatal bootstrap fallback for validation."
        stateCode="FORCED_BOOTSTRAP_FATAL"
      />
    </React.StrictMode>,
  );
} else if (!window.operatorDesktop) {
  root.render(
    <React.StrictMode>
      <MissingBridgeScreen />
    </React.StrictMode>,
  );
} else {
  root.render(
    <React.StrictMode>
      <BootstrapErrorBoundary>
        <BootstrapReadyMarker />
        <App />
      </BootstrapErrorBoundary>
    </React.StrictMode>,
  );
}
