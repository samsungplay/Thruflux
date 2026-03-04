import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import App from "./App";

const container = document.getElementById("app");

if (!container) {
  throw new Error("Missing app root");
}

createRoot(container).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
