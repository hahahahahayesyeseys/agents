import { cloudflare } from "@cloudflare/vite-plugin";
import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import agents from "agents/vite";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [agents(), react(), cloudflare(), tailwindcss()],
  resolve: {
    // The harness is imported from ../pi; resolve pi from one place.
    dedupe: [
      "react",
      "react-dom",
      "@earendil-works/chord",
      "@earendil-works/pi-ai",
      "@earendil-works/pi-durable",
      "@earendil-works/pi-telemetry"
    ]
  }
});
