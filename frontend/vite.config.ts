import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";

export default defineConfig({
  base: "/app/",
  build: {
    // Preserve the Vite 5 browser target while updating the build toolchain.
    target: ["es2020", "edge88", "firefox78", "chrome87", "safari14"],
  },
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": {
        target: "http://localhost:5002",
        changeOrigin: true,
      },
      "/static": {
        target: "http://localhost:5002",
        changeOrigin: true,
      },
    },
  },
  test: {
    environment: "jsdom",
    setupFiles: "./src/test/setup.ts",
    globals: true,
  },
});
