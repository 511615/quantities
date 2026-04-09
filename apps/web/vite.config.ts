import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    host: "127.0.0.1",
    proxy: {
      "/api": {
        target: "http://127.0.0.1:8000",
        changeOrigin: true,
      },
    },
  },
  build: {
    sourcemap: true,
    chunkSizeWarningLimit: 700,
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (id.includes("/src/pages/BacktestReportPage") || id.includes("/src/features/backtest-report/")) {
            return "page-backtest";
          }
          if (id.includes("/src/pages/ComparisonPage") || id.includes("/src/features/model-comparison/")) {
            return "page-comparison";
          }
          if (id.includes("/src/pages/Benchmark")) {
            return "page-benchmark";
          }
          if (id.includes("node_modules")) {
            if (id.includes("echarts") || id.includes("zrender")) {
              return "vendor-charts";
            }
            if (id.includes("@tanstack/react-table")) {
              return "vendor-table";
            }
            if (id.includes("@tanstack/react-query")) {
              return "vendor-query";
            }
            if (id.includes("react-router")) {
              return "vendor-router";
            }
            if (id.includes("react-dom") || id.includes("react/")) {
              return "vendor-react";
            }
          }
          return undefined;
        },
      },
    },
  },
  test: {
    environment: "jsdom",
    setupFiles: "./src/test/setup.ts",
  },
});
