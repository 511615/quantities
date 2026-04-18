import react from "@vitejs/plugin-react";
import { defineConfig, loadEnv } from "vite";

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  const apiProxyTarget =
    process.env.VITE_PROXY_API_TARGET || env.VITE_PROXY_API_TARGET || "http://127.0.0.1:8015";

  return {
    plugins: [react()],
    server: {
      port: 5173,
      host: "127.0.0.1",
      proxy: {
        "/api": {
          target: apiProxyTarget,
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
  };
});
