import fs from "node:fs";
import path from "node:path";

const distAssetsDir = path.resolve("dist", "assets");
const files = fs.existsSync(distAssetsDir)
  ? fs.readdirSync(distAssetsDir).filter((file) => file.endsWith(".js"))
  : [];

if (files.length === 0) {
  console.warn("[bundle-check] no js assets found, skip.");
  process.exit(0);
}

const sizes = files.map((file) => {
  const stat = fs.statSync(path.join(distAssetsDir, file));
  return { file, bytes: stat.size };
});

sizes.sort((a, b) => b.bytes - a.bytes);
console.log("[bundle-check] js chunks:");
for (const item of sizes) {
  console.log(`  - ${item.file}: ${(item.bytes / 1024).toFixed(1)} KB`);
}

const entryMaxKb = Number(process.env.BUNDLE_ENTRY_MAX_KB ?? "320");
const chartsMaxKb = Number(process.env.BUNDLE_CHARTS_MAX_KB ?? "600");

const entryChunk = sizes.find((item) => item.file.startsWith("index-"));
const chartsChunk = sizes.find((item) => item.file.includes("vendor-charts"));

let failed = false;
if (entryChunk && entryChunk.bytes / 1024 > entryMaxKb) {
  console.error(
    `[bundle-check] entry chunk ${entryChunk.file} exceeds ${entryMaxKb} KB`,
  );
  failed = true;
}
if (chartsChunk && chartsChunk.bytes / 1024 > chartsMaxKb) {
  console.error(
    `[bundle-check] charts chunk ${chartsChunk.file} exceeds ${chartsMaxKb} KB`,
  );
  failed = true;
}

if (failed) {
  process.exit(1);
}

console.log("[bundle-check] passed.");
