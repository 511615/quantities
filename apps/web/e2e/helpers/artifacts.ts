import { cp, mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";

const repoRoot = path.resolve(process.cwd(), "..", "..");
const artifactRoot = path.resolve(
  repoRoot,
  process.env.PLAYWRIGHT_ARTIFACT_ROOT || ".tmp/playwright-artifacts",
);

async function rewriteJson(filePath: string, updater: (payload: Record<string, unknown>) => void) {
  const payload = JSON.parse(await readFile(filePath, "utf-8")) as Record<string, unknown>;
  updater(payload);
  await writeFile(filePath, `${JSON.stringify(payload, null, 2)}\n`, "utf-8");
}

export async function ensureQualityBlockedMarketRun(runId: string) {
  const sourceRunId = "api-market";
  const sourceDatasetId = "canonical_five_modality_live_debug_1776457986_fusion";
  const blockedDatasetId = "smoke_dataset";
  const modelsDir = path.join(artifactRoot, "models");
  const predictionsDir = path.join(artifactRoot, "predictions");
  const trackingDir = path.join(artifactRoot, "tracking");
  const targetModelDir = path.join(modelsDir, runId);

  await mkdir(modelsDir, { recursive: true });
  await mkdir(predictionsDir, { recursive: true });
  await mkdir(trackingDir, { recursive: true });
  await cp(path.join(modelsDir, sourceRunId), targetModelDir, { recursive: true, force: true });
  await cp(path.join(predictionsDir, sourceRunId), path.join(predictionsDir, runId), {
    recursive: true,
    force: true,
  });
  await cp(path.join(trackingDir, `${sourceRunId}.json`), path.join(trackingDir, `${runId}.json`), {
    force: true,
  });

  await rewriteJson(path.join(targetModelDir, "train_manifest.json"), (payload) => {
    payload.run_id = runId;
    payload.dataset_id = blockedDatasetId;
    payload.dataset_ref_uri = `dataset://${blockedDatasetId}`;
    payload.dataset_manifest_uri = path.join(
      artifactRoot,
      "datasets",
      `${blockedDatasetId}_dataset_manifest.json`,
    );
    payload.feature_scope_modality = "market";
    payload.source_dataset_quality_status = "failed";
  });

  await rewriteJson(path.join(targetModelDir, "metadata.json"), (payload) => {
    payload.run_id = runId;
    payload.model_name = "quality_block_market_probe";
    payload.artifact_uri = path.join(targetModelDir, "metadata.json");
    payload.artifact_dir = targetModelDir;
    payload.training_sample_count = 0;
    payload.feature_scope_modality = "market";
    payload.source_dataset_quality_status = "failed";
    payload.feature_scope_feature_names = payload.feature_names;
    const modelSpec = payload.model_spec;
    if (modelSpec && typeof modelSpec === "object") {
      const specRecord = modelSpec as Record<string, unknown>;
      specRecord.model_name = "quality_block_market_probe";
      payload.model_spec = specRecord;
    }
  });

  await rewriteJson(path.join(targetModelDir, "evaluation_summary.json"), (payload) => {
    payload.run_id = runId;
    payload.dataset_id = blockedDatasetId;
  });

  await rewriteJson(path.join(trackingDir, `${runId}.json`), (payload) => {
    payload.run_id = runId;
    const params = (payload.params ?? {}) as Record<string, unknown>;
    params.dataset_id = blockedDatasetId;
    params.feature_scope_modality = "market";
    payload.params = params;
  });

  const predictionScopes = ["full", "test", "train", "valid"];
  for (const scope of predictionScopes) {
    const predictionPath = path.join(predictionsDir, runId, `${scope}.json`);
    try {
      await rewriteJson(predictionPath, (payload) => {
        const metadata = (payload.metadata ?? {}) as Record<string, unknown>;
        metadata.source_dataset_id = blockedDatasetId;
        payload.metadata = metadata;
      });
    } catch {
      // Keep the copied prediction artifact if a scope is missing.
    }
  }

  return {
    runId,
    datasetId: blockedDatasetId,
    sourceDatasetId,
  };
}
