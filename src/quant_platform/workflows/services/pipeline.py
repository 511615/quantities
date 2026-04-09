from __future__ import annotations

from quant_platform.common.types.core import ArtifactRef
from quant_platform.workflows.contracts.requests import (
    BacktestWorkflowRequest,
    PredictionInputRef,
    WorkflowRunRequest,
)
from quant_platform.workflows.contracts.results import WorkflowRunResult
from quant_platform.workflows.contracts.state import (
    WorkflowRunStatus,
    WorkflowStageName,
    WorkflowStageResult,
)
from quant_platform.workflows.runtime import WorkflowRuntime
from quant_platform.workflows.services.backtest import BacktestWorkflowService
from quant_platform.workflows.services.predict import PredictWorkflowService
from quant_platform.workflows.services.prepare import PrepareWorkflowService
from quant_platform.workflows.services.review import ReviewWorkflowService
from quant_platform.workflows.services.train import TrainWorkflowService


class WorkflowPipelineService:
    def __init__(self, runtime: WorkflowRuntime) -> None:
        self.runtime = runtime
        self.prepare_service = PrepareWorkflowService(runtime)
        self.train_service = TrainWorkflowService(runtime)
        self.predict_service = PredictWorkflowService(runtime)
        self.backtest_service = BacktestWorkflowService(runtime)
        self.review_service = ReviewWorkflowService(runtime)

    def run(self, request: WorkflowRunRequest) -> WorkflowRunResult:
        completed_stages: list[str] = []
        stage_results: dict[str, WorkflowStageResult] = {}
        artifact_refs: list[ArtifactRef] = []
        prepare_result = None
        train_result = None
        predict_result = None
        backtest_result = None
        for stage in request.stages:
            if stage == WorkflowStageName.PREPARE:
                if request.prepare is None:
                    raise ValueError("workflow run requested prepare stage without prepare request")
                prepare_result = self.prepare_service.prepare(request.prepare)
                completed_stages.append(stage.value)
                stage_results[stage.value] = prepare_result.stage_result
                artifact_refs.extend(prepare_result.stage_result.artifacts)
            elif stage == WorkflowStageName.TRAIN:
                if request.train is None:
                    raise ValueError("workflow run requested train stage without train request")
                train_request = request.train
                if train_request.dataset_ref is None:
                    if prepare_result is None:
                        raise ValueError("train stage requires dataset_ref or prior prepare result")
                    train_request = train_request.model_copy(
                        update={"dataset_ref": prepare_result.dataset_ref}
                    )
                train_result = self.train_service.train(train_request)
                completed_stages.append(stage.value)
                stage_results[stage.value] = train_result.stage_result
                artifact_refs.extend(train_result.stage_result.artifacts)
            elif stage == WorkflowStageName.PREDICT:
                if request.predict is None:
                    raise ValueError("workflow run requested predict stage without predict request")
                predict_request = request.predict
                updates: dict[str, object] = {}
                if predict_request.dataset_ref is None:
                    dataset_ref = (
                        train_result.dataset_ref
                        if train_result is not None
                        else prepare_result.dataset_ref
                        if prepare_result is not None
                        else None
                    )
                    if dataset_ref is None:
                        raise ValueError(
                            "predict stage requires dataset_ref or prior prepare/train result"
                        )
                    updates["dataset_ref"] = dataset_ref
                if not predict_request.fit_results and not predict_request.fit_result_refs:
                    if train_result is None:
                        raise ValueError("predict stage requires fit_results or prior train result")
                    updates["fit_results"] = [item.fit_result for item in train_result.items]
                if updates:
                    predict_request = predict_request.model_copy(update=updates)
                predict_result = self.predict_service.predict(predict_request)
                completed_stages.append(stage.value)
                stage_results[stage.value] = predict_result.stage_result
                artifact_refs.extend(predict_result.stage_result.artifacts)
            elif stage == WorkflowStageName.BACKTEST:
                if request.backtest is None:
                    raise ValueError(
                        "workflow run requested backtest stage without backtest request"
                    )
                backtest_request = request.backtest
                if not backtest_request.prediction_inputs:
                    if predict_result is None:
                        raise ValueError(
                            "backtest stage requires prediction inputs or prior predict result"
                        )
                    backtest_request = BacktestWorkflowRequest(
                        prediction_inputs=[
                            PredictionInputRef(
                                model_name=item.model_name,
                                run_id=item.run_id,
                                prediction_frame_uri=item.prediction_frame_uri,
                            )
                            for item in predict_result.items
                        ],
                        backtest_request_template=backtest_request.backtest_request_template,
                        dataset_ref=backtest_request.dataset_ref
                        or (
                            train_result.dataset_ref
                            if train_result is not None
                            else prepare_result.dataset_ref
                            if prepare_result is not None
                            else None
                        ),
                        benchmark_name=backtest_request.benchmark_name,
                        data_source=backtest_request.data_source,
                        market_bars=backtest_request.market_bars,
                    )
                backtest_result = self.backtest_service.backtest(backtest_request)
                completed_stages.append(stage.value)
                stage_results[stage.value] = backtest_result.stage_result
                artifact_refs.extend(backtest_result.stage_result.artifacts)
            elif stage == WorkflowStageName.REVIEW:
                if request.review is None:
                    raise ValueError("workflow run requested review stage without review request")
                review_request = request.review
                if not review_request.experiment_refs and train_result is not None:
                    review_request = review_request.model_copy(
                        update={
                            "experiment_refs": [
                                ArtifactRef(
                                    kind="train_manifest", uri=item.fit_result.train_manifest_uri
                                )
                                for item in train_result.items
                            ]
                        }
                    )
                if not review_request.input_artifacts and backtest_result is not None:
                    review_request = review_request.model_copy(
                        update={
                            "input_artifacts": [
                                ArtifactRef(
                                    kind="backtest_report", uri=item.backtest_result.report_uri
                                )
                                for item in backtest_result.items
                            ]
                        }
                    )
                review_result = self.review_service.review(review_request)
                completed_stages.append(stage.value)
                stage_results[stage.value] = review_result.stage_result
                artifact_refs.extend(review_result.stage_result.artifacts)
            elif stage == WorkflowStageName.BENCHMARK:
                raise ValueError(
                    "benchmark is not part of workflow run; call workflow benchmark separately"
                )
        status = (
            WorkflowRunStatus.SUCCESS
            if len(completed_stages) == len(request.stages)
            else WorkflowRunStatus.PARTIAL
        )
        return WorkflowRunResult(
            workflow_id=request.workflow_id,
            completed_stages=completed_stages,
            stage_results=stage_results,
            artifact_refs=artifact_refs,
            status=status,
        )
