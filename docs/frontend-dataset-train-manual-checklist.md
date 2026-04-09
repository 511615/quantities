# 前端数据集申请到训练联调人工验收脚本

## 目标
验证“申请数据集 -> 进入 `/models` -> dataset-aware 训练”主链路可用，并确保 `not_ready` 数据集不会误触发训练。

## 固定验收路径
1. 打开数据集页，从 `/datasets` 或 `/datasets/browser` 发起新的数据集申请。
2. 等待 job 状态变为 `success`。
3. 在成功态点击“用此数据集训练”。
4. 确认页面跳转到 `/models?launchTrain=1&datasetId=<new_dataset_id>`。
5. 确认训练抽屉自动展开。
6. 确认页面处于 dataset-aware 模式，未显示 dataset preset selector。
7. 提交训练时，在请求体中确认包含 `dataset_id`。
8. 训练成功后确认能进入 run 详情页。
9. 打开 `/datasets/training`，对同一数据集点击“发起训练”，确认同样跳转到 `/models?launchTrain=1&datasetId=<id>`。
10. 对 `readiness_status = not_ready` 的数据集重复以上入口检查，确认两个入口都不会发起训练提交，且页面显示明确阻断提示。

## 联调阻塞补任务判据
- 如果 `/api/datasets/<id>/readiness` 缺少 `LaunchTrainDrawer` 所需字段，或字段命名/语义与页面规则不一致：补 `数据层`。
- 如果 `/models` 传入 `dataset_id` 后，请求参数正确但训练运行结果、deeplink、job/result 结构不稳定：补 `训练框架`。

## 自动化回归基线
- `DatasetRequestDrawer.test.tsx`
- `LaunchTrainDrawer.test.tsx`
- `ModelsPage.test.tsx`
- `TrainingDatasetsPage.test.tsx`
- `App.datasetTrainFlow.test.tsx`
- `tests/webapi/test_workbench_api.py`
