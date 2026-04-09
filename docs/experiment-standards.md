# Experiment Standards

- 每次实验必须具备唯一 `run_id`
- 每次训练必须保存 resolved config 或可重建 config 组合
- 每次训练必须记录 `dataset_hash` 与 `model_spec.version`
- 不允许只汇报 in-sample 指标
- 新模型必须至少与一个 baseline 比较
