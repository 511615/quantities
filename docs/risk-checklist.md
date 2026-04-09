# Risk Checklist

- [x] 特征层禁止目标变量
- [x] 数据集层强制标签归属
- [x] 切分层校验时间顺序
- [x] 样本层校验 `available_time <= as_of_time`
- [x] 回测层只消费预测桥接对象
- [x] Agent 层默认禁止核心状态写入
