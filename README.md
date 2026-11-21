# sre-alert-deduplication-window-planner

规划多源告警去重窗口、状态恢复和抑制链。

## 定位

**本规划器只消费离线事件。** 它不连接 Alertmanager、不订阅实时告警流、
不做任何网络 IO：调用方把一批已经发生的告警状态变化事件（firing /
resolved）一次性交给 `DedupPlanner.ingest()`，再通过 `evaluate(now)`
获取分组、当前状态、抑制链和恢复证据。它适合离线复盘、窗口参数调优
和恢复策略仿真。

## 模型

- `AlertEvent`：单条状态变化，含 `source`（metrics / logs / probe 等）、
  `fingerprint`、`labels`、`state`（`firing` / `resolved`）、`timestamp`。
- 分组键 = `fingerprint` + 身份标签（默认 `service`，可通过
  `identity_labels` 调整）。指纹仅做精确匹配：相似但不同的指纹属于
  不同根因，绝不合并；非身份标签的变化只记入 `label_history`，不拆组。
- 恢复判定按来源独立记账：某来源的 `resolved` 时间戳必须不早于它自己
  最后一次 `firing` 才算恢复，因此"恢复事件早于另一来源的最后一条
  firing"不会误关事故。所有来源恢复后再静默一个 `window_seconds`，
  状态才从 `pending_recovery` 转为 `resolved`。
- 恢复确认后再次 firing 记为复发（`recurrence_count` + 新 episode）；
  确认窗口内的 firing 则撤销过早的恢复、重开当前 episode。
- `InhibitionRule`：上游服务有 firing 告警时，下游匹配告警被标记为
  `inhibited`（证据保留、不计为独立事故）；上游恢复后抑制解除，
  下游仍在 firing 的告警重新可见。`inhibition_chains` 中
  `active=False` 表示已解除的抑制。

## 用法

```python
from planner import AlertEvent, DedupPlanner, InhibitionRule

planner = DedupPlanner(
    window_seconds=300,
    inhibition_rules=[InhibitionRule(upstream_service="db",
                                     downstream_service="api")],
    dependencies={"api": ["db"]},
)
planner.ingest([
    AlertEvent(source="metrics", fingerprint="fp1",
               labels={"service": "api"}, state="firing", timestamp=0),
    AlertEvent(source="logs", fingerprint="fp1",
               labels={"service": "api"}, state="resolved", timestamp=120),
])
report = planner.evaluate(now=600)
# report: groups / inhibition_chains / recovery（含每来源恢复证据）
```

事件可以乱序到达，`ingest` 内部按时间戳排序后再应用。

## 输出

`evaluate()` 返回字典：

- `groups`：每组的状态（`firing` / `inhibited` / `pending_recovery` /
  `resolved`）、`inhibited_by`、episode 列表、复发次数、各来源
  最后 firing/resolved 时间、标签变化历史和全部证据事件。
- `inhibition_chains`：每条规则命中的上游 → 下游链及是否生效。
- `recovery`：每组的恢复时间、确认时间和逐来源恢复证据。

## 测试

```bash
python -m unittest
```

覆盖多源重复、相似指纹不合并、标签变化、恢复后复发、抑制与抑制解除、
恢复乱序与过早恢复重开。代码兼容 Python 3.8.2+。
