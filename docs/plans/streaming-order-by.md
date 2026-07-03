# 流式 ORDER BY 设计与测试计划

## 背景

ReactorQL 的 `ORDER BY` 运行在 `Flux` 处理链上。精确的 SQL 全局排序必须等上游结束后才能确定第一条输出，在大量数据、长流或无限流中会形成全量物化，带来高延迟、GC 压力和 OOM 风险。

相同约束也出现在主流流式框架中：Reactor `Flux.sort` 会收集全部元素后再排序；Spark Structured Streaming 不支持对输入流直接排序，因为需要跟踪全部已接收数据；Flink SQL 流式 `ORDER BY` 要求主排序字段为递增时间属性，Top-N 和 Window Top-N 则作为受限排序能力提供；Beam 对无界集合的聚合要求窗口或触发器把数据切分成有限集合。

## 目标

1. 保留已有小数据/有界数据 `ORDER BY` 的全局排序语义。
2. 为无 `LIMIT` 的全局排序提供默认最大物化行数，超过后失败而不是持续占用内存。
3. 对 `ORDER BY ... LIMIT [offset,] rowCount` 使用有界 Top-N 策略，只保留 `offset + rowCount` 条候选，语义等价于全局排序后分页。
4. 提供显式窗口排序设置，用于用户接受“只在窗口内有序”的流式场景。
5. 排序表达式的求值保持响应式组合，不再在 comparator 中同步等待异步值。

## 非目标

- 不实现数据源下推排序；数据库、搜索引擎、时序库等源侧优化由数据源实现或上层查询规划负责。
- 不承诺窗口排序满足 SQL 全局 `ORDER BY` 语义。
- 不引入外部状态存储或磁盘 spill；后续如需要可单独设计。

## 方案

### 默认全局排序

- 新增 setting：`orderBy.maxRows`。
- 默认值：`10000`；硬上限：`1000000`。
- 无 `LIMIT` 且未开启窗口排序时，最多物化 `orderBy.maxRows` 条记录进行排序；超过立即抛出 `UnsupportedOperationException`。

### LIMIT Top-N

- 当 SQL 存在 `LIMIT rowCount` 或 `LIMIT offset,rowCount`，计算 Top-N 的候选数量为 `offset + rowCount`。
- 候选数量不得超过 `orderBy.maxRows` 和 `Integer.MAX_VALUE`。
- 使用 `PriorityQueue` 保留当前最优 N 条，空间复杂度从全量 `O(total)` 降到 `O(offset + rowCount)`。
- 排序完成后再交给既有 `offset` 和 `limit` 阶段，保持当前执行链和结果语义。

### 窗口排序

- 新增 setting：`orderBy.windowSize`。
- 默认值：`0` 表示关闭；硬上限：`100000`。
- 设置为正数时，按固定条数窗口进行局部排序，只保证窗口内有序。
- 这是显式降级模式，适合实时处理“局部有序足够”的场景，不适合作为 SQL 全局排序替代。

## 边界与安全

- setting 可通过 builder 或 SQL hint 进入 metadata，因此保留硬上限，避免查询侧绕过资源保护。
- `orderBy.maxRows <= 0`、`orderBy.windowSize < 0`、超过硬上限均在构造阶段失败。
- `LIMIT` 或 `OFFSET` 为负数时失败。
- `ORDER BY ... LIMIT 0` 不消费排序候选，直接输出空流。
- `NULLS FIRST/LAST` 按 JSqlParser 的 `OrderByElement.NullOrdering` 显式处理；未指定时，ASC 默认 null first，DESC 默认 null last。

## 测试目标

- 既有 `ORDER BY ASC/DESC` 结果保持不变。
- 无 `LIMIT` 超过 `orderBy.maxRows` 时失败，错误消息包含 setting key。
- `ORDER BY ... LIMIT N` 在输入远大于 N 时只要求 `orderBy.maxRows >= N`，结果等价于全局排序后取前 N。
- `ORDER BY ... LIMIT offset,rowCount` 使用 `offset + rowCount` 作为候选上限，结果正确。
- 动态绑定的 `LIMIT ?` 能在执行时识别并走 Top-N。
- Top-N 支持 DESC、`LIMIT 0`。
- 固定 `orderBy.windowSize` 只做窗口内排序，验证局部有序结果。
- 多列排序和 `NULLS LAST` 行为正确。
- 非法 setting 构造失败。

## 验证命令

```bash
mvn -q '-Dtest=ReactorQLTest#testOrderBy*' test
mvn -q test
```

## 参考资料

- Reactor `Flux.sort` API 文档：`sort` 会先收集并排序，长流/无限流可能 OOM，建议用 `window` 切分批次。
- Reactor Reference Guide：大量元素可用 grouping/windowing/buffering 做批次化处理。
- Apache Flink SQL `ORDER BY`：流模式下主排序键必须是递增时间属性。
- Apache Flink SQL Top-N / Window Top-N：连续流用 Top-N 或 Window Top-N 约束排序状态，窗口 Top-N 可在窗口结束后清理中间状态。
- Apache Spark Structured Streaming：流式 Dataset 仅在聚合后且 Complete 输出模式下支持排序。
- Apache Beam Programming Guide：无界 PCollection 通过 windowing 拆成有限逻辑窗口，聚合按窗口处理。
