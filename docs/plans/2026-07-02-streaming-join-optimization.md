# 流式 Join 与双数据源合并优化开发计划

## 背景

ReactorQL 当前 `join` 实现位于 `DefaultReactorQL#createJoin()`，整体是嵌套循环风格：

- 对每一条左侧记录重新获取右侧数据源。
- 对右侧结果逐条执行 `ON` 条件过滤。
- `from t1,t2 where t1.v=t2.v` 会先构造交叉结果，再由 `where` 过滤。
- join 链路里存在 `flatMap(..., Integer.MAX_VALUE)`，没有使用 `ReactorQLMetadata#getConcurrency()` 的并发上限。

这对小集合或有限数据是可用的；但对流式数据、设备遥测、事件订阅或较大右表，会放大成 `leftCount * rightCount`，还可能重复订阅右侧数据源。

当前已有的双数据源合并能力：

- `zip((select ...), (select ...))`：按位置一一配对，最短流完成即结束。
- `combine((select ...), (select ...))`：使用各数据源最新值组合，适合少量最新状态流。
- `union all` / `union`：同结构数据追加或去重追加。
- `except` / `intersect` / `minus`：集合操作，当前会收集两侧数据，只适合有界集合。

本计划的优先业务场景是：合并两个较大的 CSV 或 JSONL 文件。文件会被转换成两个 ReactorQL source，按相同时间戳把两边记录合并到同一行。

这个场景不应优先使用通用 SQL join；最佳路径应是 `merge_by_key(...)` 这类面向有序文件的流式归并能力。

## 目标

- 为两个 CSV / JSONL source 按时间戳合并提供 `merge_by_key(...)` 能力。
- 在输入按 key 有序时，使用 sort-merge 思路流式合并，内存只与当前 key 的重复行数量和预取窗口相关，不与文件总行数相关。
- 减少等值 join 场景下不必要的笛卡尔积。
- 保持现有 SQL 行为默认兼容，避免直接改变无界流输出语义。
- 为高频流式数据提供更明确的合并方式，而不是把所有场景都塞进通用 SQL join。
- join 链路遵守响应式边界：不使用无界 `collectList()`、不无限并发、不重复执行可避免的数据源订阅。
- 提供可配置的资源上限，避免 hash index、窗口状态或 lookup 调用失控。
- 补充 README / dataset help 的用法指引，引导智能体在不同合并场景选择正确写法。

## 非目标

- 不把 ReactorQL 变成完整数据库优化器。
- 不支持无窗口、无 TTL 的两个无限流全量 join。
- 不改变 `zip`、`combine`、`union` 现有语义。
- 不在第一阶段重写 `where` 谓词下推框架；`from t1,t2 where t1.v=t2.v` 先通过文档引导改成显式 `join ... on`。
- 不把 hash join 默认用于未知边界的数据源，因为右侧不完成时会导致左侧长期无输出。
- 不在未确认语义前修正 `right join` 现有行为；先用测试固定当前兼容行为，再讨论是否按标准 SQL 改造。

## 影响范围

- `src/main/java/org/jetlinks/reactor/ql/DefaultReactorQL.java`
  - 拆分 `createJoin()` 内的策略选择和执行逻辑。
  - 将 join 内部并发控制从 `Integer.MAX_VALUE` 收敛到 metadata setting。
- 新增 `src/main/java/org/jetlinks/reactor/ql/supports/join/`
  - 放置 join plan、key extractor、strategy、settings 等内部类。
- `src/main/java/org/jetlinks/reactor/ql/ReactorQLMetadata.java`
  - 复用现有 `setting` 机制，增加 join 相关 setting 读取辅助；不强制新增公共 API。
- 可选：`src/main/java/org/jetlinks/reactor/ql/ReactorQLContext.java`
  - 第二阶段再考虑通过 default method 暴露 lookup 能力，避免破坏现有 `Function<String, Publisher<?>>` 数据源入口。
- `README.md`
  - 增加双数据源合并选择说明。
- `cloud.jetlinks/modules/jetlinks-ai-agent/ai-agent-general/src/main/resources/tools/dataset/help.md`
  - 后续同步补充 dataset 工具提示词，推荐显式 `join ... on`、`zip`、`combine`、`union all` 和 JSON 函数的选择边界。

不涉及权限、i18n、持久化结构、事件协议或数据库迁移。

## 推荐方案

### 0. CSV / JSONL 按时间戳合并优先使用 `merge_by_key`

目标 SQL 形态建议：

```sql
select *
from merge_by_key(
  (select timestamp, temperature temp from file1),
  (select timestamp, humidity hum from file2),
  'timestamp'
) m
```

建议支持可选参数或 hint：

- `joinType` / `mode`：`inner`、`left`、`right`、`full`，默认建议 `full`，因为文件合并通常要保留两侧时间戳。
- `sorted`：默认 `true`，表示两侧输入已按 key 升序；如果不是有序输入，必须显式选择 `window` 或 `hash-small-side` 策略。
- `duplicateStrategy`：`error`、`first`、`last`、`zip`、`array`、`cartesian`；默认建议 `error` 或 `array`，避免重复时间戳时静默丢数据。
- `maxRowsPerKey`：限制同一时间戳下最多缓存多少行。
- `prefetch`：控制 Reactor `mergeComparing` 的预取大小。

在两侧输入都按 timestamp 排序时，推荐实现方式：

1. 给左右记录打上 source tag 和 key。
2. 使用 Reactor Core 3.4 已支持的 `Flux.mergeComparing(prefetch, comparator, left, right)` 按 key 合并两个有序流。
3. 使用 `bufferUntilChanged(tagged -> tagged.key)` 收集当前 key 的左右记录。
4. 对每个 key 的小 buffer 做行合并并输出。

这种方式不是 hash join，也不是嵌套循环。它的状态边界是：

- 两个 source 的预取缓存。
- 当前 key 的全部重复记录。
- 输出合并时的临时记录。

只要输入有序，并且单个 timestamp 下重复行数量受控，内存不会随文件总行数增长。

重要前提：

- `mergeComparing` 要求每个输入 source 自身已经按相同 comparator 排序。
- 它不会修正单个 source 内部乱序，只会在多个已排序 source 的当前头部元素之间做归并选择。
- 如果某一侧出现 `10:00, 10:02, 10:01`，归并后的全局 key 顺序可能被破坏。
- `bufferUntilChanged(key)` 只合并连续相同 key；同一个 timestamp 如果被乱序数据隔开，会被拆成多个 buffer，最终导致相同时间戳没有合并到同一行。

如果两侧文件不是按 timestamp 有序，必须先选择其中一种策略：

- 先外部排序两个文件，再进入 `merge_by_key`。
- 如果乱序范围有限，使用 `merge_by_key(..., sorted=false, maxLateness='30s')` 这类带水位线 / TTL 的窗口归并。
- 如果一侧很小，使用 bounded hash join，把小侧建索引。
- 如果两侧都大且完全无序，又要求精确全量匹配，只能外部排序或落临时索引存储，无法做到纯内存稳定流式处理。

#### 流式排序策略

严格意义上的全量排序不是纯流式能力：如果数据完全无序，必须看到全部数据后才能保证第一个输出就是全局最小 key。因此需要按数据特征选择排序边界。

1. 已有序或可在读取前排序：直接使用 `merge_by_key(sorted=true)`，这是 CSV / JSONL 大文件合并的首选。
2. 有限乱序：使用 bounded reorder buffer。按 timestamp 建小顶堆或 `TreeMap<key, rows>`，维护 watermark，例如 `maxSeenTimestamp - maxLateness`；只输出小于等于 watermark 的 key。内存与乱序窗口内的数据量相关。
3. 完全无序但文件有限：使用外部排序。分块读取、块内排序、写临时 run 文件，再多路归并输出有序流，最后进入 `merge_by_key(sorted=true)`。内存与 chunk 大小和归并路数相关。
4. 完全无序且不允许临时文件 / 外部索引：不能保证精确全量同 timestamp 合并，只能选择近似窗口合并或失败提示。

建议第一版 `merge_by_key` 默认要求有序输入；如果检测到 key 倒退，直接抛出明确异常，例如 `source file1 is not sorted by timestamp`。后续再增加 `sorted=false,maxLateness(...)` 和外部排序能力。

#### 大文件排序最佳实践

面向 CSV / JSONL 大文件，排序能力应分成三个明确模式，而不是统一叫“流式排序”：

1. `verify-sorted`：边读边检查 key 是否单调递增，只保存上一条 key。内存 O(1)，失败时提示用户先排序或开启外部排序。
2. `bounded-reorder`：只处理有限乱序。维护 `TreeMap<key, rows>` 或小顶堆，按 `maxSeenKey - maxLateness` 输出已安全的 key。内存 O(乱序窗口内行数)，不是全量排序。
3. `external-sort`：处理完全无序的大文件。按内存上限分块读取、块内排序、写 run 文件，再 k-way merge 输出有序流。内存 O(chunkSize + mergeFanIn)，磁盘 O(inputSize) 到 O(2 * inputSize) 量级。

`merge_by_key` 的第一版建议只依赖 `verify-sorted`。如果要内置排序能力，优先加 `external_sort(source, key, options...)` 或 `merge_by_key(..., sorted=false, sortMode='external')`，并必须配置：

- `sort.memoryLimit`：单 chunk 最大内存或最大行数。
- `sort.tempDirectory`：临时文件目录。
- `sort.maxTempBytes`：临时文件总上限。
- `sort.mergeFanIn`：每轮最多归并多少个 run。
- `sort.compression`：临时 run 是否压缩。
- `sort.stable`：同 key 是否保持原始顺序。
- `sort.onUnsorted`：检测到乱序时 `fail` / `external-sort` / `bounded-reorder`。

不建议在 Reactor 链路中直接使用 `sort()` 或 `collectList().sort(...)` 支持大文件；这会把整个文件收集到内存，违背 dataset 大文件处理目标。

### 1. 先拆出 Join Planner 与 Nested Loop Strategy

先不改变默认 join 行为，把当前实现迁移为内部策略：

- `JoinPlan`：保存 join 类型、右侧来源、原始 `ON` 表达式、已编译 predicate、可选 key 信息。
- `JoinSource`：封装 table / subselect 右侧来源构造逻辑。
- `NestedLoopJoinStrategy`：保留现有语义，但使用 `metadata.flatMap(...)` 或 `join.concurrency` 控制并发。
- `JoinSettings`：集中读取 join setting 和资源上限。

第一阶段可立即解决无限并发问题，并为后续 hash / lookup / window 策略留出扩展点。

建议 setting：

- `join.strategy`：`auto` / `nested_loop` / `hash` / `lookup` / `window`。
- `join.concurrency`：join 内部左侧并发，默认沿用 `metadata.getConcurrency()`。
- `join.cartesian.enabled`：无 `ON` 条件或逗号 join 是否允许笛卡尔积，默认保持兼容为 `true`；dataset 场景可设为 `false`。
- `join.maxCartesianRows`：可选保护阈值，超过后失败；只在能估算或计数时生效。

### 2. 增加等值 Hash Join，但只在有边界时启用

对这类谓词提取 join key：

```sql
select ...
from t1
join t2 on t1.deviceId = t2.deviceId
```

支持范围：

- 单 key：`left.key = right.key`
- 复合 key：`left.productId = right.productId and left.deviceId = right.deviceId`
- key 两侧顺序可互换：`right.id = left.id`
- `ON` 中除等值条件外的其他条件保留为 residual predicate，例如：

```sql
on t1.deviceId = t2.deviceId and t2.temp > 30
```

执行方式：

1. 右侧数据源在明确有界时构建 `Map<JoinKey, List<ReactorQLRecord>>`。
2. 左侧流继续流式处理，只按 key 查找候选右侧记录。
3. 对候选记录再执行原始 predicate，保证复杂条件结果一致。
4. inner join 输出匹配结果；left join 对无匹配左侧输出左记录。

启用规则：

- `join.strategy=hash`：显式启用；必须配置或使用默认上限。
- `join.strategy=auto`：仅在右侧明显有界时启用，例如 `values`、带 `limit` 的子查询、或后续可识别的 bounded source 标记。
- 普通 table 数据源在边界未知时仍使用 nested loop 或 lookup，不自动 hash。

资源上限：

- `join.hash.maxBuildRows`：默认建议 10000。
- `join.hash.maxMatchesPerKey`：默认建议 1000。
- `join.hash.maxKeySize`：限制复合 key 字段数量或字符串化长度。
- 超限时抛出明确异常，不静默退回笛卡尔积，避免性能问题被隐藏。

第一版建议只覆盖 inner / left join。`right join` / full join 需要 unmatched right tracking，状态和兼容语义更复杂，放到独立任务确认。

对于两个大 CSV / JSONL 文件，hash join 不是首选。除非能证明其中一侧足够小，或者允许分片 / 溢写到外部存储，否则 hash index 仍然会随输入行数增长。

### 3. 增加 Lookup Join 数据源能力

hash join 适合“小右表维表”；lookup join 更适合“左流大、右侧可按 key 查询”的场景，例如设备事件流关联设备档案、产品信息、资产信息。

建议新增可选能力，不破坏现有上下文入口：

```java
public interface ReactorQLLookupContext extends ReactorQLContext {
    Flux<Object> lookupDataSource(String table, Map<String, Object> keys);
}
```

或在 `ReactorQLContext` 增加 default method：

```java
default Optional<Function<Map<String, Object>, Flux<Object>>> getLookupDataSource(String table) {
    return Optional.empty();
}
```

执行方式：

- planner 提取 `t1.id = t2.id` 后，为每条左侧记录计算 key。
- 如果 context 提供右表 lookup 能力，则调用 lookup，而不是扫描整张右表。
- lookup 返回结果仍包装成右侧 `ReactorQLRecord`，再执行 residual predicate。
- 用 `join.lookup.concurrency` 和 `join.lookup.timeout` 控制并发与超时。

这比 hash join 更适合无界左流，因为不需要等待右侧全量完成。

### 4. 为真正的双无限流增加 Window / Latest 合并函数

两个无限流不能做无界 SQL join，否则必须无限保存历史状态。建议新增 table function，而不是把语义藏在普通 `join` 里。

推荐两个函数方向：

#### `latest_join` / `combine_by_key`

适合“按 key 合并两个最新状态”的场景：

```sql
select r.t1.deviceId, r.t1.temp, r.t2.humidity
from combine_by_key(
  (select deviceId,temp from t1),
  (select deviceId,humidity from t2),
  'deviceId',
  '5m'
) r
```

语义：

- 每个 key 分别保存左右两侧最新值。
- 任一侧更新时，如果另一侧在 TTL 内有值，则输出组合结果。
- 状态按 TTL 清理。
- 适合设备属性、指标快照、在线状态等“最新值”关联。

#### `window_join`

适合“时间窗口内事件匹配”的场景：

```sql
select r.t1.deviceId, r.t1.event, r.t2.command
from window_join(
  (select deviceId,ts,event from t1),
  (select deviceId,ts,command from t2),
  'deviceId',
  'ts',
  '30s'
) r
```

语义：

- 按 key 分桶。
- 只在指定时间窗口或处理时间 TTL 内匹配。
- 状态过期清理。
- 必须有 `maxStatePerKey`、`maxKeys`、`ttl` 等上限。

这两个函数是比通用 SQL join 更适合流式场景的合并方式。

### 5. 用法引导规则

后续文档和 dataset help 建议引导智能体按场景选择：

| 场景 | 推荐方式 | 原因 |
| --- | --- | --- |
| 两个同结构流追加 | `union all` | 不需要关联，成本最低 |
| 追加并去重 | `union` | 语义明确 |
| 两个流按顺序一一对应 | `zip(...)` | 避免笛卡尔积 |
| 多个低频状态流组合最新值 | `combine(...)` | 当前已支持最新值组合 |
| 按 key 组合最新状态 | `combine_by_key(...)` | 有 TTL 状态边界 |
| 小维表关联大左流 | hash join 或 lookup join | 避免扫描右表 |
| 右侧可按 key 查询 | lookup join | 不需要构建全量右表 |
| 两个无限事件流匹配 | `window_join(...)` | 必须有窗口或 TTL |
| 任意复杂非等值关联 | nested loop | 保持兼容，但要求数据量小或显式上限 |

## 被拒绝方案

- **直接把所有 join 改成 hash join**：右侧无界时永远等不到构建完成，流式结果会卡住。
- **自动把 `where t1.id=t2.id` 改写成 join**：短期风险较高，需要 predicate 下推和来源识别，后续可作为 optimizer 单独做。
- **对所有右表加 `cache()`**：会把无界右流缓存到内存，风险比当前重复扫描更隐蔽。
- **无 TTL 的双流 join**：状态无限增长，不适合响应式流处理。
- **静默降级**：例如 hash 超限后偷偷退回 nested loop，会让用户误以为 SQL 已优化，实际仍可能爆炸。

## 任务拆分

### 阶段 1：兼容性重构与并发收敛

1. 先实现 `merge_by_key(...)` 的有序流式归并能力，覆盖 CSV / JSONL 按 timestamp 合并主场景。
2. 新增 join 内部 package 和 `JoinPlan` / `JoinSettings` / `NestedLoopJoinStrategy`。
3. 保留当前 join 输出语义，迁移 `DefaultReactorQL#createJoin()` 为 planner 调用。
4. join 内部 `flatMap(..., Integer.MAX_VALUE)` 改为受 `join.concurrency` / `metadata.getConcurrency()` 控制。
5. 增加 `join.cartesian.enabled` setting；默认兼容，dataset 场景可关闭。
6. 增加当前行为回归测试，尤其是 inner / left / right / subselect join。

### 阶段 2：等值条件识别与 Hash Join

1. 实现 `JoinKeyExtractor`，识别 `EqualsTo` 和 `AndExpression` 下的等值 key。
2. 支持单 key、复合 key、左右顺序互换。
3. `ON` 中不能转成 key 的条件作为 residual predicate 保留。
4. 实现 bounded hash join，支持 inner / left join。
5. 增加 hash 资源上限 setting 和超限错误。
6. 增加数据源订阅次数测试，证明右侧不会按左侧记录重复订阅。

### 阶段 3：Lookup Join 能力

1. 设计并确认可选 lookup context API。
2. 实现 `LookupJoinStrategy`，仅在 context 明确提供 lookup 能力时启用。
3. 支持 lookup 并发、超时、空结果、异常传播。
4. 补充模拟数据源测试：验证只按 key 查询，不扫描全量右表。

### 阶段 4：流式专用合并函数

1. 先实现 `combine_by_key(...)`，解决最常见“两个状态源按 key 合并最新值”的需求。
2. 再实现 `window_join(...)`，支持事件流窗口匹配。
3. 每个函数必须有 TTL / maxKeys / maxStatePerKey 上限。
4. 使用虚拟时间或短周期测试验证状态过期、乱序、同 key 多事件、空侧不输出等行为。

### 阶段 5：文档与智能体提示词

1. README 增加 join 性能说明和双数据源合并选择表。
2. dataset help 增加提示词规则：
   - 优先显式 `join ... on`，不要生成 `from a,b where a.id=b.id`。
   - 能用 `zip` / `combine` / `union all` 时不要用 join。
   - 两个无限流必须使用窗口或 TTL 合并函数。
   - 小维表关联使用 hash / lookup join，并设置边界。
3. 给出典型 SQL 示例和反例。

## 测试目标

### 回归测试

- 当前 `testJoin`、`testLeftJoin`、`testRightJoin`、`testSubJoin`、`testJoinWhere` 保持通过。
- 多 join 链路输出顺序和字段命名保持兼容。
- `join.concurrency=1` 时按顺序执行；并发大于 1 时结果数量正确。

### Hash Join 测试

- `t1.id = t2.id` 只输出匹配记录。
- 复合 key 正确匹配。
- 一对多 key 输出多条结果。
- left join 对无匹配左侧输出左记录。
- `null` key 行为用测试固定：建议不参与 hash 匹配，left join 输出左记录。
- residual predicate 仍生效。
- 右侧超过 `join.hash.maxBuildRows` 时失败。
- 右侧数据源订阅次数为 1，而不是 left row 次数。
- 右侧无界且未显式 bounded 时不自动 hash。

### Lookup Join 测试

- context 提供 lookup 能力时，planner 选择 lookup。
- 每条左侧记录按 key lookup，右侧全量数据源不被扫描。
- lookup 返回多条时输出多条。
- lookup 空结果时 inner join 不输出，left join 输出左记录。
- lookup 超时或错误按响应式错误传播，不吞异常。

### Window / Latest 合并测试

- `combine_by_key` 在左右两侧同 key 都有最新值后输出。
- 任一侧更新时使用另一侧 TTL 内最新值输出。
- TTL 过期后不再用旧值。
- `maxKeys` / `maxStatePerKey` 超限失败。
- `window_join` 只匹配窗口内事件。
- 使用 `StepVerifier` / 虚拟时间，不能靠 `Thread.sleep`。

### 文档与提示词测试

- README 示例能通过单元测试或已有 parser 流程。
- dataset help 中反例和推荐写法与实际函数名一致。

## SQL 性能与响应式边界

- nested loop 只适合小数据量或显式上限，复杂非等值条件继续走该策略。
- hash join 只在右侧有界时使用，构建 hash index 前必须有行数上限。
- lookup join 避免右侧全量扫描，但需要控制并发、超时和错误传播。
- window / latest 合并必须有 TTL 和状态上限。
- 不在主链路使用 `block()` 或嵌套 `subscribe()`。
- 所有 `collectMap` / `collectMultimap` 都必须在明确边界后使用，并在代码注释中说明边界来源。

## 链路追踪与 MBean 决策

- 本次主要是 ReactorQL 内部算子与函数能力，不直接引入业务链路、外部命令或持久化副作用；第一阶段不新增 TraceHolder / MonoTracer。
- lookup join 如果后续接入平台外部数据源，可由平台数据源自身 tracing 负责；ReactorQL 只保留策略选择和计数类日志或调试信息。
- window / latest 合并会维护内部状态，但它属于单次查询生命周期内的临时状态；第一版不新增 MBean。
- 如果后续把 keyed state 做成可复用、长生命周期组件，再单独设计 MBean 指标，例如 key 数、左右状态数、过期清理次数、超限次数。

## 待确认问题

1. `right join` 是否要按标准 SQL 语义修正？当前测试只断言数量，现有实现与标准语义可能不完全一致。建议第一阶段保持兼容，后续单独确认。
2. hash join 是否允许通过 SQL hint 显式启用，例如 `select /*+ join.strategy(hash),join.hash.maxBuildRows(1000) */ ...`？现有 metadata 已支持 Oracle hint 写入 setting。
3. dataset 场景是否默认关闭笛卡尔积，即设置 `join.cartesian.enabled=false`？
4. 流式合并函数命名采用 `combine_by_key` / `window_join`，还是使用更 SQL 化的 `latest_join` / `stream_join`？
5. lookup 数据源能力放在 reactor-ql 公共 API，还是先由 JetLinks dataset 工具侧通过自定义 `FromFeature` 实现？

## 建议推进顺序

1. 先确认本计划，尤其是默认兼容策略、hash join 启用边界和流式函数命名。
2. 基于 master 新建 PR 分支。
3. 先提交阶段 1，锁定当前行为并收敛并发。
4. 再提交阶段 2，实现 bounded hash join。
5. lookup join 和 window/latest 合并根据业务优先级拆成后续 PR，避免一个 PR 同时改动 join 核心、公共 API 和新 table function。
