# ReactorQL 性能优化与验证边界

## 当前目标

在不改变 SQL 语义、Reactor 响应式契约和公开扩展点的前提下，降低常用 SQL／操作符的临时对象分配并提升吞吐。

优化必须是通用实现，不以某个 SQL 文本、字段比例、输入来源或异常类型设置资格特调；不新增跨订阅全局缓存、对象池、预取、私有 `Context` key、复制的协议状态机或错误补偿框架换取局部数据。

目标仍未完成。当前 PR 交付已验证的通用改动和可复现实测框架，不宣告所有场景收益、全局常驻堆下降或已接近 Java 原生计算。

## PR 与交付状态

| 项 | 当前事实 |
| --- | --- |
| Base | `d430595837d17608a4010438d051c5d273d6b1b8` |
| 实现提交 | `03ec6ee042362d253fc09b6259d8238957097d67` |
| Draft PR | [jetlinks/reactor-ql#33](https://github.com/jetlinks/reactor-ql/pull/33)，`master` ← `codex/operator-fastpath` |
| CI gate | [Java 8 Pull Request Unit Test](https://github.com/jetlinks/reactor-ql/actions/runs/37717746461/job/113118061814) 在 testCompile 失败：新增测试使用 `List.of` 等 Java 9 API；Codacy 为 ACTION_REQUIRED。JDK 17 的 649 tests 通过不证明 Java 8 兼容。 |

`03ec6ee…` 是性能对比的生产实现提交；本轮后续变更仅涉及文档和 benchmark tools，不计入实现收益。

## 范围与结构决策

本 PR 覆盖的已审查路径包括：

- 同步 SPI、行表达式和 `Record` 容器的临时对象与执行路径。
- `SELECT` 投影、函数、排序、派生表／子查询、集合路径和资源预算。
- 数值文本与 Duration 时间桶解析。
- 全局、分组和窗口聚合的原生执行边界。
- 对应的真实 SQL JMH 夹具、JFR 热点取证、语义回归和反例。

保留的结构决策：

- 子查询可做订阅级、计划内复用，但每次订阅仍隔离；相关子查询和非安全扩展保持冷执行。
- 聚合保持现有原生 reduce／merge 生命周期。`AVG`、`MAX`、`MIN` 等实时维护增量状态，不收集历史输入行。
- Duration 解析先使用既有非抛异常数值后备，再进入原有 Duration 路径，避免可预期类型失败的异常分配；不扩展格式语法或解析器。
- 整数文本在既有返回类型与精度门槛内走 JDK 整数解析；小数、指数、大精度、十六进制、日期和异常后备仍走原逻辑。
- 默认资源限制保持 `Integer.MAX_VALUE`。生产 Reactor 与其他运行时依赖不变；仅可选 JMH profile 使用 JMH 1.37。

以下方案已经撤回，不能作为后续优化的变体重开：

- 聚合融合、raw 聚合和终止时清理 keys 的捷径。
- JSON 同步边界捷径。
- 条件函数的错误边界捷径。
- 删除公开组键复制、`Record` 快照、冷订阅或原生聚合生命周期的捷径。

撤回原因是其会改变错误作用域、动态 Hook／SPI、来源订阅、取消或公开容器语义；正确性优先于局部基准分数。

## 公开契约与响应式约束

必须保持以下边界：

- 值、具体类型、数值精度、`Double` 位模式、字段集合和有序结果的既有语义。
- SPI／Function／ValueMapper 扩展、动态 Hook 和 `Record` 的公开读取、快照及可变性边界。
- 原始错误作用域、错误对象／来源身份、错误恢复次数与终止信号。
- `Context`、背压需求、冷订阅、取消和活跃源的取消次数。
- 分组键的类型、顺序、隔离和父级 `Context` 绑定。

静态审查仍有 Draft gate：`DefaultReactorQLRecord` 的 `addNamedRecords`／`bindNamedRecords` 对其子类直接读取私有容器，可能绕过子类覆写的 `getRecords(false)`。现有回归只覆盖接口 Proxy；在可 ready 前需要子类来源视图回归并收敛实现。

还需补充并固化以下公开说明：

- `ScalarFilter` 的 `test/apply` 权威语义与 `Context` 异步副作用。
- `RawScalarValueMapper` 的 raw／record 等价性和空值语义。
- `DefaultReactorQLRecord` 的容器所有权与生命周期。

## 已验证边界

历史阶段已完成以下证据，不将其当作当前 PR 相对 `master` 的累计提升：

- 完整离线 package 的有效证据为 649 tests、65 reports、0 failures/errors/skips；日志为 `target/duration-interval-candidate-full-package-verified-20261008.log`。
- 冻结生产验证制品 `49dd58582f72b1bbbaab19e2bb8682e233ee396eb3b8413125866470c0e9002d` 覆盖 Duration A/B 与完整测试。
- 当前取证制品 `8971e0189bd8210d22e0ba1857f90f030b6a0a49ea5b3bfff057b6921ad594f5` 相比 49dd 的 10,505 个既有 class 字节一致，仅增加 14 个 JMH 夹具 class；因此复用 649 测试，未将其表述为本次重新执行。
- 既有固定 SQL 阶段 A/B 证明 Duration 混合投影与时间桶分组存在局部吞吐／分配收益；整数文本宽列和两层子查询证明分配下降，但未证明稳定 CPU 提升。
- 宽列函数、深层子查询、高基数聚合、HAVING／排序／Top-N 的取证覆盖用于定位热点与反例，不把其绝对值作为 native parity 或全局堆结论。

JFR 样本只用于定位 CPU／分配所有者，不能换算为 CPU 百分比、精确常驻堆或端到端业务收益。每输入行 B/op 的下降是分配速率指标，不是 live heap。

## 未解决的内存与兼容风险

- 精确分组需要 O(active keys) 状态；这不是 `AVG`／`MAX` 等对历史行的驻留。
- 调用方若持有带显式 group budget 的 completed group，预算包装器仍可能额外保留 keys；该问题尚未修复。取消外层订阅不能直接清空 keys，因为被选择的内部组仍可能继续执行。
- 尚未完成的 `DefaultReactorQLRecord` 子类视图兼容风险阻止 PR 从 Draft 进入 ready。
- 本地历史验证使用 JDK 17；Java 8 CI 在 testCompile 失败，且 Codacy 仍需处理。

## 当前相对 master 的正式对比

正式对比为 Base `d430595837d17608a4010438d051c5d273d6b1b8` 与生产实现
`03ec6ee042362d253fc09b6259d8238957097d67`；后续文档提交不计入收益。共同 SQL、输入、
harness 与参数保持冻结，性能入口先用独立 oracle 校验正常完整值、具体类型、字段集合、
SQL 定义的行序和源订阅次数；错误／`Context`／需求／取消由 649 项边界测试独立覆盖。

环境为 macOS 26.5.2 (25F84)、Mac17,9、18 physical/logical CPUs、64 GiB RAM；
JDK 17.0.18+8、G1、512m 固定堆、1 线程、2 forks、3×1s warmup／5×1s measurement、GC profiler。
按用户决定保留空闲 Java PID 64398（0.0% CPU、约 34,400 KiB RSS）。计时前早期曾有其他
工作树 Maven 测试短暂活跃，随后回落并退出；采样非全时 CPU 观测，故本次为非独占环境结果，
不声称无外部干扰。

15 个场景的 30 次 JMH 运行均完成且其 Base／PR 的 JMH 99.9% 区间不重叠：吞吐提升范围
23.4%–786.3%，B/输入行减少范围 25.9%–91.2%，没有测得回退。这些范围不做平均，不外推为
所有 SQL、全局内存下降或 Java 原生等价；B/输入行是分配指标，不是 live heap。

| 场景 | Base → PR 吞吐（k 输入行/s，±99.9%） | 吞吐 | Base → PR B/输入行 | 分配 |
| --- | ---: | ---: | ---: | ---: |
| 宽函数投影 | 262.1±7.5 → 593.9±3.9 | +126.6% | 14833.8 → 7606.3 | −48.7% |
| JSON 宽投影 | 527.2±24.7 → 1422.4±39.7 | +169.8% | 8079.9 → 3656.0 | −54.8% |
| 运算符混合宽投影 | 401.5±3.3 → 1922.5±52.8 | +378.8% | 11893.2 → 1695.7 | −85.7% |
| 数值文本投影 | 348.4±12.9 → 778.1±31.1 | +123.3% | 15965.7 → 5642.3 | −64.7% |
| 数值文本两层子查询 | 250.3±18.9 → 552.5±11.7 | +120.7% | 20963.8 → 8708.2 | −58.5% |
| Duration 分组 | 538.2±18.9 → 710.0±21.7 | +31.9% | 5507.2 → 2993.1 | −45.7% |
| 全局聚合 | 6193.2±95.0 → 13248.3±372.8 | +113.9% | 872.0 → 128.0 | −85.3% |
| 窗口聚合 | 2542.1±334.3 → 4605.8±247.7 | +81.2% | 1503.1 → 417.5 | −72.2% |
| ORDER BY + LIMIT | 1794.6±110.3 → 8959.0±543.1 | +399.2% | 2279.7 → 200.0 | −91.2% |
| 多行 INNER JOIN | 84.8±5.7 → 751.3±13.1 | +786.3% | 46701.8 → 5444.0 | −88.3% |
| UNION | 1813.9±148.7 → 9781.9±473.1 | +439.3% | 3018.8 → 517.0 | −82.9% |
| 高基数：1 值/键 | 4.6±0.8 → 6.5±0.3 | +41.6% | 10855.0 → 8039.2 | −25.9% |
| 高基数：2 值/键 | 21.7±0.4 → 26.7±0.9 | +23.4% | 6146.9 → 4211.0 | −31.5% |
| 高基数：50 值/键 | 2044.6±119.9 → 2965.4±182.1 | +45.0% | 1610.0 → 511.6 | −68.2% |
| 分层 GROUP BY／HAVING／Top-N | 476.4±7.1 → 926.5±67.0 | +94.5% | 8839.5 → 3408.4 | −61.4% |

所有 `@OperationsPerInvocation` 指标已经归一到输入行，禁止再次除以行数。JOIN 分母为 20k 左输入
（每左行订阅一次 21 行右源，共 20k 次右源订阅，输出 105k）；UNION 的两源合计为 20k；高基数为 50k 行、1 KiB payload，
每键值数 1／2／50 分别对应 50k／25k／1k 键，setup 含输入驻留，故分配不是常驻堆。

## 复现与归档

构建已有 fat JAR、准备独立输出及复现实测的命令（构建跳过测试，不构成 CI／兼容性证明；新输出目录拒绝覆盖）：

```bash
export JAVA_HOME=/path/to/jdk-17
mvn -o -q -Pjmh -Dmaven.test.skip=true package
python3 tools/benchmark/prepare.py target/base-compare --base d430595837d17608a4010438d051c5d273d6b1b8
python3 tools/benchmark/preflight.py target/base-compare
python3 tools/benchmark/paired-run.py target/base-compare
```

新的 prepare／preflight 已验证：harness、PR engine 与 runtime JAR
与正式测量一致；Base JAR 的 ZIP header 时间不同，但 152 个 class 及全部 JAR entry 内容逐字节一致。
preflight 在新 JVM 上完成 30/30 oracle／engine 来源检查；`paired-run.py` 已按正式参数静态审查，未重复
正式测量。原始 JAR、日志、JSON 和收据保留在本地 `target/`，不纳入 Git；源码、测试、JMH 夹具和本文档纳入 Git。
计划压缩前的完整原件保留为 `target/reactor-operator-fastpath-pre-compression-20261008.md`，用于可恢复审计。

下一步为处理 `DefaultReactorQLRecord` 子类兼容、公开契约和 Java 8／Codacy gate；CI 结果以远端检查的实际终态为准。
