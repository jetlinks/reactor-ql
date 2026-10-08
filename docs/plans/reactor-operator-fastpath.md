# ReactorQL 行级操作符优化计划

## 当前交付边界（Draft PR，2026-10-08）

- 目标分支：`master`；来源分支：`codex/operator-fastpath`；本次为 Draft，整体性能目标仍处于 blocked，未宣告完成或可直接合并。
- 覆盖已验证的通用改动：同步 SPI／行表达式与 `Record` 容器、排序／子查询、可选资源预算、通用数值／时间解析，以及真实 SQL 的 JMH 和反例回归。
- 生产实现保持 Reactor 订阅、`Context`、需求与取消链路；默认限制仍为 `Integer.MAX_VALUE`，未升级生产 Reactor 或其他依赖，仅可选 JMH profile 使用 JMH 1.37。
- 聚合仍为原生增量 reduce／merge，`AVG`／`MAX` 等不收集历史行；旧聚合融合、raw 聚合、JSON 与条件函数的错误边界捷径均已撤回。
- 验证复用完整离线 package：649 tests、65 reports、0 failures/errors/skips；详见 `target/duration-interval-candidate-full-package-verified-20261008.log` 与 class/test audit。
- `49dd5858…` 为冻结的生产验证制品；`8971e018…` 仅在其基础增加基准夹具，10,505 个既有 class 字节一致，故本轮复用 649 测试而非重新执行。
- Draft gate：静态审查发现 `DefaultReactorQLRecord` 对 `DefaultReactorQLRecord` 子类的 `addNamedRecords`／`bindNamedRecords` 直接读取私有容器，可能绕过子类覆写的 `getRecords(false)`；现有回归仅覆盖接口 Proxy。需补子类来源视图回归并收敛实现后才可 ready。
- 另有待补契约说明：`ScalarFilter` 的 `test/apply` 权威语义、`Context` 异步副作用，`RawScalarValueMapper` 的 raw／record 等价与空值，以及 `DefaultReactorQLRecord` 的容器所有权／生命周期。
- 局限：精确分组仍需 O(active keys) 状态；若调用方保留带显式 group budget 的 completed group，keys 仍可能被额外保留，尚未修复。
- 本地证据运行于 JDK 17；CI Java 8 尚未重新验证。源码、测试、夹具和本文档进入 Git；`target` 下 JAR、JFR、日志和收据仅作为本地复现证据。
- 实现提交：`03ec6ee042362d253fc09b6259d8238957097d67`；Draft PR：[jetlinks/reactor-ql#33](https://github.com/jetlinks/reactor-ql/pull/33)。截至 2026-10-08 02:22:43Z，`build (Pull Request Unit Test Java8)` 与 Codacy 均为进行中，尚未报告通过或失败。

## 当前有效边界

### 全局覆盖矩阵（2026-10-08，只读证据收敛）

本矩阵只汇总现有功能、归档性能和 JFR 证据；当前冻结制品为
`target/duration-interval-candidate-20261008-benchmarks.jar`（SHA256
`49dd58582f72b1bbbaab19e2bb8682e233ee396eb3b8413125866470c0e9002d`）。其 649 项测试及
Duration A/B 是既有证据，不是本次对历史制品的复测；完整 setup oracle 是正常基准做法，热路径不得为此加入完整校验。

| SQL／算子家族 | 当前保留或撤回状态 | 已有功能、性能／JFR 证据 | 尚未满足的目标 |
| --- | --- | --- | --- |
| 普通属性／运算／宽函数 SELECT | 已保留通用行与原生边界；拒绝以原始行、投影或条件快路绕过公开语义 | 16 列宽函数 SELECT 的现有 JFR（session 6172）为 755 CPU／4469 分配样本，仍见参数流、投影订阅与结果 Map；功能回归在各保留阶段归档 | 热点仍主要是必要计算与原生边界；没有新、低复杂度且有真实收益证据的候选，不能声称整体 SELECT 已优化完成 |
| JSON／日期数值转换 | JSON 标量捷径已撤回为正确性修复；通用整数文本与 Duration 预期异常修复保留 | JSON 历史边界／JFR只定位解析、规范化和公开读取；Duration 的 49dd 制品有 649 测试、固定 SQL A/B 及 after JFR，`TypeCast` 内部异常所有者消失 | 不把历史 JSON 制品数据表述为 49dd 的复测；其余格式、解析与错误边界仍须保持，未证明 JSON 或全部转换总体收益 |
| 多层子查询 | 别名转换函数查询期复用保留；相关语义捷径未准入 | 既有多订阅、Context、错误／取消、快照隔离回归；现有深层子查询 JFR 为 828 CPU／4462 分配样本，仍是独立订阅、Record 和 result Map | 闭包／Publisher 样本不是删除冷执行或 Record 隔离的依据；没有稳定的整体吞吐收益，也没有新候选 |
| 原生全局／高基数／开放窗口聚合 | 保留原生增量 AVG／MAX 等路径；融合及 raw 聚合捷径撤回；预算终止清理撤回 | 高基数函数键 JFR 为 968 CPU／4408 分配样本；开放 AVG／MAX probe 显示仅每活跃键最后一轮 payload、取消后为 0；预算私有键驻留由本节下方当前 JFR 确认 | 不等同于全局／高基数／窗口都已优化；显式预算 Scope 的保留键驻留仍未解决，不能以历史输入不存在替代该问题 |
| 排序／JOIN／集合操作 | 未准入新性能候选；既有公开排序、关联与集合语义保持 | 分层遥测夹具新增了有限分组后的排序／Top-N组合 JFR 与绝对基线；JOIN／集合操作本轮没有复测 | 新组合仅定位已关闭的通用 owner，缺少新的可准入真实 SQL owner；不得以猜测性排序／JOIN 特调、缓存或资格分支填补空白 |
| 展开集合／集合聚合 | `collect_list`列参数构建期绑定与原生收集保留；`collect_row`／数值归约的同步边界捷径及`WindowedAggregateStage`融合撤回 | `CollectListIncrementalTest`覆盖列结果、限额、源错误／取消、窗口状态和分组键；相邻独立原生生命周期、动态 Hook 与错误边界回归已保存；高基数原生路径的 JFR 仅定位必要状态和完成扫描 | 未证明集合展开／聚合整体吞吐或常驻堆目标达成；不重开已失败的终止清理或融合候选 |

矩阵结论：全局目标仍未完成；现有证据只保留已经验收的通用改动和已关闭路线。下一次评估必须从新的真实 SQL 热点所有者证据开始，不能重复录制已关闭的宽函数、深层子查询／高基数、或失败的终止清理路线。

### 分层设备遥测 HAVING／排序取证夹具

仅新增 `src/jmh/java/org/jetlinks/reactor/ql/LayeredGroupHavingBenchmark.java`：固定 16,384 行、64 设备和有限 batch 的冷遥测源，覆盖过滤、文本温度转换、多层字段计算、`GROUP BY`／`HAVING`、确定性排序及 `LIMIT 20`。setup 以独立命令式 oracle 核验 SQL 与同输入原生参考的完整字段集合、值、具体类型、输出行排序和一次订阅；热路径只消费、计数和检查终止信号，不预存结果或作为优化候选。范围不包含生产、测试、依赖、缓存／池／资格分支或已关闭的三类原 SQL。阶段末由主代理集中执行构建（skiptests；production class 一致时复用冻结 649 项）及 JFR（标准 1 fork／3×1s／1×15s／512MB G1／stackDepth128、串行）；无新候选即如实关闭，不在此处记录未运行数据。

结果：离线package会话14622成功；冻结制品SHA256为`8971e0189bd8210d22e0ba1857f90f030b6a0a49ea5b3bfff057b6921ad594f5`。相对49dd，10,505既有class字节相同、生产class未变，仅新增14个夹具相关class；复用既有649项测试，不声称重跑。授权JFR会话37068完成四场景（1fork／3×1s／1×15s／1线程／512MB G1／stackDepth128）且无失败或截断；每fork setup四入口oracle通过16,384输入／14,336在线／512组／240 HAVING输出（过滤272）／Top20，以及完整字段集合、值、具体类型、Double位、行排序和一次订阅。

| 绝对基线（会话37065；2fork／10样本；输入行/s） | 吞吐±误差 | B/输入 |
| --- | ---: | ---: |
| native full | 27,988,222±746,043 | 89.797641 |
| native Top-N | 28,608,344±463,581 | 81.681359 |
| SQL full | 993,270±12,366 | 3408.557213 |
| SQL Top-N | 1,004,198±29,049 | 3408.358442 |

JFR会话63838无截断：native full／Top-N分别1027／1046 worker Java CPU样本及4364／4386 allocation，SQL full／Top-N为868／884及4455／4443。SQL主要仍是`GroupByMain`、`FlatMap`、`MonoDefer`、`CastUtils`及已关闭的结果snapshot、公开组键和BinaryMap契约owner；不因组合再次重开。样本不推导CPU百分比、B/输入或live heap；原生只是正常数据计算参考，不代表错误、取消或Context等价。本轮不是A/B或优化收益，无新生产候选，整体目标ACTIVE。审计见`target/layered-group-having-evidence-audit-20261008.json`及`target/layered-group-having-jfr-owners-20261008.jsonl`。

### 时间桶Duration输入：内部预期异常成本先行取证

范围仅为`DefaultReactorQLMetadata.durationMillis`的数字尝试后备。此前正常'1m'/'15 minutes'/'PT1M'先进入默认castNumber并构造内部TypeCastException，再按Duration解析。测量期JFR确认异常owner后，只使用现有无异常后备API，保留全部解析顺序和原生响应式边界；固定真实SQL压测及完整测试通过，已保留这一低复杂度改动。

准入边界：仅使用已有`castNumber(text, ignored -> null)`后备API避免内部默认异常，仍保留解析顺序、RuntimeException后备、日期/十六进制/空值/长度/最终非法参数及原Reactor边界；不新增格式/来源资格、预编译/缓存、解析器、SPI或操作符。补原生oracle及错误/Context/需求/取消回归，集中完整package后，用固定8场景2forks/3×1s/5×1s/1线程/512MB-G1/GC验收。真实SQL≥10%吞吐且区间分离或≥5%B/输入行，其它场景无稳定回退；否则撤回，不更换格式比例/字段规模/门槛重试。本次满足原门槛。

先行证据：8192条混合遥测，以Number、数值文本、'1m'、'15 minutes'、'PT1H'等量出现；另一个控制仅把interval_value规范化为同值Number，不预存目标桶/聚合结果。投影5列、时间桶/设备类型分组AVG/MAX/COUNT，以及两种控制，全部值/类型/组数/源一次订阅oracle通过。专项包54027的4项CommonFunctionCoverageTest通过，冻结58de03f5…全部旧10,495个class字节不变，仅新增10个基准class；复用完整644项生产证据。JFR55836三项成功，原配置1fork/3×1s/1×15s/1线程/512MB-G1/stackDepth128，无截断；混合投影66个Java CPU/4411分配样本，durationMillis栈命中54/398、TypeCastException57；分组85/4395，命中37/299、TypeCastException60；数字控制851/4473，命中7/0、TypeCastException0。NativeMethodSample混合两项10/8，仅Throwable.fillInStackTrace叶子，不能把Java样本分布换算CPU比例。证据`target/duration-interval-before-jfr-complete-owners-20261008.jsonl`及`target/jfr-duration-interval-before-20261008/`。

实现与阶段验证：只改`src/main/java/org/jetlinks/reactor/ql/supports/DefaultReactorQLMetadata.java`中的`durationMillis`，使用已有`CastUtils.castNumber(text, ignored -> null)`并在null时继续原Duration分支。保留Number入口、数字/日期/Duration顺序、原RuntimeException后备、长度/非法参数及原生函数算子。新增`TimeBucketNativeCompatibilityTest`5项，以独立旧算法覆盖类型/负epoch、空值/限长/坏格式/错误恢复、独立聚合/层级分组、异步Context/冷订阅/参数完成/活动取消及源错误身份。初次失败来自oracle提前读取第二参数和测试在参数订阅前立即取消，按原生消费顺序与活动源观察修正；另一个测试方法引用重载歧义通过显式List参数消除，失败日志保留，没有放宽断言或改生产资格。完整离线package74721通过：649项／65份当次报告／0failures/errors/skips，保留DebugAgent和许可证门禁。冻结并保留`target/duration-interval-candidate-20261008-benchmarks.jar`的SHA256为`49dd58582f72b1bbbaab19e2bb8682e233ee396eb3b8413125866470c0e9002d`；全部10,505个class仅metadata主类变化、无增删，433个基准class字节相同，收据`target/duration-interval-candidate-class-test-audit-20261008.json`。

固定A/B结果：before35675、after60193均成功，8场景/参数/配置/完整oracle/基准class一致，每项2forks／10次测量，期间没有并行Java或构建。混合时间桶投影884,503.578→982,216.598输入行/s（+11.047%，置信区间分离），2721.083901→2425.149635B/输入行（−10.876%）；时间桶分组AVG/MAX/COUNT为646,536.944→711,609.928（+10.065%，区间分离），3316.233314→3009.920450B/输入行（−9.237%）。数字投影−0.48%、数字分组−3.19%、原生混合投影−5.66%、宽函数SELECT+0.58%、深子查询+0.03%、高基数50事件/键聚合−0.37%，吞吐区间均重叠，未发现区间分离的控制吞吐回退；数字分组+3.666B、高基数+0.480B的分配区间也重叠。原生控制的+0.000294B/输入行归一化微小差值保留，不宣称所有指标均降低。两项真实SQL满足原始分配准入门槛，未更换输入/比例/门槛或重试正式测量。原始`target/jmh-duration-interval-{before,after}-20261008.{log,json}`和收据`target/duration-interval-ab-summary-20261008.jsonl`保留。

保留后JFR77119两项成功：原配置1fork/3×1s/1×15s/512MB-G1/stackDepth128。混合投影64个JavaCPU/8个NativeCPU/4377个分配样本，duration栈53/0/705；分组89/9/4467，duration栈38/0/463，均无截断。两者duration内TypeCastException样本从57/60降为0；剩余BigDecimal、NumberFormatException、文本/日期/Duration解析仍是原有必要后备成本，不扩展为新格式资格或完整数值解析器。录制`target/jfr-duration-interval-after-20261008/`，收据`target/duration-interval-after-jfr-complete-owners-20261008.jsonl`。样本不换算CPU百分比或精确B，分配下降不等于常驻堆下降，也不代表全引擎已接近原生。阶段git diff --check通过；没有commit/push/PR，整体目标仍ACTIVE。

### 数值字符串转换：保留通用整数解析内核，混合SQL分配下降8.56%／5.72%

目标仅为`CastUtils.castNumber`的通用字符串解析内核。当前普通整数文本先new BigDecimal，再取longValue；这是CSV/设备报文的潜在临时分配，不是原生响应式订阅开销。先新增固定的16列混合遥测查询与两层派生表、同值直接计算对照，输入同时包含整数、小数、科学计数、前导零、大精度字段，完整结果/类型/行数/一次订阅oracle独立使用当前BigDecimal规则。JFR必须先证明该owner的成本，再考虑能否用JDK整数解析去掉不需要的BigDecimal；不改数值精度17门槛、返回类型、其它格式/错误/日期后备/十六进制语义，不加SQL、输入来源、缓存、资格标记或SPI。

阶段末集中验证；保持相同基准与输入，以2forks、3×1s/5×1s、1线程、512MB/G1/GC覆盖两个真实SQL及原生、宽函数/多层子查询/多事件分组控制。至少真实SQL吞吐≥10%且区间分离，或B/行下降≥5%，其它场景不得稳定回退；不改字段数量/格式比例、规模或门槛重试至通过。下述实际结果已通过分配门槛，保留候选。

准入取证：修正夹具保留字offset→clock_offset，且独立oracle的abs结果按已发布Double类型计算后，两种SQL完整值/类型/顺序/组数/一次订阅通过；没有生产改动。初次失败日志与制品保留。33d0d592…制品的全部原10,486个class字节不变，只增加8个基准class；标准专项包14项CastUtilsTest通过。官方JFR2825完成三项原生/投影/派生表场景（1fork/3×1s/1×15s/512MB-G1/stackDepth128），混合投影809个CPU样本，BigDecimal构造/castNumber叶子96/78，337个BigDecimal分配样本来自castNumber；派生表901个CPU样本，对应69/57和242个BigDecimal样本。录制无截断，计数不能换算精确CPU比例或B/行。收据`target/numeric-text-ready-baseline-jfr-owners-20261007.jsonl`。正式A/B前额外加入同一数据上的纯小数/科学计数SQL控制，防止整数路径获益掩盖其它文本解析回退；不调整目标场景或验收门槛。

实施与验证：只在castNumber现有CharSequence分支中识别整数语法并使用JDK Long.parseLong；有效数字仍按原17位返回类型门槛，前导零不计精度，Unicode十进制数字和符号仍由标准解析处理。小数/指数/大精度继续原BigDecimal路径，其它输入、十六进制、日期后备、异常与回调次数不改。未增加SQL/来源资格、缓存、SPI或响应式算子。`NumericTextParsingCompatibilityTest`6项以独立原实现覆盖固定/随机9000种格式、BMP十进制字符、返回类型/scale/Double原始位、后备异常对象/次数及投影/分组Hook/继续、Context/请求/取消/源异常身份。完整离线package95534成功：644项／64份当次报告／0failure/error/skipped，时间00:04:52–00:05:37，排除过时报告，保留DebugAgent和许可证检查。

正式before64821／after32047均成功：七项各2forks/10测量，同JVM/堆/GC/配置/SQL/输入/oracle/基准class，无并行Java/构建。before制品7e89f4bb…全部10,486份旧class不变，仅新增9个基准class；候选4cb47726…与before的10,495个class仅CastUtils.class改变，无增删。通过原分配≥5%门槛、无区间分离回退；吞吐全部区间重叠，不能宣称稳定CPU改善，也不能把置信区间重叠当成严格等速证明。

| 场景 | before→after 输入行/s±JMH误差 | 吞吐均值变化 | B/输入行 before→after |
| --- | --- | --- | --- |
| 16列混合数值文本 | 818,366±38,079→849,250±16,658 | +3.77% | 6170.790→5642.263（−8.56%） |
| 两层派生表 | 582,046±23,507→585,273±12,793 | +0.55% | 9236.737→8708.210（−5.72%） |
| 小数/科学计数控制 | 2,704,071±89,444→2,646,765±95,415 | −2.12% | 1966.456→1966.456 |
| 同值原生计算控制 | 3,020,030±69,031→3,062,420±84,256 | +1.40% | 2032.193→2032.193 |
| 常用宽函数控制 | 522,259±14,884→520,666±10,869 | −0.31% | 7622.497→7617.097 |
| 三层子查询控制 | 6,558,886±70,808→6,700,748±103,099 | +2.16% | 592.142→592.140 |
| 多事件函数键4聚合控制 | 2,675,316±100,815→2,673,416±155,673 | −0.07% | 782.502→782.662 |

两个目标均每输入行少528.527 B；字段、格式比例和数据规模未变。控制小幅B均值变化不归因为候选，更不外推为常驻堆改善。原始`target/jmh-numeric-text-integer-{before,after}-20261008.{log,json}`、配置/维度/门槛收据`target/numeric-text-integer-ab-summary-20261008.jsonl`、完整日志`target/numeric-text-integer-candidate-full-package-20261008.log`。不是整体已接近Java原生：直接计算仍约3.06 M输入行/s，SQL约0.85 M，响应式/投影/资源/扩展契约的剩余成本仍存在。整体目标ACTIVE。

after官方JFR45529成功（同原场景/输入、1fork/3×1s/1×15s/1线程/512MB-G1/stackDepth128）：投影915个worker CPU/4406个分配样本，castNumber所属BigDecimal为122、char[]为133；派生表938/4473，该owner的BigDecimal98、char[]71。两份均无截断，剩余小数/指数/大精度仍走原BigDecimal，保留必要结构与返回类型；仅样本来源改变与源码、正式GC差分一致，不按样本比例申领CPU或精确字节。目录`target/jfr-numeric-text-integer-retained-20261008/`、详细收据`target/numeric-text-integer-retained-jfr-owners-detailed-20261008.jsonl`。当前冻结`target/numeric-text-integer-retained-20261008-benchmarks.jar`及正常构建制品SHA256均为`4cb47726e5d3a2d71074b31df0e9b624d54530fd7a4c318194564a051f41f45d`，与正式after制品逐字节相同；完整644项证据有效，不再构建。未提交/推送/创建PR。

### JSON原生错误边界收敛：保留正确性修正，完成成本对照与热点取证

范围仅为`JsonPathFunctionMapFeature.createMapper`的同步参数分支及`JsonOperatorMapFeature`的同步文档Supplier分支。当前冻结制品5fd6de5…的独立原生组合探针确认：文本资源超限时，函数Hook数据args→record、继续数据null→record；访问操作符Hook数据document→null，聚合路径又被Supplier的Callable优化转移到record边界。正常值相同不证明错误契约透明。`target/json-native-boundary-probe-final-20261007.log`保留32种对照；源模式探针的类加载错误及首次classpath失败日志也保留，不作为有效证据。

执行：只删除两个不等价分支，保留参数concatMap/fromDirect/collectList及文档fromDirect/flatMap的原生边界，既有JSON限制、静态路径、解析与类型不变。补独立原生oracle覆盖参数读取失败、资源错误、投影顺序、global/keyed/window独立聚合、Hook/继续数据、源错误身份、Context和需求取消；阶段集中package。没有异常补偿、类型/SQL资格、执行框架、缓存、调参或依赖变更。该修正不是ROI候选；之后回到真实场景JFR，只有简单且达到高收益门槛的改动才保留。

验证：离线完整package会话66383正常退出，638项／63份当次报告／0失败、错误、跳过；报告时间23:30:51–23:31:33，排除过时Benchmarks/WindowedAggregateStageTest。新增`JsonNativeCompatibilityTest`5项。既有JSON操作符测试回归原生ScalarValueMapper.apply的参数构造求值时机及同一构造异常对象，不再要求已删除Supplier额外提供的懒语义；保留正常值、异步Context、wrapper、取消等测试。只迁移旧JSON标量能力断言为false，结果断言未弱化。制品`target/json-native-scope-validated-20261007-benchmarks.jar` SHA256 `7831db951bb9c8cc578dfac5761bc99e9ba64c2fc3a35057a1d20ec04bff4c89`；全部10,486个class没有增删，变化仅两owner及其Segment内部class（外部常量池引用），全部基准class相同。日志`target/json-native-scope-full-package-20261007.log`。

性能成本：会话18750的before/after均成功，六个既有场景／各2forks／各10测量；JDK17.0.18、512MB/G1、1线程、3×1s预热/5×1s测量/GC，SQL、输入、oracle及基准class相同，无并行Java或构建。不是新增优化收益，不为追分恢复不等价代码或再次调参。

| 场景 | 吞吐均值变化／置信区间 | B／输入行 before→after |
| --- | --- | --- |
| JSON八路径Map | −31.79%，分离 | 12512.055→14168.069 |
| JSON八路径文本 | −20.52%，分离 | 15568.058→17288.071 |
| 16列预解析JSON操作符 | −7.03%，分离 | 3608.015→3688.015 |
| 常用宽列函数 | −6.61%，重叠 | 6994.280→7638.698 |
| 三层子查询控制 | −1.60%，重叠 | 592.141→592.143 |
| 同值原生函数控制 | −2.06%，重叠 | 2158.865→2158.865 |

保留均值波动，不将区间重叠解释为等速或通过ROI；三个分离的下降是明确正确性成本。此处子查询是既有带checksum消费的deeplyNestedSubquery，与参考只读消费JFR方法不同，不与其它阶段576 B/行混比。原始`target/jmh-json-native-scope-{before,after}-20261007.{log,json}`及维度核验收据`target/json-native-scope-cost-summary-20261007.jsonl`。

当前JFR：会话44101完成五项宽列/JSON/同值原生函数场景；命令中不存在的deepNestedSubquery未产生录制，随后62836用实际profilingDeeplyNestedSubquery独立补齐，未虚报覆盖。共六份有效官方测量期录制，保持原场景、JDK/堆/GC、1fork、3×1s/1×15s、1线程/stackDepth128，均无截断。JSON Map/Text为936/940个worker CPU样本，JSONPath Utils.concat仍为主要叶子76/64；宽函数750样本，ConcatMapImmediate.drain叶子64；预解析JSON操作符921样本，normalize叶子82，仍有结果Map与MonoFlatMap分配；三层子查询792样本，FlatMap.drainLoop叶子110，分配含订阅适配、Record和deferContextual捕获。样本只定位成本，不是精确CPU百分比、B/行或常驻堆。记录位于`target/jfr-json-native-{current,deep-current}-20261007/`，派生收据`target/json-native-current-jfr-owners-20261007.jsonl`。

高收益结论：没有新的低复杂度、保持现有契约的可准入候选。JSONPath公开API边界和原生参数/投影/子查询结构成本继续保留，不新增解析缓存、Supplier资格、执行层或订阅合并。已有数值集合快照分配收益的owner未变；本阶段不申领吞吐或常驻堆下降。全部改动局限ReactorQL，默认限制/依赖不变；整体目标未完成。

#### Bug Analysis: JSON正常值不证明Supplier／行级同步错误边界透明

1. Root Cause Category：B跨层契约／D覆盖缺口／E隐含假设。Supplier的Callable执行可能被flatMap在订阅前直接求值，文档校验错误因此越过原生值局部边界。
2. Why Fixes Failed：先前仅验证JSON正常值、冷Supplier及终止异常身份，漏掉Hook数据、继续范围与不同消费者；已有条件表达式修复不自动覆盖JSON。
3. Prevention Mechanisms：独立保留concatMap/collectList/flatMap及文档fromDirect/flatMap的oracle；覆盖两列顺序、global/keyed/window聚合、构造失败、限制失败、Context/需求/取消/源异常身份，生产明确保留原生边界。
4. Systematic Expansion：只收敛两个已由同一差分证明的owner，不引入异常资格、补偿、额外执行器或扩大为JSON解析重写。采样中的多次normalize不是可随意删除的重复：嵌套JSON字符串在再次规范化时可能继续解析，需保留当前值与错误契约。
5. Knowledge Capture：结论落在本原始文档及两个owner的兼容注释。ReactorQL无所属Trellis spec，不改其它模块spec/template，不自动提交。

已删除不等价的`WindowedAggregateStage`及未发布的Accumulator接口／工厂；聚合统一使用原生增量归约和独立merge／分组生命周期。MapAggFeature、CountAggFeature、CollectRowAggMapFeature保留原生参数映射订阅边界。AVG／MIN／MAX不收集历史行；精确分组仍需要活动键、代表行及原生订阅状态。本文以下融合吞吐、分配及常驻堆数据均为历史候选／取证，不是当前生产收益。清理阶段已通过584项完整回归，基准计划资格已更新而SQL／输入／值和类型oracle不变；性能目标仍未完成。其他已验证通用优化保留，默认限额／并发／Reactor依赖未改。详细边界和证据见文末“聚合融合边界撤回与原生增量路径保留”。

### 条件表达式的原生生命周期收敛：已保留正确性修正，明确披露性能成本

目标与范围：只删除`CaseMapFeature`／`CoalesceMapFeature`／`IfValueMapFeature`把原生条件链替换为同步行求值的三个分支，保留现有filterWhen／flatMap／switchIfEmpty、参数mapper及扩展入口。不新增执行器、资格标记、异常补偿、Context键、缓存、池或默认调参。常用宽投影的正常值等价不能证明错误继续范围或参数构造时机等价；本修正不是新的性能候选，不能以原不等价路径的快分数判定应恢复捷径。

先行差分`/private/tmp/ConditionalNativeBoundaryProbe.java`在当前479624a4…制品上复现：选中CASE分支的属性读取失败，原生flatMap以分支entry继续并转入后备值，行级同步路径则以record继续并丢弃该行；两行独立SUM／COUNT由原生6／2变为4／2。coalesce首个值非空但后备属性读取失败时，原生参数Publisher构造仍失败，原生SUM为3而当前同步短路为5；独立COUNT均2。predicate、选中参数及未选else还存在callback data／次数差异。错误身份未变不代表原生隔离等价。这里明确保留现有构造与继续契约，不把“改成懒构造”夹带为本次性能优化。

邻近IF差分`target/conditional-native-if-boundary-probe-20261007.log`进一步证明选中分支的属性失败在原生链以null继续、当前同步路径以record继续；同时未选分支在原生／同步两路均未求值。不能通过统一懒构造改变coalesce原来的构造契约，也不能让修正IF误求值未选分支。范围收敛到这三个条件工厂，不扩展成异常恢复框架。

步骤：补保留原生链的差分回归，覆盖选中／后备参数／条件错误、投影列顺序、global／keyed／window多聚合、异常身份与继续数据、Context、零初始需求和取消；删除三个同步分支并写明原生边界目的；阶段集中完整离线package与制品class身份检查。必要的测试／基准计划资格可更新，SQL、输入及值／类型oracle不改。使用同JMH配置的宽函数、混合运算、深子查询及多事件分组对照量化正确性成本，不申领原不等价路径的CPU／堆收益；无新性能改动不重复录旧热点。结果回填本节。

验证结果：完整离线package90700成功，632项／62份当次XML、0失败／错误／跳过，标准DebugAgent／license保留；两份历史Benchmarks／WindowedAggregateStageTest报告排除。先前15140仅旧coalesce同步能力断言失败，更新该能力期望false，不删除或改弱正常值断言；日志完整保留。此前两工厂版81859的631项不是最终三工厂版的验收数据。新增ConditionalNativeCompatibilityTest最初8项覆盖上述差分，后补一个原生发射基数反例并在同生产class上独立9项通过（93175），不是再次全量633项。

发射基数反例：原有CASE Feature的Publisher可以输出多个匹配分支，聚合消费完整流，列投影通过Mono.from取首值。两个匹配分支值3／4时，原生直接流发射3、4，列输出3，SUM输出7／COUNT(1)为1；同步单值声明不能覆盖这三个消费者。测试保留现有Feature契约，不在性能优化中夹带CASE的SQL语义变更，也不为“单WHEN／不会出错”增设资格分支。

测量制品`target/conditional-all-native-scope-validated-20261007-benchmarks.jar` SHA256为`5fd6de5fdd8dfa0e7cd74261c86574fc48c983e608c36e9384244541ca13d3fa`；相对479624a4…before，全JAR10,486份class仅IfValueMapFeature／CaseMapFeature／CoalesceMapFeature改变，无增删，385份ReactorQL基准class全部字节相同。最终类契约注释及9项相邻回归package82877成功，最终全10,486份class与测量制品字节完全一致，完整632项证据仍有效，但不声称新跑全量633项；收据`target/conditional-native-final-identity-20261007.log`。before64176／after35137均成功，各11场景、2forks／10次测量，JDK17.0.18、512MB/G1、单线程、3×1s预热／5×1s测量、GC，实际JVM参数／SQL／输入／全部setup字段、值、类型、行数及订阅oracle不变，无并行Java／构建／探针。

| 真实场景 | before→after，M输入行/s±JMH error | B／输入行 before→after | 结论 |
| --- | --- | --- | --- |
| 16列混合投影 | 0.729±0.009→0.633±0.006 | 7720.050→8828.853 | 吞吐−13.13%，区间分离 |
| 64列混合投影 | 0.164±0.004→0.138±0.002 | 33256.077→38848.083 | −15.88%，区间分离 |
| 128列混合投影 | 0.079±0.002→0.066±0.001 | 67304.115→78873.717 | −17.00%，区间分离 |
| 混合运算宽投影 | 2.465±0.040→1.659±0.021 | 1022.236→1705.900 | −32.69%，区间分离 |
| 16列常用函数 | 0.586±0.016→0.554±0.004 | 6841.018→7015.880 | −5.50%，区间分离 |
| 深层子查询控制 | 7.451±0.044→7.336±0.135 | 576.141→576.140 | −1.54%，区间重叠 |
| 函数键50事件／组控制 | 2.710±0.141→2.763±0.124 | 782.662→782.662 | +1.93%，区间重叠 |

同值直接原生宽函数及16／64／128列控制分别−1.55%／−6.19%／−6.26%／+2.93%，所有区间重叠；保留这些点估计与宽误差，不归因候选。此表是撤回不等价捷径的成本，不是满足ROI的优化。B为累计分配，不是常驻堆；当前Numeric集合快照所有者未变，其既有分配收益不扩展为条件表达式或全引擎收益。不通过重复跑分、输入资格、异常补偿、懒构造改契约或新执行层追回均值。完整11项配置／数值收据`target/conditional-native-scope-cost-summary-20261007.log`与原始JMH JSON保留，整体性能目标仍未完成。

#### Bug Analysis: 同步条件值不等于原生条件Publisher契约

1. Root Cause Category：E隐含假设／D覆盖缺口／B跨层契约。同步正常值没有证明参数构造、错误继续数据、分支后备或Publisher发射基数等价。
2. Why Fixes Failed：已修复运算／函数／聚合边界不自动覆盖CASE、coalesce、IF；只测单分支成功值或终止异常身份会漏掉独立聚合与多值消费者。
3. Prevention Mechanisms：用保留原生链的9项回归观察投影顺序、global／层级分组、Hook／继续数据、未选分支、Context、需求／取消及直接／首值／聚合三类消费者；生产不声明不成立的同步能力。
4. Systematic Expansion：三个同根因工厂一并收敛，CAST／二元运算已使用其原生值局部边界，不再复制修复；不能把该阶段验收外推为所有JSON／过滤扩展边界全覆盖。
5. Knowledge Capture：稳定契约和证据回填本原始文档及代码注释。ReactorQL没有所属Trellis spec/template，不改其它模块规范、不安装框架、不自动commit／push／PR。

### 高基数结果合并：热点确认，原生分批嵌套不满足透明替换契约

当前结论：`DefaultReactorQL.createGroupBy`最终对`columnMapper(group).contextWrite(...)`的原生flatMap，仍是大量唯一键集中完成时的主要扫描来源。不能把分组发现流先window再嵌套flatMap作为通用优化：它增加源错误恢复边界，并将已配置的全局并发上限放大为每批并发。该方向在生产修改前关闭，不增加默认／SQL资格、私有Context、异常补偿、调度或状态协调层，也不将原生归约替换成自定义聚合执行器。

当前冻结制品479624a4…的官方测量期JFR原会话33647成功，沿用同一函数键四聚合SQL、50,000行输入和全部setup结果／类型／组数／一次订阅oracle；每键1／50条，JDK17.0.18、512MB/G1、单线程、1fork、3×1s预热、1×15s测量、stackDepth128，无并行Java／构建。唯一键录到1169个worker CPU样本，其中1154个drainLoop栈直接来自innerComplete，1150个进一步经过ContextWrite及MonoSubscriber.complete；与上述最终结果合并拓扑一致，而非将全部分组发现算子都判成同一热点。50条／键为985个CPU样本、127个innerComplete来源，逐行lower／属性与publish分发等成本变得可见。样本只定位来源，不换算CPU比例、精确字节或常驻堆；诊断单fork分数不作为新的before／after收益。

有界原生契约对照`/private/tmp/NativeGroupMergeDiscriminator.java`在同一制品上成功：直接分组源错误1／3组时，回调1／3次变为2／4次；带窗口父层为2／4次变为3／5次。整批2组两路回调相同，说明未满批边界不能由正常值或整批检查覆盖。原异常身份及终止错误仍相同，但回调次数已经不等价。全局并发2的活跃订阅2→4，取消均覆盖对应订阅，取消成功不能豁免并发契约改变。真实SQL原路径同时确认global、keyed和window-keyed各自原生错误范围不同，不用单一“所有源错误不可恢复”的假设改实现。

证据：`target/jmh-highcard-scan-current-jfr-20261007.{log,json}`、`target/jfr-highcard-scan-current-20261007/`、`target/native-group-merge-discriminator-20261007.log`及`target/highcard-native-scan-discriminator-summary-20261007.log`。本阶段没有生产／基准源码变更，复用当前624项完整回归与制品身份，不重新构建、不申领新收益。精确活跃键／订阅状态不等于AVG／MAX驻留历史行；整体性能目标仍未完成。

### 数值下标集合快照复制：首次控制失败撤回，漂移判别后两对顺序控制均通过，保留分配收益

目标与范围：宽投影分配归因已证明参数collectList与原生zip占据明显来源，但不能通过删除原生收集／列订阅改变错误继续、空列、Context或订阅顺序。现阶段不实施参数融合。新的独立入口为DefaultPropertyFeature.getPropertyValue的Number分支：CastUtils.castArray(Collection)先new ArrayList(collection)，数值下标只消费一个元素；JDK17对非ArrayList集合还会在toArray后再次复制数组。只评估去掉中间列表包装／重复数组复制，不改castArray公开返回类型、可变性、其它用途或属性扩展点。

先行覆盖：新增IndexedPropertyBenchmark，以普通16,384条批事件、每事件24条读数和16列投影（常用位置、差值、绝对值、长度、设备字段）取证。预先固定普通可修改／只读列表两种形态；无生产类型资格、SQL文本特判、输入规模／密度放大或预存结果。SQL／直接原生／同宽属性控制核对完整结果、类型、顺序、行数和一次订阅。集中基准打包并核对旧生产class不变后，录两种输入的官方测量期JFR，确认快照包装／第二次复制再准入生产。

候选仅在原Number索引＋Collection语义内，保留完整toArray快照、一次intValue、source错误对象与原求值顺序；不将惰性集合改为单元素get／迭代，从而避免跳过其它元素的转换或错误。若用官方索引检查，越界必须仍为IndexOutOfBoundsException、同一响应式错误边界；标准库索引诊断文本可能与JDK ArrayList不同，需明确记录，不能声称错误文本逐字相同。空／null／边界／有序集合／集合toArray错误及惰性转换差分先行，阶段集中完整package；不新增helper框架、缓存、池、配置、依赖或操作符。

性能验收固定两种真实SQL、两种同值原生与属性控制，加原混合宽投影、多层子查询、多事件分组控制。JDK17、512MB/G1、1线程、2forks、3×1s预热／5×1s测量、GC profiler，输入／SQL／oracle／参数不变。至少一种真实SQL吞吐≥10%且区间分离，或B／输入行下降≥5%；其他场景无after吞吐上界低于before下界的稳定回退，目标分配不增加。否则撤回，不改读数规模或形态求通过，不申领常驻堆收益。

准入取证：基准打包96242成功，既有582份class全部字节一致，只新增8份IndexedPropertyBenchmark及生成class；before e1ecdad7…。官方JFR69090成功，两种输入的SQL／native／属性三路完整oracle通过。mutable录制783个CPU／4352个分配样本，castArray命中5／392；readonly为788／4478，命中20／1080；全为Object[]分配。readonly数组栈中1028个来自原toArray快照，52个来自ArrayList构造器第二次copy；前者是仍需保留的成本，不声称全部1080可删除，不换算精确CPU比例／B或存活堆。JDK17 ArrayList(Collection)源码确认先toArray，再对非ArrayList来源复制。

独立内核预检`/private/tmp/NumericIndexSnapshotAllocationProbe.java`与`target/numeric-index-snapshot-allocation-preflight-20261007.log`以相同24值／七个原索引、512MB/G1、ThreadMXBean测量三组配对：mutable旧／快照均112 B／lookup；readonly均224→112 B／lookup。只对纯快照算法准入，不是端到端SQL收益，不按输入类型给生产加资格或选择分支。正式九项before记录在`target/jmh-numeric-index-snapshot-before-20261007.{log,json}`；旧castArray公共方法不改。

实施与验证：before会话37458、after1216均终态成功，JSON核验九项各2forks／10次测量、配置和单份512MB/G1参数一致。候选仅在上述Number＋Collection分支使用一次toArray与既有Guava标准索引检查，未知来源仍走castArray。NumericIndexSnapshotTest四项覆盖旧实现差分、惰性集合完整求值／源错误身份，以及SQL零需求、分次请求、Context、空值和错误继续。离线完整package80231成功：624项、61份当次报告、0失败／错误／跳过；排除两份过时报告而非申领673项。初次沙箱运行的8项错误均为既有DebugAgent外部self-attach失败；保留该检查并在允许attach的环境重跑，没有弱化测试。候选590份共享class仅DefaultPropertyFeature改变，385份基准class全部字节相同；候选制品473f3b6e…。首次after因本机socket被沙箱阻止，在任何测量前退出；重跑保留相同输入／配置，无并行构建、探针或其它Java。

正式结果：mutable SQL为0.857±0.016→0.854±0.015 M输入行/s（-0.30%），6416.063 B/行不变；readonly为0.842±0.013→0.861±0.034 M（+2.19%，区间重叠），7200.063→6416.063 B/行（-10.89%，每行少784 B）。原生两路、属性两路、混合宽投影与多事件分组吞吐区间均重叠，但多层子查询控制为7.594±0.093→7.445±0.033 M（-1.95%，after上界低于before下界）。该读数不能单独证明因果，也不能忽略事先控制回退门槛；ROI分配门槛通过，控制门槛失败，候选已撤回。不调数据／fork／阈值，不加输入类型资格，不将局部分配下降计入当前生产收益或常驻堆收益。证据：`target/numeric-index-snapshot-ab-summary-20261007.log`、前后JMH JSON及`target/numeric-index-snapshot-candidate-class-identity-20261007.log`。

最终恢复验证：只撤回本候选分支和import，既有脏改动保留。恢复package69246成功，新增四项及相邻属性回归共16项、3份当次报告、0失败／错误／跳过；不是再次完整624项。590份生产及基准class全部与e1ecdad7…before字节一致，当前恢复制品为baf3c273…，归档`target/numeric-index-snapshot-restored-20261007-benchmarks.jar`，收据`target/numeric-index-snapshot-restored-class-identity-20261007.log`。保留代表性基准和功能测试，无新增生产收益，无提交／推送／PR。本阶段关闭；后续只准入新的、JFR支持的简单通用高收益点，不将已撤回候选换资格／框架重试。

控制回退因果判别：关闭的是原性能准入，尚未证明回退由候选造成。当前深层子查询SQL只有id及缓存标量读取，没有集合数值下标；不能因路径不同就排除JIT布局效应，也不能从原单次时序比较断定因果。本次不修改或重新准入生产，只以完全相同e1ecdad7…原始before归档做一次同九项A2测量（原2forks／3×1s／5×1s／512MB/G1／GC／参数不变）。若恢复后仍与旧after相近而低于旧before，时间／运行漂移假设获得支持；若恢复后返回旧before区间且高于旧after，候选引起通用运行时回退假设获得支持；否则为不确定。所有结果留存，不以重试次数、改阈值、合并挑选样本或新资格分支替代判别。即便支持漂移，这次观察也不单独授权保留旧候选；原控制失败及撤回保持记录。证据将写入`target/jmh-numeric-index-restore-causal-control-20261007.{log,json}`，不重复构建，不并行Java／探针。

A2结果：21959成功，九项各2forks／10次测量，制品与原始before的SHA256一致、配置／输入未变。子查询为7.369±0.020 M，相比原始before下降2.96%，也低于候选7.445±0.033 M；分配仍576.140 B/行。原生两路约下降2.79%／3.04%，其区间与原始before和候选均重叠；readonly SQL分配恢复7200.063 B/行，明确不是残留候选。观察未满足预声明“与候选区间重叠”或“恢复到before且高于候选”的分支，因此候选自身回退归因仍为INCONCLUSIVE；同时原始制品自身的稳定下降直接证实存在非候选因素。只凭第一次A/B无法分离这部分影响，不将之前1.95%回退全部归因候选或全部归因环境。收据`target/numeric-index-restore-causal-summary-20261007.log`。

有界顺序控制计划：以该新增同制品漂移证据为前提，不改变源代码或降低门槛，固定只作一次完整B–A–A–B四轮（B2候选473f3b6e…，A3/A4原始e1ecdad7…，B3同候选），每轮仍是同九项／原2forks和全部参数。分别比较B2/A3、B3/A4，两对都必须满足原ROI分配或CPU门槛、目标分配不增加和任意控制无区间分离回退；不合并四轮来扩大区间、不丢弃原失败数据、不只选readonly或子查询。任一对失败则停止，不再加轮求通过。原候选在全部结果完成前继续撤回；观察无效仅修复已证实工具故障，不把正常波动当作无效。该取证属于顺序／运行漂移判别，不是新的SQL／类型资格或框架设计。日志及JSON固定为`target/jmh-numeric-index-balanced-{B2,A3,A4,B3}-20261007.*`；序列汇总保留原始A1/B1/A2事实，决定仅基于完整证据。

顺序控制结果：57841成功结束；36个场景全部各2forks／10次测量，JVM单份512MB/G1、参数／SQL／输入／385份基准class不变，没有并行Java／构建／探针。B2/A3与B3/A4分别通过原ROI、目标分配和全部CPU控制门槛，未池化数据。readonly SQL分别0.841±0.010→0.880±0.019 M（+4.67%）和0.836±0.016→0.872±0.022 M（+4.22%），均7200.063→6416.062 B/输入行（每行减少784 B，-10.89%）。第一对吞吐区间分离，第二对略重叠且两次均未达到CPU≥10%门槛，所以只申领明确分配收益，不申领稳定CPU高收益。mutable SQL为-1.25%／+0.75%，分配6416.063 B/行不变；深层子查询+1.38%／-1.24%，两对区间都重叠，分配576.140 B/行不变。原生mutable控制第二对点估计-11.54%但区间很宽且重叠，该不确定性也保留，不把点估计当作因果。收据`target/numeric-index-balanced-summary-20261007.log`记录全部场景，原A1/B1控制失败及A2漂移仍完整保留。

决定与边界：顺序控制填补了原始单次时序比较的因果缺口，保留同一小范围Collection数值索引快照实现，不添加新分支资格、框架、缓存或配置，不修改castArray。完整快照／source错误身份／原Reactor组合保留；标准Guava与ArrayList的越界诊断文本差异仍明确记录。只申领此类真实SQL瞬时分配下降，不申领高基数分组常驻堆收益；不再追加性能对照求通过。

最终保留验证：完整离线package56829成功（保留DebugAgent／license），624项、61份当次报告、0失败／错误／跳过。最终制品`target/numeric-index-snapshot-retained-20261007-benchmarks.jar`的SHA256为479624a47954b449c781c79e510524df8ed5def2079651429c0a1381ea82803d；全JAR 10,486份class（含依赖）与四轮测量候选完全字节一致，590份相关class只在DefaultPropertyFeature上与原始before不同，385份基准class不变。收据`target/numeric-index-snapshot-retained-class-identity-20261007.log`。这次是保留版本的完整624项，不是复用恢复后的16项声称全量。

最终官方测量期JFR69528成功，两种输入、参数／时长与先行JFR一致，源码／制品已与完整测试和四轮性能测量绑定。mutable为834个worker CPU／4509个分配样本，数值属性Object[]所有者410个均来自仍必需的toArray；readonly为827／4323，408个数值属性数组均来自toArray。数值属性下ArrayList构造器及castArray分配所有者未出现，符合只删除中间包装／第二次复制、保留全部元素转换的源码。不能把样本数换算CPU比例、精确B/行或常驻堆；分配收益只由上述完整GC/JMH证明。收据`target/indexed-property-retained-jfr-owners-20261007.log`，录制位于`target/jfr-indexed-property-retained-20261007/`。没有并行Java／构建／探针，未提交／推送／PR，整体跨SQL性能目标仍未完成。

### 文本规范化覆盖：replace长度上界候选未达高收益门槛，已撤回

当前结论：本阶段仅保留TextNormalizationBenchmark及ReplaceTextLimitTest，不保留新的生产优化。虽然JFR证明重复计数存在，实际端到端吞吐仅+3.80%且区间重叠、分配不变，未达到CPU≥10%／分配≥5%的事先门槛；不扩大文本／替换密度或调参数寻求通过，不把本候选收益算入当前生产成果。响应式约束使本轮只在有界trial setup收集夹具输出，业务链、默认限制与原生增量聚合不变。

目标与范围：先补普通事件日志清洗的代表性覆盖，仅新增TextNormalizationBenchmark，生产入口仍为DefaultReactorQLMetadata.replaceText／countMatches。原16列operatorMixWideProjection的当前d7cc6134…制品测量期JFR原会话96895成功，1010个worker CPU／4435个分配样本中replaceText只命中10／47，countMatches仅3／0，不支持在该短文本场景准入高收益优化。LikeFilter换行检查有41个CPU叶样本，但已匹配字面量区间缩小扫描的候选已验证并撤回，不重复尝试。

覆盖计划：16,384条预建事件，16列路由名、日志换行、标签规范化、大小写、长度和算术投影；保留普通长短日志、空文本与Unicode，不制造高密度替换或扩大原混合SQL输入。SQL、直接原生和同宽属性控制分别验证完整值／类型／字段数／行数／顺序与一次冷源订阅，测量逐行计算而不预存输出。新增基准集中离线打包后先核对既有生产class身份，再录官方JFR；这是未覆盖场景的取证，不算性能提升，也不先改生产。

仅在新场景证明countMatches的重复扫描是明显CPU所有者后，才评估通用保守输出上界跳过精确计数：全部可能输出都落在原配置限制内时直接调用原String.replace；可能超限则保留原精确计数／错误。保留UTF-16、空search、转换与错误时序、原限制和参数流；不增加字符／SQL／类型特判、缓存、池、API或执行框架。准入时先补原语义差分，阶段集中完整测试，再以不变输入／SQL／oracle／JDK17、512MB/G1、单线程、2forks、3×1s预热／5×1s测量的GC/JMH验收。高收益门槛为真实SQL吞吐≥10%且区间分离，或B／输入行下降≥5%；任一控制after吞吐上界低于before下界即撤回，不改数据或门槛求通过。未确认热点则关闭该方向，不实现。

准入证据：基准打包原会话4723成功，既有572份生产／基准class全部字节一致，仅新增10份夹具及生成class；before归档e3fc0472…。官方JFR原会话17611成功，SQL／native／属性三路setup完整oracle通过；943个worker CPU／4485个分配样本中replaceText命中132／331，countMatches命中58／0，其中45个indexOf、12个计数循环CPU叶样本。采样仅定位重复扫描，不换算CPU百分比／精确B／存活堆。允许评估上述通用长度证明，原生参数收集／zip／列延迟及错误边界保持不变。

正式对照固定六项：sqlNormalize目标、nativeNormalize和16列propertyControl、原operatorMixWideProjection、profilingDeeplyNestedSubquery、valuesPerKey=50的sqlFunctionKeyAggregates。首轮基线原会话54934因深子查询方法名误选且短暂并发一次Java列表查询，被主动终止EXIT143并弃用，日志仍保留；未改测量设置或SQL寻求更好结果。正确完整基线使用`target/jmh-replace-upper-bound-before-complete-20261007.{log,json}`，after需同六项完全完成，任一控制稳定回退即拒绝。

候选功能：正确before原会话69623成功，六项各2forks／10次测量，无并行构建或Java探针；目标796,626±16,137输入行/s、5739.461 B／输入行。仅replaceText新增一个保守长度证明，可能超限仍走原精确计数及错误检查；基准输入／SQL／oracle不变。阶段集中完整离线package原会话15816成功，620项／60份当次XML、0failure／error／skip，DebugAgent／许可证检查未关闭。新增ReplaceTextLimitTest的810组JDK完整结果／类型差分及1,728组旧计数门禁／错误优先级通过。582份共享class无增删，只有DefaultReactorQLMetadata及其三个内部class改变；全部377份基准／生成class字节一致。未保留候选归档`target/replace-upper-bound-candidate-validated-20261007-benchmarks.jar` SHA256 `5a1a03de99a944ef5612477bdd4d0ebaee4ec1457fd47752fcd06e8c444b0827`；功能通过或采样归因不是生产收益。纯同步私有计算不新增Tracer／MBean。

正式after原会话62157成功，逐项配置、原始JVM参数、输入／SQL／oracle及基准class完全相同，六项各2forks／10次测量，期间未并行构建／其他Java探针。输入行/s±JMH error与B／输入行如下：

| 场景 | before | after | 吞吐变化 | B／输入行 before→after |
| --- | ---: | ---: | ---: | ---: |
| 16列日志规范化目标 | 796,626±16,137 | 826,936±33,723 | +3.80% | 5739.461→5739.461 |
| 同值直接原生控制 | 4,216,900±126,698 | 4,244,550±86,940 | +0.66% | 1304.404→1304.404 |
| 同宽属性控制 | 5,054,237±988,677 | 4,975,243±1,116,886 | −1.56% | 736.034→736.034 |
| 原混合宽投影控制 | 2,441,230±42,751 | 2,482,478±50,901 | +1.69% | 1022.236→1022.236 |
| 多层子查询控制 | 7,540,775±119,243 | 7,578,298±222,969 | +0.50% | 576.141→576.140 |
| 函数键多事件4聚合控制 | 2,729,830±99,934 | 2,717,152±95,318 | −0.46% | 783.142→782.182 |

六项吞吐区间均重叠，没有命中控制停止门槛；仍因目标两项ROI门槛均未满足而撤回。控制均值和聚合小幅分配变化不归因于候选。原始`target/jmh-replace-upper-bound-{before-complete,after}-20261007.{log,json}`，汇总`target/replace-upper-bound-ab-summary-20261007.log`。不做反向重跑／录已拒绝候选after JFR，亦不申领常驻堆改善。

恢复验证：原会话26212离线package成功，仅运行ReplaceTextLimitTest的2项、0failure／error／skip，810+1,728组合在原生产门禁上仍通过；不是一次新的620项全量运行。全部582份生产／基准class与本轮before e3fc0472…字节一致，无增删，故复用此前613项完整生产回归及恢复代码上已通过的GroupBudgetLifecycleTest3项、InFilterScalarTest10项，新增两项在恢复代码上独立验证；候选620项不冒充恢复后全量。当前及归档`target/replace-upper-bound-restored-20261007-benchmarks.jar` SHA256 `5cf75208c8ebdcd1d5f3062475ce9e5ae5635804a62eea4f6f40e576e2fb9928`，身份日志`target/replace-upper-bound-restored-class-identity-20261007.log`。既有脏改动保留，未commit／push／PR。

后续判断：该普通16列日志SQL当前约0.797 M输入行/s、5739.461 B／行，直接原生约4.217 M、1304.404 B／行；原生没有完整自定义Feature、参数流或错误隔离契约，差额不是可直接删除的开销。新JFR的MonoZip内部订阅器333、Object[]209、ConcatMapInner207等分配样本需要按宽投影／函数参数来源拆分。下一步只核对这些原生边界中是否有可用公开API消除的真实冗余，不重新实现已撤回的参数融合、错误补偿或自定义执行框架。目标仍未完成。

### 正向IN的恒等结果映射：低于高收益门槛，候选已撤回

目标与范围：InFilter动态右参数的标量左值分支及兼容doPredicate末尾都有`any(...).map(matched -> not != matched)`。正向IN的not=false时该map仅返回同一个Boolean；仅省去此恒等层，NOT IN保留原map及捕获参数，不把any换成all。不更换比较、左值replay/refCount、右值展开、默认并发／预取、扩展protected方法或Context／错误／需求／取消边界，不新增操作符、资格标记、缓存、Subscriber、值／类型／SQL文本特判。

先行JFR：当前恢复制品c2ddccf9…，原WideSqlWorkloadBenchmark.operatorMixDynamicIn完整输入／SQL／oracle不变。官方测量期JFR原会话95765成功，JDK17.0.18、512MB／G1、1线程、1fork、3×1s预热、1×15s测量、stackDepth128；788个worker CPU／4394个分配样本，InFilter栈分别命中61／628，包含117个MonoMapFuseable分配样本。该数仅定位可删装配，不能换算CPU百分比、精确B/行或常驻堆。日志`target/dynamic-in-identity-jfr-{owner,summary}-20261007.log`、`target/jmh-dynamic-in-identity-current-jfr-20261007.{log,json}`及`target/jfr-dynamic-in-identity-current-20261007/`。

实施与风险：先完成同配置正式before，只有已有布尔结果不需要反转时直接返回同一Mono，保留原比较和错误所在操作符。使用原生旧链作为独立oracle，验证正／反向、匹配／未匹配／空流、来源错误身份、Context与分次需求／提前取消；复用既有真实SQL、动态Feature及集合左值回归。阶段末集中完整离线package，保留DebugAgent／许可证检查；全部基准class保持字节一致，仅InFilter源owner可变化。

验收：动态IN真实混合SQL目标、等价无IN／常量IN／直接原生控制、深层子查询与多事件分组控制，JDK17.0.18、512MB／G1、1线程、2forks、3×1s预热、5×1s测量、GC profiler，HighCardinality参数保持valuesPerKey=50。每项完整10次测量。至少目标SQL吞吐提升10%且区间不重叠，或B／输入行下降5%；任意场景after吞吐上界小于before下界即撤回，不调输入／门槛／预热重跑至通过，不推广为整个引擎或常驻堆收益。若收益低或边界不等价，关闭候选而不扩展实现。

当前结论：所有控制通过，但目标吞吐+6.87%、分配−3.49%，未达到事先声明的CPU≥10%或分配≥5%门槛，撤回两个生产返回点，保留两项旧原生链独立对照测试。不降低门槛、不新增IN密集SQL或扩大函数选路来寻求通过，不申领当前生产收益或录已拒绝候选的after JFR。其分配收益仅属于被删除的恒等层，不能解决必要参数订阅或整个SQL／原生性能差距。

候选功能与身份：完整离线package原会话78008成功，618项／59份当次XML报告、0failure／error／skip，标准DebugAgent／许可证检查保留；新增原生oracle验证两种极性、空／匹配／未匹配、Context／零需求后分次请求及提前取消轨迹、左／右来源错误对象身份，InFilterScalarTest共10项均通过。与before c2ddccf9…相比，572份生产／基准class仅InFilter.class变化，无新增／删除，全部367份基准／生成class字节一致。未保留候选`target/dynamic-in-identity-candidate-validated-20261007-benchmarks.jar` SHA256 `0810afbacc6cf812c97c1e2e8f5db4b5a1fc5cc209730b375ba9a323f12810fc`；618项全量属于候选，不声称恢复后重跑618项。

正式GC／JMH：原会话9176／68824均成功，六项各2forks／10次测量，逐项JVM原始参数、预热／测量／线程／参数完全相同，只有上述InFilter生产class变化；原SQL、输入、完整oracle、源订阅不变。输入行/s±JMH error及B／输入行如下：

| 场景 | before | after | 吞吐变化 | B／输入行 before→after |
| --- | ---: | ---: | ---: | ---: |
| 动态IN混合SQL目标 | 1,495,836±29,653 | 1,598,632±25,150 | +6.87%，区间不重叠 | 1835.571→1771.571（−3.49%） |
| 常量IN混合SQL控制 | 2,471,860±30,910 | 2,479,957±24,987 | +0.33%，区间重叠 | 1022.236→1022.236 |
| 无IN等价混合SQL控制 | 3,317,204±45,789 | 3,365,221±62,001 | +1.45%，区间重叠 | 720.835→720.835 |
| 直接原生控制 | 18,379,744±973,848 | 18,012,128±540,442 | −2.00%，区间重叠 | 140.705→140.705 |
| 深层子查询控制 | 7,779,303±294,262 | 7,617,438±224,145 | −2.08%，区间重叠 | 576.141→576.140 |
| 函数键多事件4聚合控制 | 2,644,737±257,321 | 2,755,513±153,398 | +4.19%，区间重叠 | 784.102→782.982 |

所有控制的吞吐区间重叠，没有命中稳定回退停止规则；不把它们的均值涨跌或小幅分配变化归因为候选。目标实际减少64 B／输入行；虽有区间不重叠的CPU提升，但两项高收益门槛均未达标，不重试至通过。原始`target/jmh-dynamic-in-identity-{before,after}-20261007.{log,json}`，完整功能日志`target/dynamic-in-identity-candidate-full-package-20261007.log`。

撤回后验证：原会话42710离线package成功，仅运行InFilterScalarTest的10项、0failure／error／skip，其中两项新增原生对照保留；不是新的618项完整运行。恢复制品的全部572份生产／基准class与before c2ddccf9…字节一致，无新增／删除，因此复用此前613项完整生产证据和恢复代码上已通过的3项GroupBudgetLifecycleTest。当前及归档`target/dynamic-in-identity-restored-20261007-benchmarks.jar` SHA256 `d7cc613471b9f9aff7af815111eb8f408a57fe2242a2f96fbaf002102b52c49f`；日志`target/dynamic-in-identity-restored-package-20261007.log`。纯布尔链不新增MBean／TraceHolder，未提交／推送／创建PR。

后续入口核对：当前FunctionMapFeature.scalar2第130行只委托同一个scalar参数流／List回调，不是旧历史阶段的直接双值执行入口。仅将date_part等改注册为scalar2并不能减少参数列表，不据此新增迁移、资格标记或执行框架。下一个独立核查对象为replaceText的输出长度门禁：它已先验证source长度，但仍对所有替换完整countMatches；先以真实文本规范化JFR确认这次计数是否为CPU热点，再评估通用保守输出上界能否减少重复扫描，同时保持扩张输出的精确超限判断和原JDK替换语义。尚未录该场景或修改生产代码，不算新优化成果。

### 分组预算终止后私有键集合：候选已撤回，驻留问题仍未解决

目标与范围：仅检查显式启用分组预算时GroupStateBudget.Scope的已见键集合。默认未启用预算的原生分组路径不变，不更换公开group-key List、归约器或Reactor算子，不变更预算／并发／预取。当前Scope终止只释放原子计数，若调用方保留一个已完成的BudgetedGroupedFlux，整个Scope.keys仍被连带保留；它不是AVG／MAX历史行驻留。

先行证据：`/private/tmp/BudgetRetainedKeyProbe.java`对相同20,000键、每键独立2048-byte载荷、完整消费及一次保留分组比较原生／预算包装。before原会话32213成功，native保留1键／2048-byte载荷，budget保留20,000键／40,960,000-byte载荷；释放该分组后二者均为0。官方JFR `target/budget-retained-keys-before-20261007.jfr`及`target/budget-retained-keys-before-jfr-audit-20261007.log`记录相同phase事件，预算阶段四次GC后均20,000个Key、480,000 shallow bytes。分配采样未命中Scope.reserve，不能凭此宣称分配热点比例；scope.keys的源代码引用及保留分组／释放分组反事实定位额外驻留。载荷数字只计被这些键持有的独立byte[]内容，不等于整个堆或精确owner-size。

已撤回候选边界：仅在现有outerTerminated的ON_COMPLETE／ON_ERROR分支清空私有HashSet，保留原计数和引用释放顺序，不增操作符、锁、缓存、接口或输入类型分支。CANCEL不能直接清空：原生groupBy外层取消后，已经选中的分组仍可继续接收同键输入；此时集合仍参与路由／预算兼容判断。完整成功终止后已无新键输入，clear不会与合法onNext并发；不以每行锁或新的终止框架补偿。HashSet自身及空桶数组仍可留在保留的Scope，不声称零状态。

验证方式：补成功完成／保留分组身份与键、外层取消但内层继续消费、来源错误身份回归，连同既有预算、需求／取消、窗口／多层分组在一个阶段完整验证。用相同probe及JFR比较before／after，要求预算完成后保留键降至原生1键、释放控制均0；原生不变。再用同配置GC/JMH的直接函数键多更新、50k高基数控制及宽投影／子查询确认吞吐不稳定回退，控制不变；未达标或改变信号则撤回。此处是生命周期存活堆修正，不把它外推为默认无限流活跃键内存下降。

当前结论：内存门槛通过，但性能门槛未通过，生产清理已撤回；不把候选内存下降或宽投影上涨计入当前收益，也不对回退无证据归因给清理、噪声或JIT。保留GroupBudgetLifecycleTest的三项通用完成／取消／错误契约回归，防止后续把外层取消等同于输入终止。默认行为、此前已验证优化及全部既有脏改动保留；该驻留问题仍然存在，不用新框架或输入分支替代本候选。

当前制品复现（2026-10-08，只读审计）：probe SHA256仍为`622e0541e8e303628f0d2160bb7ebfc1ac28efe5f33c6f14ac0eaab5b448a66b`；现有`target/budget-retained-keys-current-20261008.{log,jfr,jfc}`成功。完整消费并保留一个分组后，native 为 1 key／2,048 payload bytes，显式 budget 为 20,000 keys／40,960,000 payload bytes；释放该分组后二者均为 0。主审计已读 JFR：2 个 Snapshot 事件完整且无截断，`bounded=false`为 retained 1，`bounded=true`为 retained 20,000；256 个`ObjectCountAfterGC`事件中，`BudgetRetainedKeyProbe$Key`只在 gcId 12／13／14／15出现，每次 count 20,000、totalSize 480,000 shallow bytes。未报告的小类不能解释为 0，payload 内容字节不是整个 owner-size；启动时的 invalid recording 不作为证据。短收据见`target/budget-retained-keys-current-jfr-audit-20261008.json`。

最新独立源码审查结论：将`Set`引用 O(1) 解除虽然不同于`clear()`扫描，仍是同一 COMPLETE／ERROR 终止清理候选的换写法；没有新证据显示它会避开此前深层子查询的稳定回退，故不重开、不写生产。CANCEL不得清理：外层取消后，已选中的内层仍可接收同键；`Scope`由保留的`BudgetedGroupedFlux`持有，当前`close()`仅释放计数。此结论不是新的准入方案。

候选功能与身份：完整离线package原会话29887成功，616项／59份当次XML报告，failure／error／skip均0，标准DebugAgent及许可证检查未关闭。候选归档`target/budget-terminal-keys-candidate-validated-20261007-benchmarks.jar` SHA256 `8ef1494a8a9596b0e0934cc66f82a32e15c0f14bf8d9f3da62c4a747718b133a`；与before f173da03的572份共享class相比，仅GroupStateBudget及其Scope、BudgetedGroupedFlux三个class变化，无新增／删除，全部367份基准／生成class字节一致。616项全量结果属于未保留候选，不冒充撤回后的新一轮全量验证。

存活堆取证：相同probe SHA256 `622e0541e8e303628f0d2160bb7ebfc1ac28efe5f33c6f14ac0eaab5b448a66b`，before／after各三次均成功，相同20,000键、2048-byte独立载荷、512MB／G1／stackdepth128／profile及ObjectCountAfterGC配置。before预算均20,000键，after预算均1键；所有原生控制均1键，释放分组后全部0。独立键载荷最低驻留40,960,000→2,048 bytes，不是整个堆或精确owner-size；只归属已撤回候选。after官方JFR的phase事件同样为native1／budget1，未报告Key的ObjectCountAfterGC事件，不能据此声称键为0；247个分配样本中reserve命中0，不作为CPU／分配下降证明。日志／录制为`target/budget-retained-keys-{before,after}[-repeat2/-repeat3]-20261007.{log,jfr}`，审计为`target/budget-retained-keys-after-jfr-audit-20261007.log`。

正式性能：原会话64163／5809均成功，五项各2forks／10次测量，JDK17.0.18、512MB／G1、1线程、3×1s预热、5×1s测量、GC profiler；仅HighCardinality参数为valuesPerKey=50。before原始JVM参数重复同值的三项堆／G1选项，after仅一份，实际选项相同；记录此原始参数差异，不把它当作豁免回退或重跑至通过的理由。吞吐为输入行/s，误差为JMH error：

| 场景 | before | after | 变化 | B／输入行 before→after |
| --- | ---: | ---: | ---: | ---: |
| 原生函数键4聚合 | 48,349,079±2,580,063 | 46,086,922±1,728,972 | −4.68%，区间重叠 | 32.179→32.179 |
| SQL函数键4聚合 | 2,665,011±134,602 | 2,658,738±207,182 | −0.24%，区间重叠 | 783.622→784.102 |
| 深层子查询控制 | 7,823,347±82,295 | 7,397,663±151,762 | −5.44%，区间不重叠 | 576.140→576.140 |
| 50k键预算聚合 | 5,148±2,368 | 5,729±1,420 | +11.29%，区间重叠 | 7,860.136→7,900.135 |
| 宽列投影控制 | 4,692,965±111,459 | 5,139,147±149,122 | +9.51%，区间不重叠 | 736.009→736.009 |

停止规则命中：深子查询after上界7,549,425小于before下界7,741,052，因此不验收。五项事实完整保留，不只挑目标或上涨项。函数键、50k键分配区间重叠；深子查询／宽投影存在小于0.001 B的测量差异，不宣称精确不变或归因。本阶段不继续反向／调参确认来寻找通过结果。原始`target/jmh-budget-terminal-keys-{before,after}-20261007.{log,json}`。

撤回后验证：恢复离线package原会话36986成功，保留的GroupBudgetLifecycleTest三项在恢复代码上全部通过，0failure／error／skip；本次仅运行这三项，不能称为新的616项全量。恢复制品与before f173da03的全部572份生产／基准class字节一致，无新增／删除，因此复用此前613项完整生产回归；默认限额、并发、预取和依赖未变。当前及归档`target/budget-terminal-keys-restored-20261007-benchmarks.jar` SHA256 `c2ddccf9e6a1cde1b812bbf356984dd2210af684d37ac7cd623e01a4d04d9276`。日志`target/budget-terminal-keys-restored-package-20261007.log`；三项新增契约测试保留，已撤回Scope清理不再计为当前实现。此次没有新增后台管理器或外部I/O，不新增MBean／TraceHolder。未提交、推送或创建PR。

### 多事件分组增量更新：覆盖缺口取证

现有HighCardinalityNativeBenchmark的50,000条固定输入仅配置每键1／2条，主要体现50k／25k活动键的原生完成扫描，不能代表持续更新同一键的常用窗口聚合。仅增加valuesPerKey=50的取证点，即1,000键／每键50次更新；不改变原1／2点、SQL、数据生成规则、每次总输入、值／类型／完整键集合oracle、源订阅次数、并发／预取／默认限制。复用相同COUNT／SUM／AVG／MAX、lower函数键、派生表及正常属性键／原生增量参考入口，先确认setup完整结果再录测量期JFR。此举是新增覆盖，不是为已存在候选调整输入，生产实现保持已通过613项完整测试的b0c129c…不变。

阶段步骤：单独构建基准并核对所有生产class字节不变；固定JDK17、512MB／G1、1线程、3×1s预热／1×15s测量／stackDepth128，先录直接函数键多归约JFR，再按真实owner决定是否存在简单通用优化。原生及属性键参考不得预存结果或省略必要计算；不以采样计数推算CPU占比、精确B／行或常驻堆。若剩余成本仍为必须保留的独立订阅／错误边界，关闭这个候选方向而保留覆盖，不改标准归约、不重建融合框架、不作参数特调。正式收益只在新候选准入后用相同夹具成对验证。

准入取证：基准单独离线package原会话8423成功，572份共享class仅HighCardinalityNativeBenchmark的参数注解改变，所有生产与生成基准class逐字节相同，无新增／删除；复用613项功能证据，不声称重新跑全量。当前／归档`target/multi-event-group-evidence-20261007-benchmarks.jar`的SHA256为`f173da0352da6bc316d1662363778fa3a11d58f2759c9469b7dc3dd72002337b`。直接函数键多归约JFR原会话56991成功，全部原有setup及一次订阅断言通过，951个worker CPU／4430分配样本；FluxMapFuseable.MapFuseableSubscriber804、Tuple2在GroupByValueFeature下273、LinkedList.Node在writeGroupKey下237，主要CPU叶包括drainLoop162、toLowerCase105、FluxPublish.drain55。降低到1,000键后不再只有完成扫描一种主导来源，但这些数量仍只作来源定位，不能推算CPU百分比、B／行或常驻堆。日志`target/multi-event-group-jfr-{summary,focus}-20261007.log`。

官方Reactor Math3.4.10源码证实SUM／AVG的Subscriber使用原始数值字段增量更新，没有每事件装箱累加或历史集合。官方映射重载不能代替当前独立castNumber map：`/private/tmp/NativeMathMappingBoundaryProbe.java`用同一"1","bad","3"与onErrorContinue验证，独立map下SUM=4.0／AVG=2.0、恢复1次、取消0次；合并映射下两者都终止TypeCastException、恢复0次、取消1次，双方各订阅1次。`target/native-math-mapping-boundary-20261007.log`成功，且MathSubscriber.onNext的reset／onOperatorError终止范围与该结果一致。该候选不进入生产、不增加错误补偿或类型分支。接着仅对新负载录五入口同配置GC/JMH绝对基线，区分COUNT／四归约、函数／属性键、派生表和必要计算参考；不是优化A/B，也不将输入键数变化当作生产收益。

该负载五项正式测量`target/jmh-multi-event-group-current-20261007.{log,json}`原会话66416成功：各2 forks／10次measurement／完整setup oracle，实际JVM参数同为512MB／G1（注解及CLI重复传入相同三参数，最终有效配置不变），单线程、3×1s预热、5×1s测量。以下均为绝对输入行成本，不是候选收益：

| 入口 | 输入行/s ± JMH error | B/输入行 |
| --- | ---: | ---: |
| 原生增量函数键四聚合参考 | 45,090,265 ± 2,562,199 | 32.179 |
| SQL函数键COUNT | 4,219,148 ± 177,285 | 654.181 |
| SQL函数键四聚合 | 2,698,037 ± 191,904 | 783.142 |
| SQL已有规范化属性键四聚合 | 3,759,642 ± 342,361 | 514.498 |
| SQL派生表现场计算函数键四聚合 | 2,105,019 ± 101,593 | 1343.787 |

函数键SQL距直接正常计算参考仍约16.71倍吞吐差距；参考不具备所有扩展／错误隔离／取消合同，不能直接删除差距对应的订阅步骤。属性控制使用已有规范化字段，不是在测量外偷算生产结果的候选；派生表仍现场lower原字段并经原有错误预计算字段反例验证。派生形态在本负载下吞吐比直接函数键约低22%、分配约多72%，故先前融合实现下的“前置键计算可能降低成本”不能推广到当前原生路径；不自动改写SQL或根据基数选路。

分组元数据同样不是完全私有容器：当前SQL `select _group_by_key group_keys,count(1) total from test group by key`直接输出java.util.LinkedList，`target/group-key-metadata-visibility-20261007.log`已验证其值、类型与COUNT。不能把LinkedList替换当成没有公开类型影响的纯内部内存改动，也不为保留扩展类型增加默认／特定输入分支。本阶段保留新增benchmark参数，生产class仍与613-test制品逐字节一致、无新吞吐／分配／常驻堆收益；stage diff检查通过，全部作业终态，无commit／push／PR。后续仅在新证据证明简单通用且保留信号／公开数据边界时实施，不重试已经被反例否决的数学映射融合、参数融合或派生自动重写。

### repeat：公共 API 同步内核已通过双顺序 CPU 验收

当前结论：保留现有Guava Strings.repeat替换逐次追加的同步构造，双顺序真实SQL吞吐分别+12.466%／+14.800%，分配−1.014%，613项完整功能回归通过，after JFR确认原StringBuilder构造／追加来源消失。仅这一函数场景的CPU收益已验证，不外推为全局SQL或高基数常驻堆改善。没有新增操作符、缓存、输入类型分支或依赖变更，原参数订阅和响应式生命周期保持不变；长计数溢出限额修正独立披露，不作为跑分收益。控制项未触发既定稳定回退门槛，但反向分组／属性控制均值下行且误差大，仍保留这一测量风险。

目标与范围：普通SQL CAST的固定类型名已在CastFeature.createMapper绑定，不能重复优化；动态属性::仍走既有公共属性语义，不新增访问器、手写解析器或缓存。本切片只评估repeatText逐次StringBuilder.append的同步构造，优先复用当前依赖已有的Guava Strings.repeat公共API，不改FunctionMapFeature的原生参数映射、错误范围、Context、需求／取消、默认限额或依赖版本。它不是填充候选重试，也不重开已关闭的参数融合。

先新增RepeatTextBenchmark：16384个预建展示事件，动态文本及0–32次动态重复，包含空文本、单／多字符标记、Unicode和部分代理字符；真实SQL、同值原生逐次构造、属性投影控制校验全行、值／类型、顺序、数量和一次源订阅。基准不缓存计算结果、不修改oracle或按收益扩大输入。先单独构建基准，核对全部旧class字节一致并复用当前611项功能证据；固定JDK17／512MB／G1／stackDepth128、1fork／3×1s预热／1×15s测量取得JFR，仅在重复追加被证明确为CPU／分配热点后才考虑生产改动。

候选前提是公共API的值／类型、UTF-16和长计数／错误／资源边界等价，若需要异常补偿、输入／字符集分支或新的执行框架则不准入。保留门槛提前固定：真实SQL分配至少下降10%且无稳定吞吐回退，或同一SQL两个测量顺序均提升吞吐至少10%、置信区间不重叠且分配不增；均要求原生／属性控制及现有宽投影／分组／子查询负对照无稳定回退。阶段集中验证；不满足则撤回，不反复调整参数直至通过，不申领常驻堆下降。

准入证据及计数边界：基准单独离线package原会话85165成功，562份原有生产／基准class全部字节一致，仅新增10份RepeatTextBenchmark及生成class；原611项功能证据复用。before制品SHA256为4d066969687bdb864490b70463e866fa621afe0492511414531d42f0732a3ee9。JFR原会话35764正常结束、完整三路oracle／一次订阅通过；845个worker CPU／4433个分配样本中，repeatText完整栈匹配192 CPU／750分配（694个byte[]、56个String），涉及重复追加与缓冲区构造。target/repeat-text-before-jfr-{summary,focus}-20261007.log只定位，不推算精确B／事件或常驻堆。

源码边界核对还证明当前(long)text.length()*count会溢出：长度2、Long.MAX_VALUE次得到−2；长度4、2^62次得到0，后者可能绕过限额进入巨大循环。不能窄化次数或添加按溢出异常补偿的执行快路。用当前已声明Guava32.1.2-jre的LongMath.saturatedMultiply计算长度，再执行原assertGeneratedStringLength，正溢出统一按现有函数输出限额错误拒绝；合法长度及原普通超限信息不变，零／负次数及空文本仍在原位置返回空字符串。这是修复原资源检查失效，不声称溢出路径与旧NegativeArraySizeException／潜在OOM错误等价。默认限额、配置、参数订阅／值局部错误边界不变；测试独立覆盖该安全修正。随后Guava Strings.repeat只接收已按原硬上限校验且可表示为int的次数，避免手写复制／字符集分支／缓存。安全修正不计入正常吞吐或常驻堆收益；性能门槛仍适用于整个候选，若公共API不达标撤回其替换，不用调参补救。

基线夹具修正：首轮正式before原会话20351的profilingHighCardinalityAggregates在按键独立窗口的附带校验中失败：无ORDER BY的结果首行是{total=1,key=key-4630}，旧oracle却按行下标要求key-0、key-1顺序。SQL为group by key,_window(2)，没有全局排序契约；未把基线失败解释为候选回退，也不以进程退出0宣称六项完成。仅将ReactorQLBenchmark中该oracle改为与输入键集合逐项对应、每键恰好一次且值／类型为1L，保留字段数、完整行数、重复／缺失键和一次冷源订阅断言；不改SQL、输入、循环消费或生产分组实现。重新构建原始生产before并核对class，之后before／after共用这个修正夹具，正式六项A/B重新成对执行；既有repeat JFR因其输入／SQL／基准class不变仍有效。

功能阶段完成：完整离线`mvn -o -q -Pjmh -DtrimStackTrace=false package`通过613 tests、0 failures／errors／skips、58份当前报告，标准DebugAgent及许可证检查保留；日志`target/repeat-text-candidate-full-package-20261007.log`。新增81组旧构造结果差分、5种大计数拒绝及空文本Long.MAX_VALUE回归。候选制品`target/repeat-text-candidate-validated-20261007-benchmarks.jar`的SHA256为`b0c129c19fa6db00085c1d0dec976dfa5d6875b7223deccc13cf210308f08b57`；与修正夹具before的572份共享class相比，仅DefaultReactorQLMetadata变化，所有基准class相同，无新增／删除。

首轮完整正式A/B均成功完成六项、2 forks各5次measurement：`target/jmh-repeat-text-{before,after}-oracle-fixed-20261007.{log,json}`。SQL repeat为3,911,910.954±106,950.863→4,399,568.453±104,597.725输入行/s（+12.466%，99.9%区间不重叠），1242.380→1229.785 B/输入行（−1.014%）。分配10%门槛未达到；CPU门槛仍需反向完整对照，尚不宣告保留。深层子查询／50k键分组／native repeat／属性投影／无WHERE宽投影吞吐分别−1.50%／−3.00%／+2.17%／+0.02%／+0.85%，五项区间均重叠，无稳定回退。分组分配7852.136→7884.136 B/输入行（+0.408%），其他控制分配基本不变；不申领常驻堆下降。下一步按相同SQL／输入／oracle／JVM配置执行after→before六项反向对照，仅通过预定门槛后补after JFR，否则撤回公共API替换；不降门槛、不调参数重试。

反向完整对照`target/jmh-repeat-text-{after,before}-reverse-20261007.{log,json}`均成功完成六项、2 forks各5次measurement，配置逐字段核对一致。SQL repeat为3,875,707.217±36,154.797→4,449,330.037±28,949.637输入行/s（+14.800%，区间不重叠），1242.380→1229.785 B/输入行（−1.014%）。因此通过原定双顺序CPU门槛，不修改分配10%门槛。反向深层子查询／50k键分组／native／属性／宽投影均值分别+2.84%／−10.03%／+6.65%／−11.93%／+5.79%；分组和属性下行均因区间重叠未触发预先定义的稳定回退规则，但不以此证明零风险或归咎于噪声。native区间分离的上行不归因于未触及其热路径的生产改动。控制分配基本不变，分组反向均为7868.136 B/输入行。两轮门槛已完成，不再追加复测直到得到更有利的控制数值；下一步仅以相同配置after JFR确认目标构造来源。

相同配置after JFR原会话55200正常结束：`target/jfr-repeat-text-after-20261007/org.jetlinks.reactor.ql.RepeatTextBenchmark.sqlRepeat-Throughput/profile.jfr`，850个worker CPU／4375个分配样本；repeatText完整owner为97 CPU／651分配（358 byte[]、233 char[]、60 String）。独立完整栈交集核对`target/repeat-text-kernel-jfr-audit-20261007.log`中，repeatText内StringBuilder／AbstractStringBuilder构造／追加来源before为131 CPU／750分配，after均为0；原字符串／数组构造仍然存在，不能声称零分配，亦不能将采样数量相除当作CPU占比或精确字节收益。详细after摘要与owner见`target/repeat-text-after-jfr-{summary,focus}-20261007.log`，精确B/输入行只引用GC/JMH。当前与归档候选JAR的SHA256均为`b0c129c19fa6db00085c1d0dec976dfa5d6875b7223deccc13cf210308f08b57`；功能构建后没有生产／测试／基准源码变化，复用613项有效全量证据，无重复构建。阶段差异检查通过，无commit／push／PR。此切片验收完成，广义性能目标仍未完成；剩余原生参数订阅、结果Map和高基数组合扫描热点不得用重新引入融合／补偿框架或调低并发／预取解决。

### 固定宽度字符串函数：填充候选已撤回

当前结论：两个填充内核均未通过既定验收，生产实现已恢复原repeatPad＋concat，候选专用693组合测试同步撤回；常用导出场景基准和原始证据保留。独立属性控制确认宽64出现吞吐稳定回退，按预先停止规则不再追加复测、调参、分支或补救框架。填充SQL候选约42%的吞吐改善不计入当前成果；本阶段未确认常驻堆下降。恢复后离线package（skipTests，无clean）成功，562份共享生产／基准class与原始a13bf925基线全部字节一致，无新增／删除；复用此前611项完整功能证据，不声称重跑全量，候选612项仅为历史试验。当前JAR SHA256为a6ed7e89dcec5975e2f5854e77db0dc5d6b94dafba59fb0315715b9dec425a1b，target/string-padding-restored-{package,class-identity}-20261007.log；git diff --check通过。

目标与范围：检查内置lpad／rpad的纯同步结果构造。当前padText先repeatPad生成独立填充String，再concat原字段，存在可由同一个StringBuilder完成的重复缓冲区／字符串复制；不改函数参数流、错误范围、limit读取／检查、null／空填充／截断及UTF-16长度语义。先加独立StringPaddingBenchmark，以16384个预建导出字段事件、16／64两种常用字段宽度，含多字符填充、Unicode、空字符串、空填充和超长截断；SQL／同值原生计算与属性投影控制分别校验全行、类型、顺序、数量和一次源订阅。原生参考不预存结果，不跳过每行计算；所有A/B共用同一夹具。

阶段先只构建基准，核对全部原有生产／基准class不变并复用611项功能证据。以64宽度真实SQL录制显式512MB／G1／stackDepth128、1fork／3×1s预热／1×15s测量的官方JFR；只有中间构造被确认是实际分配热点才进入生产修改。候选只复用原填充循环把字符写入最终Builder，删除填充String／concat中转，无新类型分支、解析器、缓存、API或操作符。阶段末集中完整离线package与旧结果差分，再用同JDK17、512MB／G1、2forks、3×1s预热、5×1s测量、GC profiler的两种宽度／三个路由A/B验收。在正式基线和生产修改前，因下述JFR同时确认明显复制CPU成本，明确两种独立高收益门禁：至少一个真实SQL分配下降10%以上、其他SQL吞吐无稳定回退；或同一SQL两个顺序都稳定提升吞吐10%以上、区间不重叠且分配同降。两种门禁均要求原生／属性控制无稳定回退。否则撤回，不扩大宽度或输入以达门槛，不按观察结果修改门禁。不以瞬时分配申领分组常驻堆收益。

基准／准入结果：基准单独打包原会话22893成功；552份原有生产／基准class全部字节一致，仅新增10份StringPaddingBenchmark及生成class，target/string-padding-benchmark-class-identity-20261007.log。基线归档target/string-padding-before-20261007-benchmarks.jar，SHA256为a13bf92533008d585078b91db572edfcaab0a86df466b21d5ddde10c2260524c；原611项完整功能证据复用。JFR原会话75964成功、完整三路setup oracle通过、无failure且正常退出，target/jfr-string-padding-before-20261007/及同名log／JSON。1007个worker CPU／4414个分配样本中，完整栈padText／repeatPad匹配298 CPU和903分配（766个byte[]、137个String）；repeatPad直接归因509个byte[]，padText直接归因257个byte[]。其中String.getBytes有142个CPU叶样本。target/string-padding-before-jfr-analysis-20261007.log仅用于定位；不换算CPU百分比、B／事件或常驻堆。它支持仅消除已存在的中间复制，而不简化原生参数收集／订阅链。

候选功能验证：正式before原会话47332六项全部完成，每项2forks／10测量、完整oracle通过、无failure。只把原repeatPad填充循环写入最终Builder，改为appendPadding；保留所有前置转换、长度上限、空填充／截断顺序和原生参数计算位置。原会话11952完整离线package成功，612项测试、0失败／错误／跳过、58份本次报告；新增真实SQL693组合与旧填充String＋concat独立oracle逐值、类型、字段存在性及行顺序差分，覆盖部分代理字符、Unicode、多字符填充、负／零／正宽度、数字与布尔输入。target/string-padding-candidate-{full-package,test-summary}-20261007.log。562份共享class中，仅DefaultReactorQLMetadata及其BoundedStringJoiner内部class变化（后者位置在修改行之后），全部基准class一致，无新增／删除，target/string-padding-candidate-class-identity-20261007.log；归档target/string-padding-candidate-validated-20261007-benchmarks.jar，SHA256为dce565587a793adc32a142d8a874081c752a3ab52461be72093c80703b97c43b。after同六项原会话83199进行中，性能尚未验收。本次只有纯同步StringBuilder构造复用，不涉及新异步／资源边界，不新增Trace span或MBean；代码旁说明中间复制与UTF-16边界。

第一种结果构造方案已拒绝：after原会话83199成功结束、六项各10测量、完整oracle通过且无failure。SQL宽16为2.513±0.057→2.351±0.039 M输入事件/s（−6.44%，区间不重叠）、2198.389→2162.967±3.246 B／输入事件（−1.61%）；宽64为1.655±0.022→1.689±0.011 M（+2.04%）、2592.507→2451.962±38.247 B（−5.42%）。属性64控制30.607±0.898→24.700±1.044 M也出现不重叠回退；不能把控制波动归因给填充算法，仍按门禁拒绝，不降低阈值或扩大字段宽度。target/string-padding-ab-summary-20261007.log及target/jmh-string-padding-{before,after}-20261007.{log,json}。原JFR的String.getBytes调用栈进一步核对来自StringBuilder.append→repeatPad逐小片段写入，而非仅最后concat；target/string-padding-append-cpu-before-20261007.log。因此补候选JFR只判别剩余重复append成本，不为第一种方案补救参数。

后续候选准入限于同一同步计算内核：若JFR确认多次append仍占主要CPU，改用JDK已有String.getChars写原字段、普通UTF-16字符循环写填充、new String完成输出；无按字符集／填充长度分支、扩大限额、源码复制依赖、缓存或额外函数框架。这是一份最终字符缓冲区，避免逐片段StringBuilder调用，不是解析器。保留原有前置限制／截断／空填充顺序，复用同693组旧构造差分和完全不变的六项JMH夹具／原始a13bf925基线；阶段末完整验证与同门禁A/B。只有本通用方案满足已定高收益门禁才保留，否则将整个生产候选撤回。

CPU判别及第二内核：原会话5246成功完成候选Builder的同配置JFR；858个worker CPU／4410个分配样本，padText／appendPadding完整栈307 CPU／874分配。100个String.getBytes CPU叶样本中96个来自appendPadding的StringBuilder.append、4个来自原字段追加；原始实现的142个同叶样本中140个来自repeatPad、仅2个来自concat。这是调用栈归因，不把不同录制采样比解释为CPU百分比。target/string-padding-builder-jfr-analysis-20261007.log证明复制消除没有解决多次小片段写入；停止第一种方案，不调参数或加分支补救。第二内核改为一次char[]、一次String.getChars原字段复制及按原UTF-16字符顺序循环填充，new String返回值；删除整个Builder辅助方法，长度／输入校验和响应式链完全保留。完整测试原会话60442进行中，基线仍用未经任何生产修改的a13bf925，不用失败Builder当较弱before。

第二内核功能／初轮性能：原会话60442完整离线package成功，612项测试、0失败／错误／跳过、58份当前报告；同693组旧构造差分通过。562份共享class仅DefaultMetadata及其BoundedStringJoiner改变，全部基准class一致，无新增／删除，target/string-padding-character-{candidate-test-summary,class-identity}-20261007.log。制品target/string-padding-character-candidate-validated-20261007-benchmarks.jar的SHA256为211779085cc2ccfc2eaf0b26d7b684f9ed683f192126e93cf0059bcfe681ddc6。after原会话16422成功、六项各10测量、完整oracle通过、无failure：SQL64为1.655±0.022→2.354±0.023 M输入事件/s（+42.23%，区间不重叠）、2592.507→2504.880 B／输入事件（−3.38%）；SQL16为2.513±0.057→2.574±0.022 M（区间重叠）、2198.389→2174.055±38.247 B（−1.11%）。原生两宽度与属性16控制区间重叠、分配不变，但属性64控制30.607±0.898→26.719±0.076 M（−12.70%，区间不重叠）；首轮仍不验收，不把该控制变化无证据归因为内核或噪声。target/string-padding-character-ab-summary-20261007.log保留所有六项事实及拒绝验收断言，目标收益不能掩盖控制回退。按原参数先after再原始before反向复测全部六项，确认目标CPU收益及所有控制；回退复现则撤回，无新字段宽度／预热／参数／特调分支。

最终反向／控制确认：原会话31408成功，反向SQL64仍为1.656±0.041→2.345±0.082 M输入事件/s（+41.61%，区间不重叠），2592.507→2504.880 B／输入事件（−3.38%）；该轮四个控制区间重叠。随后仅一次独立属性控制确认原会话5067成功，两宽度均保持原JDK17／512MB／G1／2forks／3×1s预热／5×1s测量及完整oracle。宽16为28.461±5.133→24.947±0.872 M（−12.35%，区间重叠）；宽64为31.342±0.616→25.332±0.872 M（−19.18%，区间不重叠），两宽度分配均保持208.031 B／输入事件。原始target/jmh-string-padding-property-{before,after}-confirm-20261007.{log,json}。属性控制SQL／输入／热路径在两宽度中完全一致，宽度只影响旁路setup验证；不无证据归因给内核、噪声或JIT。按照确认前明确的停止规则撤回整个生产候选，不继续复测直至通过，不录制已拒绝候选的after JFR，也不改门槛或扩大输入。

### 集合属性读取：立即解包的Optional包装删除候选已撤回

当前结论：本阶段不保留生产改动。两个顺序的正式A/B都确认属性::转换吞吐稳定回退，尽管普通属性分配下降约24%。已撤回两处方法替换及仅服务该候选的三项试验测试，原有全部脏改动和已验证高收益优化保留。没有新增类型／位置分支、辅助调用层或其他软特调；默认限制与响应式边界未变。当前有效功能证据为此前611项完整回归，614项仅归属已撤回候选，不作为当前全量测试数。

目标与范围：当前de252c38制品的属性::测量期JFR中，282个worker分配样本属于DefaultPropertyFeature.getProperty创建的Optional，调用方为CastUtils.listToMap，随后立即orElse(null)。该调用明确使用不可替换的DefaultPropertyFeature.GLOBAL；其getProperty仅为已有getPropertyValue结果包装Optional。候选只把listToMap的两次读取改用同一GLOBAL的已有原始值方法，不提前解析属性名，不改变集合stream、读取次序／次数、Tuple2、toMap及重复键异常，也不绕过自定义PropertyFeature选路。

先复用当前611项有效功能证据及归档制品，以相同ArrayToRowBenchmark的普通属性场景补测量期JFR，确认包装未被JIT消除；未确认则关闭候选。确认后补旧实现独立差分，覆盖普通／嵌套／转换／引号／数字／缺失／null、空集合非法字段、重复键及读取失败时序。阶段末集中完整离线package；核对只有CastUtils生产class变化、全部基准class不变。在JDK17.0.18、512MB／G1、1线程、2forks、3×1s预热、5×1s测量、GC profiler下，对原有四项SQL／原生场景做同制品before／after。只有至少一个真实SQL场景分配下降10%以上、吞吐无稳定回退且原生控制健康才保留；吞吐区间重叠不申领稳定CPU收益。不引入缓存／池、新API、类型／数据分支、手写解析器或Subscriber；默认限制／并发／预取不变。此阶段没有分组常驻堆收益主张。

候选排除：splitDot生产调用都为limit=2，早已使用indexOf／substring；剩余非热fallback不修改。splitCast的Matcher成本虽明显，但目前没有已证明语义等价的简单公共API替换，不以自写分隔器或准备缓存扩展本阶段。

准入取证：原会话41891以de252c38归档制品、普通属性原有基准、显式512MB／G1／stackDepth128、1fork／3×1s预热／1×15s测量录制成功并正常退出。target/jfr-list-to-map-optional-before-20261007/及同名log／JSON的4480个worker分配样本中，996个Optional属于GLOBAL.getProperty→listToMap；对象未完全被JIT消除。target/list-to-map-optional-before-jfr-analysis-20261007.log仅用于归因，样本不能换算B／输入事件、CPU百分比或常驻堆。当时实施两处直接原始值读取，补三项旧stream／Optional实现的独立差分测试；这些候选代码最终均已撤回。

功能／制品阶段结果：原会话93681完整离线package成功退出，614项测试、0失败／错误／跳过、58份当次报告；按本次构建起点时间及当时现存测试源码排除旧Benchmarks／WindowedAggregateStageTest报告。日志target/list-to-map-optional-candidate-{full-package,test-summary}-20261007.log。552份共享class中仅CastUtils改变，无新增／删除class，所有基准class一致；target/list-to-map-optional-class-identity-20261007.log。候选归档target/list-to-map-optional-candidate-validated-20261007-benchmarks.jar，SHA256为8649b900d4aff7ecd4ca6c6d40cb47eed57262b5d465f350a1f0a108589de0d6，仅供复现已撤回试验。正式before原会话16716与after原会话55774四项全部完成、各10次测量无failure。本次没有新增API、复杂控制流或异步／资源边界，只是使用已有同步返回值入口，不增Trace span／MBean或无意义逐行注释。

首轮性能门禁未通过：after原会话55774正常退出，四项各10次测量且完整setup oracle通过。普通属性8383.471→6340.962 B／输入事件（−24.36%），嵌套属性24255.499→22212.990（−8.42%），两路吞吐区间重叠；原生分配不变、吞吐区间重叠。然而属性::转换95554±4535→74823±549输入事件/s（−21.70%，区间不重叠）、51621.176→52906.684 B／输入事件（+2.49%）。不能按普通属性子集验收或修改门槛。按相同配置先after再before反向复测转换／普通／原生三项；若回退复现则撤回两处调用替换，不采用位置／类型分支、辅助调用层或JIT参数特调。嵌套场景首轮无稳定回退证据，不新增其重复测量。target/jmh-list-to-map-optional-{before,after}-20261007.{log,json}是正式定量来源；摘要验收断言已正确拒绝候选，后续仅保留完整事实报告，不把断言失败误当构建失败。

反向复测／最终撤回：原会话82427先after再before，三项各10次测量、完整oracle通过、无failure且正常退出。转换91366±695→75250±1168输入事件/s（−17.64%，区间仍不重叠）；其分配54949.179→52906.684 B／输入事件（−3.72%），与首轮分配方向不同，不能择一申领收益。普通属性384273±36651→417457±18717（吞吐区间重叠）、8383.471→6308.961 B／输入事件（−24.75%）；原生650906±43287→696898±21606（区间重叠）、3301.060→3301.059 B／输入事件不变。target/jmh-list-to-map-optional-{after,before}-repeat-20261007.{log,json}及target/list-to-map-optional-ab-{summary,repeat-summary}-20261007.log保留完整事实。回退原因未继续归因，不能无证据声称是某个JIT决定，更不通过辅助调用层调试编译器达到当前夹具分数。

候选后JFR原会话70856正常退出，同配置普通属性场景4393个worker分配样本，按完整栈精确匹配GLOBAL.getProperty方法的分配／CPU样本均为0；包装删除实际发生但不足以抵消转换吞吐回退。target/list-to-map-optional-after-jfr-analysis-20261007.log只用于归因，非精确零分配或常驻堆证明。撤回后原会话47602完整离线-DskipTests package成功，未重跑仍有效的全套测试；552份共享生产／基准class全部与此前de252c38已通过611项完整测试的制品字节一致，无新增／删除。target/list-to-map-optional-restored-class-identity-20261007.log；恢复归档target/list-to-map-optional-restored-20261007-benchmarks.jar（SHA256为39f2de6ae666832c6b6912c1c0bd02e3566db1dc4dfa93e8f7c892452982982d）。仅服务该撤回候选的测试代码也已原位撤回。本阶段无新吞吐／分配／常驻堆收益，不简单叠加历史收益；所有任务已终态，整体目标仍ACTIVE，尚无新的已准入高收益候选。

### 固定类型空白折叠：用既有字符匹配API消除Matcher分配

目标与范围：当前ArrayToRowBenchmark属性::转换仍经过CastFeature.normalizeType的固定Java默认\\s+空白折叠。已有after测量期JFR的normalizeType Matcher／int[]分配仍为热点；当前15cf83bf制品与该已验证d03418eb制品的12份CastFeature／ArrayToRowBenchmark及生成class全部字节一致，可以复用取证而不重复录制相同热点。候选仅把静态Pattern替换为既有Guava CharMatcher对同六种ASCII空白的collapseFrom；不使用包含Unicode空白的whitespace()，不改变trim、Locale.ENGLISH、小括号截取或转换规则，不写分词器／解析器、数据类型分支、输入缓存或新执行框架。

实施与验收：先在当前归档制品录四项同配置GC／JMH基线，再修改CastFeature的固定匹配器／调用两处，扩展原CastFeatureNormalizationTest的全UTF-16分隔字符差分（原Java正则为独立oracle），保留234组合、转换错误、未知类型身份及异步Context证据。阶段末集中完整package／测试；核对仅CastFeature生产class变化且全部基准class不变。用同512MB／G1、2forks、3×1s预热、5×1s测量、1线程量化属性转换及原生／普通／嵌套控制，再录candidate测量期JFR定位Matcher消除。只有实质分配下降（10%以上）且吞吐无稳定回退、控制无稳定退化才保留；CPU区间重叠不宣称稳定吞吐收益。失败则完整撤回匹配器候选，不加特殊分支；默认限制、并发、预取和依赖版本保持。

初筛观察与下一判别：完整611项测试通过，首轮相同四项A/B中属性转换吞吐提升12.99%、分配下降8.58%，控制吞吐区间重叠、分配不变。原“内存主导”10%门槛没有满足，不能按该条件验收，也不把阈值下调到实测值。因原目标同时要求吞吐和堆收益，另外验证“CPU高收益”：相同参数、输入及完整oracle，先after再before反向复测属性转换／原生／普通控制；仅当两个顺序都证明稳定两位数吞吐提升（各自区间不重叠）、分配同降且控制无稳定回退，才按CPU收益保留。复测不成立则撤回，不修改SQL、夹具、预热／测量参数或生产实现以通过门槛。候选目前仍未验收。

验收结果：按后续明确的CPU高收益门禁保留，不宣称满足原10%分配筛选线。CastFeature仅使用一个不可变的现有CharMatcher处理固定六字符集合，collapseFrom替换Matcher.replaceAll；没有手写扫描、类型／SQL分支、缓存、Subscriber或新的API。完整离线package原会话75270正常退出，611项测试、0失败／错误／跳过、58份当前报告；旧Benchmarks／WindowedAggregateStageTest按当前来源／时间戳排除。原234组合、未知类型／null身份、转换失败及异步Context测试均保留，新增全65,536个UTF-16分隔字符的结果／类型／未知类型身份差分通过。日志target/cast-whitespace-candidate-{full-package,test-summary}-20261007.log。

552份共享class中只有CastFeature变化，无新增／删除class、全部基准class不变，target/cast-whitespace-class-identity-20261007.log。前制品15cf83bf与已验证后制品target/cast-whitespace-candidate-validated-20261007-benchmarks.jar（SHA256为de252c3824bdb577be6c7d51c02098c774c852cd90fff1421426d6d8c4452594）使用相同SQL／输入／输出oracle。相同JDK17.0.18、512MB／G1、1线程、2forks、3×1s预热、5×1s测量及GC profiler，首轮原会话19657／57862各四项、反向复测原会话3860各三项，每项均10次测量且无failure：

| 顺序／场景 | before→after输入事件/s | before→after B／输入事件 |
| --- | ---: | ---: |
| 首轮，属性::类型转换 | 83,947±1,160→94,853±2,432（+12.99%） | 56466.872→51621.177（−8.58%） |
| 先after后before复测，同场景 | 82,799±1,542→95,165±2,437（+14.94%） | 56466.872→51621.177（−8.58%） |
| 首轮，原生控制 | 657,530±36,505→639,572±20,281 | 3301.060→3301.060 |
| 首轮，普通属性控制 | 411,682±11,366→409,677±15,333 | 8383.469→8383.470 |
| 首轮，嵌套属性控制 | 152,716±20,885→137,999±2,735 | 24239.494→24239.497 |
| 反向复测，原生控制 | 632,480±29,583→661,716±57,762 | 3301.060→3301.060 |
| 反向复测，普通属性控制 | 367,136±28,939→378,271±27,131 | 8383.471→8383.471 |

两种顺序的转换吞吐区间都不重叠，控制区间均重叠、分配不变；每输入事件约少4845.695B（4.73KiB）瞬时分配。证据target/jmh-cast-whitespace-{before,after}-20261007.{log,json}及对应-repeat文件；汇总target/cast-whitespace-ab-{summary,repeat-summary}-20261007.log。不按控制点估计值上下波动宣称回退／改善，不把两轮或历史Pattern收益简单相加。

后测量期JFR原会话7633成功完成，target/jfr-cast-whitespace-after-20261007/及同名log／JSON，显式512MB／G1／stackDepth128、1fork／3×1s／1×15s。943个worker CPU／4380个分配样本；完整栈按CastFeature.normalizeType匹配CPU37、分配0，原取证该位置有int[]469／IntHashSet[]41样本。当前仍有splitCast／属性路径和其他订阅／输出分配；采样0不是精确零分配证明，定量收益来自上述GC/JMH。分析target/cast-whitespace-after-jfr-analysis-20261007.log。改动仅在属性::与公共castValue重复归一化路径取得本场景收益，已在编译期归一化的SQL CAST不能据此申领逐行收益，更不代表分组常驻堆下降或全SQL接近原生。阶段diff检查通过，制品与测试覆盖的源码未再改变，未提交／推送；整体目标仍ACTIVE。

### 标签集合成员判断：补齐真实函数路径取证

目标与范围：新增CollectionMembershipBenchmark，以16,384个预建遥测事件执行contains_all／contains_any／not_contains，包含2～8个普通标签、可命中／缺失候选、空／多元素候选集合和disabled标签。SQL同时输出序号与设备ID，消费完整结果引用而不收集；直接Java成员判断仅为该输入的计算参考，不冒充完整SQL扩展／错误合同。另用相同五列宽度的属性投影控制输出容器成本。三路setup验证所有行的完整Map／类型／顺序／条数和一次来源订阅。

阶段步骤：先集中构建基准并与42804cce制品比对生产class，复用610项生产测试；录SQL测量期JFR，按所有者区分TreeSet比较状态、参数首值／顺序展开及多余包装。仅在有简单通用且等价的候选时实施，阶段末集中验证及同配置A/B；否则补正式绝对GC／JMH基线后关闭。保持混合类型比较、空／嵌套集合、短路、错误范围、Context、需求及取消；不替换为按元素类型选路的HashSet，不引入缓存、池、自定义Subscriber或集合执行框架。

阶段结果：仅新增基准，无生产改动。离线package（-DskipTests，原会话54825）成功；544份原有共享class全部字节一致，仅新增8份基准／生成class。日志target/collection-membership-{benchmark-package,class-identity}-20261007.log；当前归档制品target/collection-membership-current-20261007-benchmarks.jar，SHA256为15cf83bf68620ffe2cdd8d0567392f7ab780147bcfb63e80e2ee85bd59982ed2。复用610项生产测试，未重跑全套；SQL／直接参考／普通投影三路完整setup oracle全部通过。

测量期JFR原会话54322成功完成，target/jfr-collection-membership-current-20261007/及同名log／JSON，显式512MB／G1／stackDepth128，1fork、3×1s预热、1×15s测量。668个worker CPU／4524个分配样本：TreeMap.Entry468、SpscArrayQueue368、ConcatMapInner356、ConcatMapImmediate261、DefaultIfEmptySubscriber152、SwitchOnFirstMain146、TreeMap124；树／比较相关完整栈匹配135个CPU／761个分配样本。它们分别属于比较集合构建、参数顺序／首值与内层展开，不把采样数量换算CPU百分比、精确B／事件或常驻堆。分析target/collection-membership-jfr-analysis-20261007.log。

通用包装删除门禁：直接把Flux.just(val).as(CastUtils::flatStream)替换为单值展开，虽然省去内层concatMap，实际恢复范围不等价。公开API探针/private/tmp/MembershipFlattenBoundaryAudit.java对两条链使用同一个Iterable装配Hook异常、同一个输入、一次来源订阅与一次onErrorContinue；原生内层恢复后触发defaultIfEmpty(true)，结果true，直接外层恢复后触发defaultIfEmpty(false)，结果false。探针正常退出并校验异常／输入身份，target/collection-membership-flatten-boundary-audit-20261007.log。候选未进入生产，不通过特定输入分支、吞错误或补偿状态修补。TreeSet又承载既有数字／字符串比较合同（现有testContains覆盖2与"3"），不能用普通HashSet或仅字符串路径绕过。

同JDK17.0.18、512MB／G1、1线程、2forks、3×1s预热、5×1s测量与GC profiler的正式绝对基线（原会话81626，三项各10次测量，无failure）：

| 相同输入，引用消费 | 输入事件/s | B／输入事件 |
| --- | ---: | ---: |
| 集合成员判断SQL | 583,151±4,783 | 5637.531 |
| 本String标签输入的直接Java参考 | 15,364,761±1,279,038 | 283.409 |
| 同五列宽度普通属性SQL控制 | 15,602,762±2,427,544 | 288.049 |

证据target/jmh-collection-membership-current-20261007.{log,json}及target/collection-membership-jmh-summary-20261007.log。SQL距直接参考约26.35倍，仍未接近原生；直接参考不具备混合类型／嵌套集合、参数Publisher、Context和逐表达式恢复合同，差距不是可直接删除的冗余或本轮回退。没有确认低复杂度等价的新候选，关闭此取证，不改默认预取／并发／限制，不引入共享比较缓存或执行框架。本轮不申领新吞吐／分配／常驻堆收益；阶段diff检查通过，整体目标仍ACTIVE。

### JSON资源限制读取：验证中间装箱是否为高收益热点

目标与范围：以当前42804cce诊断制品运行既有JsonMultiPathBenchmark的Map／文本输入、8条路径，用官方测量期JFR确认JsonFunctionSupport.intSetting／jsonLimits的配置读取是否贡献实质CPU或分配。SQL、输入、完整输出oracle及512MB／G1配置不变，不新造定向数据。仅当热点成立，才评估把同一公开getSetting的Optional映射链改成普通值读取／原转换；仍逐调用读取配置，不缓存、不特判来源／类型、不改默认限制、错误优先级、配置扩展或函数订阅。

实施门禁：先JFR再决定；没有显著配置读取热点则关闭，不为微小成本增加设计。有候选才补动态配置／转换错误身份／限制边界测试，在阶段末集中验证并用同配置成对JMH／GC及文本控制验收。当前仅取证，无生产改动，不申领新的吞吐或堆收益。

取证结果与关闭边界：JMH原会话23838正常退出，Map／文本两项均完成完整setup oracle和一次15秒测量，录制位于target/jfr-json-limit-read-current-20261007/及同名log／JSON；日志确认显式512MB／G1／stackDepth128且无failure。Map为903个worker CPU／4495个分配样本，文本为899／4417；完整采样栈按JsonFunctionSupport.intSetting／jsonLimits匹配，两项CPU／分配均为0。这不是证明配置读取成本精确为零，而是不支持本阶段高收益准入，因此不改Optional读取链，也不构造有setting的定向数据来制造收益。短诊断548,482／414,132输入事件/s不是正式A/B，不申领性能改善。分析target/json-limit-read-jfr-analysis-20261007.log。

相邻主热点已定位：Map路径Utils.concat的54个CPU叶样本，经PathToken.handleObjectProperty／PropertyPathToken.evaluate进入，调用方是jsonExtract与jsonContainsPath的staticPath.read；文本还包含JSON解析及结构规范化。当前已经用预编译静态JsonPath和公共read接口，未重复编译路径。对当前json-path2.10.0的Configuration／Option／ReadContext公开API核对，未发现保持当前值／缺失／异常语义又能关闭这部分路径构造的简单读取选项；不能用改返回模式、抑制异常、内部类／复制源码或手写路径求值器绕过。证据target/json-limit-read-jfr-path-cpu-20261007.log及target/json-path-public-read-api-audit-20261007.log。本轮仅补取证、关闭未证实的配置装箱候选，生产／基准代码及42804cce制品未改，复用610项生产测试、不重复构建。阶段diff检查通过，整体性能目标仍ACTIVE。

### 当前原生AVG／MAX开放窗口：常驻堆与历史输入判别

目标与范围：不引用已撤回融合的旧堆数据，检查当前原生AVG／MAX（COUNT控制）是否保留额外历史输入。仅扩展HighCardinalityLiveHeapProbe，补avg／max模式及显式--payloads，使1／8值每键的输入都携带相同256B、未被选入输出的独立payload；按输入轮次统计WeakReference存活，区分每键代表行／极值本身与历史。输入保持惰性且遵守需求，订阅者不收集输出。

验证步骤：集中构建探针、核对生产class与390d5143制品一致，复用610项功能证据。先在10,000键、1／8值每键的三种聚合核对开放窗口所有输入已消费、零结果、一次来源订阅／取消及post-GC活跃／取消堆；用同进程open减cancel基线降低WeakReference跟踪器随总行数增长的干扰。关闭窗口按request(1)校验首行完整值／类型并取消剩余队列，只有前述门禁通过才扩大至50,000键。若额外历史payload或取消后持有被确认，再用JFR／所有者证据定位，才实施通用最小修正；必要键／代表行／原生订阅状态或队列输出不算泄漏。不改聚合生命周期、输出所有权、默认限制或Reactor配置，不恢复融合、增加来源类型特路、缓存或状态机。

阶段结果：仅扩展诊断探针，无生产改动。离线package（-DskipTests，原会话40018）成功；544份共享class中仅HighCardinalityLiveHeapProbe及Arguments／HoldingSubscriber变化，无新增／删除class，生产class全部字节一致。复用610项生产回归，未重跑全套。证据target/native-avg-max-heap-probe-{package,class-identity}-20261007.log；诊断制品target/native-avg-max-heap-probe-20261007-benchmarks.jar，SHA256为42804cce458ed179d93982dc79826c8aeafee81ef2185dc05fffa0f9bf52aec7。

同JDK17.0.18、512MB／G1，COUNT／AVG／MAX × 10,000／50,000键 × 1／8值每键 × 开放／关闭，共24个独立进程、48次post-GC阶段检查全部通过。每项来源一次订阅、所有输入已接收；开放窗口零结果且来源取消一次；关闭窗口request(1)恰好一行，完整Map／值／类型校验通过。开放窗口仅最后一轮每键一份payload存活，前七轮为零；关闭并消费一行后仅最后一轮keys−1份存活；24项取消后所有轮payload均为零。日志target/native-heap-{count,avg,max}-{10000,50000}-{1,8}-{open,closed}-20261007.log，汇总target/native-avg-max-heap-summary-20261007.log。

活跃减取消的同进程post-GC堆差（MiB，包含键、代表行、原生订阅／队列等，非聚合器单独大小）：

| 聚合 | 10,000键开放，1→8值／键 | 50,000键开放，1→8值／键 | 50,000键关闭等待需求，1→8值／键 |
| --- | ---: | ---: | ---: |
| COUNT | 25.158→26.204 | 126.810→129.918 | 182.216→182.161 |
| AVG | 25.090→26.139 | 126.346→129.536 | 183.184→183.011 |
| MAX | 24.788→25.860 | 124.790→128.010 | 180.580→180.404 |

结论：本输入下状态规模主要随活动键数增长，而非随每键历史行数增长；1→8值的开放堆差约增加2.5%～4.3%，不是八倍。小幅差异未做所有者归因，单次GC堆差不作精确性能／泄漏证明，WeakReference也只覆盖被跟踪的payload。原生AVG／MAX逐条增量归约不保存历史行，精确开放分组仍需O(活动键数)状态；不能据此承诺固定常驻堆。关闭窗口待需求的合法队列保留不算泄漏。本阶段未确认额外历史payload或取消泄漏，因此不增加聚合融合、状态框架或新的生产分支；无新增吞吐／堆下降申领，既有高收益通用优化保留。阶段diff检查通过，整体接近原生目标仍未完成。

### 16／64／128列混合投影：宽度增长成本取证

目标与范围：现有真实宽查询主要16列，补LargeProjectionBenchmark的16／64／128列精确宽度，包含sequence／deviceId、独立测量字段、校准算术、round及coalesce。预建16,384个遥测事件，覆盖缺失与空字符串标签；每列正常计算参考独立读取／计算，不共享跨列结果，也不预计算结果。普通属性投影为同宽度容器／读取控制；setup校验全部输出、类型、顺序、条数及各一次来源订阅。

阶段步骤：只构建基准并核对生产class与e82535bc制品一致，复用610项生产回归；先录64列官方测量期JFR，再以同JDK17.0.18、512MB／G1、1线程、2forks、3×1s预热、5×1s测量和GC profiler量化三种宽度。若出现非线性成本，再按具体所有者补128列JFR；只有具有普遍性、功能等价且低复杂度的热点才实施。无可靠候选则停在证据，不为列数／来源类型设置快路，不调容器容量、预取／并发或默认限制，不加缓存、池化、Subscriber或错误补偿框架。

取证完成：仅新增LargeProjectionBenchmark，无生产变更。离线package（-DskipTests）成功，日志target/large-projection-benchmark-package-20261007.log；534份原有生产／基准class全部字节一致，只新增10份本基准／辅助／生成class（target/large-projection-class-identity-20261007.log）。当前／归档制品target/large-projection-current-20261007-benchmarks.jar的SHA256为390d51431fac32a1afaf9c536d59360850ae170477eb6790d02faf444fbcb98d。复用同字节码610项生产回归，不宣称重跑全套；三种宽度的SQL／控制／直接链全部通过setup的完整值／类型／顺序／条数及一次来源订阅验证，未降低断言或预计算输出。

64列官方测量期JFR为target/jfr-large-projection-64-current-20261007/及同名log／JSON，1fork、3×1s预热、1×15s测量、stackDepth128、显式512MB／G1；有效录制、正常退出、无failure。883个worker CPU／4414个分配样本；CPU叶子为MonoDefer.subscribe127、Mono.subscribe70、concatMap排空61、zip coordinator初始化55／signal44。主要分配包含二元计算的PairwiseZipper630／Publisher[]290、division装箱Double412、参数FluxIterable322、ConcatMapInner290、MonoJust289、FlatMapMain263、结果Map桶262、ZipInner262及CollectList260。采样不换算CPU百分比／B/事件／常驻堆。此前二元zip改数组重载的wrapper.zipWith第三来源／错误／discard反例仍成立，不因PairwiseZipper采样较多重开；也不冻结Publisher或移出原计算错误范围。

正式GC／JMH（既定JDK17.0.18、512MB／G1、1线程、2forks、3×1s预热、5×1s测量）九项均有10次测量、正常退出且无failure；target/jmh-large-projection-current-20261007.{log,json}。吞吐与分配按输入事件计，不按列数放大吞吐。

| 输出列数 | 混合SQL 输入事件/s；B/事件 | 同计算直接函数链 输入事件/s；B/事件 | 普通属性SQL控制 输入事件/s；B/事件 |
| --- | ---: | ---: | ---: |
| 16 | 721,006±34,514；7720.051 | 8,925,573±252,131；872.010 | 7,840,673±150,067；736.033 |
| 64 | 165,941±4,444；33256.076 | 1,775,416±57,798；3368.013 | 1,366,532±48,014；2656.037 |
| 128 | 79,264±479；67304.115 | 858,142±22,558；6696.017 | 630,652±28,620；5216.042 |

结论：混合SQL平均每列分配约482.5／519.6／525.8 B，每列处理时间约86.7／94.2／98.6 ns；16列有2个元数据字段、完整计算组比例略低，不能把比例变化算作新回退。64→128列分配约2.024倍、耗时约2.093倍，普通属性控制及直接链同样随列数增加，没有发现二次方增长或足以单独取证的非线性放大，因此不为重复验证相同owner追加128列JFR。64／128列SQL距直接链约10.70／10.83倍；直接链不具备所有SQL扩展、需求／取消和逐表达式错误合同，不将差距全部视为可安全删除的冗余。

本切片关闭于证据，无可确认的低复杂度高收益新生产候选，不为宽查询调容量、加资格标记、缓存、预取补偿或自定义框架。不申领新的吞吐、分配或常驻堆改善；既有通用收益保留。AVG／MAX常驻状态与行历史尚应以当前原生制品核对，不能引用已撤回聚合融合的旧堆数据。阶段git diff --check通过，仅回填文档／检查点，无需再构建、未提交／推送，总体目标仍ACTIVE。

### 多参数字符串拼装与集合参数展开：覆盖缺口取证

目标与范围：补常见设备标签／路径拼装查询，包含concat、concat_ws、upper／lower、coalesce及集合参数展开。新增StringAssemblyBenchmark，预建16,384个遥测事件，含空可选值和空／多元素tags；setup逐行核对完整Map、字段类型、顺序、行数及一次来源订阅。直接函数链只作正常数据成本参考，另有同输入普通投影控制；不预计算输出或共享跨列函数结果。

步骤：先仅构建基准、核对全部生产class与当前d03418eb制品一致，复用610项功能证据；以JDK17.0.18、512MB／G1、1线程录官方测量期JFR，并记录同配置正式GC基线。只有出现可独立删除的通用重复计算且保持现有错误／Context／需求／取消／多值与扩展工厂边界时，才实施最小生产候选。否则关闭切片，不删除必要的字符串输出、参数顺序订阅或集合展开状态。不改默认限制、并发／预取、依赖、函数值／类型或StringBuilder容量，不引入类型／SQL分支、缓存、池化、Subscriber或执行框架。

取证结果：仅新增基准，无生产改动。离线package（-DskipTests）成功，日志target/string-assembly-benchmark-package-20261007.log；524份原有生产／基准class全部字节一致，仅新增10份StringAssemblyBenchmark及其辅助／生成class，详见target/string-assembly-class-identity-20261007.log。新制品target/string-assembly-current-20261007-benchmarks.jar的SHA256为e82535bc8056c4d5af676ce9658ead03a0c64d8c26173a631416c2f51d514d49。复用同字节码的610项生产测试证据，不宣称重跑完整测试。JMH setup完整行／类型／顺序／条数及每路一次来源订阅全部通过。

官方测量期JFR位于target/jfr-string-assembly-current-20261007/及同名log／JSON，1fork、3×1s预热、1×15s测量、stackDepth128，显式512MB／G1；有效录制、正常退出且无failure。728个worker CPU／4355个分配样本，CPU叶子包含原生无预取concatMap.onNext73、MonoDefer.subscribe34、CAS27及concatMap排空26；分配包含WeakScalarSubscription321、ConcatMapInner319、ScalarValueMapper的MonoJust212、IterableSubscription205、flatStream的FluxJust197、ConcatMapImmediate144及结果Map节点128。字符串生成也有byte[]，其中BoundedStringJoiner.add139／toString83；不存在可据此删除的逐行Pattern重复编译。采样仅定位，不换算CPU百分比、精确B/事件或常驻堆。

同配置正式GC／JMH（2forks、3×1s预热、5×1s测量、1线程）三项均有10次测量且无failure，target/jmh-string-assembly-current-20261007.{log,json}：

| 相同输入，引用消费 | 输入事件/s | B/输入事件 |
| --- | ---: | ---: |
| 字符串拼装SQL | 689,872±7,839 | 4248.384 |
| 正常输入直接函数链参考 | 6,151,282±47,903 | 788.653 |
| 普通两列投影控制 | 38,418,015±240,833 | 176.031 |

结论与关闭边界：当前SQL距直接函数链约8.9倍，这不是本轮新增收益；直接参考不具备SQL扩展、逐表达式恢复、Context及参数需求／取消的全部合同，差距不能全部视作可删除冗余。参数concatMap与集合flatStream按顺序处理普通值、Iterable／数组与Publisher；标量参数直接handle合并已在“显式同步参数的原生流适配优化”通过真实原生oracle证明零需求／分次请求求值时序不等价，当前新JFR不构成重开依据。没有确认可保持合同且低复杂度的高收益新候选，关闭这次取证，不添加状态机／预取补偿或自定义执行框架。既有固定正则与mapNotNull收益保留；本切片不申领吞吐／分配／常驻堆下降。阶段diff检查通过，后续文档回填不改制品、无需再构建，未提交／推送；整体目标保持ACTIVE。

### 集合转Map与属性类型转换：优先删除固定正则重复编译

目标与范围：检查CastUtils.listToMap对同一keyField／valueField逐元素重复调用DefaultPropertyFeature.getProperty的CPU和分配成本。先新增4096个预建事件、每事件64个属性的array_to_row基准，含嵌套路径、普通路径控制和相同输出／类型的正常数据直接计算参考；setup核对全部行、空字段处理及一次输入订阅，吞吐以输入事件计。仅构建基准并核对生产class不变，复用605项功能门禁；随后录制嵌套路径的官方测量期JFR和正式GC基线。

批内提前准备候选已关闭：公开API探针证明空集合原本不读取非法字段并返回空Map，提前prepare却抛错；多层引号的普通／cast分支还会重复清理名字，导致动态读取与准备读取不同。证据为target/batch-property-preparation-equivalence-audit-20261007.log。不新增惰性缓存、状态容器或异常补偿。仅修正共享preparePropertyValue回退将原始名字传给原虚方法，清理一次；用多层引号／cast／空源差分与扩展调用回归验收，不将正确性修正计为性能收益，也不改listToMap算法。

新增同输入属性::int场景后，正式before制品target/array-field-cast-before-20261007-benchmarks.jar的SHA256为059b546733b1cb0d1d26a20b452cbfe7f410feded634dba7625d0084dd477230，195个生产class与前一已接受制品一致。有效JFR位于target/jfr-array-field-cast-before-20261007/及同名log／JSON；cast场景1111个worker CPU／4269个分配样本，CastFeature.normalizeType所属int[]1330、Pattern640、Pattern$TreeInfo232；splitCast所属int[]767。采样仅定位，不换算CPU百分比、B／事件或常驻堆。正式before四项均为2forks／10测量，日志无failure：cast约76,217输入事件/s、88,650.773 B/事件；普通嵌套路径约148,496／24,255.495；普通字段控制约377,344／8,383.471；正常输入直接计算约678,426／3,301.060。配置为JDK17.0.18、512MB／G1、1线程、3×1s预热、5×1s测量与GC profiler；证据target/jmh-array-field-cast-before-20261007.{log,json}。

最小通用实施范围：CastFeature.normalizeType只复用不可变静态Pattern.compile("\\s+")，每次调用仍创建独立Matcher；不缓存输入／类型／mapper，不手写空白解析器或splitter。保留trim→Locale.ENGLISH→括号裁剪→默认Java正则空白压缩的顺序，转换／错误时机及原生响应式边界不变。补公共castValue规范化差分、ASCII／Unicode空白、未知类型值／类型／身份、转换错误及已有异步Context回归；阶段末完整package，同基准class／配置A/B及after JFR。若负对照波动，反向顺序复测；无明确收益即撤回，不改默认配置、依赖或测试门禁。

验收结果：保留固定Pattern复用及共享属性名一次清理修正，未改listToMap。CastFeatureNormalizationTest补充234个类型／空白／参数后缀差分组合，以及默认ASCII正则语义、Unicode空白保留、未知类型身份和转换异常；原异步Context及重订阅测试继续通过。DefaultPropertyFeatureTest补多层引号／cast／空源的动态读取差分及原虚方法调用回归。完整离线package通过610项测试，0失败／错误／跳过，58份当前报告；排除旧WindowedAggregateStageTest及Benchmarks报告，不降低DebugAgent或license门禁。日志target/array-field-cast-candidate-full-package-20261007.log。候选／最终制品target/array-field-cast-candidate-validated-20261007-benchmarks.jar的SHA256为d03418ebad1cd12ff7fb33837e2524bea7dead804faa0a26ba1f70e559d95e66；524份共享生产／基准class仅CastFeature与DefaultPropertyFeature改变，无新增／删除class，所有基准class一致（target/array-field-cast-class-identity-20261007.log）。

正式A/B与before配置、SQL、输入、完整输出／类型oracle和消费端均一致，四项各有2forks／10测量，无failure。单位为输入事件，不按64个属性放大吞吐。

| 首轮场景 | before → after 输入事件/s | before → after B/输入事件 |
| --- | ---: | ---: |
| 嵌套属性::int集合转换 | 76,217±2,006 → 82,784±1,914（+8.6%） | 88,650.773 → 56,466.873（−36.3%） |
| 普通嵌套路径控制 | 148,496±18,198 → 160,051±4,593 | 24,255.495 → 24,255.492 |
| 普通字段控制 | 377,344±21,672 → 370,078±24,592 | 8,383.471 → 8,383.471 |
| 正常输入直接计算参考 | 678,426±27,356 → 668,965±42,208 | 3,301.060 → 3,301.060 |

为确认较小CPU收益，追加相同配置的反向顺序复测（先after后before），只测目标及普通字段／直接计算负对照，不调输入或配置。目标before75,422±810／after81,647±4,441输入事件/s（+8.25%），88,650.774／56,466.873 B/事件（−36.30%）；两种顺序目标吞吐区间均不重叠。普通字段before399,928±31,174／after405,103±11,392，直接计算before679,362±18,227／after668,511±43,709，区间均重叠；普通字段after复测8,367.470 B/事件有16 B的fork／JIT差异，不计为本轮生产收益。负对照没有稳定退化，普通嵌套路径的首轮吞吐变化也不申领。证据为target/jmh-array-field-cast-{before,after}-20261007.{log,json}（各4项）与target/jmh-array-field-cast-{after,before}-repeat-20261007.{log,json}（各3项），全部正常退出且结果完整。

after官方测量期JFR位于target/jfr-array-field-cast-after-20261007/及同名log／JSON，有效录制、无failure且正常退出；含885个worker CPU／4465个分配样本。normalizeType所属主要分配变为Matcher的int[]469及IntHashSet[]41，重复编译的Pattern与编译内部节点不再出现在主要摘要；splitCast所属int[]761、splitDot所属byte[]827／String710仍存在。源代码直接复用固定Pattern，不复用可变Matcher。不把top-N缺席解释为精确零样本，也不把采样数换算CPU百分比／B/事件。JFR前后仅显式设置stackDepth128，使用JDK默认堆／G1（before的GCHeapConfiguration为初始1GB／最大16GB），用于定位而非正式分数；正式GC A/B两种顺序都显式512MB／G1。after JFR吞吐不参与收益申领。

收益边界：这项通用改动降低公共castValue及属性::转换的重复正则编译成本；已在计划阶段规范化的普通SQL CAST不申领逐行收益。每事件减少约32.2KB瞬时分配，不是高基数分组常驻堆下降；共享引号修正只算正确性。AVG／MIN／MAX、活动分组状态、默认限额／并发／预取及依赖均未改。阶段末仅文档／检查点回填，复用610项功能证据，不重复构建；未提交／推送，总体性能目标仍ACTIVE。剩余动态路径／Matcher成本不作为新缓存、手写解析器或执行框架的理由。

### 集合值函数的同步逐值转换取证

目标与范围：补齐row_to_array／rows_to_array的集合转换场景，只检查DefaultReactorQLMetadata注册的同步tryGetFirstValueOptional经concatMap(Mono.justOrEmpty)生成逐值Publisher的成本。先新增预建遥测事件和每事件24个单字段读数的JMH，覆盖rows_to_array、多个参数的row_to_array、直接计算参考及array_len控制。setup核对全部输出、值／类型、空首值处理和一次输入源订阅；吞吐与分配以输入事件计，不以读数数量放大。

步骤：先保留当前已验证制品，只构建基准并逐类核对生产class不变，复用599项回归；录制新场景的官方测量期JFR并量化GC基线。只有逐值Publisher确为主要通用开销时，才评估原生mapNotNull或等价同步转换，且先用公开API核对空值、顺序、输入错误、onErrorContinue／onOperatorError／discard、Context、需求和取消。不得为通过反例增加特定函数／输入分支、吞错误、能力标记或Subscriber；若边界不等价则关闭候选。只有语义成立、正式A/B有实质收益且负对照无稳定退化，才保留生产修改并集中完整验证。当前不改flatten、集合结果类型、参数上限、默认并发／预取或依赖，也不申领常驻堆下降。

有效before JFR为target/jfr-collection-values-before-20261007/及同名log，JDK17.0.18、512MB／G1、1线程、1fork、3×1s预热、1×15s测量、stackDepth128；1058个worker CPU／4459个分配样本。由rows_to_array逐值转换直接生成MonoJust831、tryGetFirstValueOptional生成Optional657；原生concatMap内部及标量订阅状态也有采样，CPU叶子含原子状态迁移448。采样用于定位，不视作CPU百分比或B／输入行。公开API探针证明同步转换的结果、空值drop、discard、恢复输入／异常身份和onOperatorError数据一致（target/first-value-operator-equivalence-audit-20261007.log），仍需SQL边界回归及正式A/B。初次夹具错误地将array_len期望为Integer，实际原生count返回Long；原API确认后修正完整类型oracle，保留失败日志target/jfr-collection-values-invalid-oracle-20261007.log，不把空JSON及无效录制当作成功结果。

最小生产候选：仅将row_to_array和rows_to_array两个相同的同步concatMap(Mono.justOrEmpty(tryGetFirstValueOptional))改为mapNotNull(CastUtils::tryGetFirstValue)。空首值继续不发射，Map／Iterable／普通值仍调用原计算内核；不对元素类型或来源作新分支，不改变CastUtils.flatStream。先补真实原函数Feature覆盖作为独立旧实现oracle，集中验证输出／多值、冷源Context、活动取消和原生错误范围，只有满足前述门禁才保留。

功能阶段完成：FirstValueCollectionFunctionTest的6项回归同时覆盖真正旧FunctionMapFeature和候选实现，验证空首值、空Map／嵌套Iterable／Object[]身份与顺序、多参数输出、冷Publisher多值／重订阅／Context、零下游需求及活动源取消、转换错误的恢复输入／异常身份、失败恢复和onOperatorError数据。旧SQL探针确认恢复回调抛错后会再次进入外层恢复范围，后一次输入为null（target/collection-recovery-scope-equivalence-audit-20261007.log）；测试按此真实边界断言，未增加生产错误补偿。初次阶段日志target/collection-first-value-initial-stage-failure-20261007.log包含新测试对恢复范围的错误预期及DebugAgent沙箱初始化失败；修正前者并在允许标准JVM自附加的环境保留代理重跑，最终完整离线package通过605项测试，0失败／错误／跳过，58份当前报告。证据为target/collection-first-value-candidate-full-package-20261007.log，不把初次失败算作通过。

before基准制品SHA256为08d24bb60452e09164b683440aed8e51c77fd3ed21bdaaa1dfd1af0e886c361c；候选制品SHA256为337512efd3d79d0a8e892f9e9e0c1ecb721378ea0339f95e905ff083cc225af2。513份共享生产／基准class仅DefaultReactorQLMetadata改变，无新增／删除class，所有基准class一致（target/collection-values-class-identity-20261007.log）。原属性拆分优化的已验证d6e27dee制品保留于target/property-first-dot-validated-20261007-benchmarks.jar。

正式A/B均为JDK17.0.18、512MB／G1、1线程、2forks、3×1s预热、5×1s测量和GC profiler；每项均有2forks／10次测量，无failure。

| 场景 | 首轮before → after 输入事件/s | before → after B/输入事件 |
| --- | ---: | ---: |
| rows_to_array，24个读数／事件 | 785,752±21,868 → 1,865,681±54,270 | 2227.478 → 1372.046（−38.4%） |
| 多参数row_to_array | 3.395±0.263 M → 3.894±0.281 M | 1187.472 → 1016.044（−14.4%） |
| array_len SQL控制 | 3.271±0.268 M → 3.695±0.147 M | 840.044 → 828.044 |
| 正常数据直接计算参考 | 5.922±0.159 M → 5.358±0.130 M | 608.00979 → 608.00989 |

首轮直接计算对照也下降约9.5%，因此追加相同配置的反向顺序复测（先after后before），不修改预热／输入或调参。复测rows_to_array为before790,149±26,906／after1,941,454±49,856输入事件/s，分配2227.478／1364.046 B/输入事件；两个顺序均得到约2.4倍SQL吞吐及38.4%～38.8%的每事件分配下降。row_to_array首轮吞吐区间仍重叠，不宣称其CPU收益稳定，只保留明确的171.428 B/事件分配下降。直接计算复测before5.712±0.159／after5.480±0.102 M输入事件/s，差约−4.1%、区间重叠；array_len控制before3.160±0.141／after3.639±0.151 M，840.044／816.044 B/事件。控制路径不是新生产收益：array_len首轮两个after fork分别840.044／816.044 B/事件，复测两个均816.044，存在JIT／fork敏感性；不能把控制分配或吞吐变化归因于删除集合转换。没有观察到SQL控制回退，不用直接计算分数缩放目标SQL或外推全场景提速。

正式证据为target/jmh-collection-values-{before,after}-20261007.{log,json}（各4项）及target/jmh-collection-values-{after,before}-repeat-20261007.{log,json}（各3项）。after JFR为target/jfr-collection-values-after-20261007/及同名log，有效录制、正常退出；1080个worker CPU／4422个分配样本。逐值转换的MonoJust／Optional不再出现于主要分配项，剩余主要为集合输出Object[]、List和原生订阅状态；不把top-N缺席或CPU叶子重定位作为精确零采样／CPU百分比。保留两个通用mapNotNull替换，不新增任何生产class／缓存／分支或参数策略。

结论边界：这是集合值函数的吞吐与瞬时分配改进，不是高基数分组常驻堆下降。集合输出本身仍需要保留实际结果值；AVG／MIN／MAX和原生分组实现未改。后续文档回填未改变已验证代码，复用605项完整回归，不重复构建。阶段末git diff --check通过，最终制品身份仍337512ef…；未提交／推送，总体接近原生的目标仍未完成。

### 相关 VALUES 多行表达式的编译成本取证

目标与范围：补齐FROM／多行展开操作符的覆盖，只检查FromValuesFeature.MapperBuilder.visit(ExpressionList)的订阅期createMapperNow。该方法将AST编译放在Flux.map中；相关VALUES子查询会按外层输入重复执行。先新增两行VALUES、每行含算术／取模／大小写函数的相关CROSS JOIN基准，以及相同输入／完整输出／类型的正常数据直接计算参考。预建事件在测量前生成，完整行及一次输入源订阅oracle在setup验证；吞吐以外层输入行计，不混为展开输出行。

实施边界：先集中构建基准并确认生产class不变，沿用既有功能证据；对SQL运行官方测量期JFR，正式GC／JMH量化绝对成本，再决定是否存在通用且低复杂度的高收益候选。暂不提前编译、缓存mapper或跳过原生订阅；编译阶段、扩展工厂、冷求值、多值／异步参数、错误／取消及Context均属于后续等价验收边界。无可靠收益或边界反例即关闭该候选，不引入SQL形态特路或恢复框架。

功能门禁先发现既有扇出缺陷：初次JMH预热的完整结果oracle失败，无有效JFR或吞吐结果。独立三行输入探针确认每个输入的两个派生JOIN输出共享左Record及结果Map，收集后均变成第二个VALUES行；HEAD原实现同样在每个右结果上调用record.addRecord，非本轮基准或编译优化造成。相邻flat_array也重复在同一个Record上setResult，是同一种零到多输出所有权错误。不能把基准改成只读即时字段或弱化完整行断言以绕过它。

必要修正计划：仅在两个真实扇出生产边界分别用已有ReactorQLRecord.copy()创建每个输出的独立浅快照，再绑定右别名或展开值；不深复制输入值，不给普通一对一投影增加快照，不改变原生flatMap／map、默认并发或订阅范围。覆盖完整结果收集与修改隔离、多列数组展开、派生LEFT JOIN无匹配不泄漏被过滤候选、Context／需求／活动源取消／错误身份。阶段末集中完整测试和原SQL基准oracle；此修正是性能验收的功能前置，不申领吞吐或堆收益，也不以性能目标省去必要的结果所有权。通过后才重新录制有效JFR并判断VALUES编译成本。

修正阶段完整package通过596项测试，0失败／错误／跳过，56份当前报告；补充调用方预置结果／别名隔离后RowExpansionIsolationTest的6项定向package通过，合计覆盖597项而非宣称新运行597项完整套件。最终功能修正版制品SHA256为62174fca202d1bae8a1a48d30facd8dde6370450c8161000888ff6143b26c6b9。独立原SQL六输出值／类型和一次输入订阅恢复正确；日志为target/row-expansion-isolation-full-package-20261007.log、target/row-expansion-caller-record-stage-package-20261007.log及target/correlated-values-oracle-after-isolation-20261007.log。相对原数值试验前493份生产／基准class，只有DefaultReactorQL及其3个行号变动内类、ArrayValueFlatMapFeature改变；新增基准不改原有SQL或oracle。

有效测量期JFR（target/jfr-correlated-values-isolated-current-20261007/，JDK17.0.18、512MB／G1、1线程、1fork、3×1s预热、1×15s测量、stackDepth128）含889个worker CPU／4370个分配样本。PropertyMapFeature.createMapper所属固定分段正则有int[]446、boolean[]291、Pattern191及两类Pattern内部节点各158个分配样本；CPU叶子包含Pattern.compile34、Matcher构造23及Column.getName49。采样只定位，不换算CPU百分比或字节。VALUES工厂不能直接提前：公开Feature探针证明build后工厂调用0，订阅期工厂失败可由onErrorContinue恢复到[{}, {value=7}]，每次重订阅再调用工厂；提前编译会移出这个原生错误范围（target/values-factory-assembly-scope-audit-20261007.log）。不添加mapper缓存／异常补偿来规避。

已定位的最小通用候选：只把PropertyMapFeature.createMapper的固定split("[.]",2)换成indexOf('.')及前后substring，保持同一完整列名、先清理name后清理tableName的调用顺序、引号／空段／嵌套后缀、扩展Feature及工厂错误时机。不新增Pattern缓存、来源／SQL分支、能力标记或操作符。先以正确输出的同一SQL、直接计算参考和既有混合宽投影负对照记录2forks／GC基线；随后补与原split算法的差分回归，集中完整验证，同配置A/B及after JFR，只有稳定吞吐／分配收益且无功能回退时保留。不把之前错误的共享结果当作性能基线，也不申领常驻堆下降。

验收结果：保留固定分隔符的indexOf／substring替换。PropertyMapFeatureNameSplitTest以17个显式名字和500个生成名字与原split算法差分，覆盖引号、Unicode、空段、完整嵌套后缀、实际table／property查询及错误；同时验证扩展工厂查找没有提前到mapper构建之外。公开VALUES工厂探针再次确认build后调用0、每次订阅调用1，原onErrorContinue恢复范围及错误身份不变。最终完整离线package通过599项测试，0失败／错误／跳过，57份当前报告（排除旧Benchmarks报告）；日志为target/property-first-dot-candidate-full-package-20261007.log。最终制品SHA256为d6e27deec9419562fc4c4ef4e4a5706029290e88a80ccfef3df82ccdff28e333。测试、默认配置及依赖门禁未弱化；后续只更新文档，不重复构建。

正式A/B均使用JDK17.0.18、512MB／G1、1线程、2forks、3×1s预热、5×1s测量和GC profiler。before制品已经包含扇出所有权修正，SHA256为62174fca202d1bae8a1a48d30facd8dde6370450c8161000888ff6143b26c6b9；502份共享生产／基准class只有PropertyMapFeature及其两个内类改变，基准class不变。所有结果以外层输入事件计，两路VALUES输出不翻倍计算吞吐。

| 相同输入与消费端 | before → after 输入行/s | before → after B/输入行 |
| --- | ---: | ---: |
| 相关VALUES SQL | 340,318±3,353 → 395,836±4,353（+16.3%） | 20,984.057 → 14,552.055（−30.7%，减少6,432 B） |
| 正常数据直接计算参考 | 16.729±0.134 M → 16.633±0.100 M | 648.00476 → 648.00477 |
| 混合操作符宽列投影负对照 | 2.480±0.087 M → 2.471±0.039 M | 1022.23612 → 1022.23613 |

证据为target/jmh-property-first-dot-{before,after}-20261007.{log,json}，各有三项完整结果，无failure。两个负对照误差区间重叠，分配不变；不申领它们的收益，也不外推为所有SQL加速。必要copy修正不计入优化收益；该收益是每输入行瞬时分配下降，不是高基数分组常驻堆下降，SQL仍远低于直接计算参考。

after测量期JFR位于target/jfr-property-first-dot-after-20261007/及同名log，录制有效且任务正常退出；含872个worker CPU／4385个分配样本。主要分配为byte[]784、String539、FlatMapInner379、HashMap293、节点238、Record206、IterableSubscription199、String[]174及ValueMapFeature$1共171。固定分段正则的Pattern编译／内部节点不再出现在主要热点摘要中；不把top-N缺席解释为精确零采样。CPU叶子包含CharacterDataLatin1.getProperties48、原生flatMap排空39、String.equals33和Arrays.copyOfRange33；进一步栈归因显示大小写成本来自现有metadata Feature ID查找，部分字符串复制来自AST列名与属性路径准备。不能据小helper采样直接调分支或宣称可消除CPU比例；当前不追加缓存、池化、预编译或订阅框架。

扇出修正的通用预防原则：零到多输出可以共享输入值，但不能共享会继续写入的输出别名／结果容器；onNext返回后，下游仍可能收集、延迟或再次展开该行。仅在真实扇出边界使用已有浅copy，完整结果及跨输出修改隔离回归用于防止即时读取基准掩盖错误；普通一对一路径不因此增加快照。阶段末git diff --check通过，制品身份未变，未提交／推送。

剩余编译分配的有界复核：同一after录制中，String[]经DefaultPropertyFeature.preparePropertyValue有96个样本，visitor经ValueMapFeature.createMapperByExpression有171个；visitor所属字符串构建也包含Column.toString用于现有true／false识别。它们属于订阅期通用AST／属性路径处理，不能以工厂调用可提前或布尔列名可忽略为前提删除。未建立新的低复杂度高收益候选，本阶段不继续强行重写visitor或属性路径系统；采样不构成新收益申领。

### 原生精确去重计数：当前基线与高收益筛选

当前adfb4935…制品的COUNT DISTINCT／UNIQUE原生参数订阅已重新取证。沿用UniqueAggregateBenchmark原SQL、65,536个预建Integer输入和完整oracle：255个键各重复256次，另256个单例，DISTINCT=511、UNIQUE=256。测量阶段JFR（JDK17.0.18、512MB／G1、1线程、1fork、3×1s预热、1×15s测量、stackDepth128）分别记录1011／1001个worker CPU样本及4398／4445个分配样本；主要分配是Record（2340／2276）和MonoJust（2018／2125），集合节点仅28／32、桶数组10／11。UNIQUE的最终频次扫描不是主要CPU叶子；addUnique已经将频次饱和为1L／2L，不重复提出该优化。精确键状态在此重复值分布下较小，不外推到全唯一或高基数输入。

正式GC／JMH为同配置2forks、3×1s预热、5×1s测量：DISTINCT 84.641±1.509 M输入行/s、48.406706 B/输入行；UNIQUE 80.438±1.334 M、48.406731 B/输入行。它们是当前绝对基线，不是新增收益。证据：target/jfr-distinct-unique-native-current-20261007/及同名log；target/jmh-distinct-unique-native-current-20261007.{log,json}，两项均有完整结果且无failure。

两项公开API有界探针关闭错误归因／不等价方案：DefaultReactorQLContext.newContainer计数证明两个查询在65,536行输入阶段均未创建别名Map，最终均只创建3个容器（target/count-container-materialization-audit-20261007.log），因此StringLatin1.hashCode采样不能作为逐行别名Map或字符串缓存的理由。原生分组flatMap与flatMapSequential差分在首组保持打开时分别输出[2,3,4]和[]，首组关闭后才相同（target/ordered-group-merge-equivalence-audit-20261007.log）；有序合并会阻塞无界流的后续已闭组，不能用于规避完成扫描。没有新增生产、测试或JMH源改动，沿用同字节码的587项生产全量验证及还原后的比较回归。

宽列函数计算对照已完成：使用现有WideSqlWorkloadBenchmark.wideProjectionWithFunctions及nativeWideProjectionWithFunctions的相同预建输入、完整函数结果／类型oracle和消费端，两处JSON表达式均独立解析，不共享跨列结果。官方测量期JFR（同JDK、堆及GC，1fork、3×1s预热、1×15s测量、stackDepth128）正常退出，两个录制均有效且无failure。SQL为807个worker CPU／4460个分配样本，主要分配含Object[]304、byte[]285、ConcatMapImmediate218、MonoJust215、ConcatMapInner210；直接函数链为893／4449，主要分配含byte[]1098、Map节点614、桶数组486、String432、LinkedHashMap337。直接链的主要CPU叶子包含Map.putVal105、nativeFunctionRow39、String.indexOf34与JSON parser读取；SQL的原生参数顺序求值、投影订阅及布尔空值处理仍有显著成本。采样标签会受JIT内联影响，不将单个小helper的叶子计数当作可消除CPU比例，也不重开已否定的参数concat-array或Publisher hoist。

正式GC／JMH（2forks、3×1s预热、5×1s测量、1线程、512MB／G1）两项各10次测量均成功：当前SQL宽列函数585,355±19,938输入行/s、6823.618 B/输入行；同输入直接函数链3,074,415±145,556输入行/s、2158.865 B/输入行。SQL两个fork分别为6841.018／6806.218 B/输入行，这是相同源码的fork级JIT差异，不是新的内存优化。证据为target/jfr-wide-functions-native-discriminator-20261007/及同名log、target/jmh-wide-functions-native-current-20261007.{log,json}。首次沙箱执行因JMH本机回环socket受限未开始有效fork，经授权后标准fork录制成功；未改压测配置绕过隔离。

结论：仍未接近直接计算参考，差距不能全视为可删除冗余；直接链没有SQL通用扩展、资源校验及逐表达式错误恢复的完整合同。当前没有确定且低复杂度的新增高收益候选，不新增缓存、池化、框架、SQL特路、能力标记或参数调优。本阶段只补齐取证与绝对基线，不申领吞吐增益或常驻堆下降；源代码／测试／基准均未改，最终制品SHA256仍为adfb4935c03af4baf82d72b94386cb180271cae98a2d5279e81cd66141f58851，git diff --check通过，复用既有完整生产验证而未重复构建。

### 当前真实查询热点复核：不以采样强行增加复杂度

范围：当前已验证制品`add844beb89715b945d57500ad1c79fcd90c0a0e94107dd1dc6d1b9c73dc6f44`，复用现有宽列JSON操作符与冷Publisher双参数函数基准；不改SQL、输入、结果／类型oracle、引擎代码、测试或基准代码。两次均为JDK17.0.18、512MB／G1、1线程、1fork、3×1s预热、1×15s测量、stackDepth128；官方JMH JFR只录制测量阶段，不把诊断吞吐当正式收益。

`target/jfr-parsed-json-operators-current-20261007/`：16列投影包括两处JSON文本操作符，998个worker CPU／4445个分配样本；CPU叶子含String.equals76、assertJsonStructure61、HashMap.putVal60、HashMap.getNode51、JsonValueSupport.normalize49、JSONPath Utils.concat37、HashMap.resize35。4302个分配样本是经DefaultReactorQLRecord.setResult的结果Map桶数组。输入为预解析对象只是基准对照，不是引擎按输入类型绕过校验的优化。不能根据桶数组采样直接调容量、跳过快照或引入行缓存。

`target/jfr-two-argument-publisher-current-20261007/`：736个worker CPU／4460个分配样本；CPU叶子以原生flatMap排空、Mono订阅和concatMap顺序求值为主。分配样本含Object[]569、MapFuseableSubscriber393、IterableSubscription364、结果HashMap350、MonoDefaultIfEmpty340、ConcatMapImmediate331。进一步核对31个IteratorSpliterator.tryAdvance叶子，均在FluxIterable／concatMap原生订阅链；它们不是Java Stream重复计算的证据。现有双参数concat-array替换已经正式测得吞吐回退，当前采样不构成重开理由；Flux.from的ScalarCallable边界、空列保留行、参数冷订阅及多值顺序也不能绕过。

结论：本次只完成新热点取证，没有生产改动或新收益申领。JSON normalize不能假定幂等：解析文本只校验解析树，不递归规范化树中的文本，后续normalize可能改变内容；去掉复制也会改变可变输入隔离、Map扩展调用与资源限制。当前未建立可安全落地的高收益简化，不引入缓存、池化、执行框架或错误补偿。样本数不是CPU百分比、精确B／行或常驻堆证据；仍复用同制品587项通过测试，阶段末检查文档差异与制品身份，不重复构建。

### 聚合清理后的跨SQL性能差距验收

目标：补齐当前原生聚合制品的跨SQL基准，避免把已撤回聚合融合的历史全局验收当作当前性能。Owning module仍为ReactorQL；只使用已验证的add844be制品与现有JMH，覆盖全局多聚合、WHERE／COUNT、高基数COUNT／多聚合、多层子查询、JOIN、Top-N、DISTINCT与集合操作；复用同制品宽函数与真实多层查找查询结果。SQL／输入／输出oracle、消费端和默认配置不改，不新增基准或生产路径。

同JDK17.0.18、512MB／G1、1线程、2forks、3×1s预热、5×1s测量与GC profiler执行；记录完整JSON、准确吞吐单位及每输入行分配。原生控制仅用于已存在的同场景参考，不用固定手写状态的上界宣称所有扩展查询等价，也不把当前与历史不同源码／口径数据直接相减。按真实差距和已有JFR决定后续优先级；如剩余成本属于必要语义，不强行删操作符。当前阶段不改引擎，复用587项功能门禁，结束时校验制品身份和差异。

首轮进程正常退出但仅14／16项有结果：WHERE及其原生对照都在共用wideProjectionInput的预热校验中失败，不能据退出0称全套通过。独立公开API差分在当前add844be和先前binary-boundary-native-correct制品上，对0／1／31／1023／999999均证明整行与字段类型符合原oracle，只有HashMap字段遍历次序不符合旧断言；这不是行排序或计算结果回归。仅在JMH的assertWideProjectionRow移除非SQL合同的HashMap遍历顺序条件，保留完整Map.equals、逐字段类型及百万行计数；不改生产或预热SQL／输入。阶段末打包并逐条比对生产class，复用同字节码的587项测试与成功14项，只补测失败的WHERE两项。临时差分源码为/private/tmp/ReactorQlWideOracleAudit.java。

离线package（-DskipTests）成功，所有195个生产class与add844be逐条字节一致，当前JMH校验修正版制品SHA256为73c55239895b809c2a2b65426b4c1dc032be4510bf202aae5ea6b279b36c5c53；未重跑或声称新运行587项测试。修正后的WHERE两项都有完整2forks／10测量结果且日志无failure，合并覆盖16项；其余14项在生产逻辑相同、相应基准方法／输入／消费端未改的条件下复用，不重复构建或全表压测。首轮日志／JSON：target/jmh-native-cleanup-cross-sql-current-20261007.*；补测：target/jmh-cross-sql-where-oracle-corrected-20261007.*。全局五聚合在相同百万行、1024份预建Map循环输入下另用公开API核对整行、Long／Double／Integer类型及两路各一次来源订阅，均通过（target/cross-sql-global-oracle-current-20261007.log）；这个手写参考只对应当前有界整数输入，不是未知Number或扩展Feature的等价实现。

| 当前场景 | 输入吞吐（行/s） | B/输入行 |
| --- | ---: | ---: |
| 全局五聚合 | 9.984±0.782 M | 128.006 |
| 同输入手写五聚合参考 | 130.303±1.336 M | 15.999 |
| WHERE／COUNT，预建Integer输入 | 35.099±0.339 M | 48.002 |
| 同输入原生WHERE／COUNT | 495.560±14.737 M | 接近0 |
| 50k键、50k行窗口三聚合 | 5047±935 | 9242.502 |
| 50k键、50k行窗口COUNT | 5594±198 | 5754.502 |
| 三层单值子查询，缓存启用 | 6.630±0.046 M外层行 | 592.140／外层行 |
| INNER JOIN | 6.319±0.242 M | 571.938 |
| Top-N | 12.077±1.055 M | 200.019 |
| 同输入原生Top-N | 27.740±0.481 M | 32.540 |
| DISTINCT | 32.752±1.083 M | 177.169 |
| INTERSECT | 12.399±0.185 M | 528.587 |
| 同输入原生INTERSECT | 118.775±15.850 M | 21.562 |
| UNION | 10.088±0.090 M | 553.922 |
| UNION ALL | 10.761±0.308 M | 615.873 |
| EXCEPT（既有右减左方向） | 11.112±0.216 M | 547.782 |

本表是当前绝对值及计算参考，非本阶段前后收益，不证明已接近原生。全局聚合约为其固定参考的7.7%，WHERE约7.1%，高基数原生分组完成扫描仍是大差距；高基数三聚合分配两个fork为9234.503／9250.502 B/行，JOIN为575.938／567.938，不把fork级变化归为新回退。缓存单值三层查询与无缓存多层聚合查找不是相同形态，不能拿6.630 M／外层行替换既有9015／外层行、748857 B／外层行的无缓存场景。宽函数同制品0.589 M输入行/s及原生独立函数边界成本复用已有证据，不因本表扩展为普遍收益。下一取证仅为同一正式globalAggregates／wherePrebuiltInput的测量阶段JFR，保持输入、消费端和参数，不为追回历史融合吞吐增加执行／恢复框架。

测量阶段JFR已完成（target/jfr-cross-sql-global-where-current-20261007/，同正式方法／输入／消费端，1fork、3×1s预热、1×15s测量、512MB／G1、stackDepth128）。全局聚合801个worker CPU／4489个分配样本，主要分配为ScalarValueMapper.apply的MonoJust3336、Record984、输入Integer168；CPU叶子含FlatMap.onNext85、Publish.drain79、tryEmitScalar48、String.equals47、CAS43、CastUtils.castNumber40。WHERE有942个CPU／4393个分配样本，Record4355；CPU叶子BinaryFilterFeature.test299、FlatMap.onNext91、newRecord62、tryEmitScalar54、CompareUtils.compare46。这些是定位，不换算CPU占比、精确字节或常驻堆；原生多归约订阅边界仍不可删。

通用比较分派候选已撤回：仅试验把BinaryFilterFeature.test已有“两侧都为Number”分支放到日期分类前，仍先展开两个单入口Map；无新类型特路、操作符或对象，protected重载、混合转换和原catch/false未改。候选完整离线package通过591项测试，0失败／错误／跳过，55份当前报告；195个生产class只有BinaryFilterFeature改变，298个基准class字节不变。补充四项有效回归仍保留：Map与Number双接口展开／读取顺序、protected重载参数身份、固定时间的日期／数字双向优先级及Throwable转false。

同JDK17.0.18、512MB／G1、单线程、2forks、3×1s预热、5×1s测量／GC，首轮WHERE为35.099±0.339→37.330±0.367 M输入行/s；独立串行保存制品复测仅35.882±0.920→36.369±0.866 M，区间重叠，未确认稳定CPU收益。宽列operatorMix为2.416±0.029→2.407±0.035 M，无IN对照3.226±0.091→3.234±0.083 M，解析后JSON1.446±0.014→1.429±0.039 M；原生混合对照17.138±0.637→16.698±1.406 M，均无稳定变化。各场景分配不变，WHERE约48.002 B/输入行，宽列1022.236／720.835／3608.015 B/输入行。不申领首轮6.4%增益或堆收益；恢复原生产分派，不为过测继续调整分支、SQL、输入、预热或配置。

证据：target/numeric-dispatch-candidate-package-20261007.log；target/jmh-numeric-dispatch-controls-before-20261007.*、target/jmh-numeric-dispatch-after-20261007.*及target/jmh-numeric-dispatch-where-{before,after}-repeat-20261007.*。所有JMH均有完整结果／无failure，不仅检查进程退出码。还原后离线定向package成功，LessTanFilterTest的5项回归全部通过（target/numeric-dispatch-restored-package-20261007.log）；195个生产class及298个基准class与改动前逐条字节相同，复用未改变生产的587项既有完整验证，不重复全量测试。最终制品SHA256为adfb4935c03af4baf82d72b94386cb180271cae98a2d5279e81cd66141f58851，git diff --check通过。后续不重开数值分类重排微调，优先选择尚未证明属于必要语义的真实多层查询／宽列热点。

### 当前宽列纯计算与三层缓存子查询：结构成本取证

当前制品adfb4935…的生产／基准class仍与数值分派试验前相同；本轮没有生产、测试或基准改动，复用587项完整生产验证及还原后的比较回归。既有operatorMixWithoutIn与deeplyNestedSubquery均使用原SQL、输入、消费端与值／类型oracle，JDK17.0.18、512MB／G1、单线程、1fork、3×1s预热、1×15s测量、stackDepth128，官方JMH JFR仅录制测量阶段。无failure，诊断分数不作正式吞吐或优化收益。

宽列无IN对照（target/jfr-operator-mix-without-in-current-20261007/）有901个worker CPU／4437个分配样本。CPU叶子包含BinaryFilterFeature.test82、String.equals63、LikeFilter.hasLineTerminator36、CompareUtils.compare35、Mono.subscribe32；分配包含结果Map节点511、MonoZip.ZipInner485、ScalarSubscription252、MonoJust218、DefaultIfEmptySubscriber215。二元运算原生Mono.zip的PairwiseZipper及数组、逐列延迟求值与空列占位是可明确定位的结构成本；不以采样计数推导CPU百分比、B/行或常驻堆。

公开API差分排除了两个直接“少分配”方案，均未写入生产：把二元Mono.zip换成查询级数组组合函数会失去后续zipWith的原生组合行为。计算器失败时，现有二元重载仍订阅第三个来源一次，错误Hook／discard数据为[1,2,3]；数组重载则不订阅第三个来源，数据为[1,2]。证明该变化会影响表达式wrapper及组合扩展，不是仅少两个对象。另在查询构建后安装onEachOperator，现有常量mapper按当前Hook得到99，构建期保存的MonoJust仍得到42；共享该Publisher会冻结原装配边界。不加Hook探测、输入／metadata特路或新缓存规避。差分源码分别为/private/tmp/ReactorQlBinaryZipOverloadAudit.java、/private/tmp/ReactorQlConstantPublisherAssemblyAudit.java；日志为target/binary-zip-overload-equivalence-audit-20261007.log及target/constant-publisher-assembly-equivalence-audit-20261007.log。

三层缓存单值子查询（target/jfr-deep-cached-subquery-current-20261007/）有855个worker CPU／4382个分配样本。主要分配为newContainer的HashMap479、MapSubscriber470、FlatMapInner467、结果Map节点450、桶数组308、Record276；CPU叶子为FlatMap.drainLoop90、FluxDefer.subscribe64、StringLatin1.hashCode52。进一步核对64个FluxDefer叶子均经过FluxDeferContextual的缓存读取；它不是每外层行重复执行内层来源的证据。48个StringLatin1.hashCode叶子在PropertyMapFeature→getRecordValue→HashMap.get路径，仅定位属性读取，不据此增加字符串哈希缓存。SelectFeature仍在订阅时读取根SubscriptionContext，CompletedManyCache仍在订阅时选择已完成读取或原in-flight/replay/refCount链，保留异步首次访问、错误与取消重连语义。输出容器及快照不直接删；不为去掉读取判断重新设计缓存状态模型。

结论：本轮完成两个新形态的当前热点归因及两个公开API反例，没有新增可确认的高收益生产优化。宽列无IN的正式基线仍为3.226 M输入行/s、720.835 B/输入行；三层缓存查询仍为6.630 M外层行/s、592.140 B/外层行；不把后者替代无缓存多层聚合查找的9015外层行/s、748857 B/外层行。瞬时分配、精确键状态及原生响应式生命周期分别处理，目标仍未完成。未重复构建，阶段末制品SHA和git diff --check通过。

### 原生元数据配置直接读取

目标与范围：当前测量阶段高基数JFR将353个CPU叶子样本定位到`ReactorQLMetadata.getConcurrency`，调用来自原生CountAggFeature参数映射；另800个样本仍在FluxFlatMap.innerComplete的drainLoop。仅评估getConcurrency中去掉Optional.map及缺省并行度装箱，保持每次调用实时读取同一getSetting扩展点与原数值转换。样本用于定位，不推导可节省CPU比例。

实现仅把`getSetting("concurrency").map(...).orElse(...).intValue()`改为读取同一个Optional后直接取数值或原缺省int，消除中间转换及缺省值装箱。不缓存配置、不减少公开getConcurrency调用次数、不改默认值、并发、prefetch、SQL选路或聚合订阅／恢复边界。MetadataConcurrencyReadTest覆盖缺省／多种原有值转换、覆写getSetting的动态读取和转换错误身份；全量离线package通过587项测试、0失败／错误／跳过、55份当前报告，保留DebugAgent／许可证门禁。日志`target/metadata-concurrency-read-candidate-package-20261007.log`；制品SHA256为`add844beb89715b945d57500ad1c79fcd90c0a0e94107dd1dc6d1b9c73dc6f44`，保存为`target/metadata-concurrency-read-candidate-validated-20261007-benchmarks.jar`。

同一SQL／输入／值及类型oracle、55个相关JMH／generated class字节不变，2forks、3×1s预热、5×1s测量、512MB／G1、单线程／GC的正式结果：50k输入、每键2行的函数键四聚合由26988±582→26537±414输入行/s，4505.450→4441.449 B/输入行，两个fork均少64 B（−1.42%，即128 B/键）。吞吐区间重叠，不能声称提速，也没有常驻堆下降证据；这只是低复杂度的通用瞬时分配消除，不是高基数主热点已解决。日志／JSON为`target/jmh-metadata-concurrency-read-{before,after}-20261007.*`。

宽列函数对照0.588±0.019→0.589±0.022 M输入行/s；无缓存多层子查询9068±33→9015±424外层行/s，吞吐均未显示稳定变化。宽列分配after两fork为6841／6884 B/行，与既有同运行时fork级JIT变化一致；子查询after两fork为748857／748873 B/外层行，不申领这两个场景的分配收益或普遍CPU改善。基线对照JSON为`target/jmh-aggregate-native-cleanup-current-real-sql-20261007.json`。不为此继续添加配置缓存、表达式证明或SQL专属分支。

测量阶段after JFR为`target/jfr-metadata-concurrency-read-after-20261007/`：1167个worker CPU、510个分配样本；1149个CPU叶子样本仍为`FluxFlatMap.drainLoop←innerComplete`，getConcurrency叶子样本消失。before总CPU样本1168，drainLoop／getConcurrency分别800／353，故调用位置变化不能解读为整体CPU减少；正式吞吐也未证明改善。当前多层子查询JFR仍以resultToRecord的Map快照／别名容器为主要分配来源，不绕过公开读取、快照或恢复合同来消除它们。整个吞吐／常驻堆优化目标尚未完成。

## 当前切片：时间窗口背压边界诊断

目标：用虚拟时间和有限、遵守 request 的可计数源，验证时间窗口融合路径 `source.window(duration).concatMap(..., 1)` 在零下游 demand 时是否继续无界拉取或积压，以及取消、错误、Reactor Context 是否保持。影响范围仅为 `WindowedAggregateStageTest` 和本文档；不改生产、默认限制或窗口语义，也不将违反 Reactive Streams request 的测试源归为引擎问题。

实施：增加确定性测试，分别覆盖零 demand 跨多个窗口后取消时的上游请求/发值有界与取消；窗口关闭后逐条请求的结果与完成；上游错误及 Context 传播。若实际 Reactor `window` 契约与预期不同，保留可观测事实并停止，不弱化断言；只有发现错误才由 owning agent 另行提出最小修复。

验证：阶段末运行该测试类；若通过且没有生产改动，再运行完整测试。完成后记录确切方法、源码边界、观测和是否需要修复，并执行 `git diff --check`。

结果：`WindowedAggregateStageTest` 的既有虚拟时间计数源已经覆盖零 demand（直接 `query.start` 的 `MAX request / 7 emits / cancel / overflow`）、窗口关闭后按 `2/2/1` 逐条请求、上游错误和 Reactor Context。补充 `shouldCancelTimeWindowSourceAfterDeliveringClosedWindow`，以有限的 `take(5)`、遵守 request 的计数源可证伪地覆盖不同于溢出路径的取消边界：先请求一个已关闭窗口，虚拟时间 100ms 后收到 `{total: 3}`，再取消；源观测为 `Long.MAX_VALUE` request、3 次发值、收到 cancel、无源错误。这里的 MAX request 是 Reactor 3.4.34 `window(Duration)` 的既有边界语义，不应归为 ReactorQL 的无界缓存或错误。未改生产代码、默认限制或窗口编排。

验证结果：`mvn -q -Dtest=WindowedAggregateStageTest test` 与全量 `mvn -q -Pjmh package` 均通过，`git diff --check` 通过。日志中已有的预期 `onErrorDropped`（时间窗口错误传播用例）及混合行属性告警不影响退出状态；不把这些测试探针信号误记为引擎异常。

## 当前切片：异步排序键 JFR 取证

目标：为 ReactorQL `ORDER BY` 的单异步键与双异步键取得干净 JFR/JMH，区分必要的冷 Publisher、排序状态与多键 `fromIterable + concatMap + collectList` 编排成本，并与同步排序键建立负对照。使用同一预构造 `Integer[]` / `Flux.fromArray` 输入，以 Blackhole 进行引用消费，避免源构造和消费端 hash 干扰；setup 严格核对 Top-N 结果值、顺序、类型及冷源订阅次数。

范围与步骤：仅修改 `src/jmh/java/org/jetlinks/reactor/ql/ReactorQLBenchmark.java` 与本文档；不改生产实现、默认限制、排序/Top-N 语义、背压、取消、Context 或错误契约。新增单异步键、双异步键及同步负对照的 profiling-only 入口，复用同源输入与 Blackhole 消费；先集中构建 JMH，再在 JDK 17、512 MB/G1 下采集 JFR，必要时以同配置成对正式 JMH 比较吞吐和分配。setup 必须验证 Top-N 结果值、稳定顺序、数值类型、结果数量，以及每个冷 Publisher 的订阅次数。

停止条件：若热点仅对应必要的冷 Publisher 订阅、精确排序状态或通用多键编排，且没有跨 `ORDER BY` 形态、低复杂度且不改变语义的候选，则记录当前下界并停止；不按 SQL 文本或单一键类型特调，不引入自定义 Subscriber、手写操作符、无界收集或改变默认限制。JFR 仅用于定位调用栈，不能从采样数推导精确 B/op；任何生产候选均须经过完整语义、请求/取消/错误/Context 和成对基准验证。

结果：使用同一 JDK 17/JMH jar、预构造 `Integer[]`/`Flux.fromArray` 输入和 Blackhole 引用消费完成诊断；JFR 为 `target/jfr-async-order-clean/`，JMH 为 `target/jmh-async-order-clean.json`。同步、单异步、双异步诊断分数分别为 6.356M、4.063M、2.576M 输入行/s，仅用于定位。单异步 JFR 出现 41 个 `MonoMap` class 样本、115 个 `MonoMap.subscribeOrReturn` 栈样本；结合 Reactor 3.4.34 `Mono.cast(Object)` 实现为 `map`，形成低复杂度的通用候选。未将诊断分数当作正式吞吐结论，也未将 JFR 样本换算为 B/op。

## 当前切片：异步排序键恒真 cast 消除 A/B

目标：验证异步 `ORDER BY` 排序键中恒真 `cast(Object.class)` 是否产生可跨异步排序形态移除的 Reactor `map` 层。Reactor 3.4.34 的 `Mono.cast(Object.class)` 实现为 `map(clazz::cast)`；`OrderBySupport.orderValue` 的单异步 JFR 已观察到 `MonoMap` / `MapSubscriber` 分配。当前同 JAR 正式基线（每输入行）为：同步 Top-N 6.826M、192.149 B；单异步 4.029M、536.159 B；双异步 2.684M、1208.159 B。

范围与方案：仅评估 `OrderBySupport.orderValue` 中移除恒真 `cast` 的 A/B；保留 `Mono.from`（确保首值语义和取消边界）及 `defaultIfEmpty`，不得替换为 `fromDirect`。不改排序状态、默认限制、结果顺序、背压、取消、错误或 Context，不改变第三方 Publisher 的适配边界。同步 Top-N 作为负对照。定向覆盖多值、空值、错误、取消、Context 和第三方 Publisher；随后集中执行全量测试与同配置成对 JMH，比较同步、单异步、双异步的吞吐和分配。

判定：只有结果/信号契约完全等价、异步路径分配明确下降且吞吐无稳定回退时才保留；若分配未降或任一代表场景稳定回退则撤回。该候选仅减少一个标准 `map` 操作符层，不引入自定义 Subscriber、Hook、Scannable 特判或新的执行状态。

结果：`OrderBySupport.orderValue` 仅移除恒真 `cast`，保留 `Mono.from` 和 `defaultIfEmpty`，没有使用 `fromDirect`。JDK 17.0.18、512 MB/G1、3×1s 预热、5×1s 测量、2 forks、GC profiler 的成对结果（`target/jmh-async-order-cast-before.json` / `target/jmh-async-order-cast-after.json`）为：同步 Top-N 6.826→6.899M、192.149→192.149 B/行；单异步 4.029→4.327M、536.159→464.159 B/行；双异步 2.684→2.945M、1208.159→1064.159 B/行。优化后单异步方向性 JFR `target/jfr-async-order-cast-after/` 的 `MonoMap` class 样本为 3（基线 41）；该 JFR 仅用于确认热点消失，不替代正式 GC 指标。

补充测试覆盖 Top-N、全局和时间窗口排序，多键、空值、多值首值取消、错误、Context、下游取消及外部 Feature；定向 `mvn -q -Dtest=ReactorQLTest test` 通过。追加测试后再次运行完整 `mvn -q -Pjmh package`，exit 0，Surefire 汇总 `tests=401 failures=0 errors=0 skipped=0`；`git diff --check` 通过。Hook/Scannable 算子树少一层符合减少操作符目标，但不承诺完整拓扑等价；GC B/行是瞬时分配而非常驻堆。候选保留，剩余多键 `concatMap` 编排仍承担顺序、首值和背压语义，不能因此删除；该结果也不表示已达到原生 Reactor 性能。

## 当前切片：多行右源与异步 ON JOIN 的干净 JFR

目标：弥补现有单行右源样本的盲区，使用每个左行匹配 `0/1/4/16` 条右行的同源夹具，取得 INNER 同步 ON 与冷 Publisher 异步 ON 的干净 JFR；必要时增加 LEFT 负对照，区分右源匹配、异步谓词和结果输出的共同成本。输入使用同一预构造左右 `Map[]` / `Flux.fromArray`，以 Blackhole 引用消费，避免源构造和消费端 hash 干扰。

范围与步骤：仅修改 `src/jmh/java/org/jetlinks/reactor/ql/ReactorQLBenchmark.java` 与本文档；不改生产实现、默认限制、背压、取消、Context、错误或 JOIN 语义。setup 严格逐行/多重集合核对输出数量、字段值和类型，并验证右源每左行订阅次数、异步谓词每个候选的订阅次数；不对 `flatMap` 默认并发下跨左行的输出顺序作强行断言，仅核对集合与行内结果。阶段末先集中构建，再在 JDK 17、512 MB/G1 下以短时 JFR 比较 0/1/4/16 匹配度、同步/异步 ON 及必要的 LEFT 负对照。

停止条件：JFR 仅用于定位共同调用栈，不能从采样数估算字节；若剩余成本仅是必要的 Record 建立、右源/谓词订阅或结果物化，则记录当前下界并停止。不按 SQL 文本或匹配数量特调，不缓存或物化右源，不引入自定义 Subscriber；只有发现跨 JOIN 形态、低复杂度且不改变信号语义的冗余，才另行提出生产 A/B。

结果：新增 `profilingMultiRowInnerJoin` 与 `profilingAsyncOnMultiRowInnerJoin`，setup 预构造左侧 20,000 行、右侧 21 行，并严格验证每个左行 `0/1/4/16` 匹配、总输出 105,000 行、字段值/类型、右源 20,000 次订阅，以及异步 ON 的 420,000 次候选订阅；不依赖 `flatMap` 跨左行顺序。`mvn -q -Pjmh -DskipTests package` 与 `git diff --check` 通过。

JDK 17、512 MB/G1、1×1s 预热、2×1s 测量、1 fork、Blackhole 引用消费的诊断文件为 `target/jfr-multirow-join-clean/` 与 `target/jmh-multirow-join-clean.json`；同步和异步诊断分数分别为 0.736M、0.149M 左输入行/s，仅用于定位。同步 JFR 有 126 个 CPU、629 个分配样本，主要为 `HashMap$Node` 279、`HashMap` 156、桶数组 90 和 Record 53，集中于右候选/投影容器。异步 JFR 有 107 个 CPU、620 个分配样本，主要为 `FluxFlatMap` Main 43、`MonoZip` Inner 41、`MonoAll` 40、`ScalarSubscription` 38、`MonoJust` 30、`MonoZip` 23，属于异步 ON 的真实候选编排成本。JFR 样本不能换算 B/行，也不能证明常驻堆占用。

源码核对：`BinaryFilterFeature` 的 mixed scalar/async 路径仍通过 `Mono.zip` 组合两侧。直接改成单侧 `map` 会改变 mapper 调用、空值分支、订阅/取消/错误时序及外部 Feature 行为，因此不提出该生产候选；不缓存或索引右源，不增加特调能力标记或自定义 Subscriber。本切片无生产改动，停止于必要的 Record、右候选和异步订阅编排下界。

## 当前切片：LEFT JOIN 空匹配路径的 JFR 诊断

目标：确认 LEFT JOIN 的空匹配回退是否引入了可跨场景移除的逐行状态或分配。影响范围仅为现有 JMH 夹具 `src/jmh/java/org/jetlinks/reactor/ql/ReactorQLBenchmark.java` 及本文档；生产实现、测试、配置和 SQL 默认行为均不修改。源码已确认 LEFT JOIN 使用标准 Reactor `filterJoin(...).defaultIfEmpty(left)`：该算子必须保持空右流时原样发送左记录的背压、取消、Context 与 discard 语义，不按 LEFT SQL 或当前输入特调生产路径。

实施：在 JMH 中增加复用 `consumeForProfiling` 的 `profilingLeftJoin(Blackhole)`；setup 对现有 20,000 行交替左右值的夹具严格验证总行数、匹配/未匹配各 10,000、输出值与类型，并验证右源按左行订阅，避免只验证 count 或依赖 `flatMap` 的实现顺序。阶段末集中构建 JMH 夹具；以 JDK 17、512 MB/G1、3×1s 预热、5×1s 测量、2 forks、GC profiler 记录 LEFT 正式数据，再以相同 JAR 顺序采样 INNER / LEFT 的 Blackhole JFR。

风险与退出条件：JFR 采样只能定位热点，不能用样本数推导精确 B/op；若 LEFT 独有成本仅为保证空匹配语义所需的标准 `defaultIfEmpty` 状态，则不提出生产优化。只有发现可跨 JOIN 形态复用、低复杂度且不改变信号语义的冗余边界，才另行进入独立实现计划。

结果：夹具 setup 已验证 LEFT JOIN 的 20,000 行输出、`value` 的 `Integer` 类型、匹配值 `0` / 空匹配回退值 `1` 各 10,000 行，以及右源逐左行订阅 20,000 次；不依赖 `flatMap` 行顺序。`mvn -q -Pjmh -DskipTests package` 通过。JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler 的正式 LEFT JOIN 为 6.052M 输入行/s、687.938 B/op（`target/jmh-left-current.json`）。

同一 JAR 顺序运行的 1×1s 预热、2×1s 测量、1 fork Blackhole JFR（`target/jmh-join-paired-clean.json`、`target/jfr-join-paired-clean/`）中，INNER / LEFT 诊断分数分别为 6.420M / 5.123M 输入行/s；该短样本只用于热点定位，不作正式吞吐比较。INNER 有 117 CPU、660 allocation samples，LEFT 有 109 CPU、615 allocation samples。LEFT 独有且由 `DefaultReactorQL.createJoin` 的 `defaultIfEmpty(left)` 装配点产生的分配为 `FluxDefaultIfEmpty` 与 `FluxDefaultIfEmpty$DefaultIfEmptySubscriber`；CPU 样本也落在其 request、onNext 与 onComplete。它保存每个左记录的 fallback，并在空匹配时原样发射该记录，是 LEFT JOIN 的必要 Reactor 状态，而不是可删除的重复操作。没有发现不改变背压、取消、Context/discard 与空右流语义的低复杂度、跨场景生产候选；不引入自定义合并 Subscriber/操作符。`git diff --check` 通过。


## 当前切片：单参数 Publisher 函数的操作符合并

目标：让非标量 `FunctionMapFeature` 只有一个参数时，不再逐行构建 `Flux.fromIterable(singleton).concatMap(...)` 参数流，减少所有这类函数共有的订阅/操作符成本。Owning module 为 `FunctionMapFeature`、兼容测试和现有 JMH；不按函数名或 SQL 文本特调。参数 mapper 仍在订阅时调用；非默认参数保留全部发值，配置默认值时仍按现有 `Mono.fromDirect(...).defaultIfEmpty(...)` 语义只取首值。多参数顺序、`DISTINCT/UNIQUE`、自定义 protected `apply`、错误、取消、背压和 Context 不变。

先增加使用现有冷 Publisher 属性 Feature 的单参数包装函数 JMH，并与直接调用的结果逐行比对；以冷 Publisher Feature 和普通投影为负对照。随后只在 `createParameterStream` 的单参数分支返回一个惰性 `Flux`，其他参数个数沿用原实现。测试覆盖冷调用时机、多个参数值、空值/默认值、错误、取消和 Context。阶段末集中执行完整构建和同配置成对基准；只有真实 SQL 每行分配明确下降、吞吐无稳定回退且功能等价才保留，否则撤回。

结果：保留单参数 `Flux.defer` 路径；参数 Feature 仍在订阅时调用，无默认值时保留多次发值，有默认值时仍用原有 `Mono.fromDirect` 首值语义。新增测试覆盖冷调用、Reactor Context、多值、默认值、错误，以及确认参数源已订阅后的取消传播；多参数仍走原 `concatMap`。JDK 17.0.18、512 MB/G1、3×1s 预热、5×1s 测量、2 forks 的同源真实 SQL 基准：单参数冷 Publisher 函数 4.740→8.416 M 输入行/s（约 +77.5%），887.937→631.936 B/行（减少 256 B/行）；直接 Publisher Feature 10.970→10.753 M、均约 487.936 B/行，普通投影 18.105→18.090 M、均约 255.994 B/行，负对照未见稳定退化。基准 setup 逐行比较包装函数与直接 Feature 的输出；证据为 `target/jmh-single-arg-publisher-before.json`、`target/jmh-single-arg-publisher-after.json`。最终 `mvn -q -Pjmh package` 通过，387 tests、0 failures/errors/skipped；`git diff --check` 通过。该优化减少每行瞬时分配，不改变无界分组的必要活跃键状态或缓存结果常驻堆；包装函数吞吐仍低于直接 Publisher Feature，不能把后者当作 Java 原生性能。

## 当前切片：内置当前行属性的共享读取入口

目标：减少普通投影、WHERE、聚合参数等场景对内置 `this` 属性重复执行通用解析的 CPU 成本。Owning module 为 `PropertyMapFeature`；这是内置属性语义的共享入口，不按完整 SQL 文本或测试数据特调。仅当 `PropertyFeature` 正是默认单例且表达式是当前行属性时，对非 null 的 `getRecordValue("this")` 直接返回；null、派生结果及自定义 PropertyFeature 保持原 resolver，避免改变宽松 SQL 回退。不得增加每行对象、订阅状态或操作符。

先测当前投影、普通 WHERE 和无 WHERE 五聚合；再改一处构建期 mapper，补当前行 Map/标量/派生记录及自定义属性等价测试。阶段末集中运行完整测试和同配置 JMH。若投影或 WHERE 没有可靠吞吐收益、分配增加或聚合负对照稳定回退，则撤回，不为单个基准保留独立 SQL 分支。

结果：保留 `PropertyMapFeature` 对默认属性单例的当前行直读；来源值为 null 时仍调用原 resolver，自定义属性实现完全不进入该分支。测试覆盖标量与 Map 输入、空来源派生记录的结果 Map 回退及自定义 `this` 属性覆写；未新增 Reactor 操作符、每行对象或订阅状态。JDK 17.0.18、512 MB/G1、3×1s 预热、5×1s 测量、2 forks：普通 WHERE 40.289→47.636 M 行/s，候选复测 46.033 M，均约 48 B/行；投影 15.453→17.757 M，候选复测 17.406 M，均约 256 B/行。WHERE 吞吐收益明显；投影各次的误差区间较宽，不能宣称其提升幅度稳定。无 WHERE 五聚合负对照 39.311→38.325 M、均 16 B/行，误差区间重叠，未确认稳定回退。证据为 `target/jmh-self-property-before.json`、`target/jmh-self-property-after.json`、`target/jmh-self-property-repeat.json`。本切片改善 CPU 热路径而不减少每行分配；更接近原生吞吐的总目标尚未完成。

最终 `mvn -q -Pjmh package` 通过，385 tests、0 failures/errors/skipped；`git diff --check` 通过。未提交或推送。

## 验证门禁：时间窗口测试的调度确定性

`ReactorQLTest.testGroupByWindowEmpty` 目前把两次 `delayElements(1s)` 发值放在 `interval(500)` 的精确窗口边界上，用墙钟运行时反复得到 4 或 5 个窗口。先只改测试：在订阅前启用 Reactor 虚拟时间，并让输入在窗口边界之间发出，逐个断言原先期望的五个窗口计数 `0,0,1,0,1` 与正常完成。这样覆盖空窗口和非空窗口，同时排除同一时间戳的调度先后不确定性。不修改 `GroupByIntervalFeature`、窗口输出语义或生产延迟配置；定向及全量测试在阶段末验证。

结果：`ReactorQLTest.testGroupByWindowEmpty` 现使用 `StepVerifier.withVirtualTime` 和相对 500 ms 窗口错开的 1100 ms 发值间隔，断言五个具体计数及完成；没有降低窗口数量断言，也不再依赖墙钟睡眠。定向测试与最终 `mvn -q -Pjmh package` 通过。该变更只是稳定测试门禁，不计入吞吐或堆收益。

## 非融合多聚合的订阅私有结果容器试验（已撤回）

目标：降低第三方及其他非融合多聚合查询的结果收集成本。`DefaultReactorQL.createMapper` 的多个聚合结果经 `Flux.merge` 串行交给每次订阅独有的 `collect`，中间 Map 不对外发布，也不需要并发写入；试验以普通 `HashMap` 代替 `ConcurrentHashMap`，不改变最终结果 Map/List 的类型、值、顺序、订阅次数和错误/取消信号。不改内置增量聚合、分组状态、默认限制或 Reactor 操作符结构。

先用当前代码测多值兼容聚合及普通全局聚合负对照；随后仅替换中间容器，复用 `LegacyMultiValueAggregateTest` 的同步/异步、单值/多值、错误与取消用例，并补最终结果等价检查。阶段末集中运行完整构建和相同配置 JMH。只有分配或吞吐有明确收益且负对照无稳定回退才保留，否则恢复原容器；并发安全以 Reactor `merge` 对下游 `onNext` 的串行化和 `collect` 的订阅私有状态为边界，不引入锁或共享状态。

结果：改为 `HashMap` 的试验已撤回，生产代码和新增的订阅隔离测试均恢复到试验前。JDK 17.0.18、512 MB/G1、3×1s 预热、5×1s 测量、2 forks 的成对 JMH：多值兼容聚合 20.991→20.485 M 输入行/s，109.266→109.180 B/行，分配收益仅约 0.09 B/行且吞吐略低；无 WHERE 五聚合负对照的分配仍为 16 B/行。证据为 `target/jmh-multi-map-before.json` 和 `target/jmh-multi-map-after.json`。试验代码下定向兼容聚合测试与真实时钟窗口测试通过，首次完整测试仅既有 500 ms 窗口用例偶发 4/5 输出，未改断言。`HashMap` 不带来有意义的堆收益，因此不为它放弃原容器；不能仅凭 `merge` 串行化推断性能会提升。

## 当前切片：同步过滤后的原始行增量聚合

目标：让构建期可证明由内置原始行映射器组成的同步 WHERE，在单表全局增量聚合前直接处理 Map 行，避免每条通过/未通过的 Map 输入先创建 `ReactorQLRecord`。复用现有 `RawScalarValueMapper` 和 `WindowedAggregateStage.applyRaw`，新增仅表达原始行谓词能力的可选契约；任一子表达式、自定义 Feature、checkpoint、JOIN、子查询或分组不符合时仍回退原 Record/Publisher 路径。无界 `avg/max` 仍逐行更新常数状态，不保留历史输入行；默认限制、输出时机、背压和取消不变。

Owning module：ReactorQL 根模块的 `feature/`、`supports/filter/`、`DefaultReactorQL`、测试和现有 JMH。先增加同输入、同输出的带 WHERE 五聚合基准；实现时仅将内置二元比较和 AND/OR 组合成原始行谓词，构建期核对表别名及 raw 聚合能力，非 Map 输入继续按现有 ScalarFilter 构造 Record 求值。两侧 AND/OR 仍都执行。验证 Map/非 Map、别名、空值、嵌套/自定义回退、request(1)、错误、取消、Context 和并发订阅；阶段末集中运行完整测试与成对 JMH。保留门槛：结果/信号完全等价，带 WHERE 全局聚合的吞吐和 B/行明显改善，且无 WHERE 全局聚合、普通 WHERE/投影及高基数分组无稳定回退；否则撤回。不增加自定义 Subscriber、全局缓存或按 SQL 文本触发的分支。

结果：保留可选 `RawScalarFilter` 与原始行收集路径。构建期只在内置单表、同步二元比较及 AND/OR、可读取同一别名的原始行聚合器均成立时选择；Map 行在同一个 `collect` 中完成过滤和常数状态累加，非 Map 行仍构造一个 Record 供过滤和聚合共用。普通 WHERE 使用 `recordFilter()`，不让原始行适配层进入逐行热路径。未新增 Subscriber、输入行缓存、跨订阅状态或默认上限。

验证：`WindowedAggregateStageTest` 定向测试通过；最终 `mvn -q -Pjmh package` 通过（384 tests，0 failures/errors/skipped），`git diff --check` 通过。上一次完整构建曾因原有真实时钟用例 `ReactorQLTest.testGroupByWindowEmpty` 偶发得到 4/5 个窗口而失败，本次完整复测通过；未放宽断言。JDK 17.0.18、512 MB/G1、3×1s 预热、5×1s 测量、2 forks：带 WHERE 五聚合从 26.856M 提升至 37.535M 行/s，分配从 48 降至 16 B/行（`target/jmh-filtered-raw-before.json`、`target/jmh-filtered-raw-fused-after.json`）。无 WHERE 五聚合约 38.440M 行/s、16 B/行。普通 WHERE 从此前 40.228M 到一次独立复测 38.630M 行/s，均为 48 B/行，约低 4%；另一次当前代码复测约 37.718M，提示仍有吞吐回归风险，不能宣称普通 WHERE 稳定持平或提升（`target/jmh-compare-once-before.json`、`target/jmh-filtered-raw-where-final.json`、`target/jmh-filtered-raw-where-repeat.json`）。

为核对普通 WHERE 的因果关系，在同一机器上临时关闭二元谓词的 raw 能力并完成 A/B/A（保留/关闭/恢复），随后恢复最终生产代码：41.108 / 41.705 / 39.090 M 行/s，三组均约 48 B/行，配置仍为 3×1s 预热、5×1s 测量、2 forks。相同源码的两次 A 本身出现约 4.9% 差异，故不能把 B 与 A2 的差距归因于 raw 谓词，也不能据此宣称已排除普通 WHERE 回退；证据为 `target/jmh-raw-where-a1.json`、`target/jmh-raw-where-b.json`、`target/jmh-raw-where-a2.json`。投影和高基数分组未见明确 B/行增加；后续基准继续将普通 WHERE 作为负对照，若稳定确认超过 5% 回退，则修正或撤回该切片。

## 当前切片：共享比较入口去重

目标：减少 `CompareUtils.compare(Object,Object)` 对不相等对象连续两次调用 `equals` 的通用 CPU 成本。该入口由数值/日期/字符串比较、最值聚合等多个 SQL 场景复用；改动仅保留一次 null/同一引用/相等判断，不按 SQL 或运行时类型特调，不增加状态或 Publisher。Java `equals` 契约要求稳定无副作用；其非常规副作用次数不是 SQL 结果契约。

影响范围：`utils/CompareUtils`、对应比较单测、现有 JMH 和本文档。先测当前全局五聚合、普通投影、WHERE 与高基数三聚合，随后做一处方法改动；验证数字、日期、字符串、不同类、null 和异常等价。阶段末集中运行完整测试、JMH 构建和同配置成对基准。若真实 SQL 吞吐无可靠改善，或任何代表场景稳定回退超过 5%，即撤回；B/行不得增加。不更改累计聚合的常数状态、无界分组的活跃 key、资源上限或背压与取消。

结果：该试验已撤回，生产比较逻辑与原测试恢复原状。JDK 17.0.18、512 MB/G1、3×1s 预热、5×1s 测量、2 forks 的前后数据在 `target/jmh-compare-once-before.json` / `target/jmh-compare-once-after.json`：高基数三聚合 2.306→2.421 M 行/s、均 2291.37 B/行，但置信区间重叠；全局五聚合 31.81→34.05 M、均 16 B/行且方差较大；投影 15.53→15.16 M、均 256 B/行；WHERE 40.23→39.41 M、均 48 B/行。没有跨场景的可靠收益。试验代码下比较单测通过，完整测试两次仅已知真实时钟 `testGroupByWindowEmpty` 得到 4/5 个窗口，单独复测通过；`mvn -q -Pjmh -DskipTests package` 通过。该窗口用例未放宽；撤回后不把试验构建当作最终功能验收。下一步不再从相同 `equals` 重排切入，转向实际跨场景对象图和执行边界。

## 当前切片：异步投影与相关子查询的剩余成本

目标：在已有 `zipDelayError` 与订阅缓存快路之后，找出可跨 SQL 形态复用、且不改变 Publisher 语义的逐行成本。Owning module 仍为 ReactorQL 根模块；先看 `DefaultReactorQL.createMapper`、`SelectFeature` 与 `DefaultReactorQLContext` 的现有执行边界，再决定是否改动。保持自定义 Feature、空列、并发订阅、延迟错误、背压、取消、Context、默认上限及查询结果不变。

实施：用当前同配置 JMH 对两/三异步列、相关子查询及第三方冷 Publisher 记录吞吐与 B/行；用短时 JFR 只定位热点，不用采样数计算精确字节。若热点指向可复用的冗余包装或状态创建，先做最小通用改动与相应行为测试，再以同源成对基准检验；不做去相关化、跨行/跨订阅结果缓存、手写 Subscriber、SQL 文本分支或无界收集。若剩余成本属于真实逐行子查询执行且低复杂度改动没有可靠收益，就保留现状并明确下界，不叠加执行状态层。

阶段末集中运行相关测试、完整 JMH 构建、同配置成对基准与差异检查。只有分配下降、吞吐无稳定回退且功能边界通过时保留改动；这不是对全局吞吐目标已完成的声明。

当前 JFR 发现相关子查询逐行创建子 `DefaultReactorQLContext` 时包含空 `ArrayList`，而已有契约规定 `transfer` 不继承上级位置参数。先试验让子上下文使用共享不可变空列表，只有首次 `bind` 位置参数时才复制成独立可变列表；根上下文及显式传入列表不变。以相关子查询为收益场景、两/三异步列和第三方 Publisher 为负对照，新增 transfer 后位置参数绑定、索引插入及多层隔离测试。此改动不缓存子查询结果，也不改变来源订阅数或内存上限；若 24 B/行左右的分配减少未出现或吞吐稳定回退则撤回。

结果：`transfer` 子上下文未绑定位置参数时共享一个仅供内部识别的不可变空列表，首次位置参数绑定时创建自己的 `ArrayList`；公开构造器传入列表的可变性不变。新增测试覆盖多层 `transfer`、追加/索引插入及父子隔离。JDK 17.0.18、512 MB/G1、3×1s 预热、5×1s 测量、2 forks 的相关子查询由此前同配置 2.868 M 外层行/s、1368.46 B/行变为 2.863 M、1344.46 B/行；减少 24 B/行，未证明吞吐提升或回退。第三方 Publisher 仍约 487.94 B/行，两/三异步缓存列仍约 1176.18/1616.30 B/行；吞吐 fork 间波动较大，不把此小改动归为它们的收益或回退。证据为 `target/jmh-cache-publisher-after.json`、`target/jmh-next-baseline.json`、`target/jmh-transfer-list-after.json` 及 `target/jfr-next-boundaries/`。完整 `mvn -q -Pjmh package` 最终通过，381 tests、0 failures/errors/skipped，`git diff --check` 通过；首次运行重现既有真实时钟 `testGroupByWindowEmpty` 偶发 4/5 窗口，随后完整复测通过，未改断言。

JFR 的相关子查询分配样本还集中在投影/具名绑定的 `HashMap`、`FluxMap`、`FluxFlatMap` 与 `Context` 传播；多异步列的 `MonoNext`、`MonoZip` 和 `FluxIterable` 对应真实首值采集、并发组合及缓存结果读取。它们不是可直接删除的冗余操作符。此次不增加对象池、跨行缓存、专用记录布局或手写订阅者；继续接近原生吞吐需要单独证明更大的通用 Record/上下文契约改变能覆盖复杂度与兼容风险。

## 目标

降低普通属性读取、比较过滤和字段投影在每一行数据上创建 `Mono`、`Flux`、`MonoZip` 与订阅对象的成本，同时保持数据源、子查询、自定义异步 Feature 等真实响应式边界不变。

## 影响范围与 owning module

Owning module 为 ReactorQL 根模块，主要影响：

- `src/main/java/org/jetlinks/reactor/ql/feature/`：增加可选的同步值与同步谓词扩展契约，并兼容已有 `ValueMapFeature`、`FilterFeature`。
- `src/main/java/org/jetlinks/reactor/ql/supports/`：让属性、字面量、二元运算、比较和逻辑条件在构建期组合成同步求值器。
- `src/main/java/org/jetlinks/reactor/ql/DefaultReactorQL.java`：普通 `WHERE` 使用单个 `filter`，普通 `SELECT` 使用单个 `map`；异步表达式继续走现有 Publisher 链路。
- `src/test/java/org/jetlinks/reactor/ql/`：补充同步与异步兼容测试，并复用现有性能 smoke test 做前后对比。

## 不做什么

- 不移除 Reactor 数据流、背压、窗口、聚合、JOIN 或异步扩展能力。
- 不在本阶段修改 JOIN / GROUP BY 策略或公共 SQL 语义。
- 不通过 `block()`、反射或线程切换把异步表达式伪装成同步表达式。
- 不破坏已有 Feature 实现；未实现同步扩展的 Feature 自动保留原执行路径。

## 实施步骤

1. 增加内部可识别的同步 mapper / predicate 契约和兼容适配。
2. 迁移字面量、属性、二元计算、二元比较、`AND` / `OR` 等高频表达式。
3. 在 `DefaultReactorQL` 构建阶段选择同步 `filter` / `map` fast path，混合或异步表达式保留原链路。
4. 仅在查询引用 `row.index` / `row.elapsed` 时启用行跟踪包装，避免无关查询逐行创建跟踪 Map。
5. 补充行为回归和 fast-path 选择测试，再执行全量测试及性能/JFR 对比。

## 风险与待验证点

- SQL `null` 目前通过空 Publisher 表达，同步路径必须保持同样的空值行为。
- `AND` / `OR` 第一阶段保持两侧求值，避免未经确认改变异常或自定义函数调用语义。
- 多列表达式当前可能并行写入结果 Map；同步 fast path 必须只在所有列均同步时启用，异步路径不改变并发行为。
- 行跟踪检测需要覆盖 SELECT、WHERE、HAVING、GROUP BY、ORDER BY 与 JOIN 条件。

## 验证方式

- `mvn -q test`
- `mvn -q -Dtest=Benchmarks,JsonFunctionPerformanceTest -Djacoco.skip=true test`
- 分别录制 `Benchmarks#testCount` 与 `Benchmarks#testWhere` 的 JFR，比较耗时、总分配量和 `MonoZip` / `MonoDefaultIfEmpty` 分配占比。
- 验证已有自定义异步 Feature 仍可执行，取消和错误仍沿 Publisher 链传播。

## 验证结果

已完成实现与验证：

- 新增 `ScalarValueMapper` / `ScalarFilter` 可选契约；旧的 Publisher Feature 无需修改，混合或异步表达式仍走原路径。
- 字面量、属性、参数、符号表达式、二元计算、单参数函数、`cast`，以及比较、`AND` / `OR`、`BETWEEN`、`LIKE`、布尔与空值判断均可在全链同步时组合为同步执行器。
- 普通 `WHERE` 收敛为一个 Reactor `filter`，纯同步普通投影收敛为一个 `map`；checkpoint 查询保留原 Publisher 包装以维持诊断信息。
- `row.index` / `row.elapsed` 只在属性解析阶段发现引用时启用 `elapsed` / `index` 包装；现有行信息与分组行信息测试通过。
- 新增 `ScalarFastPathTest`，覆盖同步组合契约、投影/条件结果、空值语义，以及自定义异步值/过滤 Feature 的调用、错误和取消传播。

功能回归环境为 Eclipse Adoptium JDK 17.0.18、Maven 3.9.9；性能对比统一使用与基线相同的 Eclipse Adoptium JDK 21.0.10：

- `mvn -q test`：通过。
- 性能 smoke test（20,000 行）：常用数据函数 407 ms，静态 JSONPath 114 ms；与基线 324 ms / 104 ms 同量级，本次未改这些异步/多参数函数路径。
- JFR 同口径 1,000,000 行基准：

| 场景 | 基线耗时 | 优化后耗时 | 基线主线程分配 | 优化后主线程分配 |
| --- | ---: | ---: | ---: | ---: |
| `count(1)` | 约 120–140 ms | 70 ms | 821.1 MB | 539.3 MB |
| 双条件 `WHERE` | 约 320–410 ms | 91 ms | 2.0 GB | 570.9 MB |

双条件 `WHERE` 的主线程分配下降约 72%。基线分配热点 `MonoZip$ZipInner`（67.37%）和 `MonoDefaultIfEmpty`（10.82%）在优化后 allocation-by-class 热点列表中消失；优化后主要剩余成本是每行 `DefaultReactorQLRecord` 使用的 `ConcurrentHashMap` 容器。GC pause 从 6 次 / 6.57 ms 降为 3 次 / 4.56 ms。

JFR 证据保存在：

- `/private/tmp/reactorql-count-20261003.jfr`
- `/private/tmp/reactorql-count-fastpath-jdk21.jfr`
- `/private/tmp/reactorql-where-20261003.jfr`
- `/private/tmp/reactorql-where-fastpath-jdk21.jfr`

剩余风险与后续优化点：当前每行仍创建两个并发 Map，已成为新的主要分配热点；直接替换需要先收紧 `ReactorQLRecord` 的共享/并发所有权契约，避免影响异步投影和 JOIN，本次不扩大范围。

## 下一阶段：查询级操作符合并与内存优化计划

### 架构结论

可以增加 ReactorQL 自己的“组合操作符”，但第一选择不是直接实现 `CoreSubscriber` 或全局 `Hooks`，而是把 SQL 编译成同步、异步和状态型执行段：

```text
数据源 Publisher
  -> 同步段：建行、WHERE、同步投影（一个 handle）
  -> 异步段：异步 Feature、子查询、JOIN（有界 flatMap / concatMap）
  -> 状态段：DISTINCT、GROUP、WINDOW、ORDER、AGG（原生 Reactor 操作符）
  -> 同步段：HAVING、最终投影（一个 handle）
```

Reactor 2020.0.38 对应 reactor-core 3.4.34。该版本的 `Flux.handle` 会根据上游是否实现 `Fuseable` 选择 `FluxHandleFuseable`，适合将同步的一对一转换和零或一输出筛选合并为一个操作符。`transform` 只负责复用装配逻辑，本身不会自动把多个操作符合为一个；`transformDeferred` 适合为每个订阅创建独立的执行状态。

因此推荐实现“查询计划级同步段 + 标准 `handle`”，而不是立即实现低层 Reactive Streams 操作符。手写 `CoreSubscriber` 需要同时正确处理 request 补偿、cancel、同步/异步 fusion、ConditionalSubscriber、Context、discard、错误模式和多订阅隔离，收益不足时风险明显高于标准操作符方案。

### 目标

1. 提升简单过滤、投影、函数、聚合和排序场景的每秒处理行数。
2. 降低每行 Publisher、Subscriber、Tuple、List 和 Map 的分配。
3. 降低 JOIN、异步函数和分组场景的峰值在途对象数量。
4. 保持 SQL 结果、空值、错误、顺序、背压、取消、Reactor Context 和自定义 Feature 兼容。
5. 优化结论由 JMH、JFR 和行为测试共同证明，不以源码中的操作符数量作为验收依据。

### 非目标

- 不把真实异步 Feature、数据源、子查询或 JOIN 强制转换为同步调用。
- 不使用 `block()`、嵌套 `subscribe()`、`ThreadLocal` 或共享可变参数数组。
- 不通过全局 `Hooks.onEachOperator` 修改应用内其他 Reactor 流。
- 不默认改变 `AND` / `OR` 两侧求值、SQL null、错误传播和输出顺序语义。
- 不在没有 JFR 证据前实现或发布通用低层 Reactor 操作符 SPI。
- 不把无限流收集到 List，也不移除 ORDER BY、聚合和窗口的既有资源边界。

### 执行计划模型

在查询构建期引入内部不可变执行计划，建议包含三类节点：

- `SCALAR`：当前调用线程可完成，不返回 Publisher，例如属性、常量、计算、比较、同步函数。
- `ASYNC`：存在 Publisher 边界，例如异步自定义 Feature、子查询、异步数据源。
- `STATEFUL`：需要跨行状态，例如 DISTINCT、GROUP、WINDOW、ORDER、AGG。

构建器把相邻 `SCALAR` 节点编译成一个 `CompiledRowStage`。该阶段用 `Flux.transformDeferred` 创建订阅级状态，再用一个 `handle` 完成：

1. 必要时创建或复用行上下文。
2. 执行同步 WHERE；不匹配时不发出元素。
3. 执行全部同步投影。
4. 每个输入最多发出一行结果。

`ASYNC` 或 `STATEFUL` 节点切断同步段，边界之后可再次生成新的同步段。构建期完成 `instanceof`、Feature 类型和 fallback 决策，逐行执行不重复做能力发现。

每个订阅创建独立的 `RowExecutionState`，用于固定长度参数槽、投影槽、行号等临时状态。状态不得跨订阅共享；槽位每行结束后清空，避免持有上一行的大对象。查询计划本身保持不可变，可安全并发复用。

### 公共兼容契约

- 保留现有 `ValueMapFeature` / `FilterFeature` Publisher 契约。
- 继续使用已增加的 `ScalarValueMapper` / `ScalarFilter` 作为可选能力。
- 新增可选的同步多参数函数契约，例如 `ScalarFunctionFeature`；只迁移确认纯同步的内置函数。
- 旧的 `FunctionMapFeature`、子类 protected `apply(...)` 重写和外部 Feature 不自动改变执行模式。
- checkpoint 开启时在执行段边界保留可读 SQL checkpoint；若无法保持等价诊断信息，该段回退到原 Publisher 路径。
- 对外 SPI 注释必须写清同步调用、线程模型、不可阻塞、不可持有订阅级参数视图、null 和异常语义。

### 分阶段实施

#### 阶段 0：建立稳定基准和计划可观测性

1. 增加 JMH profile，避免用单次 JUnit 墙钟时间作为主要结论。
2. 固定 JDK、堆大小、GC、warmup、measurement、fork 和数据规模。
3. 基准覆盖：
   - `count(1)`；
   - 简单双条件 WHERE；
   - 多列同步投影；
   - 同步与异步混合投影；
   - 多参数字符串/日期函数；
   - JSONPath；
   - GROUP BY + 聚合；
   - ORDER BY + LIMIT；
   - 等值 JOIN；
   - 自定义异步 Feature。
4. 指标包含 rows/s、p50/p95、allocation B/op、GC 次数/暂停、峰值堆占用和主要分配类。
5. 给内部执行计划增加可测试的描述信息：段类型、同步节点、异步边界及 fallback 原因。只在构建或 debug 时输出，不做逐行日志和 trace。

退出条件：基准可重复，连续 fork 波动在可接受范围内，并能断言典型 SQL 是否进入预期执行段。

#### 阶段 1：同步段组合操作符

1. 把普通 `WHERE + SELECT` 编译为一个 `CompiledRowStage`，通过一个 `handle` 执行。
2. 支持同步段之间的组合，避免 `map -> filter -> map` 形成多个查询阶段。
3. 混合投影拆成同步列和异步列：同步列直接求值，只有异步列创建 Publisher。
4. 异步列不再从多个 inner Publisher 并发修改同一个结果 Map；inner 只发出带列序号的结果，由下游序列化汇总后一次写入结果。
5. 保持错误和取消信号沿原链传播；禁止在 handler 内订阅 Publisher。

退出条件：纯同步 WHERE/投影只有一个 ReactorQL 行处理操作符；混合投影的同步列不再创建 Mono；全量行为测试通过。

#### 阶段 2：紧凑 ReactorQLRecord

当前每行固定创建 `records` 和 `results` 两个 `ConcurrentHashMap<>(32)`。优化后 WHERE 的 570.9 MB 主线程分配中，主要热点已经是并发 Map 及桶数组。

1. 将当前行的 `name` 和 `thisRecord` 保存在直接字段，不为普通单源行创建 aliases Map。
2. 仅在 JOIN、子查询、`row.index/elapsed` 或 `addRecord(s)` 时懒创建扩展 records Map。
3. results 按查询计划的投影列数定容并懒创建；纯同步段使用单所有者紧凑容器。
4. 异步投影通过序列化汇总写入，尽量避免要求 results 本身为并发 Map。
5. 保持 `asMap()`、`getRecords(...)`、`copy()`、`resultToRecord(...)` 的外部行为和空值过滤规则。
6. 先以容量从 32 降到实际字段数作为低风险对照实验，再决定是否落地完整的懒容器布局。

退出条件：简单 WHERE 的 Map/桶数组分配至少再下降 40%，并通过 JOIN、子查询、copy、select `*` 和并行订阅测试。

#### 阶段 3：扩展同步表达式覆盖

1. 为 `IF`、可完全同步的 `CASE WHEN`、`COALESCE` 增加同步组合。
2. 为字面量 `IN` 增加同步路径；仍使用 `CompareUtils` 保持数字、日期等比较语义，不能直接用改变相等规则的 `HashSet.contains`。
3. 增加同步多参数内置函数：字符串、日期、数学和 JSON 参数装配不再逐行执行 `Flux.fromIterable -> concatMap -> collectList`。
4. 多参数函数使用订阅级固定参数槽，内置同步函数不得持有参数槽引用；外部旧函数保持 Publisher 路径。
5. JSONPath 保留静态 path 预编译；当全部参数同步时直接构造 `JsonFunctionContext` 并求值。JSON 文本解析本身不伪装为操作符问题。
6. 正则、日期格式等常量参数在构建期预编译；动态参数仍逐行安全解析并保留输入限制。

退出条件：常用函数 20,000 行基准的 Reactor 参数装配类不再是主要热点，总分配至少下降 30%；JSONPath 参数装配相关分配显著下降且解析语义不变。

#### 阶段 4：状态型链路消费同步表达式

1. `COUNT/SUM/AVG/MIN/MAX` 等聚合输入为 scalar 时使用 `flux.map`，不经 `metadata.flatMap` 创建逐行 Mono。
2. `GROUP BY` scalar key 直接生成分组键；移除 `Mono.zip(..., Mono.just(record))`。
3. `ORDER BY` scalar key 直接生成 `OrderedRecord`；多排序键使用定长数组或紧凑键对象，避免逐行 `concatMap + collectList`。
4. `DISTINCT ON` 的同步键一次求值；异步键保留旧路径。
5. JOIN 的右侧数据源仍为响应式边界，但 scalar ON 条件在已获得左右行后使用同步 `filter`。
6. HAVING 为 scalar 时使用同步 `filter`。

退出条件：聚合、分组、排序和 JOIN 的 scalar key/predicate 不出现逐行 `MonoZip`、`MonoDefaultIfEmpty` 或参数收集链，SQL 结果与顺序测试保持一致。

#### 阶段 5：并发和峰值堆边界

当前 JOIN 和部分分组处理使用 `flatMap(..., Integer.MAX_VALUE)`，可能在慢右源或大量分组时造成无界在途对象。

1. 引入明确的 `join.concurrency`、`group.concurrency` 或统一执行并发配置，并设置受控默认值和硬上限。
2. 需要保持顺序的边界使用 `concatMap` / `flatMapSequential`；允许乱序时使用有界 `flatMap`。
3. 对异步 Feature 保留取消传播，并验证并发上限不会吞掉错误或延迟终止。
4. ORDER BY、窗口和聚合继续保持已有最大行数/窗口上限，不新增无界 `collectList()`。
5. 对高延迟异步源分别测吞吐与峰值堆，选择 Pareto 边界，不能只追求最大并发。

退出条件：慢异步 JOIN 压测的最大在途行数受配置约束，固定堆下无 OOM，吞吐相对旧实现不出现未解释退化。

#### 阶段 6：低层自定义 Operator 的条件式评估

只有同时满足以下条件才实现内部 `FluxCompiledRows` / `FluxOperator`：

1. 前五阶段完成后，JFR 仍显示 Reactor subscriber/operator 调度占目标场景 CPU 或分配的 5% 以上；
2. 标准 `handle` 无法保留所需的 conditional fusion 或请求补偿；
3. 微基准证明低层实现相对 `handle` 至少再提升 10%，且收益覆盖维护成本。

若触发，必须实现和验证：

- fuseable 与非 fuseable 上游；
- `ConditionalSubscriber`；
- SYNC/ASYNC fusion negotiation；
- 过滤行后的 request 补偿；
- cancel、onError、onComplete 单次终止；
- Reactor Context 和 discard；
- request(1)、分批 request、并发订阅及重入保护。

该操作符保持包内实现，不作为用户 Feature SPI。若任何协议测试无法可靠通过，回退到标准 `handle`。

### 验证矩阵

#### 正确性

- 运行全部现有测试；当前基线为 280 tests、0 failure/error。
- 对每种 fast-path 同时执行 fuseable 源和 `.hide()` 非 fuseable 源，比较输出和错误。
- 验证 SQL null、空 Publisher、数字/日期比较、CASE/IN/LIKE、select `*`、别名和参数绑定。
- 验证同步异常、异步异常、`onErrorContinue` 既有场景和 checkpoint 信息。
- 验证 request(0 -> 1 -> N)、下游取消、上游取消、空源和无限源 `take(N)`。
- 验证同一 ReactorQL 实例的并发订阅不会共享参数槽、行号或结果状态。
- 验证 Reactor Context 在同步段、异步段和分组边界不丢失。

#### 性能

- 主要结果使用 JMH 多 fork 中位数，JUnit smoke test 只用于趋势提示。
- 每个阶段记录 JFR allocation-by-class/site、thread-allocation、gc-pauses 和 heap summary。
- 固定 JDK 21 做前后对比，同时用项目最低兼容 JDK 运行全量功能测试。
- 总体验收目标：
  - 简单 WHERE 在现有 91 ms / 570.9 MB 基础上，吞吐再提升至少 20%，分配再下降至少 40%；
  - 常用多参数函数主线程分配在 215.5 MB 基础上下降至少 30%；
  - 任一代表场景吞吐回退超过 5% 时必须解释并拆分提交，不能用其他场景收益抵消；
  - 慢异步 JOIN 的峰值堆随配置并发度有界；
  - GC 总暂停和次数不劣于基线，或有明确的吞吐/延迟取舍数据。

### 提交与回滚边界

按阶段独立提交，禁止把查询编译器、Record 布局、函数迁移和并发策略一次性混在一个不可回滚提交中：

1. 基准与执行计划描述；
2. 标准 `handle` 同步段；
3. Record 紧凑布局；
4. 同步函数和特殊表达式；
5. 聚合/分组/排序/JOIN 适配；
6. 并发边界；
7. 可选低层 Operator。

每阶段保留自动 fallback：只要表达式树包含未知或异步 Feature，就从对应边界回到已有 Publisher 实现。回滚时移除该阶段计划选择，不需要修改 SQL 或外部 Feature。

### 可观测性决定

- 不增加逐行 Trace/span，它会反向制造高频分配。
- 增加查询构建期的执行计划描述和 debug 统计，用于确认 scalar/async/stateful 分段及 fallback 原因。
- 本次没有新增常驻缓存、队列或后台执行器，因此不新增 MBean；若后续加入全局查询计划缓存，再单独设计大小、命中率、淘汰和清理能力。

## 实施结果（2026-10-03）

### 已完成范围

1. `DefaultReactorQL` 在构建期识别同步 WHERE 和同步投影，并通过包内不可变 `SynchronousRowStage` 合并为单个 `Flux.handle`；开启 checkpoint、出现异步 Feature、聚合或其他状态边界时自动回退原 Publisher 路径。
2. 混合投影在构建期拆分 scalar/async 列；scalar 列直接求值，异步列只产生带序号的值，全部完成后按列序串行写入结果，避免多个 inner Publisher 并发修改同一 Map。
3. `DefaultReactorQLRecord` 将 `name`、`thisRecord` 直接保存在字段中，aliases/results 容器按需创建；默认容器改为右侧小容量 `HashMap`。`ReactorQLRecord#getRecordValue` 与内置 `DefaultPropertyFeature` 快路为高频读取消除 `Optional`，外部 PropertyFeature 继续调用原 `getProperty` SPI。
4. `IF`、同步 `CASE WHEN`、`COALESCE`、常用字符串/日期/数学多参数函数和 JSONPath 已支持 scalar 参数装配；旧 `FunctionMapFeature` 构造器、子类覆盖点和外部异步 Feature 继续使用 Publisher 路径。
5. scalar 表达式已接入 COUNT/通用聚合、GROUP BY、ORDER BY、DISTINCT ON、HAVING 和 JOIN ON；排序键使用定长数组，避免多键逐行 `concatMap + collectList`。
6. JOIN 和多级 GROUP 的无界 `flatMap(..., Integer.MAX_VALUE)` 已替换为有界并发。新增 `join.concurrency`、`group.concurrency`，默认继承 `concurrency`（旧配置为 0 时收敛为 1），合法范围为 1..1024。
7. 查询构建期生成只读执行计划描述并写 debug 日志，例如 `SOURCE -> SCALAR[where+projection,handle]`；不增加逐行日志或 span。
8. 新增 `jmh` Maven profile 和独立 shaded benchmark JAR。默认固定 JDK 调用方、512 MB 堆、G1、3 次 warmup、5 次 measurement、2 forks，覆盖 count、WHERE、同步投影、常用函数和 JSONPath。

字面量 `IN` 本轮保留响应式路径。现有实现允许右值在运行时展开 `Iterable`、`Publisher` 和单列 Map，仅凭语法节点无法证明为单值 scalar；在没有新的多值同步契约前不以改变集合展开语义换取操作符减少。

### 验证结果

- JDK 17 全量测试：294 tests，0 failure，0 error。
- 新增协议与边界测试覆盖：单 `handle` 计划、checkpoint 回退、Fuseable 与 `.hide()`、request(1)、取消、Reactor Context、并发订阅、混合同步/异步投影、Record 懒容器、scalar 状态型链路、慢 JOIN 并发上限。
- `git diff --check`：通过。
- JDK 21 完整 JMH（3 warmup、5 measurement、2 forks、512 MB、G1，结果在 `target/jmh-result.json`）：
  - count：155.67 M rows/s，48.0 B/row；
  - 双条件 WHERE：56.44 M rows/s，48.0 B/row；
  - 同步 WHERE + 双列投影：14.17 M rows/s，256.0 B/row；
  - 常用函数：1.06 M rows/s，4536.0 B/row；
  - JSONPath：3.42 M rows/s，2040.0 B/row。
- JDK 21 JFR 同口径 `ThreadAllocationStatistics`：
  - count：517.3 MB -> 73.7 MB，下降约 85.8%；墙钟约 70 ms -> 19 ms；
  - 双条件 WHERE：548.9 MB -> 73.8 MB，下降约 86.6%；墙钟约 91 ms -> 34 ms；
  - 常用函数 20,000 行：193.5 MB -> 126.4 MB，下降约 34.7%；
  - JSONPath 20,000 行：100.7 MB -> 74.6 MB，下降约 25.9%，剩余热点主要为 JSON 文本解析和结果对象。

最终 JFR 中，简单 WHERE 的主要可控逐行分配只剩 `DefaultReactorQLRecord` 本体；`MonoZip`、`MonoDefaultIfEmpty`、参数流 `concatMap/collectList` 和 `Optional` 已不再是该场景热点。CPU 采样也未显示 Reactor subscriber/operator 调度达到 5% 门槛，因此阶段 6 的低层 `FluxOperator/CoreSubscriber` 条件不成立，本轮明确不实现。

关键 JFR 证据：

- `/private/tmp/reactorql-count-compiled-final-jdk21.jfr`
- `/private/tmp/reactorql-where-final-jdk21.jfr`
- `/private/tmp/reactorql-common-functions-final-jdk21.jfr`
- `/private/tmp/reactorql-jsonpath-compiled-final-jdk21.jfr`

### 剩余风险与后续入口

- 默认 JMH profile 已完成本机完整 2-fork 运行；合并或发布前可在固定负载的隔离机器复跑，并保存 `target/jmh-result.json` 作为跨版本长期对照。
- JOIN 并发上限已由确定性测试证明；峰值堆的长期压测仍应结合真实右源延迟分布选择 `join.concurrency`，默认继承 ReactorQL 的通用并发配置。
- JSONPath 后续收益主要来自避免重复解析同一 JSON 文本或使用已解析 Map，而不是继续手写 Reactor 操作符。
- commit hash 与 Pull Request：pending。

## 第二阶段：有界分组聚合与子查询计划

### 目标与范围

在第一阶段逐行 fast-path 之上继续优化状态型查询，优先解决窗口分组、多维分组和多聚合造成的重复订阅、分组队列及中间行分配。实现落点仍由 `DefaultReactorQL` 统一选择执行计划，内置同步能力走融合路径，外部 Feature、异步表达式和无法证明等价的查询自动回退既有 Publisher 实现。

本阶段首先实现：

1. 为 `COUNT/SUM/AVG/MIN/MAX` 等内置聚合提供增量累加契约，同一输入行只计算和分发一次；
2. 将固定数量窗口与同步复合分组键编译为一个包内有界聚合阶段，避免多层 `groupBy`、每键 `GroupedFlux` 和多聚合 `publish/refCount`；
3. 分组状态、待输出结果和集合型聚合必须有显式资源上限，溢出默认报错，不通过静默淘汰改变精确 SQL 语义；
4. 查询构建期识别不关联、关联和透明子查询，为订阅级复用、最小外层字段绑定及安全的查询层合并提供计划依据。

### 明确不做

- 不把无窗口、键空间无界的精确聚合伪装成有限内存操作；这类查询保留完成时输出语义，并通过后续的流模式/状态上限契约控制风险。
- 不在本阶段引入全局缓存、ThreadLocal、后台线程池或逐行 Trace/span。
- 不缓存外部异步 Feature 或无法证明确定性的关联子查询。
- 不改变第三方 `ValueAggMapFeature`、`GroupFeature` 和数据源 SPI；未实现增量契约的扩展继续走原执行路径。
- 不在没有事件时间、水位线和迟到数据契约前改变现有 processing-time 窗口语义。

### 实施步骤

1. 增加窗口分组、多维高基数、多聚合和多层子查询的 JMH/正确性基线。
2. 增加增量聚合契约及内置实现，编译聚合列、同步输入表达式和复合分组键。
3. 实现订阅级 `KeyedAggregateStage`，按窗口关闭或上游完成输出结果；取消、错误和并发订阅不得共享状态。
4. 把满足条件的查询接入新阶段，并在执行计划中说明融合或回退原因。
5. 增加子查询关联性分析和安全复用；透明层合并、谓词/投影下推仅在不跨越聚合、DISTINCT、ORDER BY/LIMIT、集合操作和异步边界时启用。

### 风险与验证

- 无界流无法同时获得无界键空间、精确最终值和有限堆内存；本阶段只对具有明确关闭边界的窗口启用融合聚合。
- 定时窗口涉及调度、背压和取消协议；第一批先覆盖固定数量窗口，时间窗口在虚拟时间与协议测试通过后再接入同一状态模型。
- 正确性验证覆盖空窗口、最后一个不足窗口、复合键、null 键、多个聚合、HAVING、request(1)、取消、并发订阅、checkpoint 和自定义 Feature 回退。
- 性能验收以旧分组链路为对照：5 个内置聚合只扫描输入一次；窗口数和活跃键固定时 live heap 不随运行时间线性增长；窗口聚合吞吐提升且每行分配显著下降。
- 新增状态仅存在于单次订阅生命周期，不注册常驻 MBean；执行计划继续作为构建期可观测入口。

### 第二阶段实施结果（2026-10-03）

#### 融合窗口与分组聚合

1. 新增可选 `IncrementalValueAggMapFeature` 契约，内置 `count`、`sum`、`avg`、`min`、`max` 在同步输入下创建订阅/分组/窗口独立的累加器；第三方聚合或不能证明等价的表达式自动回退原 Publisher 路径。
2. 新增包内 `WindowedAggregateStage`，单次扫描完成同步复合键与多个内置聚合，不再为每个键创建 `GroupedFlux`，也不为每个聚合重复订阅分组流。当前接管：
   - `_window(n), key...` 固定数量窗口；
   - `key..., _window(n)` 按键独立计数窗口；
   - 位于首个分组表达式的 processing-time `_window('duration')`；
   - 无窗口同步键在上游完成时输出；
   - 同步投影、同步 HAVING、复合键与 `_group_by_key`。
3. 融合阶段所有可变状态按订阅隔离，并验证 request/backpressure、取消、并发订阅和错误释放。`group.maxActiveKeys` 默认 65,536、硬上限 1,000,000；超过上限明确返回资源限制错误，不静默淘汰精确结果。
4. `aggregate.fastPath=false`、checkpoint、异步键/聚合、第三方非增量聚合、`DISTINCT/UNIQUE` 聚合、滑动窗口，以及位于分组键之后的时间窗口继续使用兼容路径。

#### 集合型聚合资源边界

1. 新增统一的 `aggregate.maxCollectionSize`，默认 65,536、硬上限 1,000,000，覆盖 `collect_list`、`count(distinct ...)`、`count(unique ...)`、`distinct_count`，以及通用聚合的 `DISTINCT/UNIQUE` 修饰符。
2. 集合状态由单个订阅串行更新，移除聚合路径中的 `ConcurrentHashMap`；达到上限时返回稳定的 `RESOURCE_LIMIT` 错误。窗口关闭后对应集合状态释放，配置非法值在构建查询时拒绝。
3. `DISTINCT` 值仍采用有界流式发出，保留 `take(distinct value, n)` 的提前取消语义；`UNIQUE` 因必须看到完整输入，继续在窗口或上游完成时输出。

#### 多层子查询

1. 构建期保守识别不关联子查询；不引用外层列、参数、用户变量或未知函数的子查询，在一次根订阅内只执行并物化一次。缓存放在 Reactor Context 的 `SubscriptionContext` 中，不跨订阅、不使用全局状态或 `ThreadLocal`。
2. `subquery.cache=false` 可关闭复用；`subquery.maxRows` 默认 65,536、硬上限 1,000,000，避免不关联子查询把无界结果整体物化到堆。
3. 修正派生表作用域：派生表自己的 alias 只在其查询体分析完成后可见；JOIN 右侧子查询仍可看到已经注册的左侧内部来源。发现外层相关、WITH、运行时参数或未知 FROM/函数时保守回退逐行执行。
4. 执行计划通过 `OPTIMIZED[subquery-cache,...]` 暴露订阅级缓存选择；当前未做关联子查询 decorrelation、谓词下推或透明层物理合并，避免跨越聚合、排序、LIMIT 和异步边界改变语义。

#### 第二阶段验证结果

- JDK 17 全量测试：313 tests，0 failure，0 error。
- `git diff --check`：通过。
- 固定窗口、多维键、五聚合快速 JMH（1,000,000 行、32 键、512 MB、G1）：融合路径 16.21 M rows/s、52.764 B/row；Publisher 路径 5.72 M rows/s、336.897 B/row。吞吐约 2.83 倍，每行分配下降约 84.3%。
- 不关联子查询快速 JMH（20,000 行/调用、512 MB、G1、2 warmup、3 measurement、1 fork）：订阅级缓存 3.13 M rows/s、1512.078 B/row；关闭缓存 1.87 M rows/s、3191.938 B/row。吞吐约 1.67 倍，每行分配下降约 52.6%。

#### 仍保留的风险与后续入口

- 无窗口精确聚合只有在同步内置聚合进入融合阶段时才由 `group.maxActiveKeys` 限制键空间；`DISTINCT/UNIQUE`、第三方聚合和异步键回退到旧分组链路时，虽然单个集合状态已有上限，但分组总数仍可能随无界键空间增长。后续应给兼容分组执行器增加同一语义的 owner 级键上限，或要求显式窗口/TTL 流模式。
- 兼容分组链路仍保留 Reactor `groupBy(..., Integer.MAX_VALUE)` 的既有 prefetch。直接改成较小固定值并不安全：高基数 `groupBy` 配合低并发下游消费时，未订阅分组可能填满缓冲并阻止上游完成。后续应以统一的有界 keyed-state/window owner 替换该兼容链路；不能把参数调小当作内存治理。
- `key..., _window('duration')` 仍保留每个父分组独立计时器的旧语义，尚未融合；事件时间、水位线、迟到数据和状态 TTL 需要单独的公开契约，不能由 processing-time 优化隐式代替。
- 关联子查询仍逐行执行。下一步高收益方向是对等值相关子查询做半连接/哈希索引式 decorrelation，并为缓存键数、单键结果数和生命周期建立显式上限；在此之前不引入全局结果缓存。

## 第三阶段：有界查询状态与消费感知子查询

### 目标与影响范围

在不改变精确 SQL 语义的前提下，继续收紧可能随无界流线性增长的查询状态，并让子查询按上层实际消费模式执行。Owning module 仍为 ReactorQL 根模块，实现集中在包内 `internal` 状态边界和现有 DISTINCT、集合运算、聚合、子查询 Feature。

本阶段实现：

1. 提供通用的配置上限解析、容量检查与订阅级流式去重能力，避免各 Feature 重复实现资源错误契约。
2. 为普通 `SELECT DISTINCT` 增加 `distinct.maxRows` 精确状态上限；重复键不占新容量，每次订阅独立建立状态。
3. 为 `UNION` / `INTERSECT` / `EXCEPT` / `MINUS` 增加 `setOperation.maxRows` 上限；`UNION ALL` 继续流式输出，其他运算只物化语义所必需的一侧有界键集，另一侧流式过滤并去重。本阶段保持已有 `EXCEPT` 方向契约，不在性能优化中夹带兼容性变更。
4. `collect_row` 共享 `aggregate.maxCollectionSize`，保持同键后值覆盖前值，并在 key/value 都为 scalar 时接入增量窗口聚合。
5. 不关联 `EXISTS` 缓存订阅级布尔结果，通过 `hasElements()` 在首行后取消子查询，不再为布尔判断物化全部结果。

### 明确不做

- 不使用全局缓存、`ThreadLocal`、嵌套 `subscribe()` 或 `block()`。
- 不用 LRU、TTL 或随机淘汰让精确 DISTINCT/集合运算静默产生错误结果。
- 不针对特定 SQL 字符串增加快路，不一次性重写 GROUP/JOIN，不实现手写 `CoreSubscriber`。
- 不在未确认语义前推断 `INTERSECT ALL` / `EXCEPT ALL` 的重复计数规则；本批先保持项目已支持的集合语义。

### 风险与验证

- DISTINCT 和集合运算需验证重复值、空结果、左右方向、下游取消与多订阅隔离；超限统一返回 `RESOURCE_LIMIT`。
- `collect_row` 需验证重复 key 覆盖、新 key 超限、窗口释放，以及增量与 Publisher fallback 结果一致。
- `EXISTS` 需用“首行 + never”子查询证明早停取消，并验证根订阅内仅执行一次、根订阅之间不共享、关联子查询不误缓存。
- 本批不增加逐行 trace 或 MBean：状态只存活于查询订阅，配置和稳定资源错误已是 owner 级运维边界。
- 阶段完成后集中运行定向测试、全量测试、`git diff --check` 和相关 JMH；不在每个小修改后反复构建。

### 第三阶段实施结果（2026-10-03）

1. 新增包内 `BoundedStateSupport`，统一配置解析、硬上限校验、有界 Map/Set 和订阅级流式去重。常规未满容量路径只执行一次 `add/put`；达到上限后才额外区分重复键与新键。所有超限均以 `RESOURCE_LIMIT` 终止，不使用会改变精确结果的淘汰策略。
2. 普通 DISTINCT 使用 `distinct.maxRows`，集合运算使用 `setOperation.maxRows`；默认均为 65,536，硬上限均为 1,000,000。`UNION ALL` 保持流式；`UNION` 流式去重；`MINUS/EXCEPT` 只物化排除侧；`INTERSECT` 物化一侧并在命中输出时删除键，逐步释放状态。现有 `EXCEPT` 右减左兼容契约保持不变。
3. `collect_row` 复用 `aggregate.maxCollectionSize`，相同 key 覆盖不增加容量。同步 key/value 直接累加并接入融合窗口聚合；异步兼容路径继续使用 Publisher，任一值为空的行仍被忽略。
4. `EXISTS` 增加消费感知执行：不关联子查询在单次根订阅内缓存一个布尔终态，`hasElements()` 在首行后立即取消子查询；关联子查询仍逐外层行执行。缓存使用 Reactor Context、`replay(1).refCount(1)` 和 `singleOrEmpty()`，根订阅取消时取消未完成上游，且不跨根订阅共享。

验证环境为 JDK 17.0.18、512 MB 堆、G1：

- 全量测试：322 tests，0 failure，0 error；覆盖 DISTINCT/集合操作上限、重复键容量、取消与订阅隔离、`collect_row` 覆盖/溢出/窗口释放、EXISTS 首行取消/缓存边界/关联回退。
- `git diff --check` 与 `mvn -q -Pjmh -DskipTests package`：通过。
- 聚焦 JMH（2 warmup、3 measurement、1 fork）：
  - `collect_row` 融合路径 27.58 M rows/s、48.282 B/row；Publisher 路径 24.50 M rows/s、54.942 B/row。吞吐提升约 12.5%，每行分配下降约 12.1%。
  - 不关联 EXISTS 缓存路径 8.22 M rows/s、624.095 B/row；关闭缓存 2.40 M rows/s、2391.966 B/row。吞吐约 3.43 倍，每行分配下降约 73.9%。
  - 普通不关联多值子查询缓存路径 2.76 M rows/s、1688.085 B/row；关闭缓存 1.84 M rows/s、3191.938 B/row。吞吐约 1.50 倍，每行分配下降约 47.1%，取消感知共享实现未造成回退。

剩余风险：普通 DISTINCT 和精确集合操作虽然堆占用已有硬边界，但在无界流上仍需等到重复或新键触发上限，且 INTERSECT/MINUS 的被物化一侧若自身不完成则不会输出。这是精确集合语义的固有限制；持续流应使用显式窗口或上游边界，而不是在执行器内隐式 TTL/淘汰。

## 第四阶段计划：高基数分组常驻内存治理

### 状态

已完成（2026-10-03）。切片 A-D 已实施并验证；切片 E 未被当前 live-set 证据触发，继续保留为证据驱动的后续入口。

### 已确认事实与架构决定

1. 精确聚合无法同时满足“无界键空间、永不关闭、有限堆内存”。本阶段采用精确且有界的契约：窗口关闭时释放状态；无窗口或兼容链路超过明确上限时返回 `RESOURCE_LIMIT`，不通过 TTL、LRU 或随机淘汰静默改变结果。
2. 融合路径已用 `group.maxActiveKeys` 限制状态数量，但每个 `GroupState` 仍持有完整 `lastRecord`、聚合器数组和分组键数组。宽行输入会让常驻堆随“活跃键数 × 原始行宽度”增长，即使查询只输出分组键和 `count/sum`。
3. 窗口关闭时当前实现先把全部分组转换成 `List<ReactorQLRecord>`，形成“旧分组状态 + 全部输出行”同时存活的峰值；严格背压或慢下游会延长该列表的存活时间。
4. 兼容路径仍使用 Reactor `groupBy(..., Integer.MAX_VALUE)`。简单调小 prefetch 会在高基数、低并发分组消费时产生停滞风险，因此必须同时治理活跃键和未消费行，而不是孤立修改参数。
5. 本阶段继续使用 Reactor 原生 `defer/deferContextual`、`handle`、`concatMap`、`using`、`doFinally` 等组合，不实现自定义 `CoreSubscriber`，不引入全局缓存、线程池或 `ThreadLocal`。

### 目标

1. 对标准的“同步分组键 + 增量聚合 + 窗口”查询，使常驻堆主要由分组键和真实聚合状态决定，不再保留无关的完整输入行。
2. 窗口关闭后按下游 demand 惰性生成结果，避免高基数窗口关闭瞬间复制出另一份完整结果集。
3. 将 `group.maxActiveKeys` 覆盖到兼容分组链路，并为兼容路径增加全局未消费行上限，保证慢下游或大量未订阅分组以稳定资源错误结束，而不是持续占用堆。
4. 保持现有 SQL 结果、插入顺序、空值、HAVING、最后一行兼容语义、背压、取消、错误、Reactor Context 和多订阅隔离。
5. 低基数常规查询吞吐不得出现无法解释的回退；优化以 retained heap/live set 为主指标，不能用更高分配率换取表面上的对象数量下降。

### 影响范围与 owning module

Owning module 仍为 ReactorQL 根模块，预计影响：

- `WindowedAggregateStage`：紧凑分组状态、输出计划和惰性窗口排空。
- `DefaultReactorQL`：执行计划描述、兼容分组预算装配和配置校验。
- `GroupByValueFeature` / `GroupByBinaryFeature`：兼容路径的订阅级键数与排队预算。
- 包内 `internal`：增加统一的 `GroupStateBudget`，只管理单次查询订阅的逻辑资源单位。
- `IncrementalValueAggMapFeature` 及内置集合聚合：仅在第二交付切片需要时增加可选的 retained-entry 统计能力；旧实现必须保持二进制和源码兼容的默认行为。
- 测试与 JMH：新增高基数、宽行、慢消费及资源释放场景。

### 明确不做

- 不为保持进程存活而静默删除活跃分组，不默认引入 TTL、近似聚合或采样。
- 不把精确状态溢写到 RocksDB、文件或远程存储；当前模块没有相应事务、序列化和清理契约，复杂度与运行成本过高。
- 不改变 `key, _window('duration')` 的“每个父分组独立 processing-time 窗口”语义，不把它偷换成全局对齐窗口。
- 不为未知第三方非增量聚合猜测状态大小；无法证明有界时保留兼容路径并由查询级预算保护。
- 不按特定 SQL 字符串或字段名特调；所有选择基于表达式能力、投影依赖和状态类型。
- 不新增逐行日志、span 或 MBean。状态生命周期属于单次订阅，构建期执行计划和稳定资源错误足以定位；若以后引入全局状态后端再单独设计运维接口。

### 配置契约

1. 复用 `group.maxActiveKeys`：同时约束融合和兼容路径的活跃分组数；默认 65,536，硬上限 1,000,000，不改变已有配置含义。
2. 新增 `group.maxBufferedRows`：只统计兼容 `GroupedFlux` 路径中已经从上游接收、但尚未交给实际分组消费者的原始行。建议默认 65,536、硬上限 1,000,000；消费、窗口关闭、取消或错误时释放计数。
3. 保留 `aggregate.maxCollectionSize` 的单集合上限。若基准证明“活跃键数 × 集合聚合状态”仍是主要 live heap，再在第二切片增加 `aggregate.maxTotalCollectionSize`，约束单个订阅/窗口内所有内置集合聚合的总 retained entries；不以不可靠的对象字节估算作为运行时门禁。
4. 所有配置在构建查询时校验，超限统一产生包含 setting、建议和示例的 `RESOURCE_LIMIT`；不在逐行热路径重复解析配置。

### 实施步骤

#### 切片 A：建立高基数常驻内存基线

1. 增加三组固定负载：
   - 1,000 / 10,000 / 50,000 个活跃键，`count/sum/avg`，固定数量窗口；
   - 同样键数但每行附带 1–4 KB 无关 payload，用于识别 `lastRecord` 保留成本；
   - 兼容路径（异步键、checkpoint 或非增量聚合）配合 request(1) / 慢消费。
2. JMH 记录 rows/s、B/row、GC 次数；使用受控暂停点配合 JFR `ObjectCountAfterGC` 或 `jcmd GC.class_histogram` 记录窗口未关闭、窗口关闭待输出和取消后三个时点的 live set。JUnit 不断言具体 MB，避免把 GC 时机写成脆弱测试。
3. 分别统计 `GroupState`、`DefaultReactorQLRecord`、原始 payload Map、窗口结果行和 Reactor group queue 的 retained 数量，确认后续每个切片解决的对象所有者。

#### 切片 B：紧凑融合分组状态

1. 在查询构建期分析投影依赖，把输出分为：直接分组键、聚合结果、简单常量，以及确实依赖原始最后一行的兼容投影；只保存最终的 `retainSourceRecord` 执行计划决定，不增加逐行判断。
2. 对只依赖分组键和聚合结果的标准查询，`GroupState` 不再保存 `lastRecord`；输出时使用 Reactor Context、分组键和累加器结果创建紧凑结果记录。无法证明安全的表达式继续保留最后一行，不改变现有宽松 SQL 兼容行为。
3. 对 `_window(...)` 位于首个分组位置的常见形态继续复用单一 `globalPrefix`，避免创建前缀 Map；复合前缀/按键独立窗口仍使用现有结构。本阶段继续使用 JDK Map，不引入自定义紧凑哈希表。
4. 保持 LinkedHashMap 的首次出现顺序；复合键只在首次出现时复制，查询时继续复用临时 lookup key。

#### 切片 C：窗口结果惰性排空

1. 将 `close(...) -> List<ReactorQLRecord>` 改为包内 `ClosedGroupWindow`：窗口关闭时只从活跃状态表中摘除所有权，不立即创建全部输出记录。
2. 使用 `Flux.fromIterable(GroupState)`、`handle` 与 `concatMap(..., 0)` 按 demand 转换一个 `GroupState`；iterator lookahead 只预读已有状态引用，不提前执行 `Accumulator.result()`。每发出或过滤一个结果就释放对应累加器、最后行引用和键引用。
3. 任一时刻最多排空一个关闭窗口；取消、错误和 HAVING 过滤都必须执行 `ClosedGroupWindow.close()`，释放尚未发出的分组。
4. 不手写订阅协议。若 Reactor 3.4 的标准组合无法同时保证零预取和可靠清理，先保留单窗口对象而不实现低层 operator，并以测试证据决定是否继续。

#### 切片 D：兼容 GroupedFlux 预算

1. 用 `transformDeferred`/`defer` 为每个分组层和每次订阅创建 `GroupStateBudget`，在进入 `groupBy` 前计算键并预留活跃键/排队行；重复键不增加活跃键计数。
2. 分组消费者收到一行时释放一个 buffered-row 单位；窗口完成、取消、错误和被丢弃元素统一清理。计数若存在并发消费，只在预算层使用原子计数，不把业务状态改成并发 Map。
3. 保留 `groupBy(..., Integer.MAX_VALUE)`：Reactor 3.4 会把该值映射为 256 大小的小块链式队列；若改为 `group.maxBufferedRows + 1`，该值还会成为每个分组队列的数组块大小，高基数下反而显著放大堆占用。前置预算门禁仍能读取并拒绝第一个超限行，不会因低 prefetch 停滞。
4. 执行计划标记 `STATEFUL[group, maxActiveKeys=..., maxBufferedRows=...]`，但不增加逐行统计和日志。

#### 切片 E：集合聚合的总量预算（证据触发）

1. 若切片 A/B 后 live heap 显示 `collect_row` 等集合累加器成为主热点，增加订阅/窗口级 retained-entry 预算；新增 entry 预留单位，重复 key 覆盖不增加，窗口关闭或取消释放。
2. 通过向 `AccumulatorFactory` 增加带默认实现的可选预算创建方法，保持现有第三方实现兼容；只有内置、可准确计数的集合累加器接入总量预算。
3. 无法准确报告状态大小的第三方累加器继续受活跃键和兼容排队上限保护，不伪造“已完全按字节有界”的承诺。

### 正确性与协议验证

- 窗口内第 N 个键成功、第 N+1 个新键报 `RESOURCE_LIMIT`；重复键不重复占用预算。
- 窗口关闭后预算归零，下一窗口可再次使用全部容量；无窗口精确聚合只在上游完成或达到上限时终止。
- 优化路径与原路径在限制内对 group key、聚合值、HAVING、空值、插入顺序、最后一行兼容投影保持一致。
- request(1) 时只创建一个输出记录；取消后上游被取消，活跃状态、关闭窗口和 buffered-row 预算全部释放。
- 覆盖 Fuseable、`.hide()`、异步源、时间窗口虚拟时间、多级分组、checkpoint、异步键、第三方非增量聚合及同一 ReactorQL 实例并发订阅。
- 禁止通过 sleep、吞异常或放宽断言验证；时间窗口使用 virtual time，生命周期使用可观察预算计数和取消信号。

### 性能与内存验收标准

1. 标准 `group by _window(n), key` + 标量聚合、宽行输入场景：窗口未关闭时 retained payload 数量不再与活跃键数线性增长；50,000 键基准的 post-GC live heap 相对当前实现目标下降至少 40%。
2. 窗口关闭且下游 request(1) 时，不同时保留“全部 GroupState + 全部 ReactorQLRecord 结果列表”；结果对象数量应随实际 demand 增长，而不是随窗口键数一次性增长。
3. 兼容路径中逻辑 buffered rows 不超过 `group.maxBufferedRows`；超过后稳定报资源错误，不出现超时、停滞或 OOM。
4. 低基数（32 键）现有融合基准吞吐回退不得超过 5%，B/row 不得增加；若紧凑状态使高基数吞吐提升则记录但不以吞吐换取更高 live heap。
5. 全量测试、`git diff --check`、JMH 和高基数 JFR/heap histogram 在阶段末集中执行；不在每个机械编辑后重复构建。

### 风险与停止条件

- 运行时只按逻辑单位限流，不能承诺精确字节数；超大 key/value 仍应由上游输入契约限制。若需要真实字节配额，必须先定义可移植的序列化/估算契约，不能依赖 JVM 对象布局猜测。
- 惰性排空若无法用 Reactor 标准操作符可靠覆盖取消和 discard，不进入自定义 subscriber；保留现有输出方式并交付已验证的紧凑状态收益。
- 兼容路径预算会把过去可能最终 OOM/停滞的查询变成明确资源错误，这是预期保护行为；不会通过自动增大默认值掩盖高基数查询设计问题。
- TTL/淘汰、事件时间、水位线、迟到数据和状态后端属于新的公开语义，另立任务并先确认契约，不混入本阶段。

### 第四阶段实施结果（2026-10-03）

1. `WindowedAggregateStage` 在构建期保守分析投影和 HAVING 依赖。标准“分组键 + 增量聚合 + 简单常量”计划标记 `retainSourceRecord=false`，每个活跃键只保留分组键和真实累加器；非分组列、复杂投影或未知函数继续保存最后一行，保持既有宽松 SQL 兼容行为。
2. 关闭窗口改由 `ClosedGroupWindow` 持有尚未消费的 `GroupState`。下游 demand 到达后，`fromIterable(GroupState) -> handle(toRecord)` 才物化一条结果；HAVING 过滤、正常完成、错误和取消都释放对应状态。request(1) 回归证明只调用一次 `Accumulator.result()`，没有 iterator lookahead 导致的第二条结果预创建。
3. 新增包内 `GroupStateBudget`。同一根订阅的同一分组层通过 `SubscriptionContext` 共享总预算，每个窗口或父分组建立独立作用域；重复键不重复占用活跃键，行交给真实分组消费者时释放 buffered-row，窗口完成、错误和取消兜底释放余量。`group.maxActiveKeys` 同时覆盖融合和兼容路径，新增 `group.maxBufferedRows`，两者默认 65,536、硬上限 1,000,000，超限统一返回 `RESOURCE_LIMIT`。
4. 兼容分组只用标准 Reactor 组合和薄 `GroupedFlux` 委托，不实现 `CoreSubscriber` 状态机。预算错误由外层只传播一次，避免 `groupBy` 向多个已打开分组复制同一错误而产生 `onErrorDropped`；分组身份在 ReactorDebugAgent 和 checkpoint 下保持不变。二元异步键计算也改为复用 metadata 的有界 `flatMap`。
5. 未实现 `aggregate.maxTotalCollectionSize`。本阶段的 50,000 键标量聚合 live set 主要由真实 `GroupState`、键和标量累加器组成，宽 payload 保留问题已消除；没有证据支持现在扩大增量聚合 SPI 和所有集合累加器的复杂度。集合状态继续受 `aggregate.maxCollectionSize` 单集合上限以及分组键/排队总预算保护。

验证环境为 JDK 17.0.18、G1：

- 全量测试：331 tests，0 failure，0 error；覆盖紧凑/兼容投影、HAVING、request(1)、取消、`.hide()`、异步源、checkpoint、多级分组、窗口预算释放、重复键、N+1 新键、buffered-row 超限、并发订阅隔离和 ReactorDebugAgent。
- `git diff --check` 与 `mvn -q -Pjmh -DskipTests package`：通过。
- 50,000 唯一键、每行 1 KB 无关 payload 的最终 JMH（1 warmup、2 measurement、1 fork、512 MB）：2.307 M rows/s、2411.373 B/row；阶段前为 1.088 M rows/s、2847.368 B/row。吞吐约提升 112.0%，每行分配下降约 15.3%；每轮 GC 约从 68 次降到 18 次，GC 时间约从 270.5 ms 降到 20 ms。
- 最终二进制在同一高基数负载、64 MB 堆中完成，约 1.52 M rows/s，无 OOM 或停滞。
- 未关闭窗口的 `jcmd GC.class_histogram` 对照：紧凑投影 post-GC `byte[]` 为 60,209 个 / 3.25 MB；显式投影 payload、必须保留最后一行的兼容计划为 110,216 个 / 55.25 MB。紧凑计划少存活约 50,000 个 payload、52.0 MB，payload 字节 live set 下降约 94.1%，且没有 50,000 个 `DefaultReactorQLRecord` 常驻。
- 低基数 32 键成对 JMH（2 warmup、3 measurement、1 fork）：融合 14.92 M rows/s、51.729 B/row，Publisher 5.07 M rows/s、338.102 B/row，倍率约 2.94；旧记录为 16.21 M / 5.72 M，倍率约 2.83。两条路径在当前环境同比下降，但融合相对 Publisher 没有回退，每行分配较旧融合记录 52.764 B/row 下降约 2.0%。

剩余边界：逻辑预算不能替代 key/value 的字节大小契约；精确无窗口聚合达到上限仍会明确失败。需要 TTL、近似聚合、事件时间或外部状态后端时，应作为新的公开语义单独设计，不能在当前执行器内隐式淘汰。

## 第五阶段（已完成）：默认兼容与下一批性能热点

2026-10-04 明确兼容要求：本分支新增的资源保护不得在 setting 缺省时改变原有查询可接受的数据规模或异步并发行为。该要求覆盖并替代前述阶段中“新增限制默认有限”的计划；结构性内存优化、显式配置后的资源保护和既有 `ORDER BY` 契约继续保留。

### 目标与影响范围

1. 将本分支新增的分组、集合聚合、`DISTINCT`、集合运算和子查询结果上限改为显式启用；未配置时使用原有无有限上限语义，不因达到 65,536 行或键而产生新的 `RESOURCE_LIMIT`。
2. `join.concurrency`、`group.concurrency` 未配置时恢复本分支前的 `Integer.MAX_VALUE` 并发参数；只有显式配置新 setting 时才校验 1～1,024 并启用有界并发。
3. 显式 setting 仍执行类型、正数和硬上限校验；配置后的 N/N+1、重复值、释放、取消和错误语义保持不变。
4. owning code 为 `DefaultReactorQL`、`BoundedStateSupport`、`StatefulAggregationSupport`、`GroupStateBudget`、`SelectFeature`、`DefaultDistinctFeature`、`SubSelectFromFeature` 及相应测试。`OrderBySupport` 的 10,000 行缺省上限在本分支前已经存在，本阶段不改变。

### 明确不做

- 不通过把默认值从 65,536 调成另一个经验数字伪装兼容；缺省与显式受限必须是两个明确执行计划。
- 不移除显式资源保护，不引入 TTL、LRU、随机淘汰或近似聚合。
- 不用全局缓存、`ThreadLocal`、嵌套 `subscribe()` 或自定义 `CoreSubscriber` 降低分配。
- 本阶段只修正默认兼容；下述性能候选按独立证据门禁实施，不一次扩大为高复杂度重构。

### 实施步骤

1. 为可选资源上限提供统一解析结果：setting 缺省返回 `unbounded`，显式配置才应用当前硬上限校验。执行计划打印 `unbounded`，避免用 `Integer.MAX_VALUE` 冒充用户配置值。
2. 融合聚合仅在显式配置 `group.maxActiveKeys` 时检查活跃状态上限。兼容 `GroupedFlux` 在两个分组 setting 都缺省时直接使用原有 `groupBy` 链路，不创建预算对象、额外 `handle`、键集合或原子计数；只配置其中一个上限时跳过另一个维度的热路径统计。
3. 集合聚合、`DISTINCT`、集合运算和不相关子查询缓存保留现有算法；缺省时不触发新增容量错误，显式设置时沿用当前错误码和建议。
4. 新增大于 65,536 的默认兼容回归，并保留小上限下的 N/N+1、重复值不计容量、多订阅隔离与释放测试。阶段末统一运行全量测试、JMH 构建和高基数/兼容分组成对基准。

### 验收标准

- 未配置新增 setting 时，超过 65,536 个唯一键、去重值、集合元素、集合运算键或可缓存子查询行不会产生本分支新增的资源错误；显式配置后的限制仍精确生效。
- 缺省兼容分组不执行预算 bookkeeping；相对当前有限默认版本，Publisher 兼容基准吞吐不下降、B/row 不增加。
- 融合高基数状态压缩、惰性结果排空及宽 payload live-set 收益保持不变。
- 不改变结果值、顺序契约、取消、背压、错误传播、第三方 feature/SPI 和 ReactorDebugAgent 行为。

### 下一批性能证据与优先级

当前短 JMH（JDK 17.0.18、1 warmup、2 measurement、1 fork）显示：

| 路径 | 吞吐 | 分配 | 判断 |
| --- | ---: | ---: | --- |
| `count` | 168.32 M rows/s | 48.0 B/row | 已接近记录包装下限，不优先 |
| `windowAggregates` | 15.43 M rows/s | 51.7 B/row | 融合路径健康，不继续增加复杂度 |
| `projection` | 16.39 M rows/s | 256.0 B/row | 有收益但低于函数/子查询热点 |
| `subquery` | 2.83 M rows/s | 1688.1 B/row | 高收益、可低复杂度改进 |
| `jsonPath` | 3.36 M rows/s | 2024.0 B/row | 高分配但语义面较宽 |
| `commonFunctions` | 1.07 M rows/s | 4584.0 B/row | 当前最大通用 CPU/分配热点 |

JFR 进一步显示：函数路径主要分配来自日期字符串解析中的 JDK 日期/时区/正则临时结构，以及 `FunctionMapFeature` 每次调用创建参数容器；JSONPath 主要来自 Jayway 解析时创建完整 `LinkedHashMap`、字符串和字节数组；缓存子查询仍在每个外层行上重新组装 `deferContextual`、`singleOrEmpty`、`flatMapMany`、`FluxIterable` 等操作符对象。

后续按以下顺序单独实施和验收：

1. **P1：复用订阅缓存的最终 Publisher 图。** `SubscriptionContext` 按计划 key 缓存已经完成 `singleOrEmpty` 或 `flatMapMany` 组装的 Mono/Flux，而不是每个外层行重新创建相同操作符；嵌套 `DefaultReactorQL` 仅在根订阅没有 `SubscriptionContext` 时创建新实例，使多层不相关子查询复用同一订阅生命周期。继续使用 `replay/refCount` 保证全部等待者取消时能取消上游，不改成脱离根生命周期的永久 `cache()`。增加两层/三层子查询、空值、多行、错误、取消和并发订阅基准。
2. **P1：编译固定参数函数与日期常见格式快路。** 为同步内置函数增加固定 arity 调用计划，避免每行创建参数 `ArrayList`；SQL 常量参数在构建期固化。`CastUtils` 对当前 `hsweb-utils` 明确支持的固定宽度 SQL/ISO 日期格式使用无 formatter、无正则的数字解析快路，其他既有格式回退当前 `DateFormatter`，不缩小或扩大输入兼容面，不增加无界字符串缓存。以 `commonFunctions` 的 B/row 和吞吐为主验收，并覆盖时区、数字时间戳、非法格式及嵌套函数。
3. **P2：JSONPath 简单路径流式读取可行性验证。** 仅当标准对象属性/数组索引能够通过现有 JSON provider 或已有依赖实现等价的编译路径，并在真实不同 JSON 输入下稳定降低至少 30% 分配时再落地；复杂过滤、递归、脚本路径继续使用 Jayway。禁止用按输入值缓存解析树获得基准收益，避免高基数 JSON 反而扩大常驻堆。
4. **暂不优先：聚合器状态布局和 Record 结构共享。** 当前窗口融合只有 51.7 B/row，继续合并累加器对象的收益不足以覆盖 SPI 与类型语义复杂度；普通投影 256 B/row 也低于上述热点。只有后续 profile 显示它们成为主要瓶颈时再启动。

执行顺序为默认兼容修正、P1 子查询 Publisher 图复用、P1 固定参数函数与日期解析优化；各切片分别完成定向测试后在阶段末统一做全量验证。P2 JSONPath 仅在等价实现足够简单且真实不同输入的分配收益达到门槛时落地。

### 第五阶段实施结果（2026-10-04）

1. 新增资源限制恢复为显式启用。`group.maxActiveKeys`、`group.maxBufferedRows`、`aggregate.maxCollectionSize`、`distinct.maxRows`、`setOperation.maxRows`、`subquery.maxRows` 缺省均为 `unbounded`；`join.concurrency`、`group.concurrency` 缺省恢复为优化前的 `Integer.MAX_VALUE`。只有显式 setting 才执行正数和硬上限校验。优化前已经存在的 `orderBy.maxRows=10000` 保持不变。
2. 缺省兼容分组不再创建 `GroupStateBudget`、键集合、原子计数或额外 `handle`；只配置单个分组限制时只统计对应维度。执行计划明确打印 `unbounded`，避免把 `Integer.MAX_VALUE` 展示成用户配置。超过原 65,536 阈值的高基数分组、`collect_list`、`DISTINCT`、集合操作和缓存子查询回归均通过；显式小上限的 N/N+1、重复值、释放、取消和订阅隔离语义继续通过。
3. `SubscriptionContext` 现在按查询计划身份缓存最终组装完成的 `Mono`/`Flux`，不再为每个外层行重复创建 `singleOrEmpty`、`flatMapIterable` 等 Publisher 图。嵌套查询继承根订阅的同一 `SubscriptionContext`；缓存仍使用 `replay(1).refCount(1)`，所有等待者取消后会取消上游，不使用跨订阅永久 `cache()`。两层和三层不相关子查询都只订阅底层 lookup 一次。
4. 同步内置函数通过 `ScalarValueMapper` 直接求值；SQL 字面量在构建期固化。0～3 个实参使用单对象、字段布局的可变 `List`，扩容时透明升级为 `ArrayList`，保留自定义函数原有的 `List` 修改契约。`CastUtils` 仅对 `hsweb-utils 3.0.4` 原本支持的 `yyyy-MM-dd`、空格或 `T` 分隔的秒级时间及三位毫秒格式做固定偏移数字解析；日期类型、毫秒精度、非法格式和其他格式继续保持原行为并回退 `DateFormatter`。
5. JSONPath 未增加新的执行器。JFR 证明主要分配来自 Jayway/JSON-smart 将字符串物化为完整 `LinkedHashMap`、条目、字符串和字节数组；当前 JSON 深度/容器限制又要求完整验证。现有依赖没有提供同时满足简单路径流式读取、复杂路径回退和完整限制语义的低复杂度扩展点。手写 scanner、按输入值缓存解析树或新增解析状态机均不符合本阶段复杂度与高基数内存约束，因此按 30% 收益门禁停止。

最终验证环境为 JDK 17.0.18、G1；JMH 为 1 warmup、3 measurement、1 fork、512 MB，除特别说明外均按每行归一化：

| 路径 | 阶段前 | 最终 | 结论 |
| --- | ---: | ---: | --- |
| `commonFunctions` | 1.074 M rows/s；4584.021 B/row | 5.709 M rows/s；400.016 B/row | 吞吐约提升 432%，分配下降约 91.3% |
| 缓存标量子查询 | 2.832 M；1688.086 B/row | 2.753 M；1568.077 B/row | 吞吐在短基准噪声范围内，分配下降约 7.1% |
| 缓存 `exists` | 7.633 M；624.096 B/row | 7.709 M；592.095 B/row | 吞吐约提升 1.0%，分配下降约 5.1% |
| 两层不相关子查询 | 无独立基线 | 2.848 M；1568.122 B/row | 与单层子查询分配基本相同 |
| 三层不相关子查询 | 无独立基线 | 2.911 M；1568.184 B/row | 嵌套层数未造成逐行 Publisher 图分配增长 |
| 融合窗口聚合 | 14.92 M；51.729 B/row | 14.78 M；51.729 B/row | 保持阶段四水平，无分配回退 |
| Publisher 兼容聚合 | 5.07 M；338.102 B/row | 5.488 M；336.909 B/row | 缺省预算 bookkeeping 移除后吞吐提升、分配略降 |
| 50,000 键聚合 | 2.307 M；2411.373 B/row | 2.311 M；2411.372 B/row | 状态压缩收益保持不变 |
| JSONPath | 3.36 M；2024.009 B/row | 3.342 M；2056.010 B/row | 未达到实现门禁，不增加复杂解析器 |

质量门禁：

- `mvn -q test`：338 tests，0 failure，0 error，0 skipped。一次初始全量运行中，已有墙钟窗口测试 `testGroupByWindowEmpty` 在 500 ms 边界只产生 4/5 个窗口；该用例单独复跑和最终全量复跑均通过，未通过放宽断言或修改生产逻辑处理。
- `git diff --check` 与 `mvn -q -Pjmh -DskipTests package`：通过。
- 50,000 键、每行 1 KB 无关 payload 的无 fork `-Xmx64m` 完成性检查通过，约 1.89 M rows/s、2411.373 B/row，无 OOM 或停滞；该无 fork 数据只用于低堆完成性判断，不与正式吞吐基准横向比较。
- 代码审查未引入 `block()`、嵌套 `subscribe()`、`ThreadLocal`、自定义 `CoreSubscriber` 或按输入值增长的全局缓存。无界 `collectList()` 只保留在用户未显式配置上限且必须兼容旧行为的缓存子查询路径；设置 `subquery.maxRows` 后仍在收集前以 `take(N+1)` 明确失败。

剩余边界：默认行为兼容意味着精确无界聚合和未设置 `subquery.maxRows` 的缓存子查询仍可能随有限但超大输入增长；需要硬资源保证的部署应显式配置对应 setting。TTL、近似聚合、事件时间、水位线和外部状态后端会改变 SQL 语义，不在本阶段隐式加入。

## 第六阶段：无分组标量聚合的增量执行

目标：让无 `GROUP BY` 的同步内置 `count/sum/avg/min/max` 与已有窗口融合聚合共用增量累加器。每条输入仅更新累加状态；标准纯聚合查询不保存最后一条输入记录，常驻状态不随输入行数增长。提升吞吐和降低堆分配时，以同语义且生成同样结果 Map 的原生 Reactor/JDK 实现为对照。

影响范围：ReactorQL 根模块的 `DefaultReactorQL` 执行计划、`WindowedAggregateStage`、聚合语义测试和 JMH。无 `GROUP BY` 的普通投影、异步或第三方非增量聚合、`DISTINCT/UNIQUE` 集合型聚合继续使用现有路径；不改变默认资源限制、窗口关闭或最终输出语义，不引入自定义 Subscriber、跨行对象池或隐式淘汰。

实施步骤：

1. 将已证明同步且单值的无分组聚合接入订阅级 `GroupState`；空输入时按原聚合结果数和 null 语义输出，并保留最后一行兼容投影所需的原路径。
2. 覆盖空输入、全 null、单聚合和多聚合、同步 WHERE、取消与并发订阅；与关闭 fast path 的原路径逐项比较结果。
3. 增加同输出形态的 native 基准，阶段末集中运行定向测试、全量测试及 JMH；记录吞吐、B/row 和残余语义风险。

风险：单个 `max/min` 在空输入时旧路径可能不发出结果；多个聚合的空输入行为不同，必须按原实现验证。包含非分组列的宽松 SQL 投影仍依赖最后一行，不能在没有显式兼容契约时删除它。精确高基数 `GROUP BY` 的状态下界仍是活跃键数乘以每键累加状态。

## 第七阶段：静态 Map 列访问

目标：查询构建期确认普通固定列使用内置 `PropertyFeature` 后，对单源 Map 行先直接读取已解析好的列名。值缺失、嵌套路径、类型转换、特殊属性、表别名回退以及自定义 `PropertyFeature` 继续走现有通用解析；不缓存输入行或字段值，也不修改公开 Feature SPI。该快路覆盖过滤、投影、聚合、分组等共享列访问入口。

实施：在 `PropertyMapFeature` 构建 mapper 时选择安全的直接 Map 访问，非 null 命中立即返回，其余情况调用原 resolver。用常规聚合、null/缺失字段、特殊字段、嵌套属性、自定义 Feature 的现有测试验证；再对同一 native/record/native SQL 三层 JMH 基准测量吞吐与分配。风险是绕开内置属性的特殊 `this`、`$`、`*`、嵌套和 cast 语义，因此这些名称不得进入直读快路。

### 第六、七阶段结果（2026-10-04）

- 无分组纯标量聚合已接入 `WindowedAggregateStage`：`count/sum/avg/min/max` 逐行更新累加器，纯聚合不保存最后一条源记录；输出仍等待输入完成。`IncrementalValueAggMapFeature.Accumulator.hasResult()` 默认返回 true，内置 `sum/avg/min/max` 在没有有效值时返回 false，保持单聚合不发结果、多聚合省略空列的旧语义。包含非分组列的宽松投影继续使用兼容路径。
- `PropertyMapFeature` 为内置属性功能和静态简单列选择 Map 直读；非 null 命中立即返回。缺失/null、特殊属性、嵌套/cast、别名回退和自定义功能都继续走原 resolver。此入口同时服务过滤、投影、聚合与分组。
- 全量 `mvn -q test`：343 tests，0 failure，0 error。之后只新增了全局聚合的 request(1) 与并发订阅断言，`WindowedAggregateStageTest` 定向复跑通过。`mvn -q -Pjmh -DskipTests package` 与 `git diff --check` 通过。

JMH 使用 JDK 17.0.18、G1、512 MB，1 次预热、3 次测量、1 个 fork；分配按每输入行归一。每组测量有较宽的 99.9% 区间，以下数值用于确定热点和方向，不作为跨机器绝对指标：

| 场景 | 之前 | 当前 | 每行分配 |
| --- | ---: | ---: | ---: |
| 无分组五聚合，兼容 Publisher | 12.575 M rows/s | — | 48.006 B |
| 无分组五聚合，增量执行，Map 快路前 | 22.687 M | — | 48.001 B |
| 无分组五聚合，增量执行 + Map 快路 | — | 32.845 M | 48.001 B |
| 窗口五聚合 | 14.78 M | 21.165 M | 约 51.9 B |
| 常见函数 | 5.709 M | 6.893 M | 约 400 B |
| 简单 count（没有属性读取） | 146.78 M | 149.81 M | 48 B |
| 原生 Reactor/JDK 五聚合，同样生成结果 Map | — | 166.16 M | 16 B |
| 原生五聚合 + 每行相同 `ReactorQLRecord` 包装 | — | 90.70 M | 48 B |

判断：Map 快路在多次读取固定字段的场景提升约 45% 吞吐且不增加分配；简单 count 基本不变，说明优化没有依赖某条 SQL。原生对照显示每行 Record 包装贡献约 32 B，通用属性/表达式与聚合分派仍贡献明显 CPU 成本。下一阶段应优先研究在保持 Feature 扩展与 SQL 语义的前提下，对简单单源、静态列、同步聚合编译行输入计划；是否绕过 Record 包装必须由同语义基准与回退测试证明，不能让不支持的场景误入快路。多层相关子查询去相关化仍是独立的高收益方向。

## 第八阶段：单键分组状态压缩

目标：精确聚合仍只保留活跃键及最小累加状态。`WindowedAggregateStage` 的单分组键状态当前为每键额外复制 `Object[1]`；改为直接持有键对象，只有多键状态复制数组。输出 `_group_by_key` 时才创建与旧路径同样可写的列表包装，不改变分组键、插入顺序、窗口或最后一行兼容投影语义。

影响范围：融合分组聚合内部 `GroupState`。不改变增量聚合 SPI、默认上限、兼容 `GroupedFlux`、外部 Feature 或响应式订阅结构。先测当前高基数 JMH，再改状态结构；集中验证单键、多键、无键及前后窗口顺序的结果，并复测高基数每行分配。此项仅压缩常驻键元数据，不宣称解决精确无界高基数分组的内存下界。

结果：该形态未保留。50,000 个单键、每行 1 KB payload 的 JMH 在改动前为 2.582 M rows/s、2539.372 B/row；改动后为 2.293 M rows/s、2539.373 B/row。短基准显示吞吐变差且总分配没有可测下降，单键与复合键语义测试虽通过，也不足以证明未关闭窗口的 live heap 收益。已恢复原 `Object[]` 状态布局；只有以后通过受控暂停点的 post-GC retained 对照证明足够收益，才重新考虑该优化。

## 第九阶段：单聚合状态布局

目标：只有一个增量聚合列时，每个分组直接持有一个 `Accumulator`；多个聚合仍使用 `Accumulator[]`。此形态适用于全局聚合、窗口和高基数单聚合，避免每个活跃键额外分配单元素数组。保持 `Accumulator` SPI、空结果、组键输出、最后一行兼容投影及订阅隔离不变。

实施：增加单聚合高基数 JMH 基线；在 `GroupState` 内按构建期固定的聚合列数选择状态布局；补单聚合空值、窗口、取消和多订阅回归；阶段末统一运行全量测试、JMH 与差异检查。若吞吐退化或分配没有明确下降，则撤回该状态分支。

结果：该形态也未保留。单聚合、50,000 键 JMH 的分配由 2283.373 B/row 增为 2307.372 B/row；短运行的吞吐预热波动较大，不足以证明稳定收益。相关语义测试通过，但未满足内存门槛，已恢复数组状态布局。后续不再凭对象图直觉压缩这两个小数组；应先用 JFR/GC 分配证据定位对象边界，再选择能减少每行分配的通用输入计划。

## 第十阶段：原始行同步输入计划

决策依据：同一百万行、同输出 Map 的原生五聚合为 166.16 M rows/s、16 B/row；增加与 ReactorQL 相同的每行 `ReactorQLRecord` 包装后为 90.70 M、48 B/row。融合聚合在通用 Map 直读之后为 32.85 M、48 B/row。对照支持两个独立热点：每行 Record 对象约 32 B，以及剩余的通用表达式/聚合分派 CPU。第八、九阶段的小数组改动未降低总分配，先停止状态布局试探。

目标：为可证明同步、单源、内置语义的行级计划增加可选原始行输入能力，先覆盖无分组标量聚合，再按同一契约扩展到过滤、投影和窗口分组。原始行仅在当前 `onNext` 同步求值；最终结果仍按既有时机生成独立 `ReactorQLRecord`/Map。查询计划和所有共享 Feature 保持不可变，每次订阅拥有自己的累加状态。第三方 Feature、异步表达式、JOIN、子查询、行号/elapsed、无法证明的属性回退继续使用 Record 路径。

实施门槛：

1. 先设计最小的可选原始行标量值契约，复用现有内置聚合的数值/null 比较逻辑；不能复制出另一套 SQL 函数或绕开扩展 Feature。只有所有输入映射器和聚合器都声明可安全同步读取原始行时才选择该路径。
2. 在 `FROM` 的默认单表边界接入原始 `Flux<Object>`，保持 `Context`、request、cancel、错误和多订阅隔离。原始元素已是 `ReactorQLRecord`、字段缺失/特殊属性或运行时类型不符合计划时，应有明确等价回退，不共享可变行对象。
3. 先对 `count/sum/avg/min/max` 无分组查询做与关闭快路的差分测试：空输入、全 null、数值混合、别名、参数、自定义 Feature、异常、背压与并发订阅。验证后再评估扩大到 WHERE、投影与窗口的收益和复杂度。
4. 与同输入、同结果形态的原生 Reactor/JDK 基准比较吞吐、B/row 和小堆完成性；首个切片必须实测减少每行 Record 分配，且不能降低现有兼容语义。如果需要复制通用表达式解释器或维护两套庞大的操作符分派，则停止该方向，改为继续优化共享入口。

聚合状态边界：`count/sum/avg/min/max` 逐行更新累加器，不缓存历史行。无分组纯聚合仅保留每列常数个累加值；精确分组聚合仍需每个活跃 key 的键和值状态，`min/max` 也至少保留当前极值引用。无窗口且 key 空间无限时，这一状态下界不能通过减少 Reactor 操作符消除；默认资源限制继续沿用旧行为，显式限制与窗口关闭仍是可选治理边界。

补充优化：聚合参数与行无关时，构建期识别可消费任意原始元素类型，避免整数等非 Map 数据源逐行创建 `ReactorQLRecord`；依赖行字段的聚合继续对非 Map 行走 Record 兼容路径。

### 第十阶段验证结果（2026-10-04）

- 无分组纯聚合用标准 Reactor `collect` 对每个订阅维护独立累加状态，不收集输入行。完全与行无关的聚合输入直接消费任意原始元素；依赖字段时 Map 走原始行求值，其他元素保持 Record 兼容路径。默认资源限制未变。
- 全量 `mvn -q test`：349 tests，0 failure/error；`mvn -q -Pjmh package` 与 `git diff --check` 通过。差分测试覆盖 Map、整数、混合类型、空输入和全 null、别名、绑定参数回退、自定义属性、非 Map 属性回退、错误、取消、Context、背压与并发订阅。
- JDK 21.0.10、512 MB/G1、3 次预热、5 次测量、2 forks、同一百万行与结果 Map 的 JMH：`count(1)` 从本阶段 `doOnNext` 方案的 140.08 M rows/s 提升到标准 `collect` 的 185.41 M，分配均约 16 B/row；五聚合从 31.75 M 提升到 35.23 M，均约 16 B/row。原生 Reactor/JDK 五聚合同次对照为 148.60 M rows/s、16 B/row，通用表达式与聚合分派仍有约 4.2 倍吞吐差距。原生 `count` 对照在本次 JMH 中出现明显 JIT/GC 波动，不用于收益判断。
- 该阶段基准结果保存在 `target/jmh-raw-global.json`、`target/jmh-raw-global-collect.json`、`target/jmh-raw-global-native.json`。该阶段尚未扩展到 WHERE、普通投影或窗口原始行路径；后续 WHERE 扩展及验证结果见本文开头的“同步过滤后的原始行增量聚合”切片，普通投影和窗口仍未扩展。

## 第十一阶段：共享数值运算的操作数装箱

目标：改善普通投影、过滤、分组键及聚合参数共用的算术入口，而非针对某条 SQL 建立快路。当前 1,000,000 行投影的横向 JMH 为约 15.23 M rows/s、256 B/row；JFR 的 `Long` 分配样本主要位于 `CalculateUtils.calculate` 通过 `BiFunction<Long, Long, …>` 调用加法和乘法的路径。多层子查询约 3.19 M rows/s、1568 B/row，但主要热点是内层订阅链，另行评估。

影响范围：仅 `CalculateUtils` 的加减乘除余数值运算共享分派、数值回归测试和现有投影 JMH；不修改公开泛型 `calculate` API、位运算、SQL 表达式编译器或响应式操作符。保持 BigDecimal/BigInteger 优先级、Float 与 Long 混合时提升为 Double、溢出、除零、NaN 和现有结果类型。

实施：在共享数值分派中使用 primitive 函数接口承接 long/float/double 算术，昂贵的大数路径继续复用现有转换方法；补齐类型矩阵和异常差分。阶段末集中跑全量测试、JMH 投影/相关回归及 JFR，只有实际减少操作数装箱并提高吞吐、且无语义回退才保留。默认限制与聚合状态语义均不变。

结果：未保留此改写。原始 JFR `Long.valueOf` 样本主要位于算术结果的装箱位置；把 primitive 操作数分派接入五类运算后，类型矩阵与全量测试通过，但同配置 JMH 投影仅从 15.23 M 提至 15.60 M rows/s（约 +2.4%），分配均约 256 B/row；函数与 WHERE 约持平。增量复杂度超过可测收益，已恢复原实现。`target/jmh-breadth-triage.json` 和 `target/jmh-arithmetic-triage.json` 保留对照证据；不再把操作数装箱列为主要内存热点。

## 第十二阶段：投影路径的等价成本拆解

先补与现有整数源、筛选范围、两个算术输出和结果 Map 相同的原生 Reactor 基准；另保留一个仅增加 `ReactorQLRecord` 输入包装的对照。目标是量化每行 Record 与通用表达式解释的独立成本，再决定是否值得为同步单源投影建立原始行执行段。此切片只改 JMH 和验证记录，不改变 SQL 行为、默认限制或 Feature SPI；若 Record 占比不明显，不扩展原始行路径。

结果：JDK 21.0.10、512 MB/G1、3 次预热/5 次测量/2 forks、同一百万整数行与结果 Map：原生 `handle` 投影 35.53 M rows/s、224 B/row；增加逐行 Record 包装后 32.51 M、256 B/row；ReactorQL 投影 15.87 M、256 B/row。Record 带来明确的 32 B/row，但只解释约 8.5% 的原生吞吐差额；剩余约两倍 CPU 差距主要在共享属性、谓词和表达式执行。暂不为这一投影场景扩大原始行 SPI。`target/jmh-projection-native.json` 保存完整对照。

## 第十三阶段：共享比较谓词的数值入口

目标：对已是 `Number` 的两个比较操作数，直接调用现有数值 `doTest`，避免重复进入通用 `CastUtils.castNumber`；Map 单值解包、日期/字符串/布尔转换、异常转 false 及所有自定义表达式路径均保持原语义。影响范围限 `BinaryFilterFeature.test`、比较谓词测试和 WHERE/投影基准。先做等价差分，再以同配置 JMH 验证；若吞吐和分配没有明确收益则撤回。此处不加类型专属 SQL 特调、缓存或新操作符。

结果：保留这一通用分支。全量 `mvn -q -Pjmh package` 通过，349 tests、0 failure/error；整数/浮点/大数混合比较与现有字符串、日期回归通过。同 JDK 21.0.10、512 MB/G1、3 次预热/5 次测量/2 forks 的成对 WHERE 基准：旧路径 51.36 M rows/s，直接 Number 路径 56.83 M（约 +10.6%），两者均约 48 B/row。投影基准从 15.87 M 到 16.24 M，但两 fork 波动较大，不认定该场景有稳定吞吐收益。证据为 `target/jmh-numeric-filter-baseline-full.json` 与 `target/jmh-numeric-filter-full.json`；优化后 JMH JAR 已重新构建。

## 第十四阶段（已完成）：已完成子查询缓存的读取快路

目标：降低外层每行读取同一次订阅内已完成、不相关子查询结果时的 Publisher/Subscriber 分配与订阅调度。当前同配置 JMH：单层、双层、三层缓存子查询分别约 3.10、3.15、3.20 M rows/s，均约 1568 B/row；未缓存单层仅约 1.80 M、3144 B/row；缓存 `EXISTS` 约 9.51 M、592 B/row。结果表明增加嵌套层数不是主要逐行成本，公共的多值缓存读取链才是候选 owner。证据为 `target/jmh-subquery-layers.json` 与已有 JFR 内层 `MonoDefer`、`FluxFlatMap`、`FluxReplay` 样本。

影响范围：ReactorQL 根模块 `internal/SubscriptionContext.cacheMany`、`SubqueryCacheTest` 和现有 JMH；不改 SQL 语法、公开 Feature SPI、关联子查询、`EXISTS`、默认资源限制或输出形态。不做子查询去相关化、跨订阅缓存、提前订阅、`Mono.cache()` 或手写 Subscriber。

实施步骤：

1. 每个订阅和查询计划 key 仍只建立一个现有 `replay(1).refCount(1)` 在途结果。仅在上游完成且有界收集成功后，发布同一份结果列表的只读快照引用；后续消费者直接从快照创建 `Flux.fromIterable`，不再重复订阅 replay/flatMap 链。快照不得复制结果集合，不得在取消或错误时发布。
2. 在途、错误、全部等待者取消后的重新执行、并发首次访问、空结果和显式 `subquery.maxRows` 超限仍走现有响应式路径与错误语义。订阅结束由原 Reactor Context 生命周期释放缓存，不新增全局状态。
3. 用 `StepVerifier` 验证 request(1)、取消、错误重试、并发消费、嵌套共享、Context、结果顺序及源订阅次数；用同输入 JMH/JFR 比较一至三层子查询的吞吐、B/row 和 GC。若任一功能回退，或逐行分配与吞吐无显著收益，则撤回快路。

风险与验证门槛：快照的发布时机必须在收集完成之后，不能使并发消费者观察到半成品或绕过资源限制；只增加一份引用而非复制列表。目标是缓存命中场景分配和吞吐均改善至少 15%，同时不降低未命中、取消和错误路径的语义。阶段末集中运行全量测试、JMH 构建、成对基准及 `git diff --check`。本优化无新的长期后台任务或跨边界 I/O，不新增 MBean/Trace span。

2026-10-04 复评：单异步投影列快路已将单层缓存子查询从 3.008 M 左行/s、1568 B/左行改善到 5.266 M、864 B/左行，旧的逐行热点归因和绝对基线不能直接复用。优化后短时 JFR 仍出现 `FluxReplay$ReplayInner`、`FluxFlattenIterable$FlattenIterableSubscriber` 各 182/184 个分配样本，与 `cacheMany` 的完成结果重订阅链相符；样本不能精确量化快照收益。实施时以上述新基线复核在途/完成路径，并以新的同配置成对 JMH 判断 15% 门槛；不把先前已由投影快路解决的收益重复计入缓存快路。

独立取证补充：先用同一份已完成、只有一条结果的 `ArrayList` 做成对 JMH，只比较每个外层行订阅现有 replay/refCount/flatMapIterable 链和从快照建立 `Flux.fromIterable` 的成本。两种方法消费同样数量与内容的结果行，均不包含子查询执行或收集成本。此基准只用于判断是否值得进入第十四阶段生产改动；真实 SQL 总体收益仍须另测。

结果：JDK 17.0.18、512 MB/G1、3 次预热/5 次测量/2 forks、20,000 外层行，已完成 replay 读取为 12.186 M 行/s、367.92 B/行，快照读取为 26.281 M、183.92 B/行；证据是 `target/jmh-cache-read-isolated.json`。基准 setup 已验证两路结果相同且 replay 源只订阅一次，`mvn -q -Pjmh -DskipTests package` 与更新后的快速烟测通过。这里只降低逐行瞬时分配；缓存列表的常驻大小完全不变，不能宣称高基数/大结果集的 live heap 下降。真实单层及多层 SQL 基准仍以上述约 864 B/行新基线衡量，并须先验证完整在途、错误与取消语义。

## JOIN 成本取证：普通表等值关联

目标：量化普通表等值 JOIN 的逐行候选记录及右源重订阅成本，为后续全局优化排序提供证据。Owning module 为 ReactorQL 根模块，本切片仅增加 `src/jmh/java/org/jetlinks/reactor/ql/ReactorQLBenchmark.java` 的成对基准及本文档结果；不修改 JOIN 执行器、SQL 语义、默认并发、缓存或公开 SPI。

实施：使用相同的有限左源、单行右源、等值谓词和输出 Map，比较 ReactorQL 与每条左行订阅右源的原生 Reactor 链路。预先复用输入 Map，避免数据生成分配淹没 JOIN 成本。阶段末集中运行 JMH 构建、相关测试和同 JVM/GC 配置的成对吞吐及分配基准。结果仅能代表此有限右源形态；是否扩展优化，应再核对关联右源、无界右源、外连接、背压与取消语义。纯 `avg/max` 聚合继续逐行更新常数状态，不缓存历史输入行。

结果：`mvn -q -Pjmh package` 通过，349 tests、0 failure/error；`git diff --check` 通过。JDK 17.0.18、512 MB/G1、3 次预热/5 次测量/2 forks、20,000 条左行、单行右源、50% 命中时，ReactorQL 为 5.732 M 左行/s、943.938 B/左行；同样每条左行重订阅右源的原生 Reactor 为 13.164 M 左行/s、319.916 B/左行。差距约 2.30 倍吞吐、624 B/左行；不与前述 JDK 21 的其他场景交叉比较。成对结果保存在 `target/jmh-join-baseline.json`。

短时 JFR `target/jfr-join/org.jetlinks.reactor.ql.ReactorQLBenchmark.innerJoin-Throughput/profile.jfr` 的分配样本集中于 `DefaultReactorQLRecord.ensureRecords`、`ReactorQLContext.newContainer`、`DefaultReactorQLRecord.getRecords(false)` 创建的过滤 Map 视图及 `addRecords` 复制；不能由采样数推断精确字节占比。普通表 JOIN 在 `DefaultReactorQL.createJoin` 中为每个右侧候选构造记录并复制左侧记录，再执行 ON 谓词。原生对照同样重订阅右源，因此此基准不能证明缓存或哈希 JOIN 有收益，也不能支持对无界右源物化。下一候选是减少记录复制/视图分配的通用 Record 契约优化；先补 JOIN、子查询、`select *`、外连接及取消/背压差分，再决定是否实施，不按特定 SQL 字符串特调。

补充对照：在相同原生订阅拓扑中加入 `ReactorQLRecord` 的左右行包装和具名来源复制，输出仍为同样的 Map，用它拆分 Record 成本与剩余表达式解释成本。该基准只增强归因证据，不改变任何生产执行路径。

补充结果：JDK 17.0.18、512 MB/G1、同样 3 次预热/5 次测量/2 forks 的三组同轮基准中，原生 JOIN 为 12.996 M 左行/s、约 330 B/左行；加入 Record 包装/复制为 6.287 M、约 928 B/左行；ReactorQL 为 5.361 M、约 912 B/左行。基准证据为 `target/jmh-join-record-baseline.json`。三组对照支持“Record 对象图及来源复制是主要分配成本”，但中间对照同时改变了属性读取方式，不能将其吞吐差额精确归因于单一方法；具体的具名复制快路仍须以实施后的成对基准和撤回门槛判断。该轮只新增 JMH 方法，`mvn -q -Pjmh -DskipTests package` 编译通过；上一轮完整 349 项测试仍覆盖未改的生产源码，新增基准方法未再次运行全量测试。

## 已完成：通用具名记录复制快路

目标：消除内置 `ReactorQLRecord` 向另一记录复制具名来源时，先物化源 `records` Map、再生成 Guava 过滤视图的中间分配。预期受益于普通 JOIN、派生表和嵌套 SELECT；不以某条 SQL 或字段名触发。Owning module 为 ReactorQL 根模块，影响 `ReactorQLRecord`、`DefaultReactorQLRecord`、`DefaultReactorQL`、`SelectFeature`、对应测试及现有 JOIN/子查询 JMH。

实施步骤：

1. 在 `ReactorQLRecord` 增加有默认实现的“从另一 Record 复制非 `this` 具名来源”方法，默认委托现有 `getRecords(false)` / `addRecords`，保持第三方实现的源码和二进制兼容。`DefaultReactorQLRecord` 仅对同类型源直接遍历已存在的具名条目，或在未物化时复制隐式别名；对其他实现走默认路径。目标 Record 的 `this`、原有具名来源和独立可变性必须保持不变。
2. 仅把目前紧邻的 `addRecords(source.getRecords(false))` 迁移到新方法；`bindAll` 等真正需要 Map 的边界继续使用原接口。不得共享可变 Map、引入对象池、缓存右源、改变 JOIN/SELECT 的订阅或背压拓扑。
3. 先补内置/第三方 Record、别名重命名、`this` 与具名来源不同值、覆盖顺序、源/目标后续独立修改的契约测试；再覆盖 INNER/LEFT/RIGHT JOIN、派生表、嵌套 SELECT、`select *`、取消、背压及并发订阅。阶段末集中运行全量测试、JMH 构建和成对 JOIN/子查询基准。若任一语义回退，或 JOIN 分配下降不到 10% 且吞吐无明确改善，则撤回，不为少量样本保留公共 API 复杂度。

明确不做：不提前用原始左右行求值 ON、不将右表物化为哈希索引、不修改默认并发/资源限制、不改变 `getRecords(boolean)` 对调用方可见的 Map 语义。后续若要在谓词前避免候选 Record，须另行证明别名解析、第三方 Feature 与外连接语义，并独立确认。

兼容边界：新默认方法不要求第三方实现修改，但属于公开接口增量；内置类型的直接复制只封装在 `DefaultReactorQLRecord`，第三方实现回退原有 Map 路径，不把 `instanceof` 分散到 SQL 调用方。此优化与第十四阶段缓存读取快路独立；Record 复制主要针对 JOIN 等场景，缓存读取快路主要针对剩余子查询成本。两者均用新基线验证，不沿用“Record 优先于缓存”的旧排序。

边界复核：普通表 JOIN 在每个右侧候选行只为复制调用 `left.getRecords(false)`，未物化的左 Record 因此会创建源 Map 与 Guava 过滤视图；这是直接受益路径。派生表和嵌套 SELECT 同时调用 `bindAll(record.getRecords(false))`，即使复制改为直接遍历，绑定仍需物化 Map，不能按普通 JOIN 的收益外推。`getRecords(false)` 是公开的过滤 Map 视图，不能直接改成快照或改变其可见语义。先用隐式单别名左 Record 的同输出成对 JMH 量化普通 JOIN 的可消除成本，再决定是否值得引入通用复制契约；该对照只是上界，不覆盖多别名来源和第三方 Record。

隔离结果：`ReactorQLBenchmark.namedCopyViaFilteredView` 与 `namedCopyDirectly` 在相同左/右 Map、相同 Record 包装及相同结果 Map 下，仅比较未物化单别名来源的复制方式；setup 核对两路目标 Record 的全部具名数据相同。JDK 17.0.18、512 MB/G1、3 次预热/5 次测量/2 forks、20,000 行，过滤视图路径约 13.215 M 行/s、647.91 B/行，直接别名复制约 27.662 M、351.91 B/行，证据为 `target/jmh-named-copy-isolated.json`。过滤视图路径两 fork 分配分别约 664 和 632 B/行，绝对值有 JIT 波动；方向与大幅度差距明确。该测量不包含真实 ON 谓词、右源订阅、外连接及多别名复制，不能直接当作 JOIN 全链路收益；正式实现仍以原计划的功能差分和真实 JOIN 成对门槛为准。

## 单异步投影列的通用执行快路

目标：降低所有“同步列若干 + 异步列恰好一个”的 SELECT 每行操作符和集合分配，包括子查询和第三方异步 `ValueMapFeature`；按编译期投影结构选择，不按 SQL 文本、函数名或数据源特调。Owning module 为 ReactorQL 根模块，影响 `DefaultReactorQL.createMapper`、`ScalarFastPathTest`、现有 JMH 与本文档。不改 Feature SPI、SQL 结果、默认限制或订阅并发配置。

实施：先以同 JDK、堆/GC 和输入分别测单层/嵌套缓存子查询及 `EXISTS` 负对照。构建期只有一个异步投影列时，直接将该列 Publisher 转成 `Mono` 并设置结果，避免每行 `Flux.fromIterable`、`flatMapDelayError`、`ProjectionValue` 和 `collectSortedList`；空 Publisher 仍输出当前行且不设置该列，错误和取消继续传递。多个异步列保留当前并发执行与顺序收集链。同步列仍先求值，`select *` 后处理仍在异步列完成后执行。补同步+异步、空值、错误、取消、Reactor Context、双异步列的差分测试；阶段末集中跑全量测试、JMH 构建、成对基准及差异检查。若功能回退或单层子查询的分配与吞吐没有明确收益，则撤回此分支。

范围边界：不在这一小步修改 `SubscriptionContext.cacheMany`、缓存生命周期、跨订阅共享、JOIN 或 Record API。短时 JFR `target/jfr-subquery-priority/org.jetlinks.reactor.ql.ReactorQLBenchmark.subquery-Throughput/profile.jfr` 在 909 个分配样本中出现大量 Reactor 订阅/收集对象（`FluxFlatMap$FlatMapMain` 155、`FluxFlattenIterable$FlattenIterableSubscriber` 94、`FluxRefCount$RefCountInner` 85、`MonoCollectList$MonoCollectListSubscriber` 62），但采样栈深不足以把每个对象精确归给投影还是缓存；因此必须靠前后基准验证，而不能把 JFR 样本当作单方法收益。Record 本体仅 3 个样本，支持先处理这一 Publisher 链路，随后再评估具名 Record 复制及第十四阶段缓存读取快路。

结果：在构建期仅针对“恰好一个异步投影列”选择 `Mono.from(...).map(...).defaultIfEmpty(record)`；多个异步列原路径不变，空值仍保留输入行，错误、取消和 Reactor Context 回归均通过。`mvn -q -Pjmh package` 完成，352 tests、0 failure/error；差异空白检查通过。JDK 17.0.18、512 MB/G1、3 次预热/5 次测量/2 forks、20,000 行同输入的成对 JMH：单层缓存子查询 3.008 → 5.266 M 行/s（+75.0%）、1568 → 864 B/行（-44.9%）；双层缓存子查询 2.998 → 5.204 M 行/s（+73.6%）、1568 → 872 B/行（-44.4%）。`EXISTS` 负对照分配保持约 592 B/行，吞吐 8.245 → 8.411 M 行/s，未见同量级变化。证据为 `target/jmh-single-async-before.json` 与 `target/jmh-single-async-after.json`。优化后 JFR 位于 `target/jfr-subquery-after-projection/`，剩余的 replay/iterable 订阅对象需由第十四阶段独立验证，不把当前已获得的收益算作其预期收益。

## 多异步投影列成本取证

目标：量化单异步列快路之后，普通外层行同时投影两个或三个不相关子查询时的逐行吞吐与分配，判断现有多列 `flatMapDelayError + collectSortedList` 是否值得进一步优化。Owning module 为 ReactorQL 根模块；本切片只增加 `ReactorQLBenchmark` 基准及本文档结果，不修改生产执行器、子查询缓存、默认限制或 SQL 语义。

实施：复用当前 20,000 行外层源与单行 lookup，为两列/三列子查询构建独立投影；与已有单列基准同 JVM、堆/GC、预热/测量/fork 配置运行，并确认输出行数及缓存订阅语义。若多列成本显著，下一步先辨别并发需求、完成顺序、空列和延迟错误，再提出能覆盖任意列数的有界状态方案；不因单一 SQL 基准直接移除异步并发或排序契约。

结果：新增测试验证两列/三列均输出预期 Map，且每个独立不相关子查询在一次根订阅内只订阅 lookup 一次。`mvn -q -Pjmh package` 最终通过，353 tests、0 failure/error；一次全量运行曾有既有 `testGroupByWindowEmpty` 的 500 ms 实时窗口用例少输出一条，单独复测和随后全量重跑均通过，未修改该时间敏感测试。JDK 17.0.18、512 MB/G1、3 次预热/5 次测量/2 forks、相同 20,000 外层行：一列 5.242 M 行/s、864 B/行；两列 1.903 M、2216 B/行；三列 1.414 M、2888 B/行。证据为 `target/jmh-multi-async-baseline.json`。输出列数和子查询执行次数也随之增加，不能把全部差额归因于投影编排。两列短时 JFR `target/jfr-multi-async/` 仍见每行集合/迭代和并发装配对象，如 `MonoCollectList$MonoCollectListSubscriber`、`FluxIterable$IterableSubscription` 与 `FluxFlatMap`；同时有缓存 replay 对象。下一步若实施，应先建立只替换投影编排、保持各异步列源相同的成对对照，并保留并发订阅、输出列顺序、空列、延迟错误、背压和取消语义。优先使用 Reactor 现有组合能力，不自定义 Subscriber 或按 SQL 文本特调。

组合器可行性核对：当前 BOM `2020.0.38` 对应的 Reactor Core 3.4.34 提供任意列数的 `Mono.zipDelayError(Function<Object[], R>, Mono<?>...)`。它可并发订阅并延迟错误，且数组下标保持列顺序；但任一列空完成时整体也会空完成，因此只有把每列空结果显式转为共享哨兵值，才能保持现有“省略空列、仍输出行”的语义。先增加仅用于归因的同源成对 JMH，对比现有 `flatMapDelayError + collectSortedList` 与 `zipDelayError` 的逐行编排成本；若收益不足，不引入第二套生产编排。正式替换仍须单独验证延迟错误、空列、取消、Context、背压与输出顺序，并按本仓库 plan-first 规则确认。

编排归因结果：新增 `AsyncProjectionCompositionBenchmark`，两种方法从相同的 `Flux.just` 冷源按行生成相同列值和结果 Map；每次测量处理 20,000 行。JDK 17.0.18、512 MB/G1、3 次预热/5 次测量/2 forks：两列旧编排 4.017 M 行/s、1423.61 B/行，`zipDelayError` 10.243 M、639.61 B/行；三列旧编排 2.915 M、1719.51 B/行，`zipDelayError` 7.927 M、839.51 B/行。证据为 `target/jmh-async-composition-forked.json`；`mvn -q -Pjmh -DskipTests package` 通过。这只证明编排本身有高收益，不等于真实 SQL 的总体收益，后者仍含子查询缓存和表达式成本。建议在获确认后用同一内置组合器替换多异步列热路径，不引入自定义 Subscriber；先补语义差分，再测真实两列/三列子查询及异步第三方 Feature，若整体收益不显著或行为有回退就撤回。

### 用户确认后的三项实施结果（2026-10-04）

用户确认实施多异步列组合、已完成子查询缓存读取快路和具名记录复制快路，要求收敛复杂度、禁止按 SQL 特调。实现入口分别为 `DefaultReactorQL.createMapper`、`internal/SubscriptionContext.cacheMany`、`ReactorQLRecord.addNamedRecords` / `DefaultReactorQLRecord.addNamedRecords`；JOIN 与 `SelectFeature` 的紧邻复制调用迁移到新方法，`bindAll` 的 Map 边界不变。

- 多异步列使用 Reactor 现有 `Mono.zipDelayError`；每列空结果转换为内部哨兵，以保持省略空列但仍输出行。列函数在订阅时执行，合并后按投影顺序写入结果；未实现自定义 Subscriber 或 SQL 文本分支。
- 已完成的子查询结果发布同一列表引用供 `Flux.fromIterable` 读取；完成前仍通过 `replay(1).refCount(1)` 共享在途结果，错误、取消或容量超限不发布快照。缓存列表常驻大小、作用域和默认资源限制不变。
- 内置 Record 直接复制隐式或已物化的具名来源；外部 Record 通过公开默认方法保持原 `getRecords(false)` / `addRecords` 行为。目标记录容器独立，源的 `this` 不被复制。纯 `avg/max` 仍逐行更新固定数量的标量状态，不缓存历史输入行；精确分组仍需要每活跃 key 的状态。

真实 SQL 隔离 fork JMH：JDK 17.0.18、512 MB/G1、3×1s 预热、5×1s 测量、2 forks；同配置实施前后分别为 `target/jmh-multi-async-baseline.json`、`target/jmh-single-async-after.json`、`target/jmh-join-record-baseline.json` 与 `target/jmh-three-optimizations-after.json`。两/三列结果包含投影与缓存共同收益，不能精确归因单项；单层/双层也受缓存与投影路径共同影响。

| 路径 | 实施前 → 实施后吞吐（M 行/s） | 实施前 → 实施后分配（B/行） |
| --- | ---: | ---: |
| 两异步子查询列 | 1.903 → 4.060 | 2216 → 1272 |
| 三异步子查询列 | 1.414 → 2.891 | 2888 → 1760 |
| 单层缓存子查询 | 5.266 → 6.366 | 864 → 696 |
| 双层缓存子查询 | 5.204 → 6.322 | 872 → 688 |
| INNER JOIN | 5.361 → 7.254 | 912 → 568 |
| `EXISTS` 负对照 | 8.411 → 8.141 | 592 → 592 |

结果达到缓存命中与 JOIN 的保留门槛；`EXISTS` 分配不变、吞吐约下降 3.2%，需如实保留为基准波动/潜在回退观察项，不归为收益。上述分配是每行累计分配，不等于峰值或常驻堆；已完成子查询快路不降低缓存结果列表的 live heap。`mvn -q -Pjmh package` 通过，Surefire 合计 364 tests、0 failures/errors/skipped；测试覆盖多列空结果、延迟错误、取消、Context，缓存完成读取/在途共享/取消重试/错误，以及 Record 隐式和已物化别名、`this` 排除与具名覆盖顺序、目标独立性与第三方回退。差异空白检查通过；未产生 commit 或 PR。

## 下一切片：普通属性缺失时避免无效路径拆分

目标：减少所有 SQL 场景中普通属性查找未命中时的正则 `Pattern.split` 分配，不针对某条 SQL、字段名或输入类型特调。Owning module 为 ReactorQL 根模块，落点为 `DefaultPropertyFeature.getPropertyValue`、对应属性契约测试及现有 JMH。只在直接查找失败且属性名没有 `.` 时直接返回 `null`；含点号路径、`::` 类型转换、集合索引、Map 特殊字段和外部 Feature 的执行方式不变。不加缓存、不改 SQL 语义或默认资源限制。

证据与步骤：当前 JDK 17.0.18、512 MB/G1 的广度 JMH 显示高基数聚合约 2.244 M 行/s、2515 B/行，普通投影约 17.73 M、256 B/行；证据为 `target/jmh-current-breadth.json`，仅 1 次预热/2 次测量/1 fork，属筛选信号。高基数 JFR 的 620 个分配样本中，236 个 `[I` 样本栈顶为 `Matcher.<init>`，调用链经过 `DefaultPropertyFeature.splitDot`；355 个 `[B` 样本来自基准每行故意创建的 1 KB payload，不能算作执行器分配。先补普通缺失和嵌套/转换回归，再做上述局部改动；阶段末集中运行全量测试、JMH 构建、同配置成对高基数及普通投影基准和差异检查。只保留结果等价且至少一个真实 SQL 场景分配明确下降、吞吐不退化的改动；未达到门槛则撤回。由于这是纯属性解析的局部同步分支，不新增 Trace span、MBean 或跨订阅状态。

实施与验证：直接查找未命中后，若属性名不含 `.` 即返回 `null`；含点号路径仍调用原拆分和递归查找。新增普通缺失、`::` 缺失、直接点号键优先于嵌套键，以及嵌套命中的回归测试。`mvn -q -Pjmh package` 通过，Surefire 合计 365 tests、0 failures/errors/skipped。JDK 17.0.18、512 MB/G1、3×1s 预热、5×1s 测量、2 forks 的成对结果保存在 `target/jmh-property-miss-before.json` / `target/jmh-property-miss-after.json`：高基数聚合 2.241 → 2.334 M 行/s，2515 → 2315 B/行，约 +4.2% 吞吐、-8.0% 每行分配；`WHERE` 49.96 → 49.83 M、48 → 48 B/行，基本持平。普通投影 16.08 → 14.78 M、均约 256 B/行，但两组吞吐波动/误差区间重叠，不能据此认定有稳定回退或收益。优化前/后高基数 JFR 中 `Matcher.<init>` 的 `[I` 分配样本为 236/0；两次样本总数为 620/660，该采样只支持热点消失的方向判断，不代表精确分配字节数。宽 payload 源自身每行仍创建 1 KB 数组，此改动不减少该输入成本，也不改变分组的活跃键常驻状态。

## 已确认：补齐跨 SQL 场景的性能证据

目标：把后续优化排序从单一热点转为代表性 SQL 场景矩阵，优先定位通用执行路径，而不是按 SQL 文本或函数名特调。Owning module 仍为 ReactorQL 根模块，先只扩展 `src/jmh/java/org/jetlinks/reactor/ql/ReactorQLBenchmark.java` 与本文档；不改生产执行器、公开 SPI、SQL 语义、默认限制或并发设置。

当前 JMH 已覆盖计数/全局聚合、普通 WHERE/投影、函数/JSONPath、窗口与高基数聚合、缓存/多层子查询、INNER JOIN；尚缺 `ORDER BY`（有/无 LIMIT）、`DISTINCT`、集合运算、外连接、关联子查询及异步第三方 Feature 的同口径证据。源码复核表明 `OrderBySupport` 已在 LIMIT 下使用 Top-N、无 LIMIT 时保持 10,000 行默认边界，`DefaultDistinctFeature` 已对同步键直算，`SubSelectFromFeature` 的 INTERSECT/差集只物化语义所需的一侧；本阶段不重复实现这些已有策略。第一步从缺口各取一个有限、可复现形态，区分流式与需保留状态的查询；无 LIMIT 排序输入不超过既有上限，异步 JOIN 另观察配置并发下的在途量，输入复用预建对象，setup 校验结果与订阅次数。第二步用同 JDK/堆/GC、预热/测量/fork 配置记录吞吐、B/行和 GC，仅对明显差距追加 JFR/堆观察。第三步选一个跨场景复用且可低复杂度修复的 owner，再单独提交语义测试、成对基准和保留/撤回门槛。此阶段不把有限基准的结果外推到无界流；精确排序、去重和分组所必需的状态不能靠静默淘汰来降低内存。用户已确认本计划及同一目标内后续连续实施，不再为每个等价切片重复请求确认；仅在 SQL 语义、默认限制或外部契约出现实质分歧时再请用户决策。

### 跨场景矩阵结果与下一处通用落点

JMH 已补全有限输入的全局排序、Top-N、同步 DISTINCT、INTERSECT、LEFT/RIGHT JOIN、关联子查询和第三方冷 Publisher Feature。setup 验证输出行数、排序首尾和关联子查询逐行订阅右源；冷 Publisher 只衡量非 scalar Feature 的操作符编排，不代表真实 I/O 延迟。JDK 17.0.18、512 MB/G1、3×1s 预热、5×1s 测量、2 forks，证据为 `target/jmh-cross-sql-matrix.json`：

| 场景 | 吞吐（M 输入行/s） | 分配（B/输入行） |
| --- | ---: | ---: |
| 全局排序，5,000 行 | 18.10 | 278.5 |
| Top-N，20,000 行/保留 100 行 | 6.29 | 224.0 |
| DISTINCT，20,000 行/1,024 个唯一值 | 38.48 | 177.2 |
| INTERSECT，两侧合计 20,000 行 | 9.76 | 669.6 |
| LEFT JOIN / RIGHT JOIN，单行右源 | 5.92 / 6.12 | 均 687.9 |
| 关联子查询，逐外层行读取单行右源 | 2.49 | 1720.5 |
| 第三方冷 Publisher Feature | 10.89 | 487.9 |

这些场景不能横向按绝对吞吐排序为单一“最慢 SQL”：输入行数、输出基数和语义成本不同。短时 JFR `target/jfr-cross-sql/` 显示关联子查询仍频繁创建 `Maps$FilteredKeyMap`、`HashMap$EntryIterator`，调用点为源记录的 `getRecords(false)` 后 `bindAll`；JOIN 的复制快路已避开同类视图。INTERSECT 主要分配来自 `resultToRecord` 的必需派生行包装和结果 Map，排序已使用 Top-N，暂不引入高复杂度状态布局改写。

下一切片：在 `ReactorQLRecord` 增加有默认实现的 `bindNamedRecords(ReactorQLContext)`；默认委托现有 `target.bindAll(getRecords(false))`，内置 `DefaultReactorQLRecord` 对隐式别名或已物化来源直接调用 `target.bind`，排除源的 `this`，不改变参数优先级、别名覆盖、第三方 Record 行为或外部 `getRecords` 视图。只迁移 `DefaultReactorQL` 派生表 JOIN 与 `SelectFeature` 中紧邻的两处绑定。补别名与 `this` 不同值、覆盖顺序、第三方回退、关联/不关联子查询、取消和 Context 测试；阶段末全量测试、JMH 构建、关联子查询及 JOIN/缓存负对照成对基准和差异检查。若功能回退或关联子查询每行分配没有明确下降且吞吐无改善，则撤回，不为这一热点保留新公开方法。缓存/无界流语义和默认限制均不变。

实施结果：`ReactorQLRecord.bindNamedRecords` 的默认实现保持第三方 Record 的原 Map 绑定路径；内置 Record 直接遍历隐式别名或已有来源 Map，并在两处子查询参数绑定调用中使用。新增测试验证未物化来源不创建 Record Map、`this` 排除、具名覆盖和第三方默认方法回退。`mvn -q -Pjmh package` 通过，Surefire 合计 368 tests、0 failures/errors/skipped。同一 JDK 17.0.18、512 MB/G1、3×1s 预热、5×1s 测量、2 forks 的真实 SQL 成对结果：关联子查询 2.489 → 2.936 M 外层行/s（+17.9%）、1720 → 1432 B/行（-16.7%）；LEFT JOIN 5.916 → 6.146 M、均 688 B/行；已缓存不相关子查询 6.366 → 6.121 M、均 696 B/行，后两者不归为该方法收益。证据为 `target/jmh-cross-sql-matrix.json`、`target/jmh-three-optimizations-after.json` 与 `target/jmh-named-bind-after.json`。关联子查询短时 JFR 的 `Maps$FilteredKeyMap` 样本为 32 → 0，`HashMap$EntryIterator` 为 22 → 1，样本总数 660/615；只作为分配归因，不直接量化 live heap。尚未独立量化派生表 JOIN 的收益，不能从关联子查询外推。

## 同步排序键的 Top-N 候选保留优化

目标：所有可直接同步求值的排序键在 Top-N 扫描时只给入堆候选分配 `OrderedRecord` 和键数组，降低逐输入行分配和吞吐成本；owning module 为 ReactorQL 根模块的 `OrderBySupport`。异步排序键、无 LIMIT 全局排序、窗口排序和默认资源上限保持原路径；不按 SQL 文本、字段或输入值特调，不新增自定义 Subscriber。

对照与方案：经修正的 JMH setup 用完整 Map 集合/有序列表验证 INTERSECT 和 Top-N 输出等价；此前空的 `target/jmh-native-stateful-comparison.json` 不作证据。正式同配置对照 `target/jmh-native-stateful-comparison.json` 显示 Top-N SQL 6.357 M 输入行/s、224.03 B/行，原生 Reactor/JDK 25.974 M、32.54 B/行；INTERSECT SQL 9.528 M、669.61 B/行，原生 132.998 M、21.56 B/行。原生路径缺少 SQL 解析、Record/派生表包装及资源异常语义，只是成本下界，不能据此承诺达到原生吞吐。INTERSECT 的较大差距涉及多个语义层，本切片不改它。

实施：在现有同步排序键编译分支中复用每次订阅私有的探针键数组，只有候选优于堆顶或堆未满时复制键数组并保留 Record；现有比较器继续决定 ASC/DESC、NULLS FIRST/LAST 和相等时的准入规则。补多键、空值、升降序、重复值、offset 与取消/错误回归。阶段末统一运行 `mvn -q -Pjmh package`、同配置 Top-N/全局排序/异步排序负对照及 `git diff --check`。保留门槛：输出和终止语义完全一致，Top-N 每行分配明确下降且吞吐无稳定回退；未满足则撤回实现。该切片不改变必须等待上游完成才输出精确 Top-N 的事实，也不声称降低高基数分组的常驻堆。

实测与处置：`target/jmh-scalar-topn-after.json` 使用相同 JDK 17.0.18、512 MB/G1、3×1s 预热、5×1s 测量、2 forks。Top-N 试验前后为 6.357 → 6.483 M 输入行/s，224.026 → 224.025 B/行；吞吐误差区间重叠，分配基本完全相同。全局排序 18.322 M、278.54 B/行及 INTERSECT 9.783 M、669.61 B/行是未改路径的观察值。不能把原生实现与 SQL 的巨大差距直接解释成排序键包装成本，也不能证明 JVM 消除这些对象的具体机制。按既定门槛已撤回试验性的生产 Top-N 分支，不留下额外执行分支；保留多键、NULL、重复值、错误及取消回归测试和等价的原生对照 JMH。异步排序键路径未改，但此前没有同口径异步排序基线，本次不声称完成它的性能负对照。

阶段验证：试验实现期间的首次 `mvn -q -Pjmh package` 只遇到既有实时窗口测试 `testGroupByWindowEmpty` 偶发少一条；该用例单独复测通过。撤回生产分支后的全量 `mvn -q -Pjmh package` 通过，Surefire 合计 370 tests、0 failures/errors/skipped，`git diff --check` 通过。当前没有提交或 PR。下一轮性能取证优先隔离关联子查询中逐外层行订阅/参数绑定的剩余成本，或取得高基数分组的 live-heap 样本；不要根据原生 INTERSECT 对照直接跨越派生表及记录语义边界。

## 高基数融合分组的单数组常驻状态试验

目标：在所有已符合融合增量聚合条件的查询中，每个活跃分组用一条 `Object[]` 同时保存分组维度和累加器引用，替代当前独立的键数组与 `Accumulator[]`。Owning module 为 `WindowedAggregateStage.GroupState`；不针对 `count`、某个键数或 SQL 文本分支，不改变增量聚合 SPI、分组顺序、输出时机、默认限制、取消/错误或第三方 Feature 回退路径。

可证伪依据：用普通 `select key,count(1) total from test group by key` 订阅 50,000 个互异 key 后接 `Flux.never()` 暂停，在 JShell 进程内触发 `gcClassHistogram`。相对订阅前，GC 后仍有 50,000 个 `LinkedHashMap$Entry`（2.0 MB）、`GroupState`（1.6 MB）、count 累加器（1.6 MB）、`Object[]`（约 1.204 MB）和 `Accumulator[]`（1.2 MB），另有键 `Integer` 和 HashMap 桶；50,000 条各 1 KB 的输入 payload 未以相同数量常驻。取消后 `GroupState`、累加器和对应 Map Entry 均回到基线。该证据证明每 key 的两条小数组确实都常驻，但不证明合并后吞吐不退化；前两次分别压缩单键和单聚合的试验因短基准分配/吞吐无收益已撤回。

实施与门槛：先记录当前 `highCardinalityCount` 与三聚合高基数场景的同配置 JMH；随后只在 `GroupState` 内合并存储，构造时一次复制维度并创建累加器，输出时按维度/聚合下标读取，不引入新操作符或订阅状态机。补单键/多键、单/多聚合、空值、窗口、取消及并发订阅等价测试；阶段末统一跑全量测试与同配置 JMH，并在相同暂停点复测 post-GC 类直方图。只有常驻分组状态明确下降、吞吐无稳定回退且分配不恶化才保留；否则撤回，不以对象图直觉代替实测。JMH 每行分配不等于常驻堆，两个指标分开报告。

结果：该单数组布局未保留。同一 50,000 key、输入接 `Flux.never()` 的暂停点中，GC 后 `GroupState` 从 1.6 MB 降至 1.2 MB，50,000 个 `Accumulator[]`（1.2 MB）消失，合计约减少 1.6 MB，即 32 B/key；50,000 条 1 KB payload 没有按行常驻，取消后分组对象和累加器均释放。JDK 17.0.18、512 MB/G1、3×1s 预热、5×1s 测量、2 forks 的 JMH：高基数单聚合 2.966→2.967 M 行/s、2059.37→2027.37 B/行，三聚合 2.366→2.302 M、2291.37→2259.37 B/行；吞吐误差区间重叠。跨场景的同源码配对揭示不能接受的热路径回退：无分组五聚合 28.325→19.323 M 行/s（-31.8%，分配仍约 16 B/行），低基数窗口五聚合 14.659→13.128 M（-10.4%）。证据分别为 `target/jmh-group-state-before.json`、`target/jmh-group-state-after.json`、`target/jmh-group-state-lowcard-before.json`、`target/jmh-group-state-lowcard-after.json`；历史非配对吞吐未用于保留决定。已恢复强类型 `Accumulator[]` 和原分组键数组，只保留双分组键、多聚合顺序回归测试。本结果说明单数组布局虽降低常驻堆，但跨 SQL 场景总目标不允许其吞吐回退；后续若研究状态压缩，须避免在每行累加热循环把强类型引用改为 `Object[]` 查找或为单一 SQL 建双布局。

## 通用命名参数 Map 的小容量初始化

目标：降低上下文只绑定少量名称时的桶数组分配，覆盖子查询、JOIN、参数表达式等所有 `DefaultReactorQLContext` 使用者。Owning module 为 `DefaultReactorQLContext.safeNamedParameters`；仅将首次创建的 `HashMap` 初始容量与已有 `ReactorQLContext.newContainer()` 的小容器容量保持一致，不改变公开 `getParameters()` 可变 Map 契约、`bind` 覆盖语义、并发初始化或默认资源限制。

依据与步骤：关联子查询优化后的 JFR 分配样本仍有 23 个 `HashMap` 首帧位于 `safeNamedParameters`，14 个 `HashMap$Node` 位于 `bind`；每外层行会构造子上下文并绑定具名记录。先用真实关联子查询、缓存子查询和普通 LEFT JOIN 记录同配置基线，再改初始容量并补一到多个名称绑定、覆盖及返回 Map 可变性测试；阶段末集中跑全量测试、成对 JMH 和差异检查。小容量会让超过装载阈值的 Map 更早扩容，因此只有真实关联路径分配下降且吞吐无稳定回退、负对照无明显退化才保留。JFR 样本仅说明来源，不当成字节收益；本切片不引入自定义 Map 或按 SQL/别名个数特调。

实施结果：`safeNamedParameters` 仍用 `HashMap`，初始容量改为与上下文新容器相同的 4；新增测试覆盖预先获取 Map、绑定 4 个名称触发增长、覆盖绑定和直接修改返回 Map。JDK 17.0.18、512 MB/G1、3×1s 预热、5×1s 测量、2 forks 的真实 SQL 成对结果：关联子查询 2.935→2.881 M 外层行/s，1432.46→1384.46 B/行，分配明确少 48 B/行；吞吐误差区间重叠，不宣称吞吐提升。缓存不相关子查询 6.285→6.257 M、约 696.06 B/行不变；LEFT JOIN 6.259→6.132 M、约 687.94 B/行不变，均无明确回退或该改动收益。证据为 `target/jmh-small-named-map-before.json` / `target/jmh-small-named-map-after.json`。`mvn -q -Pjmh package` 通过，Surefire 合计 372 tests、0 failures/errors/skipped；差异空白检查通过。保留此低复杂度通用容器调整，不把减少瞬时分配误称为高基数分组常驻堆下降；仍未提交或创建 PR。

## 子上下文复用已规范化的数据源函数

目标：`DefaultReactorQLContext.transfer` 创建子上下文时直接复用父上下文已有的 `Function<String, Flux<Object>>`，避免每层再次创建 `name -> Flux.from(supplier.apply(name))` 包装。适用于所有通过公开 `transfer` 建立的子查询/JOIN 上下文，不按 SQL 文本、层数或数据源特调。保持新上下文独立的索引/具名参数与 mapper、冷 Publisher 的订阅时机、取消和 Context 传播；不改变公开接口或默认限制。

步骤与门槛：先以当前 `target/jmh-small-named-map-after.json` 为同配置基线，增加 transfer 多层调用、独立参数及非 Flux Publisher 回退测试；用私有复制构造器只复用已规范化函数，不共享参数容器。阶段末集中运行全量测试、关联子查询及非关联/LEFT JOIN 负对照 JMH、差异检查。只有真实 SQL 逐行分配下降、吞吐无稳定回退且功能等价才保留；如果 JVM 已消除包装成本则撤回，不留无收益分支。

实施结果：`transfer` 通过私有复制构造器复用已经规范化的 `supplier`；每级子上下文仍新建空索引参数容器，具名参数与数据源 mapper 仍独立。测试验证冷 `Mono` 来源只在订阅时调用、多层 transfer 不继承上层 mapper 或参数。JDK 17.0.18、512 MB/G1、3×1s 预热、5×1s 测量、2 forks：关联子查询初次基线/改后为 2.881→2.910 M 外层行/s，1384.46→1368.46 B/行；复测改后 2.853 M、1368.46 B/行，撤回代码后的 A/B/A 对照为 2.820 M、1384.46 B/行。吞吐波动不支持宣称明确提升，分配减少 16 B/行可复现。普通 LEFT JOIN 初次基线/改后 6.132→5.643 M，但同代码复测 5.798 M，撤回代码后仅 5.880 M，且三次分配都约 687.94 B/行；该不走 transfer 的路径证实初次吞吐下降不是可靠的代码回退证据。缓存不相关子查询分配保持约 696.06 B/行。证据为 `target/jmh-small-named-map-after.json`、`target/jmh-transfer-supplier-after.json`、`target/jmh-transfer-supplier-repeat.json`、`target/jmh-transfer-supplier-reverted.json`。已恢复并保留复用实现，不引入自定义操作符或缓存；最终 `mvn -q -Pjmh package` 通过，Surefire 373 tests、0 failures/errors/skipped，差异空白检查通过，未提交或创建 PR。

## 非融合多值聚合的线性累加

目标：对未实现增量契约的第三方/兼容聚合，保持现有多聚合 `Flux.merge`、输出 Map 与多次发值时的最终 `CopyOnWriteArrayList` 类型，但避免收集过程每加一项就复制整个数组。Owning module 是 `DefaultReactorQL.createMapper` 的多聚合收集器；不修改 `ValueAggMapFeature` SPI、默认结果上限、单聚合/融合聚合或输入订阅模式，也不引入自定义 Subscriber。

依据：当前收集器只在收到第二个非 List 值后创建 `CopyOnWriteArrayList`，之后每次 `add` 均复制已有元素，累计时间和临时数组量随单列发值数近似平方增长。`Flux.merge` 对下游 `onNext` 串行化，`collect` 的中间容器为订阅私有；可用普通动态数组在收集期间追加，完成时一次性构造原结果类型。原本第一项即为 List 时直接向该 List 追加的兼容行为必须保留，不得把它误判成内部累加列表。先增加有界 1,024 行的自定义多发值聚合 JMH 与结果前置断言，随后只用内部标记列表区分这两种来源，并补单值/多值/List 首值、异步源、错误与取消测试。阶段末集中运行全量测试、同配置真实 SQL JMH 及普通聚合负对照、差异检查；若最终类型/值/顺序或终止语义变化，或吞吐/分配没有明确改善，则撤回。

实施结果：使用仅在收集期间存在的 `PendingAggregateValues` 顺序追加，在终止后的结果映射中一次转换成 `CopyOnWriteArrayList`。单值保持标量；首值本身为 List 时保留原来的直接追加及对象身份；融合增量聚合（包括普通 `avg/max`）不进入此兼容路径，也不因本改动驻留输入行。新增回归测试覆盖上述形态及异步发值、错误、取消。JDK 17.0.18、512 MB/G1、3×1s 预热、5×1s 测量、2 forks 的正式 JMH：1,024 值自定义聚合从 10.653 M 提升至 20.775 M 输入行/s，分配从 2158.71 降至 109.44 B/行；单值兼容聚合负对照约 48 B/行不变。相同输出 Map/List 的原生 Reactor 收集为 134.010 M 输入行/s、33.13 B/行，setup 逐项验证结果等价；原生路径不承担 SQL 解析、Record 与聚合 Feature 语义，只作成本下界。证据为 `target/jmh-multi-result-before.json`、`target/jmh-multi-result-after.json`、`target/jmh-multi-native-after.json`。

验证：完整 `mvn -q -Pjmh package` 在可运行 JVM attach 的环境中通过，Surefire 375 tests、0 failures/errors/skipped，`git diff --check` 通过。首次全量运行中 `testGroupByWindowEmpty` 的墙钟 500 ms 窗口偶发只输出 4 个而非断言的 5 个；该融合窗口路径未经过本收集器，单独复测及随后的完整复测均通过，未修改或放宽断言。沙箱内单独执行另一时间窗用例时，`ReactorDebugAgent` 自附加因 attach socket 不响应而在测试类初始化前失败；沙箱外相同用例通过。错误回归用例的主错误已传播，但既有多聚合 `merge` / 共享源竞争下还会记录一次重复错误的 `onErrorDropped`；本次未修改该终止边界，后续若治理应单独验证其协议语义，不在收集器内吞掉日志。额外尝试将临时标记容器改为 `ArrayList` 子类，1,024 值场景的分配升至 111.58 B/行且吞吐无明确收益，已撤回。仍保留第三方聚合多次发值时最终 List 必须持有全部输出值的语义成本；本优化不改变默认限制、无界流分组常驻键状态或上游订阅策略。

## 下一切片：派生结果转换时避免物化源记录容器

目标：降低派生表、集合操作及聚合结果每次 `resultToRecord` 额外物化隐式来源 Map 的成本。Owning module 为 `DefaultReactorQLRecord`；不改变 `ReactorQLRecord` SPI、结果 Map 的隔离复制、具名来源覆盖顺序、集合操作方向/去重、默认行数上限或响应式订阅方式。旧 JMH 中 INTERSECT 为约 9.53 M 输入行/s、669.61 B/行，原生下界约 133 M、21.56 B/行；已有 INTERSECT JFR 的 660 个分配样本中，`DefaultReactorQLRecord` 191 个、`HashMap` 152 个、`HashMap$EntrySet` 92 个，其中 `resultToRecord` 和源记录物化占明显比例。旧数据只用于定位，本切片先以当前代码重测同配置基线。

实施：当源记录仍使用内置的隐式 `name` / `thisRecord` 字段时，将这些具名来源直接写入新记录的容器，再复制结果 Map；若源记录已物化 `records`，仍按现有 Map 复制。保留 `name` 与源别名冲突时的原覆盖规则，源记录与派生记录互不共享可变 Map。补隐式与已物化来源、别名冲突、空来源、结果独立性和自定义容器的测试，并用真实 INTERSECT、关联子查询及普通聚合/JOIN 负对照比较吞吐与 B/行。阶段末统一执行全量测试、JMH、差异检查；若结果语义不等价、分配无明确下降或吞吐稳定回退，则撤回。此改动只减少行转换的短命对象，不声称降低精确集合操作必需的驻留 key 状态；不新增 Trace span、MBean 或特殊 SQL 分支。

实施结果：`DefaultReactorQLRecord.resultToRecord` 对尚未物化来源容器的记录直接复制隐式具名来源，不再为源记录创建临时 Map；已物化来源仍复制现有 Map，派生结果 Map 继续独立复制。测试覆盖自定义容器调用次数、源/结果独立性、同名别名覆盖、无名来源与已物化具名来源。最终代码下完整 `mvn -q -Pjmh package` 通过，`git diff --check` 通过。

JDK 17.0.18、512 MB/G1、3×1s 预热、5×1s 测量、2 forks 的成对结果：真实 INTERSECT 为 10.046→13.324 M 输入行/s（+32.6%）、669.61→509.61 B/行（-160 B，-23.9%）；LEFT JOIN 为 5.818→5.828 M、687.94 B/行不变；普通全局聚合为 32.68→27.75 M、16 B/行不变，吞吐误差区间重叠，不能据此认定回退。关联子查询初次为 2.857→2.748 M、1368.46 B/行不变；新代码复测 2.809 M，临时恢复旧代码的 A/B/A 对照为 2.837 M、同样 1368.46 B/行，误差区间重叠，未确认稳定回退或收益。已恢复优化实现并重新全量验证；证据为 `target/jmh-result-record-before.json`、`target/jmh-result-record-after.json`、`target/jmh-result-record-correlated-repeat.json`、`target/jmh-result-record-reverted.json`。原生 INTERSECT 仍是远低于 SQL 路径的成本下界，后续须继续按跨场景分配证据定位，不为单个 SQL 形态写专用操作符。

## 下一切片：区分集合操作的 Record 成本与 SQL 编排成本

目标：在不改生产逻辑的前提下，为 INTERSECT 增加保留同样输入 Record、内层投影和 `resultToRecord` 包装的原生 Reactor 对照，量化已有原生 Map 对照遗漏的语义成本。Owning module 为 `src/jmh/java/org/jetlinks/reactor/ql/ReactorQLBenchmark.java`；不改变 SQL、Feature SPI、集合上限或订阅方式，也不把该对照当作完全等价的 SQL 实现。改动后短时 JFR `target/jfr-result-record/` 的 613 个分配样本中，Map/桶/节点仍占多数；58 个 `HashMap$EntrySet` 样本里 56 个来自集合键 `Map.hashCode`，而非结果 Map 拷贝，说明直接改写拷贝迭代方式不是当前主要落点。

步骤：让原生对照两侧像 SQL 一样分别创建源 Record、单列结果 Map 和派生 Record，再以派生结果 Map 做有界右侧 key 集合与左侧筛选，最终生成相同外层结果 Map。JMH setup 断言输出集合与 SQL/既有原生对照一致；以相同 JDK、堆、GC、预热/测量/fork 记录三者吞吐与 B/输入行。只有对照有效后，才判断下一轮是否值得改变通用记录表示或集合操作装配；本切片不靠额外功能分支追求基准数字。

结果：JMH setup 的三个输出集合等价断言通过，完整 `mvn -q -Pjmh package` 通过。JDK 17.0.18、512 MB/G1、3×1s 预热、5×1s 测量、2 forks 的同次结果：SQL INTERSECT 13.409 M 输入行/s、509.612 B/行；保留来源/派生 Record 和结果 Map 的原生 Reactor 对照 14.142 M、509.572 B/行；纯 Map 原生对照 131.794 M、21.562 B/行。证据为 `target/jmh-intersect-record-comparison.json`。前两者仅约 5% 吞吐差距且分配等同，说明现有 SQL 编排不是此场景的主要剩余成本；大幅接近纯 Map/Java 原生必须减少 Record 与投影 Map 的对象图，而不能靠再删一个集合操作符。该对照仍省略通用 SQL 表达式语义，只作为成本拆解，不证明所有集合操作已达到最优。按复杂度门槛，本阶段不为 INTERSECT 新增专用表示或操作符；下一处跨场景候选是同分配量但仍有明显 CPU 差距的普通同步表达式和增量聚合输入。

## 数值转换共享入口的 Number 优先试验

目标：在不增加 SQL 专用分支、操作符或新 SPI 的条件下，减少 `CastUtils.castNumber` 对已是 `Number` 的普通输入反复进行字符串/字符/布尔类型判断的成本。该入口被同步算术、比较和增量聚合复用；改变仅是把现有 `Number` 返回分支移到方法首位，其他输入的解析和错误顺序保持原样。JDK 17、512 MB/G1 的同配置基线先覆盖全局五聚合、普通投影、常用函数与 WHERE，再做改动；阶段末运行类型回归、完整测试和成对 JMH。只有至少一个真实 SQL 场景吞吐明确改善且其他场景不稳定回退、每行分配不增加才保留。此前 primitive 函数接口试验因收益不足已撤回，本切片不重复其复杂度。

结果：全量 `mvn -q -Pjmh package` 在试验代码下通过，但 JDK 17.0.18、512 MB/G1、3×1s 预热、5×1s 测量、2 forks 的成对 JMH 未证明稳定收益：常用函数 6.97→6.77 M 行/s（误差区间重叠），五聚合 30.74→35.01 M（高方差、误差区间重叠），普通投影 15.63→16.03 M（误差区间重叠），WHERE 37.37→40.28 M（误差区间重叠）；对应每行分配约 400/16/256/48 B 均不变。证据为 `target/jmh-cast-number-before.json` 与 `target/jmh-cast-number-after.json`。已撤回分支重排，不把 `castNumber` 判为本轮稳定收益；下一轮需进一步隔离共享表达式调用与聚合累加器分派的 CPU 成本，而非继续按单个类型判断试探。

后续以当前源码身份（`target/jfr-aggregate-current/.../profilingGlobalAggregates.../source-identity.txt`）录制的 JFR 有 96 个 CPU 样本：`WindowedAggregateStage.GroupState.addRaw` inclusive 50、`PropertyMapFeature.applyRaw` 33、`HashMap.get` 32；样本非互斥，不能相加。虽可见多聚合重复求值，但 `ScalarValueMapper` / `RawScalarValueMapper` 未承诺纯函数或可减少调用次数，自定义 Feature 的调用及错误时机均可观察，故拒绝跨聚合共享值缓存，不新增 SPI 或 SQL 特调。

当前投影/WHERE JFR `target/jfr-projection-where-current/diagnosis.md` 的 SQL/原生对照 CPU 样本为 128/145 与 113/97，只证明路径差异。纯同步空 `allMapper` 微优化已正式 A/B 否决并撤回；WHERE `count` / 双范围专用融合既不具通用性，也可能改变双侧求值的错误语义。后续应从新的跨场景热点或明确的纯函数契约继续取证，不重复这些已否决路径。

## 订阅内缓存命中的直接读取

目标：对已存在的订阅级子查询缓存，先用一次 `get` 返回既有 Publisher，只有未命中时才使用当前 `computeIfAbsent` 原子装配，避免每个外层行都构造映射函数。Owning module 为 `SubscriptionContext.cacheMany/cacheMono`；适用于所有缓存子查询和 EXISTS，不按 SQL 文本、结果数或嵌套层数特调。当前短时 JFR `target/jfr-subquery-current/` 在单层/嵌套缓存路径分别采到 44/79 个 `SubscriptionContext.cacheMany` 捕获 lambda 分配样本；样本只用于定位，不当作精确字节数。

不改变：按订阅隔离、首次并发访问只执行一次、已完成快照、错误/取消后重新执行、背压、资源上限、冷 Publisher 或默认限制。步骤：先记录单层、多层、EXISTS、关联子查询及第三方 Publisher 的同配置 JMH；增加缓存命中身份和并发首次读取测试；实现直接命中读取；阶段末全量测试、成对 JMH 与差异检查。只有真实缓存 SQL 的分配明确下降、吞吐没有稳定回退且负对照无明显恶化才保留；不增加新缓存层或自定义 Subscriber。

结果：`cacheMany` 和 `cacheMono` 命中时直接返回订阅内已有 Publisher；未命中仍由 `ConcurrentHashMap.computeIfAbsent` 原子建立冷 Publisher，现有完成快照/取消/错误与容量边界未变。新增测试检查多值和单值重复查询返回同一 Publisher、不会替换首次源；既有并发首次访问、取消重启、错误和多层共享测试继续通过。最终 `mvn -q -Pjmh package` 通过，Surefire 380 tests、0 failures/errors/skipped，`git diff --check` 通过。首次全量运行只遇到已知真实时钟 `testGroupByWindowEmpty` 偶发 4/5 窗口，随后的完整复测通过，未改断言。

JDK 17.0.18、512 MB/G1、3×1s 预热、5×1s 测量、2 forks：单层缓存子查询 5.937→6.243 M 外层行/s、696.06→672.06 B/行；双层 5.998→6.154 M、696.09→672.10 B/行；三层 6.140→6.234 M、696.13→672.13 B/行；缓存 EXISTS 7.795→7.923 M、592.06→576.06 B/行。关联子查询 2.808→2.849 M、1368.46 B/行不变，第三方 Publisher 10.847→10.997 M、487.94 B/行不变，均不归为本优化收益。证据为 `target/jmh-cache-hit-before.json`、`target/jmh-cache-hit-after.json` 和单层补跑的 `target/jmh-cache-hit-single-after.json`；第一次 after 文件因基准枚举缺少单层项目，故单层只使用独立同配置补跑。多层吞吐小幅变化的误差区间部分重叠，只确认分配下降，不夸大吞吐收益。减少的是逐外层行的短命捕获函数对象，不减少已完成子查询结果列表的驻留大小；后续仍需处理投影 `Mono.from`/`defaultIfEmpty` 与真正相关子查询每行执行成本。

## 已完成缓存结果的冷 Publisher 复用试验

目标：`CompletedManyCache` 在有界/默认兼容结果完整收集后，仅为同一份结果 List 建一次 `Flux.fromIterable`，后续读取复用这个冷 Publisher；每个订阅仍由 Reactor 独立创建迭代器和 Subscription。Owning module 为 `SubscriptionContext.CompletedManyCache`，适用于所有已完成的不相关子查询缓存，不按 SQL、层数或结果行数分支。现有单层/多层 JMH 为约 672 B/外层行；先用独立 JMH 比较逐次创建与复用 `Flux.fromIterable`，再决定是否改生产代码。

不做：不缓存迭代器/Subscriber，不复制结果 List，不提前发布半成品，不改变首次在途共享、错误/取消重试、容量限制、冷源订阅次数、Context、背压或默认限制。测试需覆盖单/多/空结果、完成后 request(1)、多个订阅、在途并发、取消及错误；阶段末跑全量测试、同配置真实 SQL 单层/多层与 EXISTS/关联/第三方负对照。仅在逐行分配明确下降、吞吐无稳定回退且持久对象增量仅为每缓存 key 一个 Publisher 时保留；否则撤回，不引入自定义操作符。

结果：`CompletedManyCache` 仅在 `collectMany` 成功完成后，用原结果 List 构建一次 `Flux.fromIterable` 并通过 volatile 引用发布；每个后续读取复用该冷 Publisher，仍独立创建迭代器和 Subscription。在途共享、错误、取消重试及结果行上限保持原路径。新增空结果完成后重复读取测试，既有双值 request(1)、多个读者、首次并发、取消和错误测试继续通过。最终 `mvn -q -Pjmh package` 通过，Surefire 381 tests、0 failures/errors/skipped，`git diff --check` 通过。

隔离 JMH 的同结果单值列表读取：逐次 `Flux.fromIterable` 为 25.643 M 读取/s、183.915 B/次；复用冷 Publisher 为 26.974 M、159.915 B/次，少 24 B/次。真实 SQL 的 JDK 17.0.18、512 MB/G1、3×1s 预热、5×1s 测量、2 forks：单层缓存子查询 672.06→648.06 B/外层行，双层 672.10→648.10，三层 672.13→648.13；缓存 EXISTS 576.06 B/行不变，关联子查询 1368.46 B/行不变，第三方 Publisher 487.94 B/行不变。吞吐：单层初次 6.243→6.144 M、候选复测 6.134 M；临时恢复旧实现的 A/B/A 对照为 6.184 ±0.118 M，与候选复测误差区间重叠，不能确认稳定回退或收益。双层 6.154→6.103 M、三层 6.234→6.254 M，误差范围重叠；负对照无明确退化。基准证据为 `target/jmh-cache-publisher-isolated.json`、`target/jmh-cache-publisher-after.json`、`target/jmh-cache-publisher-single-after.json`、`target/jmh-cache-publisher-single-repeat.json`、`target/jmh-cache-publisher-reverted.json`。已恢复并保留候选实现后再次全量测试；不把 24 B/行的瞬时分配下降表述为缓存 List 常驻堆减少：每个完成的缓存 key 反而多保留一个小型 Flux 包装，List 本身无复制。下一步优先处理多列异步投影或真正相关子查询的逐行订阅成本，不为缓存再增加状态层。

## JFR 诊断基准去干扰

目标：先以 JFR 定位可跨场景复用的热点，避免基准消费端和输入源的分配遮蔽 ReactorQL 执行路径。当前 `target/jfr-next-shared/` 的 projection 分配样本中，660 个样本有 210 个 `HashMap$EntrySet` 来自 `CountingSubscriber.hashCode`；globalAggregates 的 629 个样本中，625 个 `Integer` 来自 `Flux.range` 输入装箱。该切片只新增 profiling-only JMH，owning file 为 `src/jmh/java/org/jetlinks/reactor/ql/ReactorQLBenchmark.java`。

范围与步骤：为 projection 和全局聚合各新增独立 profiling-only 基准。前者以 JMH `Blackhole` 消费每个结果 Map 的引用并仅计数，不调用 `Map.hashCode`；后者在自身首次 warmup 调用时才将相同的 `groupedRows` 序列预构造为 boxed `Integer[]`，订阅时以 `Flux.fromArray` 映射，移除逐行源装箱。惰性初始化同时比较新旧输入的 SQL 输出，保证诊断输入不改变结果；普通 JMH setup 不创建或保留该数组。阶段末一次执行 JMH 构建，并在 JDK 17、512 MB/G1、1×1s 预热、2×1s 测量、1 fork 下对两个 profiling-only 基准录制 JFR，统计总样本和主要生产调用栈；明确区分 HashMap 首次建桶与真实扩容。

非目标与风险：不改生产代码、正式性能基准、SQL/Feature SPI、默认限制、背压或取消语义；不将 JFR 样本直接换算为字节收益，也不因单场景热点引入特调。预构造输入只在诊断基准运行时增加 State 常驻对象，不能外推至生产流。

验证结果：初版 `mvn -q -Pjmh package` 与诊断 JFR 已通过；随后修正初始化隔离后再次执行 `mvn -q -Pjmh package` 和 `git diff --check`。JMH worker 在受限沙箱中无法绑定 loopback 控制 socket；在合规本机运行 `java -jar target/reactor-ql-1.0.21-SNAPSHOT-benchmarks.jar '.*ReactorQLBenchmark\\.profiling(Projection|GlobalAggregates)$' -wi 1 -i 2 -f 1 -w 1s -r 1s -jvmArgsAppend '-Xms512m -Xmx512m -XX:+UseG1GC' -prof 'jfr:dir=target/jfr-profiling-clean' -rf json -rff target/jmh-profiling-clean.json` 后成功完成。JDK 17.0.18、512 MB/G1 下的短时结果为全局聚合 37.111 M 输入行/s、projection 19.896 M 输入行/s；只用于诊断，不与正式基准比较。该后续调整只推迟诊断源及其等价校验的初始化，不改变诊断输入内容或执行路径，故复用既有 JFR。

JFR 证据：`target/jfr-profiling-clean/` 保存两份 recording。全局聚合为 128 个 CPU 样本、1 个分配样本；唯一分配样本属于 Attach Listener 的 JFR 字符串池，不在 worker，原先由 `Flux.range` 产生的逐行 `Integer` 分配已不再出现。worker CPU 栈主要经过 `PropertyMapFeature.applyRaw`、`MapAggFeature.addRaw/addValue`、`WindowedAggregateStage.GroupState.addRaw` 与 `FluxArray`；这符合实时增量聚合，不保留历史行。projection 为 151 个 CPU 样本、660 个分配样本；不再出现 `CountingSubscriber`、`Map.hashCode` 或 `HashMap$EntrySet`，主要分配类别为 `HashMap` 275、`Long` 163、`Integer` 149、`HashMap$Node` 39、`HashMap$Node[]` 29。CPU 栈集中于同步 WHERE/算术/结果写入：`BinaryFilterFeature.test`、`CalculateUtils.multiply`、`BinaryMapFeature`、`SynchronousRowStage` 和 `DefaultReactorQL.createMapper`。

HashMap 边界：29 个 `HashMap$Node[]` 样本的共同栈为 `HashMap.resize -> put -> DefaultReactorQLRecord.setResult`。本 projection 每行只写两个结果列，且容器由 `new HashMap<>(4)` 创建，首次 put 会以 `resize` 命名方法分配首个桶数组，而两个 entry 未达到容量 4 的阈值 3；因此这里是首次建桶，不是实际扩容。JFR 的该栈本身不能区分两者，结论依赖 SQL 输出列数与容器容量边界，不能把这些样本当作扩容收益。

结论：去干扰后的 JFR 没有暴露一个既跨场景又能以低复杂度安全消除的生产热点。projection 剩余对象图是通用行 Record 和两列结果 Map 的语义成本；全局增量聚合未显示 worker 分配热点。按“禁止特调、收敛复杂度”门槛，本切片不实施生产优化；后续如继续，应先对多列异步投影或相关子查询做同样的无消费端干扰 JFR，而不是据此修改 Map 容器或增加操作符。

## 下一切片：异步投影与相关子查询的干净 JFR

目标：在已有多异步列 `zipDelayError` 和订阅缓存优化之后，区分真实逐行子查询成本与可复用的冗余编排成本。Owning module 为 ReactorQL 根模块；本阶段只扩展 `ReactorQLBenchmark` 的诊断入口并记录证据，不先改生产代码。复用现有两异步列与相关子查询 SQL、输入和 setup 结果/订阅检查，以 `Blackhole` 逐行消费结果引用，排除 `Map.hashCode` 对 JFR 的干扰。

不做：不按 SQL 文本、层数或函数名特调；不删改 `contextWrite`、首值提取、并发组合等承载 Context、背压、取消或错误语义的操作符；不增加缓存、常驻状态、自定义 Subscriber 或默认上限。先在相同 JDK 17、512 MB/G1 下短时录制两场景的 JFR，归类分配及 CPU 栈，并与现有正式 JMH 的每行分配证据交叉核对。若发现跨场景同源、可低复杂度消除的冗余，再单独做最小生产试验与语义测试、成对基准；若热点属于必要的结果 Map/Record 或逐行订阅，则保持现状。JFR 样本只用于定位，不直接估算字节收益。

取证结果：JFR 文件位于 `target/jfr-next-async-clean/`，诊断 JMH 结果位于 `target/jmh-next-async-clean.json`。双异步列的 637 个分配样本中，`FluxIterable$IterableSubscription` 55 个、`ArrayList$ArrayListSpliterator` 46 个，调用栈经过已完成缓存读取的 `Flux.defer`、子查询的 `Flux.deferContextual` 与投影 `Mono.from`；相关子查询的 660 个分配样本主要为逐行 Record、Map/Node、上下文及必要的 Reactor 订阅对象。该采样提示单行已完成缓存仍支付 List 遍历订阅成本，但不是精确字节计量，也不支持删掉相关子查询的 Context/首值/并发操作符。

## 下一切片：已完成缓存单值读取的通用冷 Publisher

目标：当任意已完成的 `cacheMany` 结果只有一个元素时，在完成时创建一次 `Flux.just(value)` 供后续冷读取；空结果和多值结果仍维持 `Flux.fromIterable`。这是按缓存结果基数选择 Reactor 内置 Publisher，不按 SQL、函数名或层数特调。Owning module 为 `internal/SubscriptionContext.CompletedManyCache`、对应缓存契约测试、现有 JMH 与本文档。不改变在途共享、结果 List 的常驻大小、错误/取消重试、订阅隔离、默认容量限制、Context 或背压。

先测当前单层/两列/三列不相关子查询，并以缓存 `EXISTS` 和相关子查询作负对照；实现仅改完成时的 Publisher 选择，补空/单/多值完成后多次订阅、`request(1)`、Context、在途取消与错误测试。阶段末一次性运行完整测试、同配置成对 JMH 和差异检查。保留门槛：真实缓存 SQL 每行分配明确下降、吞吐无稳定回退且所有信号等价；若没有收益则撤回。不增加自定义操作符或新的缓存状态层。

结果：保留 `CompletedManyCache` 的单值冷 `Flux.just`；多值/空值保留 `Flux.fromIterable`，在途共享和完成前失败/取消仍走原链。新增完成后单值 `request(1)`、Context 与来源只订阅一次测试，既有空/多值、并发、取消及错误测试继续通过。`mvn -q -Pjmh package` 通过；`git diff --check` 通过。优化后双异步列 JFR（`target/jfr-singleton-cache-after/`）未再采到此前的 `FluxIterable$IterableSubscription` 和 `ArrayList$ArrayListSpliterator`，只能作为热点消失的方向证据，不用于计算精确字节数。

JDK 17.0.18、512 MB/G1、3×1s 预热、5×1s 测量、2 forks 的真实 SQL 成对结果（`target/jmh-singleton-cache-before.json` / `target/jmh-singleton-cache-after.json`）：单层缓存子查询 6.176→7.205 M 外层行/s，648.06→592.06 B/行；双异步列 3.636→4.405 M，1176.17→1064.18 B/行；三异步列 2.636→2.949 M，1616.29→1448.30 B/行。三列吞吐置信区间较宽，收益幅度不作精确保证；每列减少约 56 B/行可复现。缓存 `EXISTS` 7.789→8.002 M、576.06 B/行不变；相关子查询 2.907→2.891 M、1344.46 B/行不变，均未见稳定回退。该优化减少完成缓存的逐外层行瞬时分配，不减少缓存 List 的常驻大小，也不解决真正相关子查询的逐行执行成本。

## 下一切片：当前 JOIN 与函数链的 JFR 复核

目标：旧 `target/jfr-join/` 仍采到已被具名记录复制快路消除的 Guava 过滤 Map；旧 `target/jfr-functions-current/` 则主要采到标量函数参数 List 和日期值。先对当前代码的 INNER JOIN、常用函数增加仅供诊断的 `Blackhole` 消费基准，排除结果 Map 哈希开销，再以 JFR 判定是否还有跨 SQL 场景可复用的冗余对象或算子。Owning module 为 ReactorQL 根模块；本阶段只改现有 JMH 与本文档，不改生产代码。

复用现有输入、SQL 和 setup 结果检查；JDK 17、512 MB/G1、短时同配置录制分配与 CPU 栈。若热点仍是自定义函数可变 List、必要的日期结果/Record、JOIN 右源订阅与谓词执行，则不为单一基准引入新 SPI、对象池或手写 Subscriber；只有确认一个可低复杂度移除的共享成本后，才另做生产试验、语义差分和成对基准。默认限制、背压、取消、错误和 Context 不变。

结果：新增 `profilingCommonFunctions` 与 `profilingInnerJoin`，只改 JMH 诊断入口；`mvn -q -Pjmh -DskipTests package` 通过。JDK 17.0.18、512 MB/G1、1×1s 预热、2×1s 测量、1 fork 的 JFR 位于 `target/jfr-join-functions-clean/`，诊断分数位于 `target/jmh-join-functions-clean.json`，不与正式吞吐基准比较。函数链的 634 个分配样本主要是 `LocalDateTime` 244、`FixedArgumentList` 236、Record 81 和 String 60；调用栈分别进入通用日期计算、标量函数可变参数契约和字符串截取。INNER JOIN 的 623 个样本主要是 HashMap/桶各 141、`FluxFlatMap$FlatMapInner` 87、`FluxFilterFuseable` 62、`FluxMapFuseable` 60 和 Record 40；Map 栈来自具名来源/结果容器，`flatMap`/`filter` 栈来自右源订阅与 ON 谓词。旧 JFR 的 Guava `Maps$FilteredKeyMap` 不再是当前热点。

结论：这两条路径未发现可在不改变公开 List 可变性、JOIN 来源语义或 Reactive Streams 信号契约下低复杂度删去的共享成本；本切片不改生产代码。下一个未充分覆盖的执行边界是**多参数冷 Publisher 函数**的逐行参数编排：先补同源 JFR 与结果/多值/顺序检查，再判断 `Flux.fromIterable(...).concatMap(...)` 是否确有可消除的操作符成本；不能仅凭其操作符数量更换实现。

## 下一切片：多参数冷 Publisher 函数编排取证

目标：测量 `FunctionMapFeature` 的通用双参数非标量路径，包括两个冷 Publisher 参数、顺序收集和最终单值输出，判断逐行 `Flux.fromIterable(mappers).concatMap(...)` 的成本。Owning module 为 ReactorQL 根模块的现有 JMH 与本文档；先不改生产函数实现。复用现有 `cold_value(id)` Feature 和 20,000 行输入，用一个普通双参数函数按参数顺序累加两个整数；setup 逐行断言结果，并以直接冷 Publisher Feature、已有单参数函数为负对照。另用 Blackhole 诊断入口录制 JFR，避开结果 Map 哈希开销。

若 JFR 确认参数编排而非参数源/函数计算主导分配，才比较同源、保序且订阅时调用 mapper 的 Reactor 内置组合方式；任何候选必须覆盖多值参数、默认值首值语义、异步顺序、错误、取消、背压与 Context。仅当真实 SQL 每行分配明确下降、吞吐不稳定回退且功能等价时保留。不引入手写 Subscriber、参数特定 SQL 分支或跨行共享状态；若没有低复杂度候选，就记录下界并停止该路径。

JFR 已确认继续试验的前提：`target/jfr-two-arg-function/` 的 631 个分配样本中，`FluxConcatMap$ConcatMapImmediate` 104、`FluxIterable$IterableSubscription` 76、`FluxConcatMap` 38、`FluxConcatMap$ConcatMapInner` 14，另有 Object[] 52；诊断入口以 Blackhole 消费结果，setup 逐行核对 20,000 行的两参数结果与顺序。这些样本说明原参数编排值得作 A/B，但不能直接换算每行字节或保证替代实现更快。候选仅用 `Flux.concat(Publisher[])` 顺序连接各参数的惰性 `Flux.defer`，保留多值和现有默认值 `Mono.fromDirect` 分支；若增加数组/闭包后净成本更高则撤回。

结果：`Flux.concat` 候选已撤回，生产代码恢复原 `fromIterable(...).concatMap(...)`。候选下新语义测试通过，覆盖两个冷参数的多值顺序、订阅时调用、Context、首参数错误和取消后不调用次参数；测试和双参数 JMH 保留。候选 JFR（`target/jfr-two-arg-function-concat-after/`）中旧 `ConcatMap` 与参数 `FluxIterable` 样本消失，但新增参数捕获闭包、`Flux.defer` 与 `Mono.defer` 等对象，操作符减少不等于整体更快。

JDK 17.0.18、512 MB/G1、3×1s 预热、5×1s 测量、2 forks 的成对真实 SQL 基准：双参数冷 Publisher 函数 4.603→3.971 M 输入行/s（约 -13.7%），1126.91→1070.91 B/行（-56 B/行）；吞吐误差区间不重叠，不能为减少瞬时分配接受明显吞吐回退。普通函数、直接冷 Publisher Feature、单参数函数三项负对照的 B/行均不变，吞吐差异在误差范围内。证据为 `target/jmh-two-arg-concat-before.json` 与 `target/jmh-two-arg-concat-after.json`。恢复原实现后完整 `mvn -q -Pjmh package` 通过，差异检查通过。该结果是多参数操作符设计的停止条件：保序、惰性、多值等语义下，现有 Reactor `concatMap` 比每参数独立 `defer` 的 `concat` 数组更符合本轮吞吐/堆综合目标；不继续仅按操作符个数替换。

## INTERSECT 的无消费端干扰 JFR

目标：为真实 SQL `select s.v from (select v from t1 intersect select v from t2) s` 增加仅 profiling 使用的 JMH 入口，以 `Blackhole` 消费输出引用，排除标准 `CountingSubscriber` 对结果 Map 的 `hashCode` 遍历。范围仅限 `ReactorQLBenchmark` 与本记录，复用既有左右源和 setup 对 SQL、原生 Map、原生 Record 结果的等价断言。

不做：不改 INTERSECT 生产算法、结果 Map/Record 表示、去重语义、来源订阅顺序、背压、取消、默认限制或 Feature SPI；不按该 SQL 写分支，也不把 JFR 样本等同于每行字节。风险是集合操作本身必须保留 right key 集合及投影对象，JFR 只用于区分这些语义成本与可消除的执行编排成本。

验证：阶段末仅运行 `mvn -q -Pjmh -DskipTests package` 与差异检查，再在 JDK 17、512 MB/G1、1×1s 预热、2×1s 测量、1 fork 下对 profiling-only 入口录制 JFR，记录 CPU/分配调用栈分类。只有发现跨集合/派生表可复用、低复杂度且不改变上述契约的热点，才另行生产 A/B；否则停止在诊断结论。

结果：`mvn -q -Pjmh -DskipTests package` 与 `git diff --check` 通过。JMH worker 在受限沙箱不能绑定 loopback 控制 socket，改在合规本机执行 `java -jar target/reactor-ql-1.0.21-SNAPSHOT-benchmarks.jar '.*ReactorQLBenchmark\\.profilingIntersectRows$' -wi 1 -i 2 -f 1 -w 1s -r 1s -jvmArgsAppend '-Xms512m -Xmx512m -XX:+UseG1GC' -prof 'jfr:dir=target/jfr-intersect-clean' -rf json -rff target/jmh-intersect-clean.json`。JDK 17.0.18、512 MB/G1 下的诊断分数为 12.023 M 输入行/s，仅作取证；JFR 位于 `target/jfr-intersect-clean/org.jetlinks.reactor.ql.ReactorQLBenchmark.profilingIntersectRows-Throughput/profile.jfr`。

JFR 录得 109 个 CPU、660 个分配样本，且没有 `CountingSubscriber` 或其 `Map.hashCode` 路径。主要分配类别为 `HashMap` 212、`HashMap$Node` 182、`HashMap$Node[]` 131、`HashMap$EntrySet` 68、`HashMap$EntryIterator` 31、`DefaultReactorQLRecord` 24。`EntrySet` 的分配栈为 `AbstractMap.hashCode -> HashSet.add/remove`，用于 INTERSECT 的 Map 键去重和删除；不是消费端噪声。CPU 与分配栈主要落在 `DefaultReactorQLRecord.resultToRecord`（复制投影结果和保留来源容器）、`SubSelectFromFeature` 派生表转换、`HashSet` 集合键哈希，以及 Reactor `map/filter/flatMap` 的左右源编排。`resultToRecord` 已保留结果 Map 独立性并避免未物化源 Map 的额外物化；继续删其 `HashMap` 拷贝会改变派生记录可变性/别名隔离。Map 键 hash 的 `EntrySet` 同样是精确集合语义所需，缓存 hash 或自定义 Map 会改变可变 Map 契约并增加复杂度。

结论：没有值得生产 A/B 的低复杂度通用候选。若未来要验证更大架构变更，唯一可证伪实验是以保持 Map 相等/可变性和结果隔离的通用 Record/投影表示替代当前对象图，再对 INTERSECT、UNION、派生表、JOIN 及第三方 Feature 做语义和成对 JMH；这超出本轮“禁止过度设计”的边界，当前停止，不为 INTERSECT 特调。

## DISTINCT 与 Top-N 的无消费端干扰 JFR

目标：为真实 `select distinct this val from test` 与 `select this val from test order by this limit 100` 各增加一个 profiling-only JMH，以 `Blackhole` 消费结果引用，排除标准 `CountingSubscriber.hashCode` 对 Map 的遍历和 `EntrySet` 分配。范围只限现有 JMH 与本文档，复用当前输入、SQL，以及 DISTINCT 数量、Top-N 值域和 Top-N 原生对照的 setup 校验。

不做：不改去重/排序/limit 的生产实现、集合与优先队列表示、结果顺序、默认限制、背压、取消、Map/Record 契约或 Feature SPI；不按输入容量或 SQL 文本特调，不从采样数量换算 B/行。风险是精确 DISTINCT 必须保持已见 key，Top-N 必须保持有界候选和稳定排序；JFR 只能识别其中的语义成本与执行编排成本。

验证：阶段末统一 `mvn -q -Pjmh -DskipTests package` 与差异检查；随后用 JDK 17、512 MB/G1、1×1s 预热、2×1s 测量、1 fork 分别录制两个 profiling-only 入口，分类 CPU/分配栈。只有热点在 DISTINCT、集合操作或排序场景间可复用且能低复杂度移除时，才单独提出生产 A/B；否则记录停止结论。

结果：`mvn -q -Pjmh -DskipTests package` 与 `git diff --check` 通过。受限沙箱内 JMH worker 不能绑定 loopback 控制 socket，分别在合规本机执行 `java -jar target/reactor-ql-1.0.21-SNAPSHOT-benchmarks.jar '.*ReactorQLBenchmark\\.profilingDistinctRows$' ... -prof 'jfr:dir=target/jfr-distinct-clean' -rf json -rff target/jmh-distinct-clean.json` 和 `java -jar target/reactor-ql-1.0.21-SNAPSHOT-benchmarks.jar '.*ReactorQLBenchmark\\.profilingOrderByLimit$' ... -prof 'jfr:dir=target/jfr-topn-clean' -rf json -rff target/jmh-topn-clean.json`；两者均为 JDK 17.0.18、512 MB/G1、1×1s 预热、2×1s 测量、1 fork。诊断 JFR 分别在 `target/jfr-distinct-clean/org.jetlinks.reactor.ql.ReactorQLBenchmark.profilingDistinctRows-Throughput/profile.jfr` 与 `target/jfr-topn-clean/org.jetlinks.reactor.ql.ReactorQLBenchmark.profilingOrderByLimit-Throughput/profile.jfr`；诊断分数不用于正式吞吐比较。

DISTINCT JFR 有 87 个 CPU、640 个分配样本，未见 `CountingSubscriber`/消费端 hash。主要类别为 `Integer` 174、`HashMap` 173、`HashMap$Node` 104、`HashMap$Node[]` 102、Record 59。`Integer` 来自 JMH `distinctInput` 的 `Flux.range(...).map(index -> index & 1023)`；其余 Map/Node 栈主要是投影结果容器写入，另有 CPU 栈 `BoundedStateSupport.distinct -> HashSet.add` 的精确 seen-key 状态。输入装箱不是生产候选；结果 Map 是输出契约；seen key 是无界精确 DISTINCT 的最小状态，且已有上限保护。把 `select distinct this` 前推到原始输入只对这个表达式形态成立，会绕过一般投影/Feature 的求值和错误语义，不做特调。

Top-N JFR 有 122 个 CPU、612 个分配样本，主要是 `HashMap$Node[]` 177、`Integer` 173、`HashMap` 106、`Object[]` 27、Record 20 与每行排序附加 lambda。CPU 栈集中在 `PriorityQueue.offer/poll/siftUp/siftDown`、`OrderBySupport.addTopNRecord`、通用 `compareOrderValue/CompareUtils.compare`；这是有界 Top-N 对每个候选与当前最差项比较的算法成本。`Object[]` 栈来自 `OrderBySupport.createOrderValueMapper`，承载通用多列 ORDER BY 的每条排序键；为本 SQL 的单排序列单独换成标量表示会是按形态特调，且会引入双表示/比较分支。Map/Integer 同样分别为结果投影与 JMH 输入装箱。

结论：两条路径都没有跨 SQL、低复杂度且不破坏精确 DISTINCT、通用多列排序或 Map/Record 契约的生产候选，停止于 JFR 诊断。若未来需要评估更大变化，应独立以所有 ORDER BY 键数、null ordering、表达式错误、稳定结果和取消/背压作为契约，比较一种统一的排序键表示；这不是本轮可接受的“减少操作符”优化。

## 高基数分组聚合的去源分配 JFR

目标：为现有 `highCardinalityCount` 与 `highCardinalityAggregates` 各增加 profiling-only Blackhole JMH。诊断源在自身首次使用时才预构造与正式冷源完全同序的 50,000 个 `{key, score, payload}` 行，并以 `Flux.fromArray` 重放，消除每次 invocation 的 Map/String/byte[] 源构造对 JFR 的遮蔽；正式基准仍保留原 `Flux.range().map(...)` 输入。

不做：不改高基数分组、聚合、最大活跃 key 限制、窗口、输出 Map/Record、背压、取消或 Feature SPI；不为 count 或多聚合单独改变状态表示，不把诊断 State 的常驻对象当作生产堆占用。风险是预构造诊断 State 会额外保留约 50 MB payload 及其行对象，且仅在 profiling 首次 warmup 后存在；首次使用对标准与诊断源分别校验 count 和五聚合输出等价。

验证：阶段末统一执行 `mvn -q -Pjmh -DskipTests package` 和差异检查，再在 JDK 17、512 MB/G1、1×1s 预热、2×1s 测量、1 fork 下分别录制 count/多聚合的 JFR，分类 worker CPU 与分配 site。只有发现跨分组场景可复用且低复杂度可移除的成本，才建议独立生产 A/B；否则只记录结论。

结果：`mvn -q -Pjmh -DskipTests package` 与 `git diff --check` 通过。受限沙箱中 JMH worker 不能绑定 loopback 控制 socket，分别在合规本机运行 `java -jar target/reactor-ql-1.0.21-SNAPSHOT-benchmarks.jar '.*ReactorQLBenchmark\\.profilingHighCardinalityCount$' -wi 1 -i 2 -f 1 -w 1s -r 1s -jvmArgsAppend '-Xms512m -Xmx512m -XX:+UseG1GC' -prof 'jfr:dir=target/jfr-highcard-count-clean' -rf json -rff target/jmh-highcard-count-clean.json` 和对应 `profilingHighCardinalityAggregates` / `target/jfr-highcard-aggregates-clean` 命令。两者为 JDK 17.0.18、512 MB/G1、1×1s 预热、2×1s 测量、1 fork；JFR 路径分别为 `target/jfr-highcard-count-clean/org.jetlinks.reactor.ql.ReactorQLBenchmark.profilingHighCardinalityCount-Throughput/profile.jfr` 与 `target/jfr-highcard-aggregates-clean/org.jetlinks.reactor.ql.ReactorQLBenchmark.profilingHighCardinalityAggregates-Throughput/profile.jfr`，结果 JSON 分别为 `target/jmh-highcard-count-clean.json` 与 `target/jmh-highcard-aggregates-clean.json`。诊断分数不用于正式吞吐比较。

count JFR 有 124 个 CPU、616 个分配样本，主要为 `HashMap$Node` 206、`HashMap` 152、Record 63、`HashMap$Node[]` 48、`CountAggFeature` 累加器 33、`GroupState` 32、key `Object[]` 32。CPU 栈集中于 `SubscriptionState.PrefixState.getGroup` 的按 key 查找/创建、`GroupState.add` 的计数、`GroupState.toRecord` 的窗口结束输出，以及属性读取。多聚合 JFR 有 134 个 CPU、625 个分配样本，主要为 `HashMap$Node[]` 177、Record 97、`HashMap` 92、`Double` 83、多个增量聚合 accumulator 与 `GroupState`；CPU 栈集中于同一组状态创建/查找、`MapAggFeature.add/addValue`、数值转换与最终 `toRecord`。

两者都未采到诊断源 Map/String/byte[] 的逐 invocation 构造；预构造输入只保留在 profiling State，不能视为生产驻留。剩余 group key、GroupState、每聚合 accumulator 与结束时输出 Map 都对应精确高基数分组的最小活跃状态或结果语义；payload 未进入聚合状态。替换为近似/淘汰/跨窗口共享会改变精确分组、窗口和默认限制；为 count 或五聚合分别布置专用状态也属于特调。结论：未发现跨分组、低复杂度的生产 A/B 候选，本切片止于诊断。

## 宽同步投影的结果 Map 容量取证

目标：新增一个八列混合同步投影的正式 JMH 与 Blackhole profiling-only 入口，用惰性预构造的 `Integer[]` / `Flux.fromArray` 输入排除 `Flux.range` 装箱。setup/首次使用逐行与简单 Java 对照验证八列的值、`Number`/`Boolean` 类型和 `Map` 迭代顺序；正式与诊断入口共用该输入，以便后续通用 A/B 对比。

不做：不改变现有两列 projection、生产 Map 容量、Record/结果 Map 契约、算术类型、背压或 Feature SPI；不为八列 SQL 建专用结果类型。风险是 `HashMap.resize` 首次分配桶与真正扩容共享同一 JDK 栈，须以容量 4、阈值 3 及八次 `setResult` 的插入序列区分；JFR 采样与 JMH 分配指标均不直接等同于生产每行常驻内存。

验证：阶段末执行 `mvn -q -Pjmh -DskipTests package` 和差异检查，再以 JDK 17、512 MB/G1、1×1s 预热、2×1s 测量、1 fork 对 profiling 入口同时采集 JFR 与 JMH `gc` 分配指标。只有宽投影证明结果 Map 扩容是跨投影形态的明确净成本，才建议一个不改变 Map 契约的生产 A/B；否则停止。

结果：新增正式 `wideProjection` 与 `profilingWideProjection`，均共用首次 warmup 惰性构造的 1,000,000 个 boxed 输入和 `Flux.fromArray`。首次使用全量逐行验证八列值、`Integer`/`Long`/`Boolean` 类型以及与相同容量 Java `HashMap` 对照的迭代顺序；现有两列 `projection` 未改变。`mvn -q -Pjmh -DskipTests package` 与 `git diff --check` 通过。受限沙箱内 JMH worker 不能绑定 loopback 控制 socket，合规本机命令为 `java -jar target/reactor-ql-1.0.21-SNAPSHOT-benchmarks.jar '.*ReactorQLBenchmark\\.profilingWideProjection$' -wi 1 -i 2 -f 1 -w 1s -r 1s -jvmArgsAppend '-Xms512m -Xmx512m -XX:+UseG1GC' -prof gc -prof 'jfr:dir=target/jfr-wide-projection-clean' -rf json -rff target/jmh-wide-projection-clean.json`。

JFR 在 `target/jfr-wide-projection-clean/org.jetlinks.reactor.ql.ReactorQLBenchmark.profilingWideProjection-Throughput/profile.jfr`，结果 JSON 为 `target/jmh-wide-projection-clean.json`。JDK 17.0.18、512 MB/G1、1×1s 预热、2×1s 测量、1 fork 下，JFR 有 126 个 CPU、660 个分配样本，未采到 `Integer` 输入装箱或 `CountingSubscriber` 消费端路径；主要分配类别为 `HashMap$Node` 265、`Long` 134、`HashMap$Node[]` 129、`HashMap` 125、Record 6。GC profiler 的短时归一化分配为 609.524 B/输入行（两次测量 610.987 / 608.061），它是总瞬时分配而非常驻堆或单类精确归因。CPU 栈集中在八个标量 mapper、`setResult` 写入、`HashMap.put/resize` 和 `CalculateUtils` 的 Long 算术装箱。

容量边界：结果容器仍由 `new HashMap<>(4)` 建立。八次 `setResult` 时第 1 次调用 `resize` 分配首个 4 桶数组（阈值 3），第 4 次扩至 8 桶（阈值 6），第 7 次扩至 16 桶；JFR 的 `HashMap.resize` 栈本身不能标识第几次，因此按 JDK 容量规则与已验证的八列插入序列区分，不能把全部 129 个样本误称为实际扩容。五个 Long 是投影结果 Map 中必需的数值对象。此证据形成一个**可证伪的通用候选**：在编译期已知标量 SELECT 输出列数时，为内置 Record 的结果 Map 提供容量 hint，按预期 entry 数计算初始容量，避免宽投影的首次后两次再散列；第三方 Record 保持现有路径。候选不得改用全局固定 16 桶（会回退窄投影），也不得改变返回 Map 可变性、顺序或数值类型。是否保留只由覆盖窄/中/宽投影、算术/属性/布尔列和第三方 Record 的生产 A/B 与全量语义测试决定；本切片不改生产代码。

## 宽投影结果 Map 容量 hint 的生产 A/B

目标：仅在 `DefaultReactorQL` 已知普通投影列数大于 3、无 `*` 展开且 Record/Context 分别为内置 `DefaultReactorQLRecord` / `DefaultReactorQLContext` 时，延迟到首个非 null 结果写入才创建按预期 entry 数计算容量的 `HashMap`。以 JDK 17、512 MB/G1、3×1s 预热、5×1s 测量、2 forks 先记录现有两列与八列正式 JMH 基线，再实施和成对复测。

不做：不新增 SPI、操作符或全局固定容量；不改变第三方 Record/Context、`newContainer()`、`resultToRecord`、聚合、`*` 展开、异步空投影、全 null 行、返回 Map 可变性/隔离、背压或取消。容量 hint 仅是内置结果容器首次分配的实现细节；窄列、全 null 与空异步不得因此额外分配。

验证与门槛：补 2/4/8 列、null、异步空列、第三方容器、可变 Map 和结果隔离测试。阶段末集中执行全量 `mvn -q -Pjmh package`、差异检查，并复测八列收益、两列/异步/分组负对照。只有八列分配明确下降、吞吐无稳定回退且全部语义成立才保留；否则撤回候选并在此记录原因。

结果：先实现过将容量 hint 保存在 `DefaultReactorQLRecord` 字段的版本。它虽然使八列投影从 607.988 降到 535.987 B/行，但两列负对照从 255.994 增至 263.994 B/行；新增字段扩大了每行 Record，故已撤回，不能以宽投影局部收益换取所有窄投影的固定分配。

最终实现不在 Record 保存状态：`DefaultReactorQL` 仅在编译期确认“全标量、普通投影列数大于 3、无 `*` 展开、精确内置 Record”时，将预期列数传给 package-private 写入方法。该方法仅在首个非 null 写入且 Context 精确为 `DefaultReactorQLContext` 时以 `ceil(entries / 0.75)` 的初始容量创建可变 `HashMap`；Record/Context 子类、第三方实现、窄投影、`*` 展开、异步投影和 null 值均继续原有 `setResult` / `newContainer` 路径。没有新增 SPI、操作符、缓存或跨行状态。

新增 2/4/8 列的数值/迭代顺序/Map 可变性与行隔离测试，null 和空异步列行为测试，`*` 展开测试，以及覆盖 `newContainer()` 覆写的 Context 子类回退测试。`mvn -q -Pjmh package`（392 tests）和 `git diff --check` 均通过；测试日志中既有的属性警告、预期 `onErrorDropped` 和除零错误信号不影响命令退出状态。

JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、`gc` profiler 的正式成对结果：基线 `target/jmh-wide-map-baseline.json` 中两列为 17.728±2.659 M 行/s、255.994 B/行，八列为 4.959±0.119 M 行/s、607.988 B/行；最终 `target/jmh-wide-map-final.json` 中两列为 16.791±1.775 M 行/s、255.994 B/行，八列为 6.233±0.691 M 行/s、527.988 B/行。两列分配保持不变，吞吐置信区间重叠；八列每行瞬时分配减少 80 B，吞吐无稳定回退。最终负对照还包括双异步子查询 4.245±0.292 M 行/s、1064.179 B/行和全局聚合 38.988±0.118 M 行/s、16 B/行；该候选不进入其异步/聚合路径。GC B/行是瞬时分配指标，并非常驻堆或单类归因。

## 两列投影与原生 Record 对照的无消费端干扰 JFR

目标：为现有两列表达式投影 SQL 与等价 `nativeRecordProjection` 各增加一个 profiling-only Blackhole JMH，统一复用 `wideProjectionInput()` 的预构造整数输入，排除 `Flux.range` 装箱和 `CountingSubscriber.hashCode` 结果 Map 遍历。首次 warmup 逐行检查两路输出的值、类型和 Map 迭代顺序等价，但不收集百万输出。

不做：不改变正式 JMH、生产/测试代码、SQL 语义、输入缓存、背压、取消、Map/Record 契约或 Feature SPI；不从 JFR 采样直接推导 B/行，也不针对单一 SQL 写快速路径。阶段末只执行 `mvn -q -Pjmh -DskipTests package` 与差异检查，并在 JDK 17、512 MB/G1、1×1s 预热、2×1s 测量、1 fork 下分别录制 JFR。只有识别到跨投影可复用、低复杂度且不改变契约的热点，才另开生产 A/B；否则记录停止结论。

结果：新增 `profilingProjectionPrebuiltInput` 与 `profilingNativeRecordProjectionPrebuiltInput`，均复用 `wideProjectionInput()`；首次使用以 `zipWith` 全量逐行检查 SQL 与原生 Record 的结果值、类型和 Map 迭代顺序，未物化百万输出。正式基准未改变。`mvn -q -Pjmh -DskipTests package` 和 `git diff --check` 通过。

JDK 17.0.18、512 MB/G1、1×1s 预热、2×1s 测量、1 fork 的诊断 JFR 位于 `target/jfr-projection-native-clean/org.jetlinks.reactor.ql.ReactorQLBenchmark.profilingProjectionPrebuiltInput-Throughput/profile.jfr` 与 `target/jfr-projection-native-clean/org.jetlinks.reactor.ql.ReactorQLBenchmark.profilingNativeRecordProjectionPrebuiltInput-Throughput/profile.jfr`，JMH 输出为 `target/jmh-projection-native-clean.json`。诊断分数分别为 18.793 M 与 41.221 M 输入行/s，仅用于解释路径差异，不作正式吞吐结论。

SQL 路径录得 140 个 CPU、660 个分配样本；主要分配类别为 `Long` 254、`HashMap` 147、`HashMap$Node[]` 139、`DefaultReactorQL` 捕获 lambda 105、Record 5。后者调用栈精确落在 `DefaultReactorQL.createMapper` 的 `allMapper.forEach(mapper -> mapper.accept(record))`：即使没有 `*` 展开，空列表的 `forEach` 参数仍每行捕获 `record`。CPU 栈还集中于通用 FROM→Record 建立、WHERE 二元比较/数值转换、两列二元计算、同步 `handle` 阶段和 `setResult`；这些是 SQL 表达式、过滤和输出 Record 契约所需工作。原生 Record 路径有 104 个 CPU、649 个分配样本，主要为 `Long` 339、`HashMap$Node` 281、`HashMap` 26，CPU 主要在 `projectNative`、`CalculateUtils`、Record 建立和 `handle`；未见 SQL 路径的空 `allMapper` 捕获 lambda。

结论：不以 JFR 样本估算该 lambda 的字节收益，也不据诊断分数直接承诺 2× 吞吐改进。唯一具备通用、低复杂度生产 A/B 条件的候选是：仅在 `allMapper` 非空时执行其遍历，避免空展开列表的每行捕获 lambda；它不依赖 SQL 文本、列数或输入规模，仍需独立覆盖 `*`/`t.*`、null、行隔离、背压/取消和正式 JMH 后才可保留。本诊断切片到此停止，未改生产代码。

## 空投影展开列表的捕获 lambda 生产 A/B

目标：消除同步标量投影中空 `allMapper` 列表的每行捕获 lambda，仅以普通 `for` 循环保留原有 mapper 顺序与异常传播。范围为 `DefaultReactorQL`、投影测试、现有 JMH 与本文档；基线为 `target/jmh-wide-map-final.json` 的两列/八列投影。若新增星号正式基准，先建立其独立基线。

不做：不新增操作符、SPI、缓存或 SQL 文本分支；不修改 Record/Map、表达式、异步投影、Context、背压、取消或默认限制。异步 `allMapper` 路径只有在能以编译期一次性 Consumer 保持 `doOnNext` 信号语义且不增加复杂度时才处理，否则保持不动。风险是 `*` / `t.*` 展开顺序、null 忽略、第三方 Feature 异常和取消必须与现有路径完全一致。

验证：补或复用普通投影、`*` / `t.*`、null、第三方 Feature、行隔离和信号测试；阶段末统一执行 `mvn -q -Pjmh package`、差异检查，以及 JDK 17、512 MB/G1、3×1s 预热、5×1s 测量、2 forks 的两列/八列与星号负对照 JMH。必要时用同源干净 JFR 验证捕获 lambda 消失。仅在瞬时分配下降、吞吐无稳定回退且星号语义等价时保留。

结果：同步标量投影将 `allMapper.forEach(mapper -> mapper.accept(record))` 改为普通 `for (Consumer<ReactorQLRecord> mapper : allMapper)` 循环；它保留 mapper 的 LinkedHashMap/select 顺序、异常传播和对同一 Record 的写入顺序，但避免空列表或 `*` 展开时每行创建捕获 `record` 的 Consumer。异步路径保持原样，未引入一次性 Consumer 或额外信号层。补充 `t.*` 行为回归；既有 `*`、null、空异步、Context 子类、行隔离、取消和第三方 Feature 覆盖继续通过。

为星号负对照新增正式 `starProjection`，复用 `wideProjectionInput()`，先在未改生产循环时测得 `target/jmh-star-projection-baseline.json`：42.079±1.135 M 行/s、168.001±12.749 B/行。阶段末 `mvn -q -Pjmh package`（392 tests）与 `git diff --check` 通过；日志中的属性警告、预期 `onErrorDropped`、除零错误信号均为既有测试行为且命令退出成功。

JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、`gc` profiler 的最终结果在 `target/jmh-empty-allmapper-after.json`：两列为 16.774±1.902 M 行/s、255.994 B/行，对比 `target/jmh-wide-map-final.json` 的 16.791±1.775 M、255.994 B；八列为 6.003±0.180 M、527.988 B，对比 6.233±0.691 M、527.988 B；星号为 41.238±0.388 M、160.001 B，对比其独立基线的 42.079±1.135 M、168.001 B。普通/宽投影吞吐置信区间重叠且分配不变；星号分配减少约 8 B/行，吞吐置信区间重叠。因此保留该通用循环改动，而不将星号吞吐的短测差异解释为稳定回退。

同源空展开两列诊断 JFR 位于 `target/jfr-empty-allmapper-after/org.jetlinks.reactor.ql.ReactorQLBenchmark.profilingProjectionPrebuiltInput-Throughput/profile.jfr`，JDK 17、512 MB/G1、1×1s 预热、2×1s 测量、1 fork 下有 135 个 CPU、660 个分配样本。主要分配为 `Long` 308、`HashMap$Node` 270、`HashMap` 65、桶数组 12、Record 4；不再有先前 105 个 `DefaultReactorQL` 捕获 lambda 样本。JFR 只证明取证热点消失，不能单独量化字节收益；正式 GC 指标是保留判断依据。

### 星号分配的 A/B/A 判别复测

目标：已有星号基线两个 fork 分别为约 160/176 B/行、改后两个 fork 为 160/160 B/行，均值差不能证明稳定收益。保留当前 `for` 改动的可恢复副本，临时还原原 `allMapper.forEach(...)` 后用至少 3 forks 复测，再恢复 `for` 并同配置复测；各阶段先记录源码身份并写入独立 JSON。

不做：不改变循环以外逻辑、异步路径、JMH 场景、测试或生产契约；不在中间状态跑完整套件。最终源码恢复后才集中执行完整构建、测试及差异检查。判定：只有改前后分配在多数 fork 明确分离、吞吐无稳定回退且星号语义不变才保留；否则撤回循环生产改动，保留诊断/JMH/测试证据并回填原因。

结果：源码身份已分别记录：原 `forEach` 为 SHA-256 `76875bb0482ca1b120ba5403c966fb560d6b0b4c81cbf8d055daab26a9779103`，临时 `for` 为 `9aaf18e3577e37e318656dd3d0dedd2cf6de61586c49d430c0c6179a72c3f682`；两阶段各以 `mvn -q -Pjmh -DskipTests package` 构建，且只改这一行。JDK 17.0.18、512 MB/G1、3×1s 预热、5×1s 测量、3 forks、`gc` profiler 的独立 JSON 分别为 `target/jmh-empty-allmapper-original-3f.json` 与 `target/jmh-empty-allmapper-for-3f.json`。

普通 projection 原/for 分别为 17.464±1.499 / 17.392±1.265 M 行/s，均为 255.994 B/行；吞吐区间重叠、分配无变化。星号原/for 分别为 40.893±1.051 / 40.606±0.690 M 行/s；原 `forEach` 的每 fork 分配均值为 `[160.00067, 160.00067, 176.00067]` B/行，`for` 为 `[160.00067, 160.00067, 160.00067]` B/行。原实现多数 fork 已是 160 B/行，唯一 176 B fork 不能归因于循环实现，两个阶段吞吐区间也重叠。因此原先两-fork均值差不足以构成明确收益，未达到门槛。

决定：已恢复原 `allMapper.forEach(mapper -> mapper.accept(record))` 生产代码，不保留循环改动。先前 JFR 中的 105 个捕获 lambda 采样仍只作为“值得 A/B”的方向证据，不能转换为每行字节收益；改后 JFR 的热点消失也不足以推翻 3-fork GC 指标。保留新增星号基准、投影/JFR 诊断入口及 `t.*` 回归测试，以便未来在不同 JVM/GC 或更长测量下复核；本切片不再继续围绕该操作符形态优化。

## WHERE 聚合与原生 Reactor 对照 JFR

目标：为现有 `where` SQL（`count(1)` 与两个数值范围比较）构造语义等价的原生 Reactor 链，并各加一个 profiling-only Blackhole JMH。两路统一使用惰性预构造 `Integer[]` / `Flux.fromArray` 输入，排除 `Flux.range` 装箱与 `CountingSubscriber.hashCode` Map 遍历；首次 warmup 仅验证最终值、值类型、完成和订阅次数，不物化百万中间行。

不做：不改正式基准、生产/测试代码、WHERE/聚合语义、背压、取消、Map/Record 契约或 Feature SPI；不按 SQL 文本增加优化。必要时只增加同表达式的半通过率输入作为诊断反例。阶段末运行 `mvn -q -Pjmh -DskipTests package` 与差异检查，并在 JDK 17、512 MB/G1、1×1s 预热、2×1s 测量、1 fork 下采集 JFR。JFR 分数仅用于定位调用栈，不能作为正式吞吐提升结论；只有发现跨 WHERE/投影/聚合共享且低复杂度的热点才建议独立生产 A/B。

结果：新增 `profilingWherePrebuiltInput` 与 `profilingNativeWherePrebuiltInput`。两者共用 `wideProjectionInput()`；首次 warmup 使用 `Mono.zip` 对两个最终单值 Map 进行值与 `total` 类型校验，同时为两条源各计数一次订阅，完成时不物化百万中间行。原生对照为 `filter(value >= 0 && value < LARGE_ROWS).count()` 后构造同形 `{total: Long}` Map；正式 `where` 基准未改变。`mvn -q -Pjmh -DskipTests package` 与 `git diff --check` 通过。

JDK 17.0.18、512 MB/G1、1×1s 预热、2×1s 测量、1 fork 的诊断 JFR 位于 `target/jfr-where-native-clean/org.jetlinks.reactor.ql.ReactorQLBenchmark.profilingWherePrebuiltInput-Throughput/profile.jfr` 与 `target/jfr-where-native-clean/org.jetlinks.reactor.ql.ReactorQLBenchmark.profilingNativeWherePrebuiltInput-Throughput/profile.jfr`，JMH JSON 为 `target/jmh-where-native-clean.json`。诊断分数 SQL 46.268 M、原生 510.650 M 输入行/s，仅用于解释栈差异，不能称为正式吞吐提升。

SQL 路径录得 128 个 CPU、660 个分配样本，其中 `DefaultReactorQLRecord` 657 个；CPU 栈集中于通用 `BinaryFilterFeature` / `AndFilter` 比较、`DefaultReactorQLRecord`、`WindowedAggregateStage.SubscriptionState` 的逐行累加与 `PrefixState` 查找。原生链录得 112 个 CPU、仅 1 个分配样本（最终容器初始化），主要 CPU 为 Reactor `FluxArray`、融合 `filter` 与 `MonoCount`；二者都不含消费端 Map hash 或 `Flux.range` 装箱。

结论：可见差异是 SQL 引擎为通用来源、表达式、Record 和聚合状态语义而逐行构建 Record/执行 Feature 的成本，不是一个可低复杂度移除的共享操作符。将 `this` 的标量范围谓词和 `count` 直接前推到原始源会绕过第三方 Feature、属性/类型错误、Record/Context 与聚合路径，属于按形态特调；为它扩展另一套原始流聚合也会增加双执行表示。无需半通过率反例：当前记录创建发生在 WHERE 前，输入通过率不能消除该通用成本。本切片不改生产代码，也不提出生产 A/B。

## 任意原始行的受限标量聚合生产 A/B

目标：JFR `profilingWherePrebuiltInput` 的 660 个分配样本中有 657 个 `DefaultReactorQLRecord`，在不按 SQL 文本/输入类型特调的前提下，使原始聚合仅对已声明“可接受任意行”的内置常量和默认 `this` 标量映射、及已声明可原始求值的过滤器绕开 Record 创建。先建立 `wherePrebuiltInput` SQL/native 正式同配置基线。

范围：只扩展 `RawScalarValueMapper` / `RawScalarFilter` 默认能力标记、内置默认 this/常量映射、二元/AND/OR 组合标记及 `WindowedAggregateStage.addRawRow` 的严格分支；测试、JMH 与本文档同步更新。非 Map 行且**聚合器、raw filter 都声明接受任意行**才执行 `testRaw + addRaw`，其余无条件回退既有 Record 链。

不做：不新增操作符、独立 SPI、缓存、跨行状态、SQL 文本或类型特例；仅在现有 raw scalar SPI 上增加默认能力方法，已有外部实现默认返回 false 并保持 Record 回退。不改变参数、第三方 Feature、Map 行、null/异常、空源、Context、背压、取消、checkpoint 或聚合语义。Binary/AND/OR 只有两侧都声明 true 才组合 true，仍完整求值两侧并将异常按既有规则转 false。验证覆盖整数/字符串/Map、AND/OR、参数/第三方回退、null/异常、空源、request(1)/取消/错误/Context、多订阅、checkpoint、count*/count(this)/sum(this)；阶段末一次全量测试、差异检查和正式正负对照 JMH。任何功能、分配或吞吐门槛失败即撤回生产候选。

结果：`RawScalarValueMapper` 与 `RawScalarFilter` 新增保守的 `acceptsAnyRow()` 默认 false；`ScalarValueMapper.constant` 和内置默认 `this` 映射显式返回 true。二元、AND、OR 原始过滤器仅在两侧均返回 true 时声明 true，并保持两侧求值以及二元比较原有的异常转 false 行为。`WindowedAggregateStage.addRawRow` 仅在非 Map 行、所有聚合器接受任意行、且 raw filter 明确接受任意行时直接 `testRaw + addRaw`；Map 行保留既有 raw Map 路径，参数、第三方/自定义 Feature、字段读取聚合与未声明能力的表达式都无条件进入 Record 回退。未新增操作符、缓存、跨行状态或 SQL/类型分支。

新增整数、字符串与 Map 行的 AND raw filter 对照和 `count(this)`/`sum(this)` 回退等价测试；既有参数、第三方属性 Feature、null/空源、异常、`request(1)`、取消、Context、多订阅及 checkpoint 覆盖继续通过。`mvn -q -Pjmh package`（395 tests）和 `git diff --check` 通过；测试日志中既有属性警告、预期 `onErrorDropped` / 除零信号不影响退出状态。

先将能力候选临时恢复为原实现并以 `mvn -q -Pjmh -DskipTests package` 构建正式基线 `target/jmh-raw-any-row-baseline.json`，再恢复候选、完成全量验证后以相同 JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、`gc` profiler 生成 `target/jmh-raw-any-row-after.json`。目标标量 WHERE/count 的 SQL 结果为 51.055±0.462 M 行/s、32.003 B/行 → 60.664±2.182 M、0.001 B/行；吞吐置信区间不重叠且逐行瞬时分配近乎消失。原生对照为 478.791±11.977 → 449.962±32.240 M、均 0 B/行，区间重叠。Map WHERE 聚合为 38.349±0.663 → 38.572±0.504 M、均 16 B；无 WHERE 聚合为 38.932±0.843 → 38.442±0.359 M、均 16 B；双异步子查询为 4.399±0.226 → 4.455±0.168 M、均 1064.179 B，均未见稳定回退。

候选后同源短时 JFR 位于 `target/jfr-raw-any-row-after/org.jetlinks.reactor.ql.ReactorQLBenchmark.profilingWherePrebuiltInput-Throughput/profile.jfr`，配套 JSON 为 `target/jmh-raw-any-row-jfr-after.json`。JDK 17、512 MB/G1、1×1s 预热、2×1s 测量、1 fork 下录得 141 个 CPU、1 个分配样本（最终容器初始化），不再有基线 JFR 中 657/660 的逐行 `DefaultReactorQLRecord` 样本。JFR 只证明热点方向；保留判断由正式 GC 分配、吞吐和完整语义验证共同给出。

## 当前源码全局性能回归验收

以当前源码重建 benchmark jar 后，统一在 JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、`gc` profiler 下覆盖 WHERE、投影、全局聚合、高基数聚合、相关/深层子查询、JOIN、DISTINCT、Top-N、函数和 INTERSECT。完整结果为 `target/jmh-global-current.json`。与配置相同且场景对应的历史正式 JSON 比较，没有已确认超过 5% 的吞吐回退：WHERE 为 61.684 M 行/s、约 0 B/行；全局聚合为 38.040 M、16 B/行；高基数 count 为 2.853 M、2059 B/行，多聚合为 2.293 M、2303 B/行；INNER JOIN 为 7.405 M、568 B/行；INTERSECT 为 13.467 M、510 B/行。高基数多聚合较对应历史均值约多 12 B/行仅由一个 fork 的 +24 B 偏差造成，不能归因于生产回退。

为排除两 fork 吞吐波动，投影与 DISTINCT 以同一 JDK/堆设置、3×1s/5×1s、3 forks、`gc` profiler 复测，结果在 `target/jmh-global-variance-repeat.json`：投影 18.541±1.134 M 行/s、255.994242 B/行，三 fork 为 18.669/18.293/18.660 M；DISTINCT 38.159±1.157 M、177.169356 B/行，三 fork 为 38.071/36.928/39.477 M。两者均与全局验收的两 fork 区间重叠，且分配稳定，未确认回退。深层子查询和函数场景没有同配置历史正式基线，仅保留本轮绝对值，不作趋势结论。

本记录是回归门禁，不证明 SQL 路径已接近原生 Reactor。GC B/行是瞬时分配指标，不等同于常驻堆；高基数聚合的活跃 key 状态是当前查询状态，不能表述为历史输入行驻留。

## 深层子查询的无消费端干扰 JFR

目标：旧深层子查询 JFR 使用 JDK 21 且 `consume()` 会计算结果 Map hash，不能用于当前 JDK 17 热点归因。为同一 SQL、同一输入新增 profiling-only Blackhole JMH，复用 `consumeForProfiling`，在不物化结果的前提下保留输出数量、值和完成信号的既有 setup 校验。

范围与步骤：修改 `src/jmh/java/org/jetlinks/reactor/ql/ReactorQLBenchmark.java`、对应 `SubqueryCacheTest` 与本文档；新增 `profilingDeeplyNestedSubquery`，必要时修正诊断 SQL 夹具及其结果/缓存回归，一次性构建 JMH jar，并在 JDK 17、512 MB/G1、1×1s 预热、2×1s 测量、1 fork 下以 JFR 采样，结果写入 `target/jfr-deep-nested-clean/` 和 `target/jmh-deep-nested-clean.json`；分类 CPU/分配栈并回填结论。

不做与风险：除诊断 SQL 夹具及对应测试外，不改生产默认列名、执行器、配置、默认限制或外部语义；不改变背压、取消、Context、异常或子查询缓存语义；不把 JFR 诊断吞吐当正式性能结论，也不从采样数推导每行字节。当前三层 JFR 只能说明三层路径；补两层同源 profiling 对照与 20 行功能断言后，只有跨两/三层可复用、低复杂度且不破坏上述契约的热点才提出后续 A/B；否则停止。

修订：首次诊断发现旧三层 benchmark 的中间 `select n1.value` 未显式别名。默认列名是完整表达式字符串（现有契约），故中间派生表输出 `n1.value`，下一层查询 `n2.value` 时查找 `value` 失败，旧三层输出空 Map；这不是生产默认列名规则缺陷。旧深层 benchmark 及刚跑的全局矩阵中该场景绝对值不代表有效值查询，不可用于改善比较。修复夹具时只为中间列加 `AS value`，保留两层/三层最终结果各自的默认 key（`n.value`/`n2.value`）；以独立严格 oracle 校验每行 id、cached key、Integer 值、顺序/行数与 lookup 单次订阅，同时补同一 SQL 的缓存回归。之后才运行正式 JMH 与 JFR；不改变公开默认列名规则或生产代码。

结果：`ReactorQLBenchmark.deeplyNestedSubquery` 的中间投影现为 `select n1.value AS value`；`profilingDeeplyNestedSubquery` 仅复用 `consumeForProfiling` 的引用消费。JMH setup 对两层与三层分别校验全部 20,000 行的 `o.id` 顺序、顶层 key 顺序、`cached` 的唯一 key（分别为 `n.value` / `n2.value`）及值为 `Integer 1`，并验证每次订阅仅订阅 lookup 一次；`SubqueryCacheTest.shouldShareThreeNestedUncorrelatedSubqueries` 同步覆盖修正 SQL 的 `{o.id=0,cached={n2.value=0}}` shape 和缓存复用。`mvn -q -Pjmh package` 成功，Surefire 汇总 395 tests、0 failures/errors/skipped。

修正夹具后的正式基线为 `target/jmh-deep-nested-aliased.json`：JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、`gc` profiler 下为 7.408±0.286 M 行/s、592.139 B/行。旧空值夹具不具有值查询语义，不能与该值作改善比较。profiling-only 结果为 `target/jmh-deep-nested-clean.json`，JFR 位于 `target/jfr-deep-nested-clean/org.jetlinks.reactor.ql.ReactorQLBenchmark.profilingDeeplyNestedSubquery-Throughput/profile.jfr`；其 JDK 17、512 MB/G1、1×1s 预热、2×1s 测量、1 fork 分数 7.816 M 行/s 仅用于诊断。

该 JFR 有 105 个 CPU、660 个分配样本；未出现 `CountingSubscriber` 或 `HashMap.hashCode` 消费端路径。CPU 主要在 `FluxFlatMap` drain/完成、`Mono` 订阅、`SelectFeature.SubqueryMapper.apply`、`DefaultReactorQL` 投影写入与 Map 操作；分配主要为 `Operators$ScalarSubscription`、`DefaultReactorQLRecord`、HashMap 桶数组、子查询 mapper lambda、`MonoMap` / `FluxMap` / `FluxFlatMap` 内部订阅者。这些是对每个外层行组合嵌套 Publisher、保持惰性订阅、错误/取消/Context 传播及结果 Map/Record 契约所需工作。把未关联子查询值提前为另一套标量缓存或压平层数将改变订阅时机、错误/取消/Context 与缓存上限边界，且需要新的计划表示；不属于跨子查询层数可复用的低复杂度候选。本切片停止，不改生产。

DN4 对照：两层与三层 `SubqueryCacheTest` 均改为有限 `collectList` 后由 StepVerifier 逐项核对全部 20 行的顶层 key 顺序、`o.id` 的 `Integer` 顺序、cached 唯一 key 和 `Integer 0` 值，并保留 lookup 仅订阅一次的断言；JMH 增加同 SQL/同输入/同 setup oracle 的 `profilingNestedSubquery`，同样只引用消费。`mvn -q -Pjmh package` 成功，Surefire 汇总 395 tests、0 failures/errors/skipped。

两次 JFR 均为 JDK 17.0.18、512 MB/G1、1×1s 预热、2×1s 测量、1 fork：两层 JSON `target/jmh-nested-clean.json`、JFR `target/jfr-nested-clean/org.jetlinks.reactor.ql.ReactorQLBenchmark.profilingNestedSubquery-Throughput/profile.jfr`，诊断分数 7.839 M 行/s、98 CPU/660 分配样本；三层 JSON `target/jmh-deep-nested-clean.json`、JFR `target/jfr-deep-nested-clean/org.jetlinks.reactor.ql.ReactorQLBenchmark.profilingDeeplyNestedSubquery-Throughput/profile.jfr`，7.816 M、106 CPU/660 分配样本。两条路径均无 `CountingSubscriber` 或 `HashMap.hashCode` 消费端干扰，共同热点为 `FluxFlatMap`、`Mono`、`FluxMap` 的订阅/完成编排、`SelectFeature.SubqueryMapper`、投影写入和 Map 操作；共同分配为子查询 mapper lambda、`ScalarSubscription`、Record、HashMap 桶和 Reactor 内部订阅者。JFR 的短采样及固定 660 个分配样本不能量化两/三层的差异，更不能替代正式 GC 分配基准；未发现跨两/三层且不改变惰性订阅、错误/取消/Context、缓存边界或 Map/Record 契约的低复杂度候选，停止于此。

## 时间窗口零需求判别

目标：确认时间窗口在下游零需求时的请求、排放、取消和溢出边界，区分瞬时分配与实际驻留内存；据此决定是否需要改动窗口编排。范围仅覆盖 `WindowedAggregateStage` 的时间窗口接线、Reactor 3.4.34 的标准 `window(Duration)` 行为及现有定向测试。不做生产算子替换、默认限制调整、静默丢弃、无限缓冲或错误吞咽。

证据：项目实际使用 Reactor 3.4.34。该版本 `Flux.window(Duration)` 先转为 `window(interval(...))`，实际实现是 `FluxWindowBoundary`，不是 `FluxWindowTimeout`。`FluxWindowBoundary.WindowBoundaryMain.onSubscribe` 对源直接 `request(Long.MAX_VALUE)`；在边界到达而外层窗口没有 demand 时，`FluxWindowBoundary` 清理队列、取消源和 boundary，并以 `Could not create new window due to lack of requests` 溢出。当前 probe 的 `concatMap(..., 1)` 观测为 `MAX request / 7 emissions / cancel / overflow`，其中后续窗口无法交付的 overflow 应归因于 `FluxWindowBoundary`。probe 的 `concatMap(..., 0)` 观测为 `0 request / 0 emissions / no cancel / overflow`；其原因是 `FluxWindowBoundary.subscribeOrReturn` 在初始 `main.emit(main.window)` 时发现 requested 为 0，立即以 `Could not emit buffer due to lack of requests` 报错并返回 null，source 与 boundary 的 interval 均未订阅，不应归因于 interval tick。两组 probe 仅说明算子级请求/错误边界；直接 `query.start` 的最终零需求实测为虚拟 349ms 内 `source MAX request / 7 emits / cancel`，下游收到 overflow，`sourceError` 为 null。当前窗口的 raw-row 队列、活跃分组累加器及关闭结果属于运行期间状态；JFR/GC 的瞬时分配样本不得写成常驻堆占用。多窗口输出 oracle 已修正并保持 `2/2/1`，覆盖逐窗口 demand 下的结果顺序和完整性。

验证：最新 `WindowedAggregateStageTest` 已覆盖标准时间窗口聚合、算子级零需求 probe、直接 `query.start` 零需求请求/取消/overflow、逐窗口 demand 的 `2/2/1` 输出、源错误与 Context 传播；本阶段全量 `mvn -q -Pjmh package` exit 0，Surefire 汇总 `tests=399 failures=0 errors=0 skipped=0`，`git diff --check` 通过。结论：不改生产代码、不改默认限制、不增加自定义窗口状态机，不使用 drop/latest/buffer 等改变语义的策略。若未来评估 `prefetch=0`，必须以直接 `query.start` 的请求/取消/错误/Context/多窗口 oracle 做独立 A/B；现有 `window(Duration)+concatMap prefetch` 组合不能同时提供完整结果、严格背压、无界源和有限内存这些保证；若需要新契约须另行设计评估。

## 高基数标量聚合 live-set 斜率与释放取证

目标：在既有“宽 payload 不被紧凑分组状态保留”的证据之外，测量当前实现中 `count/sum/avg/min/max` 每个活跃 key 的必需 live state 斜率，并分别观察窗口未关闭、关闭但下游尚未请求、以及取消后的 post-GC heap/histogram。诊断以惰性输入和无收集消费端运行，避免预构造数组、结果 `collectList` 或 sink 持有污染结论。

范围：仅新增 `HighCardinalityLiveHeapProbe` 与本节；使用标准 `ReactorQL.start`、计数窗口和单个 `BaseSubscriber` 暂停点，覆盖 10,000/50,000 key、单 count 与五个标量聚合。探针输出 PID/阶段标记及 heap 使用量，外部以 `jcmd GC.run`、`GC.class_histogram` 和可选 JFR 进行重复采样。

不做：不改生产代码、默认限制、SQL 语义、Reactor 操作符、状态表示或资源预算；不复测已有的 payload 保留对照，不把 GC 分配、JFR allocation sample 或类直方图总数解释为每行瞬时分配或历史行缓存。精确分组所需 key、`GroupState` 和累加器状态不是异常驻留。

步骤与风险：在窗口尚未关闭时让惰性源停止于 `Flux.never()`；另以完整窗口让关闭结果停在零 demand，最后取消并等待清理。每种规模重复运行，使用差分估计 key-state 的增量斜率，并以 histogram 中 `GroupState`、累加器、key/Map、Record 的数量确认所有者。GC 时机、JDK 对象布局、JIT 和 JFR 采样均会造成误差，因此不设置 MB 断言；只有出现超过 key/标量状态下界、且跨规模重复的额外所有者，才交由独立生产切片处理。

验证：阶段末一次执行 `mvn -q -Pjmh -DskipTests package`、`git diff --check`，并在 JDK 17、固定堆和 G1 下运行 probe/jcmd 重复测量；回填可复现命令、原始输出路径、差分和局限。

结果：新增 `src/jmh/java/org/jetlinks/reactor/ql/HighCardinalityLiveHeapProbe.java`。它没有为诊断配置 `group.maxActiveKeys` 等 setting，故沿用第五阶段后的缺省兼容行为；输入按需生成 `{deviceId, score}`，不含 payload，输出由一个不收集的 `BaseSubscriber` 消费。`open` 模式在 `keys + 1` 计数窗口中保持源未完成，因而每个 key 都处于活跃状态；`closed` 使用恰好 `keys` 的窗口，订阅者只 `request(1)`，因此留有 `keys - 1` 个 `ClosedGroupWindow` 状态等待 demand，之后取消。每次阶段标记前调用 GC 仅为可重复的 heap 读数，类直方图才是所有权依据。

JDK 17.0.18、`-Xms512m -Xmx512m -XX:+UseG1GC` 下，分别运行 10,000 和 50,000 key 的 count/五标量聚合，并在 `open-active-keys` 后执行 `jcmd <pid> GC.run` 与 `jcmd <pid> GC.class_histogram`。原始 marker、直方图在 `target/live-heap-*.log` / `target/live-heap-*.histogram`；代表性命令为：

```text
java -Xms512m -Xmx512m -XX:+UseG1GC -cp target/reactor-ql-1.0.21-SNAPSHOT-benchmarks.jar \
  org.jetlinks.reactor.ql.HighCardinalityLiveHeapProbe --keys=50000 --hold-seconds=20
jcmd <pid> GC.run
jcmd <pid> GC.class_histogram > target/live-heap-open-five-50k.histogram
```

| 活跃状态 | 10k histogram total | 50k histogram total | 40k key 差分 | 每 key 可见必需对象 |
| --- | ---: | ---: | ---: | --- |
| `count(1)` | 5,097,384 B | 12,277,200 B | 7,179,816 B，约 179.5 B/key | 1 `GroupState` + 1 `CountAggFeature$2`，均为 10k/50k；每个对象 32 B |
| `count/sum/avg/min/max` | 6,532,344 B | 19,391,776 B | 12,859,432 B，约 321.5 B/key | 除上述对象外，每 key 有 `MapAggFeature$1`、`$2` 各一个和 `$3` 两个，合计五个标量累加器 |

这两条斜率来自独立 JVM 的全量 histogram，因此包含 key、`LinkedHashMap` entry/array、对象数组及 JVM 噪声，不能当作 API 的字节配额；但对象数量严格随 key 数乘法增长，且五聚合相对 count 的约 142 B/key 增量与额外四个累加器一致。50k 五聚合在窗口未关闭时为 50,000 个 `GroupState`、50,000 个 count、50,000/50,000/100,000 个三个 MapAgg 内部类；没有 `DefaultReactorQLRecord`。这证明当前 live set 是精确 key + 标量累加器状态，而非历史输入行或结果收集。

关闭窗口的 50k 五聚合复测在一条输出已消费、其余 demand 为零时仍为 49,999 个 `GroupState` 与对应 49,999/49,999/99,998 个 MapAgg 状态；total 19,396,632 B，与 open 50k 的 19,391,776 B 相差仅 4,856 B，未出现 50k 输出 `ReactorQLRecord` 列表。取消后相同 JVM 的 post-GC histogram total 降为 3,374,376 B，且不再出现 `GroupState`、count 或 MapAgg accumulator；probe marker heapUsed 也从 20,687,248 B 降到 4,222,632 B。此处的取消差分是同 JVM 测量，能证明 `ClosedGroupWindow.close()` 释放；绝对数仍不是跨 JVM 内存基线。

附加启动 JFR 录制在 `target/live-heap-open-five-50k.jfr`（436 KB），记录了 5 次 GC、61 个 allocation samples 和 5 个 old-object samples，但 profile setting 未启用 `ObjectCountAfterGC`，所以它不能替代 `jcmd` histogram，也不从其 allocation sample 推导 live bytes。JFR 的作用仅是确认探针覆盖了实际建态/GC 生命周期；所有上述 retained 结论只引用 post-GC histogram。

阶段验证：`mvn -q -Pjmh -DskipTests package` 和 `git diff --check` 均通过。结论：没有异常的跨场景额外常驻对象，也没有低复杂度、通用且不改变精确分组语义的生产候选。进一步降低此状态必须采用活跃 key 限制、窗口/TTL、近似聚合或外部状态后端等公开契约选择；其中只有显式 `group.maxActiveKeys` 已存在，缺省限制继续保持原行为。本切片不改生产实现。
## 已完成子查询缓存的首行 supplier 引用诊断与最小 A/B

目标：验证已完成的 `cacheMono` / `cacheMany` 是否仍通过首次访问时的 supplier 闭包保留首行 `ReactorQLRecord`（或其 payload），即使查询根订阅持续存活；只有确认这部分引用不属于已缓存结果本身时，才在 `SubscriptionContext` 的通用缓存边界消除它。

范围：仅 `SubscriptionContext`、`SubqueryCacheTest`、可单独运行的 retention probe 和本章节。测试覆盖单值/多值的 0/1/N、并发首次访问、Context、首值取消、错误与取消后的重试、同一 Publisher 身份和重放，以及一层/三层真实子查询输出。

非目标：不物化/缓存 JOIN 源，不调整子查询最大行数或默认限制，不新增自定义 Subscriber、SPI、按 SQL 或子查询层数分支，也不将结果值本身视作可释放对象。

步骤与风险：probe 以 `WeakReference` 和 GC 后类直方图区分“缓存结果必需保留”与“完成后 supplier 闭包额外保留”，并在根订阅未终止的情况下测量。若属实，仅在成功终止（包括 empty）后断开 supplier；取消必须保留 supplier 以允许重新订阅。错误是否应保留以维持 `replay/refCount` 的重试语义，须先由测试确认。不能在 `doOnNext` / `doOnSuccess` 中提前清理，否则会与 replay 完成、并发订阅和取消竞态；若无法证明安全的终止时序，本切片停止且不改生产。

验证：阶段末执行 `mvn -q -Pjmh package`、`git diff --check`，并以既有正式 JMH 的异步子查询与负对照作同配置前后比较。保留门槛为 GC 后可达性可重复下降、功能等价、吞吐无明确稳定回退，且不增加每行对象。

结果：新增 `SubqueryCacheRetentionProbe`，在强引用 `SubscriptionContext`（模拟完成后查询根仍存活）的前提下，完成的 `cacheMany` / `cacheMono` 都只输出与 payload 无关的标量。修复前，supplier 捕获的 1 MiB `Payload` 在 10 次 GC 与分配压力后仍可达；把 payload 作为实际缓存结果的控制组也仍可达，故没有将必要结果引用误判为泄漏。修复后普通 many/mono 结果为 `payloadReachable=false`，控制组仍为 `true`。探针命令为：

```text
java -Xms128m -Xmx128m -cp target/reactor-ql-1.0.21-SNAPSHOT-benchmarks.jar \
  org.jetlinks.reactor.ql.SubqueryCacheRetentionProbe many
```

`SubscriptionContext` 以每个缓存条目一个 `AtomicReference` 保存 source supplier，并仅在源的成功终止后释放（`Mono.empty()` 也属于成功）。释放使用位于 `replay(1)` 上游的 `doFinally(ON_COMPLETE)`：其回调在终止信号已交给 replay 后运行，因此完成读取继续重放同一 Publisher；取消与错误路径不释放，保留 `refCount` 重新订阅和错误重放的既有行为。没有自定义 Subscriber、每行对象、SQL/层数分支或默认限制变化。

`SubqueryCacheTest` 补充空 Mono 重放、Mono 在途取消后重新执行，并将多值错误重放显式校验为只订阅源一次；现有多值 0/1/N、并发首次访问、Context、同一 Publisher 身份、真实一层和三层子查询覆盖继续通过。`mvn -q -Pjmh package` 成功（401 tests）；`git diff --check` 成功。JDK 17.0.18、512 MB/G1、1 线程、3×1s warmup、5×1s measurement、2 forks、GC profiler 的前后对照为：历史同配置 `target/jmh-singleton-cache-after.json` 的单层缓存子查询 7.205 M/s、592.061 B/op，当前 `target/jmh-subquery-source-release-after.json` 为 7.264 M/s、592.063 B/op，重复 `target/jmh-subquery-source-release-repeat.json` 为 6.957 M/s、592.063 B/op，吞吐区间重叠；缓存 EXISTS 历史 8.002 M/s（CI 7.276–8.727）、当前 7.486 M/s（7.288–7.685）、重复 7.519 M/s（7.347–7.691），历史区间重叠，且分配保持约 576.06 B/op。双异步子查询当前为 4.152 M/s、1064.183 B/op；关联子查询负对照为 2.663 M/s、1344.457 B/op。B/op 是瞬时分配，不等于 live heap；本变更的直接收益是完成子查询缓存未再无谓常驻首行 supplier/payload。

## 显式活跃键上限的兼容分组跨窗口驻留诊断

目标：验证关闭的 `_window(n)` 在根订阅仍因 `Flux.never()` 活跃时，兼容 `GroupedFlux` 路径（`aggregate.fastPath=false`）中每个窗口 `GroupStateBudget.Scope.keys` 是否无谓保留已消费窗口的宽分组键。范围仅新增独立 `CompatibilityGroupRetentionProbe` 与本节；查询显式设置 `group.maxActiveKeys`，以保证实际执行预算 Scope/HashSet 路径。

不做：不修改生产、既有 JMH、测试、POM、默认限制、SQL 语义或 Reactor 操作符；不预构造输入、收集结果、长期保留普通消费端 Map，也不以自定义 Subscriber 改变请求/取消行为。JFR allocation sample 和 GC profiler 的瞬时分配不能证明 live heap；只有 root 持续活跃时 WeakReference 与 post-GC histogram 才用于判断对象可达性。

步骤与风险：探针惰性生成多窗口、各窗口唯一的宽 String key，通过 `doOnNext` 只递增输出计数后以标准 `subscribe()` 消费，普通路径不保存 Map；最后拼接 `Flux.never()` 保持根订阅。普通 active 模式、完成及取消控制组分别验证根活跃、终止和清理；retain-results 控制组才故意保存输出 Map，确认 WeakReference 机制能观察到语义必需的结果引用。每个阶段输出 PID、marker、weak-key 存活数量，外部 `jcmd GC.run`/`GC.class_histogram` 在 marker 后采样。若普通 active 模式仍保留关闭窗口所有 keys，需由 histogram/引用链精确区分 Scope 键集与 Reactor 已关闭 group 的必要状态，才建议单独的最小并发安全修复；本切片不修改生产。

验证：阶段末一次执行 `mvn -q -Pjmh -DskipTests package` 与 `git diff --check`，并在固定 JDK/heap/G1 下运行各 probe 模式、保留原始 stdout 和 histogram 到 `target/`。完成后回填实际可达性、类直方图、命令和结论；无异常驻留即停止。

结果：新增 `src/jmh/java/org/jetlinks/reactor/ql/CompatibilityGroupRetentionProbe.java`。它通过公开 API 构建 `select name,count(1) total from test group by _window(n),name`，显式设置 `aggregate.fastPath=false` 和 `group.maxActiveKeys=n`，故必经 `GroupStateBudget.Scope.keys` 的兼容 `GroupedFlux` 路径。输入以 `Flux.range(...).map(...)` 按请求惰性生成三个窗口、每窗口 2,000 个唯一宽 String key；普通订阅仅递增输出计数，根源在三窗口后拼接 `Flux.never()`。全部 6,000 个结果都已消费才到达 marker，因而不是在窗口尚未关闭时取样。

JDK 17.0.18、`-Xms256m -Xmx256m -XX:+UseG1GC` 下，原始输出分别为 `target/compat-group-retention-active.log`、`target/compat-group-retention-complete.log`、`target/compat-group-retention-cancel.log`、`target/compat-group-retention-retain.log`：active、complete、cancel 在探针重复 GC/压力后的 `weakKeysAlive` 均为 `0/6000`；cancel 在 dispose 后仍为 `0/6000`。retain 控制组刻意将每个输出 Map 保存到 List，得到 `6000/6000`，证明 WeakReference 观测能识别结果契约本身的强引用，而非错误地把所有 key 都视为可回收。普通消费者不收集/保存 Map，根订阅仍活跃的 active 结果直接否定“已关闭窗口的 Scope.keys 跨窗口无谓保留宽 key”的假设；完成和取消控制组也没有相反信号。

尝试在 active marker 期间从同一主机运行 `jcmd <pid> GC.run` / `GC.class_histogram` 和 `jmap -histo:live <pid>`，两者均在 10.5 秒 attach socket 等待后报 `AttachNotSupportedException`，因此没有把空/失败的 histogram 当作证据；失败原始输出分别保留在 `target/compat-group-retention-active-gc.log` 和 `target/compat-group-retention-active.histogram`。这是当前受控执行环境的 attach 限制，不影响 probe 内显式 GC 后的 WeakReference 可达性结论，但也意味着本切片没有额外的 histogram 对象计数交叉验证。JFR allocation samples 或 JMH GC profiler 的 B/op 都是瞬时分配，不能替代这里的 live-key 可达性，也没有用于推断常驻堆。

阶段验证：`mvn -q -Pjmh -DskipTests package` 和 `git diff --check` 均通过。未发现异常驻留，故不建议清空 `Scope.keys`、增加并发清理或修改生产路径；这类改动会在取消与分组终止竞态中增加状态协调，却没有本 probe 支持的收益。本诊断切片到此停止。

## 集合操作与 RIGHT JOIN JFR 诊断

目标：补足现有跨 SQL 性能矩阵尚未单列覆盖的 `UNION`、`UNION ALL`、`EXCEPT` 和 `RIGHT JOIN`。只为 JFR 增加严格的 setup oracle 与 Blackhole 引用消费入口，隔离消费端 Map 遍历噪音，确认实际集合/连接契约及订阅形态后定位 CPU、分配栈。

范围：仅 `src/jmh/java/org/jetlinks/reactor/ql/ReactorQLBenchmark.java` 和本节；集合输入复用 `setLeftSource` / `setRightSource`，连接输入复用 `joinSource`，集合 SQL 均使用相同的派生表输出形状。不改生产代码、测试、POM、默认设置或正式基准。

非目标：不改变已兼容的 `EXCEPT`（右减左）方向；不假设 `UNION` 的发射顺序，也不将 `UNION ALL` 当作去重集合；不为 JFR 样本推导 B/行或 live heap，不物化/缓存连接右侧，不修改背压、取消、错误或 Context 语义。

步骤与风险：setup 以字段、值、重复计数和来源订阅次数验证集合操作；RIGHT JOIN 同时验证每行值/字段、匹配/未匹配分支和右源逐左行订阅次数。阶段末构建与差异检查后，在 JDK 17、512 MB/G1、1×1s warmup、2×1s measurement、1 fork 的干净 JFR 中采样。只有出现跨 SQL、低复杂度、且可保持响应式契约的具体 owner，才建议单独生产 A/B；否则停止于证据结论。

验证：`mvn -q -Pjmh -DskipTests package`、`git diff --check`；产物记录到 `target/jfr-set-rightjoin-clean/` 和对应 JSON。若候选足够明确，另行以同类 native/Record 对照做正式 `gc` JMH，不把非等价原生 Map 快路作为可达上限。

结果：新增正式 `unionRows`、`unionAllRows`、`exceptRows` 及四个 profiling-only Blackhole 入口（含已有正式 `rightJoin`）。集合 oracle 验证 `s.v` 的字段和 `Integer` 类型、全值域、精确重复数及两侧各一次订阅；由于 10,000 行输入不是 1,024 的整数倍，尾部 240 个 key 每侧恰少一次，oracle 按实际 `Flux.range` 分布而非错误地假定每 key 十次。`UNION` 不断言发射顺序，`UNION ALL` 保留重复数；`EXCEPT` 固定现有右减左契约，输出 1024..1535。RIGHT JOIN oracle 进一步固定当前兼容行为：全部 20,000 个 `{value: 0}` 结果，且右源按每个左行订阅一次。初始 oracle 对 `UNION ALL` 的错误十次假设在 JFR 前失败，修正后重新构建并通过 setup；这不是生产实现或 SQL 语义异常。

`mvn -q -Pjmh -DskipTests package` 与 `git diff --check` 通过。JDK 17.0.18、512 MB/G1、1×1s warmup、2×1s measurement、1 fork 的干净 JFR 在 `target/jfr-set-rightjoin-clean/`，结果 JSON 为 `target/jmh-set-rightjoin-clean.json`。短诊断分数为 EXCEPT 10.789 M、RIGHT JOIN 4.991 M、UNION ALL 10.221 M、UNION 9.469 M 输入行/s，只用于定位而不做吞吐对比。各录制依次有 112/628、98/660、120/613、113/660 个 CPU/allocation samples。UNION、UNION ALL 与 EXCEPT 的 CPU/分配栈均集中在派生表投影的 HashMap/EntrySet、键的 hash/equals 和集合状态所需的 Map；UNION/UNION ALL 还包含 `mergeWith` 的正常合并编排。RIGHT JOIN 则集中在逐左行 `FluxFlatMap`、右侧流的 `FluxMap`、空匹配 `FluxDefaultIfEmpty` 与 Record/结果 Map 建立。这些是当前有界集合去重/差集与既有 RIGHT JOIN fallback/订阅契约所需工作，未发现可独立删除、且跨 SQL 可复用的额外操作符层。JFR 样本不用于估算 B/行或 live heap。

作为正式场景基线，JDK 17.0.18、512 MB/G1、单线程、3×1s warmup、5×1s measurement、2 forks、GC profiler 的 `target/jmh-set-rightjoin-formal.json` 为：EXCEPT 12.550±0.110 M 输入行/s、510.855 B/行；RIGHT JOIN 5.080±0.103 M、687.938 B/行；UNION ALL 11.649±0.271 M、615.875 B/行；UNION 11.060±0.250 M、516.996 B/行。B/行仅为瞬时分配，并非常驻堆。此切片没有生产 A/B：缓存、物化或融合这些步骤会改变集合边界、`mergeWith` 非顺序语义，或 RIGHT JOIN 的取消、Context、右源重订阅/空匹配行为；没有足够证据用低复杂度通用优化抵消该契约风险，故停止于此。

## 正则函数字面量与动态参数的 JFR 判别

目标：确认内置 `regexp_like` 在同源字面量和动态正则参数下，逐行 `Pattern.compile` 是否是实际 CPU/分配热点，并区分函数参数装配与正则本身的成本。范围先限于现有 `ReactorQLBenchmark` 的诊断入口和本节；不预先修改生产逻辑、公开 Feature、默认限制或错误时机。

方法：使用相同预构造输入与 Blackhole 引用消费，setup 逐行验证布尔值、类型、结果数量及字段；以 JDK 17、512 MB/G1 的短 JFR 定位两条路径，正式 GC/JMH 只在发现安全的通用候选后做同配置 A/B。不得通过跨行无界缓存、全局可变状态或跳过第三方常量 Feature 求值来消除编译；若优化需改变自定义 Feature 调用次数、错误/取消/Context 或 `regexp_*` 动态参数契约，则记录下界并停止，不以单个函数的操作符数量作为保留依据。

取证与候选：JFR 位于 `target/jfr-regexp-like-clean/`，短诊断 JSON 为 `target/jmh-regexp-like-clean.json`；动态/字面量分别有 115/120 个 CPU、各 660 个分配样本，正则调用栈分别覆盖 81/70 个 CPU 与 604/461 个分配样本。动态输入的 `[I` 分配中，268 个直接来自 `Pattern.compile`，272 个来自实际匹配 Matcher；因此仅优化编译，不删匹配或安全检查。正式同配置基线 `target/jmh-regexp-like-before.json` 为动态 8.188 M 行/s、1336.027 B/行，字面量 8.309 M、1320.027 B/行。尝试在默认查询 metadata 上保存一个线程安全可替换的已成功编译 Pattern；key 是实际模式文本和 flags，命中前仍逐行执行输入长度、模式长度、危险嵌套量词检查与 flags 转换。只保留最近一项，不使高基数动态模式无界驻留；非默认 metadata 保持原路径。测试须覆盖动态模式交替、flags、无效模式/限制、并发订阅及 post-build setting 变化。只有正式 JMH 分配下降、吞吐无稳定回退且功能等价才保留。

结果：保留 `DefaultReactorQLMetadata.compileRegex` 的查询级单槽复用，`regexp_like`、`regexp_replace`、`regexp_extract`、`regexp_substr` 共用该入口。每行仍读取当前限制、执行输入/模式长度与危险量词校验、转换 flags；仅成功编译的最近一项以 `CompiledRegex` 不可变条目和 volatile 引用保留，key 使用原始模式文本与调用 flags，不依赖会被内联修饰符改变的 `Pattern.flags()`。非默认 metadata 沿用逐次编译。没有全局状态、自定义 Reactor 操作符或结果缓存。`RegexPatternReuseTest` 验证四函数动态交替、flags/内联修饰符隔离、非法模式不污染缓存、构建后限制变更、并发订阅和 Reactor Context；JMH setup 验证 20,000 行的字段、布尔值和结果数量。

JDK 17.0.18、512 MB/G1、3×1s 预热、5×1s 测量、2 forks、GC profiler 的同配置基线/改后为：重复动态模式 8.188→13.944 M 行/s、1336.027→712.027 B/行；字面量 8.309→14.405 M、1320.027→712.027 B/行。交替动态模式负对照为 7.053→6.843 M、1352.027→1352.027 B/行；同代码复测 7.037 M、1368.027±25.498 B/行，吞吐与基线误差区间重叠，未确认稳定回退或分配变化。证据为 `target/jmh-regexp-like-before.json`、`target/jmh-regexp-like-alternating-before.json`、`target/jmh-regexp-like-after.json`、`target/jmh-regexp-like-alternating-repeat.json`。优化后短 JFR `target/jfr-regexp-like-after/` 的 `Pattern.compile` CPU/分配栈样本在重复动态和字面量路径均为 0（之前分别为 59/316、53/281）；JFR 只证明热点消失，不用于换算 B/行。

在最终不可变条目实现上，同配置正式 JMH `target/jmh-regexp-like-final.json` 再次确认重复动态 13.358 M、712.027 B/行，字面量 14.294 M、712.027 B/行；交替动态 6.984 M、1376.027 B/行，吞吐误差区间仍与原基线重叠。最终短 JFR `target/jfr-regexp-like-final/` 的重复动态/字面量 `Pattern.compile` CPU/分配栈样本均为 0，说明热点消失仍覆盖当前源码。最终 `mvn -q -Dtest=RegexPatternReuseTest test`、完整 `mvn -q -Pjmh package`（Surefire 407 tests、0 failures/errors/skipped）与 `git diff --check` 通过。B/行是瞬时分配而非存活堆；每个实际使用正则的默认查询至多额外持有一个最近的已编译 Pattern，不能把本结果宣称为常驻堆下降。高基数不重复动态模式不会获得缓存收益，且保留原每行编译/校验语义；本切片不引入更大的缓存或自适应策略。

## 冷 Publisher JSONPath 参数的恒真 cast 判别

目标：检查 `JsonPathFunctionMapFeature` 非标量参数分支中逐参数 `Mono.fromDirect(...).cast(Object.class)` 是否产生可通用移除的 `MonoMap`，提升第三方冷 Publisher 与 JSON 函数组合的吞吐并减少每行分配。只改该共用参数装配点及验证夹具，不改 Jayway 文本解析、静态路径预编译、JSON 资源限制、默认值或第三方 Feature 调用次数。

步骤：先在既有 JMH 增加同源同步 `json_get`、单冷 Publisher 参数 `json_get` 的 Blackhole 诊断入口，setup 逐行验证字段/值/类型与冷源订阅次数；采集干净 JFR 和正式基线。只有 `cast(Object.class)` 的 `MonoMap` 明确位于冷参数逐行热路径时，尝试利用 `Mono.fromDirect` 泛型直接得到 `Object`，保留原 `fromDirect`/空值适配、惰性订阅、按参数顺序、取消、错误和 Context。补有限多值、空值、错误、取消、Context 与第三方 Feature 回归；阶段末集中全量测试及同配置成对 JMH。若真实 SQL 分配未降或吞吐稳定回退即撤回，不引入自定义操作符、状态缓存或 SQL 文本分支。

基线：JFR `target/jfr-async-json-cast-before/` 的冷 JSON 参数路径在 `JsonPathFunctionMapFeature.createMapper` 的 `Mono.cast(Object.class)` 处直接采到 `MonoMap` 与 `MonoMapFuseable` 分配；同步负对照没有该路径。短 JFR 分数只供定位。JDK 17.0.18、512 MB/G1、3×1s warmup、5×1s measurement、2 forks、GC profiler 的 `target/jmh-async-json-cast-before.json` 为：冷 Publisher `json_get` 1.829 M 行/s、3232.039 B/行；同源同步 `json_get` 3.334 M、2120.028 B/行。两者并非仅差 cast，不能把总差值作为候选收益。

结果：仅将恒真 `cast(Object.class)` 改为 `Mono.<Object>fromDirect(...)` 泛型升宽，其他参数顺序、包装和 Jayway 解析路径不变。`src/test/java/org/jetlinks/reactor/ql/supports/map/JsonPathAsyncParameterTest.java` 覆盖第三方冷参数按序订阅、Context、空值、原异常身份、订阅后取消，以及原/新 `fromDirect` 对有限多值 Publisher 的适配等价。取消用例先保证冷源真正订阅，再断言取消传递；最初使用零初始 demand 后立即取消的探针未到达冷源，属于无效测试前置而非生产异常。

同配置 `target/jmh-async-json-cast-after.json` 中冷 Publisher 路径为 2.043 M 行/s、3008.039 B/行，相对基线吞吐约 +11.7%、每输入行分配减少 224 B；同步负对照为 3.292 M、2120.028 B/行，与基线误差区间重叠且分配不变。优化后 `target/jfr-async-json-cast-after/` 未再采到 `Mono.cast` 栈；仍有一个 `MonoMapFuseable` 样本来自结果投影，不能误称所有 map 操作符被移除。完整 `mvn -q -Pjmh package` 通过，Surefire 410 tests、0 failures/errors/skipped；`git diff --check` 通过。B/行是瞬时分配，不是存活堆；未增加缓存或常驻状态。JSON 文本解析仍是主要成本，本切片不重启已否决的手写扫描器/解析树缓存。

## 异步投影星号展开闭包的 JFR 判别

目标：确认 `DefaultReactorQL` 异步投影路径中 `allMapper.forEach(mapper -> mapper.accept(r))` 是否在真实异步 `select *` / `t.*` 投影的每行热路径形成可归因的 CPU 或分配热点。范围仅为 `ReactorQLBenchmark` 的 profiling-only 夹具及本节；不修改生产路径、测试、POM、默认配置或正式基准。

方法：构造 20,000 条预构造、插入顺序稳定的 Map 输入；以同一个冷 `ValueMapFeature` 分别跑异步 `*`、异步无星号及同步 `t.*`。setup oracle 必须逐行校验字段、值、字段顺序、异步源每行恰一次订阅，并让异步 feature 在 Reactor Context 缺失时失败，避免将订阅/Context 语义改变误当作性能结果。JMH 使用 Blackhole 仅消费结果引用，不遍历 Map。阶段末一次构建后，在 `target/jfr-async-star-clean/` 录制同配置短 JFR。

判别与边界：JFR samples 只用于判断该闭包或其直接生成的 lambda 是否在异步星号相对两个负对照中成为明确热点，不能换算 B/行或存活堆。若未得到明确归因，停止且不改生产；若得到明确归因，也只提出需经过同配置正式 A/B 与完整异步语义回归验证的最小通用候选。不得以既已撤回的同步星号循环候选或样本分数作为保留依据。

结果：只新增三个 profiling-only 入口和同源 oracle；录制时生产源码未因本切片变动，唯一源码改动为 JMH 夹具。20,000 条预构造 `LinkedHashMap` 输入均逐行通过字段、值、类型与迭代顺序校验：异步 `t.*` 为 `[name, score, async_id, id]`，同列别名的异步无星号为 `[name, score, id, async_id]`，同步 `t.*` 为 `[name, score, id]`；两个异步路径各自恰订阅冷列 20,000 次，且 feature 在缺失 `profiling-context=async-star` 时会失败，故 Context 与逐行订阅没有被诊断夹具绕过。Blackhole 仅引用消费 Map。

JDK 17.0.18、512 MB/G1、单线程、1×1s warmup、2×1s measurement、1 fork 的短 JFR 在 `target/jfr-async-star-clean/`，结果 JSON 为 `target/jmh-async-star-clean.json`。仅作定位的短时分数为异步无星号 6.472 M、异步星号 6.134 M、同步星号 28.973 M 输入行/s，不能当作正式吞吐结论。worker CPU sample 数依次为 120、127、104；`DefaultReactorQL.java:805` 仅异步星号有 37/127 个，两个负对照均为 0。异步星号 worker 的 allocation samples 中，160 个 `HashMap$Node`、17 个 `HashMap$Node[]` 与 15 个 `MonoPeek` 的调用栈经过 `DefaultReactorQL$1.lambda$visit$3`（`t.*` 结果复制）以及 805 行；另有 86 个 `DefaultReactorQL$$Lambda` 直接经过 805 行。异步无星号和同步星号没有该行的 worker allocation sample。这构成“异步 + 星号”闭包与实际结果复制共同处于热路径的明确归因，但 JFR 不可由此估算 B/行或常驻堆。

建议：可单独尝试一个最小通用 A/B——在查询构建期把 `allMapper` 编译成一个复用的 `Consumer<ReactorQLRecord>`，使异步分支传给 `doOnNext` 的是每查询一个稳定 consumer，而不是每行创建捕获 `r` 的 consumer。该候选不得改变 `allMapper` 执行时点（异步列成功后）、空列、错误、取消、Context、Record/Map 可变性或字段顺序；必须以当前三条夹具和完整回归测试验证，再用同配置正式 GC/JMH 判定。同步循环的已撤回候选不是本建议的依据。

### 异步星号闭包的正式 A/B

源码身份与范围：正式基线开始前 `DefaultReactorQL.java` 的完整文件 SHA-256 为 `76875bb0482ca1b120ba5403c966fb560d6b0b4c81cbf8d055daab26a9779103`，`ReactorQLBenchmark.java` 为 `72b078e1d6cc24e11a88511cc740f256c5c476d8a874d34e08e6dc6303c57640`。该生产基线保留工作树已有改动，但本切片开始后不混入其他生产修改；JMH 只比较此身份与本节的单点实现。先以 JDK 17、512 MB/G1、单线程、3×1s warmup、5×1s measurement、2 forks、GC profiler 记录异步 `t.*`、异步同列无星号、同步 `t.*` 到 `target/jmh-async-star-formal-before.json`。

候选：仅在 `DefaultReactorQL.createMapper` 的异步 `allMapper` 分支，于查询构建期构造一个 `Consumer<ReactorQLRecord>`，用普通 `for` 保持 mapper 顺序；每行的 `doOnNext` 直接复用这个 consumer。不增加 Reactor 操作符、不引入跨订阅可变状态、缓存或 SQL 分支。回归覆盖异步星号结果及顺序、空异步值、冷 Publisher 订阅、Context、错误、取消，并复用全量外部 Feature 回归。保留门槛为正例分配明确下降且吞吐无稳定回退，两个负对照无稳定回退、语义完全等价；否则撤回生产修改但保留诊断和测试。

结果：`DefaultReactorQL.java` 改后 SHA-256 为 `9895c538939bc421116269d8e20c5327b7a769295f54d90f21c22d65e0bbcc3a`。改动只把异步 `allMapper` 的每行捕获 lambda 改为查询构建时创建、内部用普通 `for` 保持顺序的 `allResultMapper`；`doOnNext` 仍在同一异步完成边界调用，未增加操作符、跨订阅可变状态或缓存。`ScalarFastPathTest` 新增异步 `t.*` 的 Map 值/实际迭代顺序、空 async 值、冷 Publisher 每行订阅、Context、错误及取消覆盖；首次断言错误地从 `HashMap` 期望顺序推导字段顺序，在全量测试中暴露后已改为固定现有实际 `[name, async_name, score]`，没有修改生产语义。完整 `mvn -q -Pjmh package`（411 tests，0 failures/errors/skipped）和 `git diff --check` 通过；测试日志中既有的受测异常/警告不构成失败。

JDK 17.0.18、512 MB/G1、单线程、3×1s warmup、5×1s measurement、2 forks、GC profiler 的完整 before/after JSON 为 `target/jmh-async-star-formal-before.json` / `target/jmh-async-star-formal-after.json`。异步星号正例为 6.151±0.529 → 6.480±0.037 M 输入行/s，99.9% 区间重叠，不能宣称稳定吞吐提升或回退；瞬时分配明确从 719.889 降至 687.889 B/行（-32 B/行）。异步无星号负对照为 6.526±0.270 → 6.755±0.116 M、615.889 B/行保持不变，区间重叠。同步 `t.*` 负对照生产路径并未被此改动触及，但 before 的 224.027 B/行与 after 的 208.027 B/行不同，故不能把它归因于候选；同源码重复 `target/jmh-async-star-formal-sync-repeat.json` 仍为 208.027 B/行、31.578±1.345 M，说明此差异来自基线和当前工作树/环境身份而非异步分支。它不构成候选收益证据；其吞吐与 before 29.196±0.683 M 的区间边缘不重叠，也进一步证明跨独立运行的该负对照存在环境差异，不能用作归因。

保留结论：正例精确减少 32 B/输入行，且异步正例与无星号负对照均无可确认稳定吞吐回退；语义回归与全量 suite 通过。因此保留此最小通用生产变更。GC B/行是瞬时分配而非存活堆，不能将其表述为常驻内存下降；未为同步星号路径申领任何收益，也不重启已撤回的同步循环候选。

## 自定义 Metadata 表达式包装器与标量快路兼容性

目标：恢复自定义 `ReactorQLMetadata.createWrapper(Object)` 在表达式级的既有扩展契约，避免近期标量快路在 `checkpoint=false` 时直接返回 `ScalarValueMapper` 而绕过已创建的包装器；默认内置 Metadata 仍保留快路。

范围：只处理已证实同时具备且实际应用表达式 wrapper、又有标量返回分支的 `BinaryMapFeature`、`JsonPathFunctionMapFeature` 与 `FilterFeature`，以及 Metadata 的构建期能力契约和专门回归测试。`FunctionMapFeature` 的普通分支虽创建局部 wrapper 但既有实现未应用它，因此不把其纳入本次“快路绕过”的回归范围；不批量改写没有 expression wrapper 的其他 Feature，不增加运行时 Publisher 探测、反射、SQL/函数名分支或额外缓存。

方案：在 `ReactorQLMetadata` 增加默认保守的 `supportsScalarFastPath()` 能力；只有精确的内置 `DefaultReactorQLMetadata` 返回 true。其子类必须显式覆写才能 opt-in，避免仅为覆写 `createWrapper` 的子类意外继承快路。上述三处标量分支以该能力（以及既有 checkpoint 语义）作为构建期门槛，使自定义 metadata 回退原有 Publisher+wrapper 链。验证会区分查询级与表达式级 wrapper，并覆盖数值、WHERE、JSON、异常和 Reactor Context；阶段末统一全量构建、差异检查，并以既有正式基准和默认 mapper 快路断言确认热路径没有新增对象。

结果：`ReactorQLMetadata.supportsScalarFastPath()` 默认返回 false；精确 `DefaultReactorQLMetadata` 返回 true，而任意子类（包括只覆写 `createWrapper` 的观测/诊断型 metadata）默认回退，只有主动覆写才 opt-in。`BinaryMapFeature`、`JsonPathFunctionMapFeature` 和 `FilterFeature` 的相关同步分支均使用该构建期能力门槛；不添加订阅期类型判断、反射或 SQL 特调。`MetadataWrapperCompatibilityTest` 以仅覆写 wrapper 的 DefaultMetadata 子类验证：查询 wrapper 与数值表达式、WHERE 二元表达式、JSON expression wrapper 均实际订阅；`score || ''` 覆盖 FilterFeature 的 ValueMap fallback wrapper；包装器可读取 Reactor Context，并且除零错误仍传至下游。测试同时固定默认 metadata 仍 opt-in、包装型子类不继承 opt-in。

验证：`mvn -q -Dtest=MetadataWrapperCompatibilityTest test` 通过；`mvn -q -Pjmh package` 通过（Surefire 415 tests，0 failures/errors/skipped）；`git diff --check` 通过。默认路径没有新增 Reactor 操作符或对象：能力检查仅在查询构建期执行，且精确默认 metadata 维持原标量 mapper 分支；先前已记录的默认场景 JFR/JMH 证据继续适用。本切片不将此前 GC profiler 的 B/行解释为存活堆。

## 高基数集合聚合的存活堆边界诊断

目标：区分高基数窗口内 `collect_list` 必需保留的结果元素、每组 `collect_row` 的覆盖状态，以及兼容 `GroupedFlux` 路径可能额外保留的历史输入 Record/payload。只扩展现有 `HighCardinalityLiveHeapProbe` 与本节，不改生产聚合、默认资源限制或 SQL 语义。

方法：以同一惰性、遵守 demand 的源生成 10,000/50,000 个 key，每 key 1/10 个值；每行附加不进入集合元素的独立 payload，并只用 WeakReference 观察它。比较 `count`、`collect_row(deviceId,score)` 和 `collect_list(score)`，另以显式 `aggregate.fastPath=false` 将 `count/collect_row` 切到兼容路径作所有者负对照；在窗口打开、关闭后只消费一个结果、取消后三个阶段记录 post-GC heap、payload 可达数与输出是否携带 payload。必要时以同进程类直方图识别 List/Map/Record 所有者。兼容聚合可能将每组最后一行复制到结果，此时一个 payload/key 只有在输出确实携带它时才算必要；消费者不保存输出 Map。

退出条件：若 payload 未被额外保留，或 retained 状态只随 key 数和每 key 列表长度按预期增长，本切片止于诊断；不引入全局预算、静默淘汰、自定义 Subscriber 或专用 SQL 分支。若发现超过结果契约所需的 payload/Record 跨窗口仍可达，先定位所有者与取消路径，再单独进行最小生产 A/B，须保持背压、错误、Context 和精确集合语义。阶段末集中构建探针并运行代表规模与负对照；JFR 分配样本不能当作 live heap。

扩展边界补充：`ScalarValueMapper` 的快路直接调用 `applyScalar`，因此其 Javadoc 明确该方法为权威语义；自定义实现若覆写 `apply`，必须保持值、空值、错误与同步求值时机完全等价，不能借它引入异步、Reactor Context 或订阅副作用。需要这些语义的扩展应只实现普通 `Function<ReactorQLRecord, Publisher<?>>`，不声明 `ScalarValueMapper`。本补充只收敛公开能力契约，不新增运行时检查或改变生产执行路径。

### `collect_list` 同步列参数的增量融合

保留门槛：仅在构建期可证明为内置 `collect_list` 的普通列名/字符串列参数、非 `DISTINCT/UNIQUE`、有参数且第一个参数不是子查询时创建增量累加器。每个订阅、分组和窗口独立持有一个 `ArrayList`，按上游到达顺序加入与兼容路径相同的 `LinkedHashMap` 值；空字段继续省略，结果列表仍可变。单集合上限继续由既有 `aggregate.maxCollectionSize` 在同一第 N/N+1 时机报告同一 `RESOURCE_LIMIT`，缺省配置仍为原有无有限上限。`DISTINCT/UNIQUE`、无参数、子查询、checkpoint、异步路径和任意第三方聚合均保守回退 Publisher/兼容分组路径。

该切片不增加 Reactor operator、Subscriber、缓存或 SQL 文本分支。融合阶段复用它已有的背压、取消、错误和 Context 传播边界；列表的结果元素仍是聚合语义所必需的状态，优化目标只是避免兼容单聚合 cursor/groupBy 为每个活跃 key 额外保留最后一个 `ReactorQLRecord`。生产变更只有在结果值、Map 字段顺序/可变性、上限错误、Context、背压、取消和错误语义回归通过后保留；探针/JFR/JMH 由总控按冻结口径单独运行，不能用本节替代 live-heap 或吞吐结论。

补充回归门槛：同一 key 的 `_window(2)` 相邻两个窗口必须分别形成两个独立的可变 `ArrayList`，每窗恰好达到 `aggregate.maxCollectionSize=2` 不报错且元素不得串窗；全局空输入仍须输出含可变空 `ArrayList` 的单行结果，并与 `aggregate.fastPath=false` 深度等价。内层 `LinkedHashMap` 同样保持可变。该回归只验证累加器的订阅/窗口生命周期与既有值契约，不替代总控的 live-heap A/B。

### 分组输入元数据兼容修复（2026-10-05）

融合分组阶段曾只在输出记录写入 `_group_by_key`，而兼容 `GroupFeature` 会在每条进入分组的输入记录写入该元数据。因此 `collect_list('_group_by_key')` 在融合路径收集空 Map、兼容路径收集每行键列表；`collect_list('this')` 也会因 `this` 视图包含具名来源而观察到同一差异。该差异不是集合函数的字段特例，而是融合阶段漏掉了分组输入记录的通用契约。

修复在所有已通过同步分组键求值的融合输入上，按 SQL 分组维度顺序调用既有 `GroupFeature.writeGroupKey`，再将该记录交给累加器；`_window` 仍不产生 group-key 元数据，与兼容 GroupFeature 链路一致。输出记录只复制输入已经形成的完整键序列，既不覆盖上游嵌套键，也不重复追加当前键；复制后的可变列表与输入记录隔离。未增加 Subscriber、缓存、配置或默认限制，也未按字段或 SQL 文本分支。

`CollectListIncrementalTest` 对 `_group_by_key` 与 `this` 两种真实可观察输入视图分别将融合结果与 `aggregate.fastPath=false` 路径做深度对比；修复前前者可稳定复现空 Map。另覆盖预置上游键 `['pre']`、双维 `group by a,b` 的输出 `['pre','A','B']`，以及输出列表写入不影响输入列表。核查中发现该契约的根因同时在 `GroupFeature.writeGroupKey`：它对已有 Collection 的临时 copy 追加后没有回写 Record，因此会丢失前序/后续键；该文件已统一改为 copy-append 后始终 `addRecord`。该修复恢复了兼容路径原本就有的每行记录元数据物化成本；性能结论仅以下述语义完整的正式成对基准为准，不外推修复前诊断数据。

后续复审补充了两个泛化边界。融合阶段按分组维度的 SQL 顺序“求值后立即写键”；因此后续维度可读取前一维键，窗口本身不写键且不会打断前后维度的可见顺序。回归使用真实 `{a:'A'}` 输入，覆盖 `group by a,_group_by_key` 及 `group by a,_window(1),_group_by_key`，均确认一行、`total=1` 且与兼容路径等价。对于 `retainSourceRecord=false` 的紧凑聚合，`GroupState` 只保存最后一个有效输入的完整 group-key 值引用，不保存源 Record；`toRecord` 输出时创建独立可变副本。这使公开 `start(ReactorQLContext)` 输出 Record 保留上游 `['pre']` 与当前键 `['A']`，同时不变异上游共享列表，也不重复追加键。

验证与正式性能证据：`mvn -q -Dtest=CollectListIncrementalTest,GroupFeatureKeyTest,WindowedAggregateStageTest test` 通过；`mvn -q -Pjmh package` 通过，Surefire 汇总 427 tests、0 failures/errors/skipped；`git diff --check` 通过。测试日志中的预期错误源和 `onErrorDropped` 为既有生命周期回归的输出，命令退出码为 0。

正式成对 JMH 见 `target/jmh-collect-list-semantic-final.json`：JDK 17.0.18、512 MB、G1、3×1s warmup、5×1s measurement、2 forks、`-prof gc`。`collect_list` 融合为 9,707,768.765±196,204.199 ops/s，对比兼容 3,101,946.323±34,591.892 ops/s（约 3.13×）；瞬时分配为 541.057 对 830.164 B/op（约下降 34.8%）。`count` 负对照的融合/兼容分别为 19,303,817.219±749,480.978 对 3,678,080.072±175,240.050 ops/s，333.056 对 619.763 B/op（约下降 46.3%）。GC profiler 的 B/op 是瞬时分配，不是常驻堆。

高基数未关闭窗口的 live-heap 探针日志位于 `target/live-heap-semantic-*.log`。每 key 一行、10,000/50,000 keys、同一 JDK 17/512 MB/G1 条件下，`count` 融合/兼容 post-GC heap 分别为 6,443,200/27,254,296 B 与 16,367,384/120,117,848 B；`collect_list` 为 8,805,664/32,725,272 B 与 28,351,328/147,479,032 B。`collect_list` 融合的无关 payload weak-reference 存活数均为 0，兼容路径分别为 10,000/50,000；取消后两条路径均为 0，堆接近进程基线。`collect_list` 融合相对 `count` 融合约多 236/240 B/key，归因于每组实际必须保留的单元素列表状态，不能称为泄漏。环境阻断了 `jcmd` attach，未新增直方图；这些受控存活堆读数不用于推导一般性每行内存配额。
## 纯同步投影空星号展开列表的构建期分流

目标：根据已编译的 `allMapper.isEmpty()`，让纯同步、无 `*` / `t.*` 的投影不再逐行执行空 `allMapper.forEach`；保持标量列求值、`setResult` 的次数和顺序、异常时机、字段覆盖与可变性不变。

范围：仅 `DefaultReactorQL.createMapper` 和专用投影回归测试；本节记录前后 JMH 证据。不会按 SQL 文本、列数或函数类型分支，不增加 Reactor 操作符、自定义 Subscriber、公共 SPI、缓存或跨订阅状态；有星号和异步混合路径保持现有实现。

步骤：先以现有三条投影 JMH 在当前源码/JAR 身份下记录同配置基线；随后仅在构建期选择不含星号展开的标量 mapper，补充普通标量自定义 mapper 的顺序/异常、`*`/`t.*` 覆盖和异步混合回归；阶段末一次完整构建和相同 JMH A/B。若正式收益不稳定、任一路径稳定回退或分配恶化，撤回本生产改动并记录否决。

风险与验证：不允许将短 JFR 样本换算为 B/op 或存活堆。验证精确覆盖 Result 写入顺序、第三方 Record、Context、背压、取消和有星号展开；正式运行固定 JDK 17、512 MB/G1、3×1s 预热、5×1s 测量、2 forks、`-prof gc`，产物放在 `target/`。

结果：基线源码/JAR SHA-256 分别为 `9895c538939bc421116269d8e20c5327b7a769295f54d90f21c22d65e0bbcc3a` / `d8554e9c735650378feaeba03ebfb6df5995f4cc0448d8256e3f2bfc09d5367f`；候选实现时为 `b6bfd01ab697dde07eb7dd675251660f0dc94e0ea373278addd6958f9b19c029` / `2c6e282729624ff0fd828dd676a2e733afa43209618425dd8d84ae629c6e752f`。候选仅在构建期按 `allMapper.isEmpty()` 选择两个等价的标量 mapper：无星号分支不再创建/调用逐行捕获 `record` 的空 `forEach` consumer；星号分支保留原 `allMapper.forEach`、写入顺序和覆盖时机。没有 SQL 文本、列数或函数特调，也没有新 operator、缓存、状态或 SPI；该生产候选已按本节最终决策撤回。

`ScalarProjectionExpansionTest` 固定自定义同步 `ScalarValueMapper` 的逐行、逐列调用顺序和原异常身份；验证 `*`、`t.*` 都在标量列写入后覆盖同名字段并保持键顺序，以及标量/异步混合投影仍有相同字段和值。实现后首次 `mvn -q -Pjmh package` 通过，Surefire 汇总 430 tests、0 failures/errors/skipped；`git diff --check` 通过。复测期间曾在共享工作树中见到 `GroupByWindowTest.testGroupByTimeWindow` 的 `expected 3.5 / actual 4.0`，不涉及本切片的投影 mapper；撤回候选后同一全量命令再次通过，Surefire 仍为 430 tests、0 failures/errors/skipped，该失败未复现。现有完整 suite 继续覆盖第三方 Record、Context、请求和取消边界；本切片未改变这些执行边界。

正式 JMH（JDK 17.0.18、512 MB/G1、单线程、3×1s warmup、5×1s measurement、2 forks、GC profiler）基线为 `target/jmh-projection-empty-allmapper-before.json`。之前误将一次被后续静默重跑覆盖前的控制台数值写入本节；它不是当前 `...after.json` 的内容，故以下只以保存的 JSON 为准。当前 after JSON 是：预构造投影 19.739±3.727 → 18.120±2.291 M ops/s、231.996±12.749 → 223.996 B/op；宽投影 8.918±0.519 → 7.682±0.096 M ops/s、519.987±12.749 → 511.987 B/op；`t.*` 星号负对照 30.303±0.801 → 31.672±3.161 M、208.027 B/op 不变。由于宽投影首个 after 与基线区间不重叠，不能据此保留，随即以同一 after 源码/JAR 另存 `target/jmh-projection-empty-allmapper-after-prebuilt-wide-repeat.json` 复测预构造和宽投影：分别为 18.044±2.925 M、223.996 B/op 与 9.911±0.286 M、511.987 B/op。宽投影两轮 after 7.682±0.096 / 9.911±0.286 M 彼此不一致，无法形成稳定回退或提升结论；预构造同样与基线误差区间重叠。星号路径另有 `target/jmh-projection-empty-allmapper-after-star-repeat.json`，为 30.913±2.094 M、208.027 B/op，与基线区间重叠且分配相同。GC B/op 是瞬时分配，不是存活堆。此前基于“两个无星号代表路径均少 8 B/op”的初步保留判断已撤销，原因见下文的 baseline fork 原始数据。

最终决策（撤回）：重审基线的 fork 原始 GC 数据后，宽投影两个 fork 本身分别为 527.987 / 511.987 B/op，而 after/repeat 为 511.987 B/op；因此“稳定少 8 B/op”不是成立的归因。吞吐亦不能建立稳定收益：宽投影 after 的 7.682±0.096 与 9.911±0.286 M 不一致，预构造吞吐与基线重叠。故使用最小补丁仅撤回 `DefaultReactorQL` 中的 `allMapper.isEmpty()` 生产分流，恢复统一的既有 `allMapper.forEach` 路径；保留 `ScalarProjectionExpansionTest` 作为现有投影语义回归，保留 JSON 以记录否决原因。没有修改任何其他生产优化、默认限制或 Reactor 边界，也不再运行该低收益候选的 JMH。撤回后聚焦 `ScalarProjectionExpansionTest` 通过；其后一次 `mvn -q -Pjmh package` 中此前共享状态出现的 `GroupByWindowTest.testGroupByTimeWindow expected 3.5 / actual 4.0` 未复现（当前 `org.jetlinks.reactor.ql.examples.GroupByWindowTest` 8 tests、0 failures/errors）。此前文档没有该类失败记录，因此将它视为未复现的环境/时序波动，而非投影分流归因。

## 跨场景性能门禁与分组键 JFR 纠偏（2026-10-05）

最新空投影候选已撤回后，基准 JAR SHA-256 为 `477cd956…c22d52b26`；`mvn -q -Pjmh package` 的全量 Surefire 为 430 tests、0 failures/errors/skipped，`git diff --check` 通过。以 JDK 17、512 MB/G1、3×1s warmup、5×1s measurement、2 forks、`-prof gc` 对 12 个 SQL 场景运行的正式矩阵位于 `target/jmh-global-after-collect-list.json`；历史 `target/jmh-global-current.json` 不是成对运行，故只能作为跨场景门禁，不能归因到某个单独切片或宣称稳定回退。其高基数 `count` 吞吐约 -17.0%、瞬时分配 2059.372→2363.372 B/op（+14.76%），多聚合约 -8.9%、2303.372→2595.373 B/op（+12.68%）；其他多个场景吞吐亦低约 8–13%。GC profiler 的 B/op 是瞬时分配，不是存活堆。

当前源码的短 JFR 位于 `target/jfr-group-key-current/`：每行 `GroupFeature.writeGroupKey` 可见 `LinkedList`/`Node` 及 `addRecord` 的 Map 分配。该样本只用于定位，不能换算为 B/op 或 live heap。对高基数 SQL，`_window,key` 仅有一个真实 group 维度且上游没有键，所以没有 `Collection→ArrayList→LinkedList` 双拷贝；该双拷贝只可能发生在多维分组或已有上游键的链路，去掉它也不能解决一维高基数的主要开销。

写入键是 `collect_list('_group_by_key')`、多维嵌套等既有可观察语义所需。当前不存在一种既通用且保持这些语义、又不增加状态/SPI 或复杂度的低复杂度修复，故停止对这条一维路径的微优化；不调整默认资源上限，也不改变精确聚合。下一轮只选择有新 JFR 证据的跨场景热点，不重复针对该一维路径特调。本门禁不代表总体优化目标已完成。

## 当前源码子查询 JFR 复评（2026-10-05）

最新基准 JAR SHA-256 为 `477cd956…c22d52b26`。在 JDK 17、512 MB/G1、1×1s warmup、2×1s measurement、1 fork 下，当前源码的采样产物位于 `target/jfr-subquery-current-20261005/`。深层未关联子查询录得 108 个 CPU、610 个 allocation samples，热点主要是 Reactor `flatMap` 的完成/drain 及结果 Map 物化；`cacheMany` 命中只见 2 个 CPU samples，未见 allocation sample。关联子查询录得 101 个 CPU、651 个 allocation samples，主要为逐行 transfer、别名绑定、Record/Map 物化和 `flatMap`；订阅级 `cacheMany`/`cacheMono` 均未命中，符合关联子查询不得跨外层行缓存的既有语义。

JFR sample 不代表 B/op、吞吐或 live heap。该复评未发现一种同时通用于子查询场景、保持冷源订阅时机、取消、错误和 Context 传播、且复杂度足够低的生产改动；不引入未经证明的轻量 Record 新抽象。本切片止于取证，整体优化目标继续开放。

## 当前源码函数与 JOIN JFR 复评（2026-10-05）

最新基准 JAR SHA-256 仍为 `477cd956…c22d52b26`。在 JDK 17、512 MB/G1 下，短时采样产物位于 `target/jfr-functions-join-current/`：`commonFunctions` 有 96 个 CPU、660 个 allocation samples，热点主要为通用 `FunctionMapFeature.newScalarArguments` 的可变参数 `List` 以及日期/字符串转换；`innerJoin` 有 104 个 CPU、660 个 allocation samples，热点主要为别名 `Record` 合并、结果 `Map` 物化及 Reactor `flatMap`/订阅器。样本不代表 B/op、吞吐或 live heap；参数 `List` 可变性和第三方 Feature 调用、JOIN 别名及结果独立性均为功能契约。当前未找到低复杂度、通用的生产优化，故不为此引入 arity 特调或专用 `Record` 表示；后续只接受新的当前源码 JFR 或成对基准证据，不重复这些候选。

## 单键 `ORDER BY` 状态表示 A/B（2026-10-05）

目标：消除构建期可知只有一个排序键时每个输入行分配的单元素 `Object[]`，让 `OrderedRecord` 直接保存一个已归一化的排序值；多键仍保持现有数组表示与逐键比较循环。范围仅为 `OrderBySupport`、专用真实 SQL 回归、本节和成对性能产物。不会按 SQL 文本、字段类型或函数实现分支，不引入新的 Reactor 操作符、Subscriber、缓存或状态，也不修改默认排序上限。

步骤：先以当前 JAR/SHA 对同步单键 Top-N、单异步键 Top-N、双异步键 Top-N 和无 LIMIT 全局单键排序保存同配置基线。随后在查询构建期分出单键比较器与 `OrderedRecord` 的标量值表示；同步和异步单键共用该表示，多键维持 `Object[]`。补充/复用 SQL 回归覆盖 null 排序、同值 Top-N/offset/无 LIMIT、多键混排、异步单/双键的订阅与错误，以及结果 Map 可变性。阶段末一次完整 Maven/JMH 构建、差异检查和同一四入口 after 基准；只有正例 B/op 稳定降低、吞吐无稳定回退且多键负对照无稳定回退时保留，否则仅撤回本切片生产代码并记录否决。

风险与验证：必须保持 `NULL_ORDER_VALUE` 的空值归一化、ASC/DESC/NULLS FIRST/LAST、相等键的原有稳定性、Top-N/无 LIMIT/窗口输出、冷异步 mapper 的订阅次数与顺序、错误、Context、背压和取消。GC profiler 的 B/op 仅代表瞬时分配，不等同于 live heap；JFR 仅用于确认 `Object[]` 热点消失，不能从样本换算字节。

结果：基线 JAR SHA-256 为 `477cd95673eee3a970c07311f1cb693e8f71b7b11c9211a1f04ccc8a22d52b26`，最终 JAR/`OrderBySupport.java` SHA-256 分别为 `843489f1fe66b9431acdeef842d77b3066115719e95046f224f99f95fd4e8595` / `f7cbaebf4e5d8dbae0348d83b244923bc5e16529ad9479d7eb4ec699b6e2517a`。基线与最终均为 JDK 17.0.18、512 MB/G1、单线程、3×1s warmup、5×1s measurement、2 forks、`-prof gc`；原始结果分别为 `target/jmh-order-single-key-before-20261005.json` / `target/jmh-order-single-key-after-final-20261005.json`。没有覆盖基线或使用控制台中间数字。

最终实现让 `OrderedRecord` 只有一个 `Object value` 字段：单键直接保存已归一化值，多键在这个字段内保存原有 `Object[]`；构建期选择对应比较器，多键比较每次只先取一次数组，不在逐键循环内增加表示判断。同步单键 scalar mapper 和异步单键 mapper 都避免了 `new Object[]{value}`；多键 mapper、`concatMap`、`toArray`、默认限制和信号边界未改。`OrderBySingleKeyStateTest` 覆盖单键 null/Top-N/offset/无 LIMIT、输出 Map 可变性、异步单/双键订阅次数与错误；既有 `ReactorQLTest` 同时覆盖窗口、Context、取消、多值首值和多键排序。

四个入口的 score ± error（ops/s）/ GC allocation（B/op）以及两个 fork 的 allocation 值如下：同步 Top-N `6,972,489±71,730 / 208.149 [208.149,208.149]` → `8,022,937±120,073 / 168.148 [168.149,168.149]`，吞吐约 +15.1%、每输入行少约 40 B；单异步 Top-N `4,287,722±42,450 / 480.159 [480.159,480.159]` → `4,555,345±143,907 / 440.158 [440.158,440.158]`，吞吐约 +6.2%、少 40 B。无 LIMIT 单键排序 `18,048,711±870,300 / 310.540 [302.540,318.540]` → `18,260,484±614,948 / 286.540 [294.540,278.540]`，吞吐区间重叠，不申领稳定吞吐收益；两个 after fork 的分配都更低，但其下降幅度受环境波动影响。双异步多键负对照 `2,672,958±97,524 / 1080.159 [1080.159,1080.159]` → `2,565,392±75,488 / 1072.159 [1064.159,1080.160]`，吞吐区间重叠且 allocation 不具稳定、可归因变化，故不对多键申领收益或回退。

最终短 JFR（1×1s warmup、2×1s measurement、1 fork）位于 `target/jfr-order-single-key-final-20261005/`。同步单键 `profilingOrderByLimit` 在 633 个 allocation samples 中没有 `Object[]`；单异步在 660 个中仅有 1 个 `Object[]`，栈为 `OrderBySupport.topN` 的完成后 `new ArrayList<>(queue)`，而非逐输入行 `createOrderValueMapper`。此前该 mapper 是 131 个 `Object[]` 样本的叶子；最终两条单键 JFR 都没有它。该样本只验证热点归属变化，不能换算为 B/op 或常驻堆。

保留结论：同步与异步单键 Top-N 都有稳定的瞬时分配下降，且正例吞吐未稳定回退；无 LIMIT 路径没有稳定吞吐回退；多键负对照没有稳定回退。`mvn -q -Pjmh package` 通过（Surefire 432 tests、0 failures/errors/skipped），`git diff --check` 通过。GC B/op 不是 live heap；此切片没有增加常驻状态，不能据此声称无界流常驻堆下降。

独立语义审查后补充了第三方风格 `ScalarValueMapper` 单键回归，覆盖 Top-N/无 LIMIT、`null`、异常身份和逐行调用次数。该测试已包含在下节最终完整构建的 435 tests 中，不改变本节 JMH 的生产源码或结果。

## 多维分组键复制 A/B（2026-10-05）

目标：验证 `GroupFeature.writeGroupKey` 在已有分组键时通过 `getGroupKey` 的防御性副本再构建 `LinkedList` 是否造成可测的重复复制；它只可能影响多维分组或上游已存在分组键，不能外推为一维高基数分组的常驻内存改进。

范围：仅 `GroupFeature`、专用 `GroupFeatureKeyTest`、新增专用 JMH 与本节。先在同一源码/JAR 身份下对一、二、三维追加及一个外部预置键列表基线运行 JMH/短 JFR。仅当证据显示重复复制在多维场景有实质瞬时分配成本，才将 `writeGroupKey` 改为一次从原始值构建既有可变 `LinkedList`；`getGroupKey` 继续保留其现有防御性复制语义。

不做：不改变 SQL 文本、分组生命周期、默认限额、上游 Publisher、Reactor 操作符或新增状态/SPI；不按键类型或维度数分支。回归必须覆盖空键、Collection/Object[]/标量预置键、调用者持有列表与前一阶段列表的隔离、顺序和可变性；结合现有分组窗口/取消/错误测试确认信号边界未改变。阶段结束时统一构建、全量测试、同配置 after JMH/JFR 与差异检查；只有正例分配稳定下降且吞吐无稳定回退才保留，否则只撤回本切片生产改动并记录结果。

结果：保留。`writeGroupKey` 仍为每次写入创建独立、可变的 `LinkedList` 并回写 Record；它直接从原始 `null`、`Collection`、`Object[]` 或标量值构建，避免内部调用 `getGroupKey` 后再复制一次。公开 `getGroupKey` 及其 `null` 返回空列表、Collection 返回防御性可变 `ArrayList`、数组/标量返回既有 `castArray` 视图的语义完全未改。没有增加操作符、订阅器、状态或 SQL 形态分支。

`GroupKeyCopyBenchmark` 每次向 `Blackhole` 发布新写入的实际列表，防止 JIT 仅因消费 size 消除元素复制；一、二、三维分别对应空键、已有一键、已有两键，另覆盖外部预置 Collection/数组/标量。正式前 JAR/`GroupFeature.java` SHA-256 为 `99770ed6c42a23f7d85a691ee14b899de5c4c600849cb4b873156eb714872ced` / `e9efb3950e2efddc33d6fc4dac653b6e8bb61cd16b7c57a0d29a418c24987447`；after JAR/源 SHA-256 为 `a440a9cdf08d31502381b9ca968a79e1f7af8ec413bdb32a0cf6ccfc2b98a5fb` / `8f7830c436801bad82324f6cba89130a6c78188d5e196218ceb26ea114ef534c`。均为 JDK 17.0.18、512 MB/G1、单线程、3×1s warmup、5×1s measurement、2 forks、GC profiler；原始 JSON 为 `target/jmh-group-key-copy-before-blackhole-20261005.json` 与 `target/jmh-group-key-copy-after-blackhole-20261005.json`，未覆盖先前诊断 JSON。

JMH score ± error（ops/s）与每 fork GC allocation（B/op）为：一维 `36.247±0.802 M / 292.000,292.000` → `36.701±0.500 M / 292.000,292.000`；二维 `27.594±0.588 M / 388.000,388.000` → `30.678±0.208 M / 340.000,340.000`；三维 `26.126±0.267 M / 412.000,412.000` → `28.643±0.096 M / 364.000,364.000`；预置四键 Collection `23.572±0.182 M / 484.000,484.000` → `25.238±0.424 M / 420.000,420.000`；预置数组 `25.073±0.466 M / 420.000,420.000` → `25.203±0.384 M / 420.000,420.000`；预置标量 `31.224±0.142 M / 316.000,316.000` → `31.695±0.145 M / 316.000,316.000`。因此 Collection 正例减少 48 B/op，预置四键减少 64 B/op，数组/标量/一维负对照不变；所有正例吞吐向上且置信区间没有显示稳定回退。

短 JFR（1×1s warmup、2×1s measurement、1 fork）保存在 `target/jfr-group-key-copy-before-blackhole-20261005/` 与 `target/jfr-group-key-copy-after-blackhole-20261005/`。before 的 `CastUtils.castArray → getGroupKey → writeGroupKey` allocation stack 样本计数为预置 Collection 62、三维 96、二维 110；after 三条均为 0，剩余 `LinkedList`/node 分配是发布隔离列表本身所必需，不能据此换算字节或常驻堆。

`GroupFeatureKeyTest` 新增多维读副本、外部列表、数组和标量的顺序、可变性与隔离回归；既有窗口/取消/错误覆盖保持。`mvn -Dtest=GroupByWindowTest test -e` 为 8 tests、0 failures/errors。首次及第二次完整 Maven 运行曾在测试方法前的 `ReactorDebugAgent` static initializer 失败（`GroupByWindowTest` 8 errors，未进入 GroupFeature/排序代码）；未修改测试或业务逻辑来绕过。随后最终 `mvn -q -Pjmh package` 通过，Surefire 435 tests、0 failures/errors/skipped；`git diff --check` 通过。GC B/op 只是瞬时分配，本切片不申领 SQL 端到端吞吐或无界流常驻堆收益。

## 分组键真实 SQL 端到端复核（计划，2026-10-05）

目标：用预构造且有界输入确认已保留的 `GroupFeature.writeGroupKey` 一次复制实现，在真实 SQL 的一、二、三维分组中是否仍有可归因收益；覆盖默认 `aggregate.fastPath` 和显式 `false` 的兼容 Publisher 路径。

范围：仅新增 `GroupKeySqlBenchmark`、本节和为成对 A/B 临时替换 `GroupFeature.writeGroupKey` 的旧双复制实现，结束前必须用 `apply_patch` 恢复当前候选源 SHA `8f7830c436801bad82324f6cba89130a6c78188d5e196218ceb26ea114ef534c`。不会改动其他生产代码、默认资源限制、SQL 语义、Reactor 操作符、订阅器、SPI 或缓存。

路径确认：默认设置由 `DefaultReactorQL` 在构造时选择 `WindowedAggregateStage.tryCreate`；可支持的同步 `_window(n), region[,site[,device]]` + `count(1)` 在 `SubscriptionState.evaluateAndWriteGroupKeys` 对每个真实维度调用 `writeGroupKey`。`aggregate.fastPath=false` 返回既有 `createGroupBy` 路径，`GroupByValueFeature` 的 `GroupStateBudget.createGroupMapper` 在每个维度调用同一方法。因此两条路径都可达，且一维为负对照、二/三维为真正可能含已有键的正例。

步骤与门禁：基准 setup 将验证每种 SQL 的结果行数、总 count、字段和值域及每次执行的源订阅次数；每个基准调用消费完整的实际结果列表到 `Blackhole`，并按 4096 输入行归一化 GC profiler 分配。先用唯一 JSON 名记录旧双复制 before（源码/JAR SHA），再恢复候选并以相同 JDK、堆、GC、fork 和源文件记录 after；JFR 只用于定位当前源码的 `writeGroupKey` 调用栈。仅在二/三维正例分配可重复下降、吞吐没有稳定回退且一维负对照不出现异常变化时，才维持当前候选；GC B/input-row 不代表存活堆。

结果：保留。新增的 `GroupKeySqlBenchmark` 用 4096 条预构造且冷的 `Flux.fromArray` 输入，分别验证默认融合聚合和 `aggregate.fastPath=false` 兼容路径的一、二、三维 `_window(4096)` 分组。setup 对每条 SQL 验证精确 group 数（64、256、512）、每组 count、count 总和、完整维度值和恰好一次输入订阅；热路径只执行查询、作常数时间的结果数检查并向 `Blackhole` 消费实际结果，避免把 HashSet/字符串校验或订阅计数器混入 B/input-row。每个查询完成即释放窗口状态；这不是无界常驻堆探针。

正式 paired before/after 均为 JDK 17.0.18、512 MB/G1、单线程、3×1s warmup、5×1s measurement、2 forks、GC profiler，原始 JSON 是 `target/jmh-group-key-sql-before-20261005.json` 与 `target/jmh-group-key-sql-after-20261005.json`。JMH 源 SHA-256 始终为 `59d421d0d40ca7fd45c31b6668d8e491e81146f6ff3f766dffe8aa41c1a06cf0`；old 双复制源/JAR 是 `491b632d57c419957e3a6c35a009884c7df4e86247d0a4a35c2804248a3ab1f7` / `f5b5f743110ccf540bd71f9c5441a578b39f78130a96f3090f800c97f3a6d7bd`，候选源/JAR 是 `8f7830c436801bad82324f6cba89130a6c78188d5e196218ceb26ea114ef534c` / `24f1757f26255ccf5b1e6367471a475f98c3849a6a10a3deceaacb5a618ad0fc`。旧源码的 `writeGroupKey` 主体与前轮 `e9ef…` 的 `new LinkedList<>(getGroupKey(record))` 隔离实现相同，但该轮文件哈希不相同（历史 JAR 不在工作树，未把二者冒充为同一 source identity）；所以本节把 JMH 作为当前候选与语义等价旧方法的成对证据，而不替代前轮 helper A/B 的 source-identity 记录。

初始 paired 分数（M input rows/s）/瞬时分配（B/input row）为：一维 default `24.602±0.492 / 291.047` → `22.676±1.048 / 275.053`，一维 publisher `11.414±0.572 / 365.434` → `11.262±0.237 / 349.434`；二维 default `9.871±0.192 / 504.065` → `11.174±0.372 / 416.065`，二维 publisher `4.594±0.142 / 755.858` → `4.842±0.062 / 668.858`；三维 default `6.518±0.144 / 766.111` → `7.386±0.051 / 606.107`，三维 publisher `2.394±0.233 / 1273.231` → `2.550±0.037 / 1113.730`。二/三维两条路径分配分别下降约 87–88 B 与 159–160 B；default 正例吞吐上升，三维 publisher 的吞吐区间重叠，故不申领其稳定吞吐提升，但没有稳定回退。

一维的 -16 B/input-row 不是可忽略的负对照异常：这个 SQL 路径中旧代码将空 `Collections` 键作为 `LinkedList(Collection)` 的输入，而候选直接建空 `LinkedList`，所以仍消除了一个通用的空键复制/迭代边界。其初始 default 分数看似下降，因此按照 A/B/A 门禁以相同 JMH 源、JDK、堆、GC、fork 重跑：candidate A1 `23.131±1.267`、old B `22.280±1.402`、candidate A2 `22.500±1.752` M input rows/s，区间均重叠；publisher A1 `11.476±0.130`、B `10.925±0.489`、A2 `10.999±0.301`，B 与 A2 重叠。对应 default 分配为 A1/A2 `275.053/275.047` 对 B `291.053`，publisher 为 `349.439/349.434` 对 `365.427`，均稳定少约 16 B/input-row。重复 JSON 为 `target/jmh-group-key-sql-one-dimension-a1-20261005.json`、`...-b-20261005.json`、`...-a2-20261005.json`。因此初始一维吞吐差不能归因成稳定回退，分配下降也不是环境身份漂移；没有为此加入一维特调或额外分支。

候选短 JFR（1×1s warmup、2×1s measurement、1 fork）在 `target/jfr-group-key-sql-current-20261005/`，二/三维 default/publisher 的 allocation/CPU stack 分别有 39/17/46/8 条包含当前 `GroupFeature.writeGroupKey`，确认真实 SQL 的两条路径都达到该方法。JFR 只用于定位调用栈，不能用这些样本换算 B/input-row 或存活堆。最终恢复候选 `GroupFeature.java` SHA-256 `8f7830c436801bad82324f6cba89130a6c78188d5e196218ceb26ea114ef534c`；`mvn -q -Pjmh package` 通过，Surefire 为 435 tests、0 failures/errors/skipped。GC B/input-row 是瞬时分配，不是 live heap；本节不改变默认限制、精确聚合生命周期、Reactor 操作符或公共 SPI。

## collect_row 高基数常驻堆复核（2026-10-05，取证计划）

范围：只运行既有 `HighCardinalityLiveHeapProbe` 并在本节记录证据；不改生产代码、探针、基准、测试、默认限制或 SQL 语义。先核对 probe 的 SQL 确实是 `collect_row(deviceId,score)` 且 payload 不进入聚合值或结果字段；以 JDK 17、512 MB/G1、当前已编译 JAR 进行 JFR-first 录制，并在 phase marker 后尝试 post-GC `GC.class_histogram`，同时以 `WeakReference` 计数判断 payload 是否可达。

矩阵按信息增益收敛：先跑 fused 与 `aggregate.fastPath=false` 兼容路径的 50,000 key、每 key 1 value、open/cancel；再以 2 values/key 验证覆盖而非历史值保留；仅在该正例有额外 payload/Record 留存时补 10,000 key 和 closed-window 的所有者/释放交叉组。closed 模式的一个结果 demand、open 根订阅、取消清理均由探针自身 marker 验证。JFR allocation samples 和 GC profiler B/op 不用于推导 live heap；若剩余状态仅为精确 `collect_row` 的 key/value Map，且取消/窗口释放正常，本切片止于否定结论。

结果：当前工作树 `HEAD=d430595837d17608a4010438d051c5d273d6b1b8`；运行 JDK `17.0.18`、`-Xms512m -Xmx512m -XX:+UseG1GC`。探针/`CollectRowAggMapFeature`/`DefaultReactorQL` 源 SHA-256 分别为 `c2570d5e6a500225782eb5d1b4ee2187134886b0eb7f40accd78a90b91d13bcb`、`7ec353475eab5e158fdfedf6b044c0e74f7feca39e1c2358cf6ea2edb0f63c5f`、`9895c538939bc421116269d8e20c5327b7a769295f54d90f21c22d65e0bbcc3a`；实际执行的已有 shaded benchmark JAR 为 `target/reactor-ql-1.0.21-SNAPSHOT-benchmarks.jar`，SHA-256 `b52a3bbdb1d3899b5163c32e8a7f07dd26a927dddbd55cfe39dcc04e49e62367`。未为该取证重建共享 JAR，故两种身份如实分列而不冒充 source/JAR 严格配对。

探针 SQL 由源码固定为 `select deviceId,collect_row(deviceId,score) rows from test group by _window(...),deviceId`；输入 `payload` 只放入源行，`collect_row` 的 key/value 均不引用它。所有 marker 的 `outputPayloads=0`，因而结果没有携带该 payload。先对 fused 和 `--compat` 的 50,000 key、1 value/key、open 运行 `-XX:StartFlightRecording=settings=profile,dumponexit=true`；原始 JFR/日志为 `target/collect-row-live-heap-20261005/{fused,compat}-open-50k-v1.{jfr,log}`。前者有 60 个 `ObjectAllocationSample`、3 个 `OldObjectSample`，后者 112/4；两份 allocation stack 分别有 62/45 条经过 probe/`CollectRowAggMapFeature`/分组实现，确认实际路径被录制。JFR 的 old-object 样本是 JFR 内部表、反射类等，不能给出 50k payload 的所有权，也不用于估算 live bytes。

WeakReference 与探针显式 GC 的结果为：fused 在 50k×1 open、50k×2 open/closed、10k×1 closed、10k×2 open 的活跃/关闭等待阶段均 `0` 个无关 payload 存活；取消后仍为 `0`。compat 在 50k×1 open 为 `50000→0`，50k×2 open 为 `50000→0`，50k×2 closed（已消费一条结果、另 49999 个关闭窗口待 demand）为 `49999→0`，10k×1 closed 为 `9999→0`，10k×2 open 为 `10000→0`（箭头后为取消）；这里的一个差值恰为已发射首个窗口。峰值 heap 读数也同方向：fused 50k×2 open/closed 为约 `28.96/28.80 MB`，compat 为 `146.44/203.15 MB`，取消后均约 `7.8 MB`；10k compat open/closed 为 `32.41/43.75 MB`，取消后约 `4.6–5.0 MB`。heap 数字只是探针重复性辅助，不用于建立每 key 配额；完整原始 marker 位于同目录八个 `*.log`。

外部 post-GC histogram 的尝试没有成功：带 `-XX:+StartAttachListener` 的后台进程在本受控环境无法参与 attach handshake，`jcmd <pid> GC.run` 与 `GC.class_histogram` 都报 `AttachNotSupportedException`，原始失败输出为 `fused-open-50k-v1-gc.log`、`...histogram`。不能把空直方图或 JFR allocation sample 替代它。WeakReference 的结果仍直接证明 payload 可达性，而源码定位表明兼容单聚合的 `DefaultReactorQL` 在每个 `GroupedFlux` 内以 `AtomicReference<ReactorQLRecord> cursor` 保存最后源记录；这与“一 key 一份无关输入 payload”的跨规模、覆盖次数无关现象一致，且取消时正确释放。

结论：默认融合 `collect_row` 在此探针中未保留无关输入 payload；不改动它。该观测不能证明其只保留 key/value Map：精确 key/value、结果 Map 及其他 `GroupState`/Record 元数据仍可能按既有聚合和分组契约驻留。`aggregate.fastPath=false` 的通用兼容单聚合路径则存在每活跃/待发射 group 一份额外最后 `ReactorQLRecord` 的高基数常驻成本，且该记录不属于本查询输出。它不是泄漏（取消释放），但对无界高基数流是可观测堆压力。后续若实施，只能在兼容单聚合输出确实不再需要源行时采用通用 query-plan 级“最小结果上下文”而非 `cursor` 整行引用；该判定必须先覆盖普通投影、别名、`*`、异步 mapper、Context、错误、背压和取消，不能以 `collect_row` SQL 专支或静默丢弃源行解决。本取证切片不做生产改动。

语义闭环（保留 cursor）：审阅确认兼容 `aggSize==1` 的 `cursor` 不是仅供本探针输出 payload 的内部引用：`DefaultReactorQL.createMapper` 在 `DefaultReactorQL.java:823-854` 对最后源记录 `copy()` 后执行 `putRecordToResult()`、`resultToRecord(newCtx.getName())`，再进入普通/异步 result mapper；`DefaultReactorQLRecord.java:265-290` 使源 alias、`this` 及嵌套子查询和自定义 Feature/metadata wrapper 可观察到该上下文，即使最终 Map 未包含 payload。没有已证明安全且低复杂度的通用谓词能在这些契约下判断“源行永远不可见”，所以不删除 compat cursor、也不为 `collect_row` 加专支。默认 `WindowedAggregateStage.GroupState` 已通过 `retainSourceRecord` 作保守分析，并在 `WindowedAggregateStage.java:765-815` 仅在可构造紧凑输出时避免整行引用；兼容 `GroupedFlux` 高基数场景的每组最后记录仍是已知堆限制，须作为后续独立 query-plan 契约工作，而非以本次优化绕过。

## 宽投影真实 SQL 基准（计划，2026-10-05）

目标：以预构造的 Map 行建立接近常见设备数据查询的宽投影压力基线，覆盖 12–18 个输出列、算术/比较、字符串、JSON 与日期函数及选择性 WHERE，从当前源码 JFR/JMH 识别跨查询热路径；并用同列数的简单投影作为宽度对照，区分结果 Map 物化与函数求值成本。

范围：只新增 `src/jmh/java/org/jetlinks/reactor/ql/WideSqlWorkloadBenchmark.java` 和本节；setup 预构造 65,536 个多类型行，校验列数、精确字段集合/类型、选择性结果数、订阅次数及输出行顺序，并按输入行归一化。热路径只流式消费真实结果至 `Blackhole`，不以 `Map.hashCode`、结果收集或额外校验污染分配数据。

不做：不修改生产路径、默认限制、Reactor 操作符、公共 SPI 或 SQL 文本分支；不把 JFR 样本换算为 B/行或常驻堆，也不在本切片提出生产优化。总控在独立嵌套查询夹具就绪后统一构建、执行 JMH/JFR；门禁为 SQL 可编译、setup 语义检查通过，之后才以同一 JAR/SHA 的成对数据筛选高收益且通用的候选。

实现与统一门禁：新增 `WideSqlWorkloadBenchmark`，函数路径输出 16 列：标量字段、算术/取模/比较、`upper`/`substring`/`replace`/`split_part`、两次 `json_get`、`date_add`/`date_format`/`date_diff`、`pow`/`round` 与 `coalesce`；控制路径保留同一 WHERE 和相同 16 列宽度，全部为直接列投影。65,536 个预构造 `LinkedHashMap` 行含数值、布尔、字符串、JSON 和日期字符串；`optional` 每七行一次非空，setup 覆盖 `coalesce` 的原值和回退到 `name` 两条分支，宽度对照以恒非空的直接列 `category` 保持固定 schema；WHERE 由 `score`、文本函数和布尔字段共同选择。setup 对两条 SQL 的冷源恰好一次订阅、精确结果数、字段集合、首中尾完整值/类型以及所有行 `sequence` 行顺序校验。当前 JAR smoke 已观察到 16 个键齐全但结果为 `HashMap`、迭代顺序不同于 SELECT 列表；生产结果没有这一顺序契约，因此夹具只断言集合与行顺序。基准以总输入行数为 `OperationsPerInvocation`，流式 `Blackhole.consume(resultMap)`，不收集热路径结果也不调用 `Map.hashCode`。随后由总控统一门禁，结果如下。

最终阶段证据：正式 JAR SHA-256 为 `bdcc16481a4c3342fb1501c7c394322323e332f8011f06809807b72d7986600f`，宽投影源码 SHA-256 为 `5d658de223792e948885a7ed92f0b1d3fad1c36d769ad0172865c6ab056c2bbe`。夹具修正前完整 `mvn -q -Pjmh package` 通过（435 tests，0 failures/errors/skipped）；修正后 `mvn -q -Pjmh -DskipTests package` 通过，`git diff --check` 通过。正式 JMH 为 JDK 17.0.18、512 MB/G1、单线程、3×1s warmup、5×1s measurement、2 forks、GC profiler，原始 JSON 为 `target/jmh-real-sql-formal-20261005.json`：宽度对照 `6,651,044±139,556` 输入行/s、`388.818 B/input-row`；函数宽投影 `1,603,703±20,046` 输入行/s、`2,844.917 B/input-row`。这是当前源码基线，不是生产优化前后对比，不能申领吞吐或堆改进；GC B/input-row 仅为瞬时分配。

同 JAR 的短 JFR 位于 `target/jfr-real-sql-20261005/`：宽度对照为 122 CPU / 622 allocation samples，函数宽投影为 115 / 660。函数路径中 JSONPath/parser 调用栈为 26 个 CPU、334 个 allocation samples，日期函数为 7 / 62；宽度对照有 453/622 个 allocation samples、函数宽投影有 231/660 个 allocation samples 落于 Map/Record 类别。类别和调用栈可重叠；这些样本只用于热点定位，不可换算 B/行或常驻堆。没有生产代码、默认限制或 Reactor 信号边界修改。

## 多层子查询真实 SQL 基准（计划，2026-10-05）

目标：用两个真实而语义不同的多层子查询形态补足现有单行 lookup 夹具的盲区：B1 为未关联、多行 lookup 经派生表与标量聚合子查询的两层 SELECT 后由外层读取，观测当前计划的重复 lookup 订阅与结果物化；B2 为单层关联子查询负对照，保留逐外层行订阅的真实边界。两者共同用于分辨计划装配、当前缓存边界和必要的关联执行成本。

范围：后续仅新增独立的嵌套 SQL JMH 文件及本节结果；输入源均为预构造冷 Publisher，setup 将检查结果数、值/类型/精确字段集合及行顺序、B1 lookup 订阅数和 B2 每外层行订阅数。当前 JAR 的 B1 65,536 外层行 smoke 已观测为 65,536 次 lookup 订阅，说明该多行聚合形态没有被当前 planner 缓存；夹具 owner 将 B1 缩为 1,024 外层行及 1,024 lookup 行以保持真实、可完成的基准负载。热路径流式以 `Blackhole` 消费实际结果，按外层输入行归一化；与宽投影夹具共享 JAR/JFR 门禁但没有生产写入。

不做：不为 B1 添加生产缓存、不去相关化、不改变子查询缓存策略、默认限制或来源订阅时机，不加跨订阅缓存、自定义 Subscriber、操作符或 SQL 文本特调。只有当前源码同 JAR 的 JFR/JMH 显示可通用且行为可证明等价的热点，才另开最小生产 A/B；JFR 样本不用于推导 B/行或 live heap。

最终阶段证据：正式 JAR SHA-256 为 `bdcc16481a4c3342fb1501c7c394322323e332f8011f06809807b72d7986600f`，嵌套夹具源码 SHA-256 为 `4473143f479da892ed780d4a21e05f3c87a00d87e3c3857dcf1005ba63a27fc8`。同一配置的正式 JMH（JDK 17.0.18、512 MB/G1、单线程、3×1s warmup、5×1s measurement、2 forks、GC profiler）原始 JSON 为 `target/jmh-real-sql-formal-20261005.json`：B1 未关联多行聚合为 `8,344.857±103.722` 外层行/s、`732,609.373 B/outer-row`，每个外层行扫描 1,024 条 lookup；B2 关联负对照为 `162,230.802±1,977.482` 外层行/s、`28,360.218 B/outer-row`，每个外层行扫描 256 条 lookup。数值按外层输入行归一化，B1/B2 的 SQL 工作量不同，不能直接横比；GC B/outer-row 也不是 live heap。

同 JAR 短 JFR 在 `target/jfr-real-sql-20261005/`：B1 为 133 CPU / 660 allocation samples，其中 Map/Record 类别调用栈为 98 个 CPU，Map/Record allocation 类为 649/660；B2 为 120 / 641，对应为 78 CPU、618/641 allocation samples。类别可能重叠，样本只作定位。B1 的重复订阅是当前保守函数缓存策略（分析器将所有函数视为不可缓存）的结果，不自动构成缺陷；本阶段不改变生产缓存、订阅时机、默认限制或响应式边界，也不申领性能改进。

最终夹具验证身份与正式指标分列：正式 JMH/JFR 指标只归属上述 `bdcc164…` JAR 与 JMH 前的嵌套源码 `447314…`。其后仅将 B2 setup 的结果键迭代顺序断言改为精确字段集合断言，热路径未变；最终嵌套源码 SHA-256 为 `c146357068b5404bc1ccbb08c038a1107e7939b238c22648456a111fb9b60899`，最终构建 JAR SHA-256 为 `86bf84b1570b4f261de88e1db8b6446934fbac9f5980cf141b23816989e18779`。最终 JAR 上的短 smoke `target/jmh-real-sql-final-smoke-20261005.json` 已通过，只验证夹具可运行，不替代或更新上述正式指标。最终升级权限的 `mvn -q -Pjmh package` 通过（435 tests，0 failures/errors/skipped）；sandbox 全量构建曾在测试主体前因 `ReactorDebugAgent` 初始化出现 8 个 `GroupByWindowTest` errors，升级权限的定向及全量构建均通过，且没有生产代码修改。`git diff --check` 通过。

## 宽投影重复 JSON 求值负对照（计划，2026-10-05）

目标：上一节正式 JFR 将 16 列函数 SQL 的 334/660 条 allocation samples 定位到 JSONPath/JSON 解析栈，但它不能给出第二次读取同一 JSON 的边际 B/行。新增同宽度、同结果、同输入源的控制查询：只把第二个 `json_get(json, '$.meta.level')` 替换为预构造输入中的同值直接字段，其余 SELECT、WHERE、结果列和 65,536 行输入保持不变。对照不是生产实现建议，只用于量化进一步优化 JSON 求值的可得收益范围。

范围：仅扩展 `WideSqlWorkloadBenchmark` 及本节；setup 必须完整检查两条函数 SQL 结果等价、列数/字段集合、两种 `coalesce` 分支、输出顺序和一次源订阅。热路径依旧流式消费实际结果，按输入行归一化。复用现有正式双 JSON 结果作为先验定位，不直接把不同 JAR 的数字当成严格 A/B；两条查询在同一新 JAR 上以相同 JDK 17、512 MB/G1、GC、warmup/measurement/fork 运行成对正式 JMH，并以短 JFR 核对第二个解析栈的差异。无生产代码、默认限制、缓存、Reactor 操作符或公共 SPI 改动；若分配/吞吐差异不稳定，停止而不推导通用优化。

结果：新增 `wideProjectionOneJsonControl`，原 16 列 SQL 的第二个 JSON 列由预构造的同值 `jsonLevel` 直接投影，其他列和 WHERE 均相同。setup 对两条函数查询各自验证 29,492 条结果、字段集合/类型、首中尾值、全量输出行顺序、`coalesce` 两分支和源恰好一次订阅，并逐行比较两个完整结果 List/Map 等价；热路径不做这些校验。`mvn -q -Pjmh -DskipTests package`、`target/jmh-json-repeat-smoke-20261005.json` 的两入口 setup 与 `git diff --check` 通过；本切片仅改变 JMH 夹具，生产源码仍沿用上一节已通过的 435 tests，未为基准修改生产逻辑或重复执行完整 suite。

正式成对 JMH 的基准源码/JAR SHA-256 为 `328380f4f5f38999bd8925e146feb03cbe278f4d09010129f671e28b21184d01` / `f3ea2c9e64976cb327c4e653a4d779e7a33aeebc3005a2c5bf4abdd3b325dfa4`；JDK 17.0.18、512 MB/G1、单线程、3×1s warmup、5×1s measurement、2 forks、GC profiler，原始 JSON 为 `target/jmh-json-repeat-paired-20261005.json`。双 JSON 为 `1,605,224±18,231` 输入行/s、`2,850.317 B/input-row`；单 JSON 负对照为 `2,005,159±40,516` 输入行/s、`2,066.094 B/input-row`，后者约高 24.9% 吞吐、少 `784.223 B/input-row` 瞬时分配。两个置信区间不重叠，但这只是去掉一次真实 JSON 求值的控制组差异，不等同于缓存或融合在生产中的可达收益，也不是常驻堆下降。

同 JAR 短 JFR 位于 `target/jfr-json-repeat-20261005/`：双 JSON 为 126 CPU / 635 allocation samples，其中 JSONPath/parser 调用栈为 28 CPU / 324 allocation samples；单 JSON 为 122 / 660，其中对应为 21 / 226。样本方向支持 JSON 解析成本归属，不能由样本数推算 B/行。对任意 JSON 函数或自定义 Feature 跨列共享解析结果会引入行级状态，并需证明可变输入、限制校验、异常、Context 和取消语义等价；当前没有这样的低复杂度通用方案，故本切片止于取证，不做 JSON 文本或函数名特调。

## 派生结果隔离与嵌套聚合缓存边界复核（2026-10-05）

本轮只复核现有热点和公开契约，没有生产或基准代码改动。上述真实嵌套聚合 JFR 中，660 条 allocation samples 有 649 条属于 Map/Record 类；其中 `HashMap$Node[]` 的分配栈分别有 33 条经过 `DefaultReactorQLRecord.resultToRecord`、31 条经过 `ensureRecords`、26 条经过 `setResult`。这些计数证明物化是实际热点，但不是可直接删除的冗余对象。`DefaultReactorQLRecordTest` 明确要求 `resultToRecord` 保留来源别名、避免别名碰撞，且原 Record 随后修改结果或来源别名时不影响派生记录；`SubSelectFromFeature` 与 `MergeByKeyFeature` 都使用同一转换。直接共享结果 Map、改成不可变 Map 或仅为某个子查询省略复制都会改变可观察行为。因此本轮否决该复制候选，不引入自定义 Map/Record 或额外 Reactor 操作符。

另一高成本来自未关联 `sum` 标量子查询逐外层行重扫 lookup。`SelectFeature` 仅在 `SubqueryCorrelationAnalyzer.isSubscriptionCacheable` 为真时启用订阅内缓存；当前分析器将所有函数保守判定为不可共享，因为 `current_timestamp` 等内置时变函数与第三方 Feature 均可产生逐次调用可见差异。当前 `Feature`/`ValueAggMapFeature` 没有函数确定性契约，不能仅按 `sum` 名称开白名单或把所有函数视为纯函数；那既会改变第三方覆盖、冷源订阅/Context/取消语义，也不符合“不特调、低复杂度”的约束。下一步只有在找到可从既有执行契约推导的通用纯度/来源独立性证据后才评估缓存 A/B；否则维持逐行执行，转向其他跨场景、可证明等价的热点。此次复核使用的当前生产源码 SHA-256：`DefaultReactorQLRecord.java=10953c3df940595ac9953e638634afcc496a2824a977928efc97ba2bf2cb6acd`、`SubqueryCorrelationAnalyzer.java=a887f8f8af2648ad306ff5b751aa0d72be76abbb3500692ec661f8cf9923003c`，`git diff --check` 通过；不以旧 JFR 样本宣称当前源码的新吞吐收益或 live heap 改善。

## 真实宽行投影原生上界（计划，2026-10-05）

目标：前述 16 列直接投影已有 ReactorQL 正式基线，但没有相同输入、过滤、结果 Map 与消费方式的原生 Reactor/Java 上界，无法判定真实宽行是否仍有通用优化空间。在 `WideSqlWorkloadBenchmark` 新增一个原生 `Flux.handle` 基准：复用同一批 65,536 个预构造行，执行等价 WHERE 并为命中行新建相同 16 列、可变且逐行独立的 `HashMap`，与 SQL `wideProjectionWidthControl` 用同一 `ResultSubscriber`/`Blackhole` 消费。

范围仅为该 JMH 文件及本节。setup 必须用冷源各一次订阅，比较完整原生与 SQL 输出列表的行数、键和值/类型及顺序；热路径不做结果比较或输入构造。原生实现是此固定查询的手写上界，不代表通用 ReactorQL 可直接达到的性能，也不改变生产代码、默认限制、Feature/Record 契约或响应式信号边界。先统一构建和 smoke，再同 JAR、JDK 17、512 MB/G1、3×1s warmup、5×1s measurement、2 forks、`-prof gc` 成对测 SQL/原生，短 JFR 分辨结果 Map 与 Record/解释器成本；若差距主要为必要语义层，停止而不引入专用 SQL 分支。

结果：`nativeWideProjectionControl` 使用与 16 列 SQL 对照相同的 65,536 个预构造输入、WHERE 和逐行独立可变 `HashMap`，setup 完整比较 29,492 条输出的键、值、类型与行顺序，并确认两个冷源各订阅一次。`mvn -q -Pjmh -DskipTests package`、JMH smoke 通过。成对正式 JMH 使用 JDK 17.0.18、512 MB/G1、单线程、3×1s warmup、5×1s measurement、2 forks、GC profiler；原始文件为 `target/jmh-native-wide-paired-20261005.json`，当前基准源码 SHA-256 为 `2f02c235e74ae9cd8a87b65f7a295002f10653b83b35b6d2ff59c51165b6f414`。原生为 `14,142,462±295,560` 输入行/s、`316.811 B/input-row`，SQL 为 `6,722,701±506,926`、`388.818 B/input-row`；原生约 2.10 倍吞吐，SQL 每输入行多约 72 B 瞬时分配。原生是手写固定查询的上界，不是可直接申领的生产优化收益。

同 JAR 短 JFR 在 `target/jfr-native-wide-20261005/`：原生的分配样本主要是结果 `HashMap$Node` 459、桶数组 153；SQL 主要是 `HashMap$Node` 246、`FunctionMapFeature$FixedArgumentList` 135、`DefaultReactorQLRecord` 124、桶数组 48、`HashMap` 43。JFR 样本只定位归属，不能换算字节或常驻堆。直接删除函数参数列表不符合通用 `FunctionMapFeature.scalar` 的可变 `List` 契约（`FunctionMapFeatureCompatibilityTest.testScalarFunctionArgumentsRemainMutable` 覆盖零参和三参的增删改），跨行复用同一可变实例还会破坏并发/重入隔离；派生 Record/Map 复制也有别名隔离契约。因此本切片不改生产执行路径。

## 当前 JAR 真实 SQL 联合复测与热点排序（2026-10-05）

为避免跨 JAR 横比，本阶段在同一 `target/reactor-ql-1.0.21-SNAPSHOT-benchmarks.jar`（SHA-256 `02ddf2de554cb5fe5e15c4a58e67cf1a655c6fdfae29769c2ef8a2a9f608dac1`）上集中复测 16 列投影及两类嵌套查询；宽投影和嵌套夹具源码 SHA-256 分别为 `2f02c235e74ae9cd8a87b65f7a295002f10653b83b35b6d2ff59c51165b6f414`、`c146357068b5404bc1ccbb08c038a1107e7939b238c22648456a111fb9b60899`。正式 JMH：JDK 17.0.18、512 MB/G1、单线程、3×1s warmup、5×1s measurement、2 forks、`-prof gc`；原始结果 `target/jmh-real-sql-current-20261005.json`。所有 setup 仍校验结果与源订阅次数，热路径完整流式消费至 `Blackhole`，不收集输出。

| 真实查询形态 | 输入/外层行吞吐（行/s，99.9% CI 半宽） | 瞬时分配（B/输入或外层行） |
| --- | ---: | ---: |
| 原生 16 列 WHERE + 投影上界 | 14,056,253 ± 1,222,374 | 316.811 |
| ReactorQL 16 列直接投影 | 6,443,513 ± 615,194 | 388.818 |
| ReactorQL 16 列运算/字符串/JSON/日期函数 | 1,549,364 ± 65,278 | 2,850.317 |
| 两层未关联聚合子查询：每外层行扫描 1,024 条 lookup | 8,466 ± 90 | 732,609.373 |
| 关联 lookup 负对照：每外层行扫描 256 条 lookup | 158,500 ± 931 | 28,360.219 |

后两行输入工作量不同，不可凭吞吐直接互相排名；`B/op` 是 GC profiler 的分配量，不是 live heap。短 JFR（1×1s warmup、2×1s measurement、1 fork）在 `target/jfr-real-sql-current-20261005/`，仅用于定位：宽函数查询有 113 CPU / 660 allocation samples，其中 JSONPath/JSON 解析调用栈 26 / 374，`FixedArgumentList` 类 41 个分配样本；两层未关联聚合为 130 / 660，其中 Map/Record 类 658 个分配样本、Map/Record 调用栈 110 个 CPU 样本，CPU 叶子还包括 `resultToRecord` 20、`HashMap.putVal` 16、`putMapEntries` 14；关联 lookup 为 118 / 628，其中 Map/Record 类 611 个分配样本、相关栈 83 个 CPU 样本。这些采样计数不能换算绝对时间、分配字节或常驻内存。

结论与下一步：宽函数查询的首要跨场景成本是重复 JSON 文本解析（上节同 JAR 双/单 JSON 控制组显示去掉一次真实解析可少约 784 B/输入行、吞吐高约 24.9%，但不是通用缓存可达收益），其次是可变函数参数容器与结果物化。多层聚合的主要成本是当前每外层行重扫 lookup 及派生记录复制；`SubqueryCorrelationAnalyzer` 因时变和第三方函数保守禁止订阅内共享，`DefaultReactorQLRecord.resultToRecord` 必须隔离来源别名和可变结果。没有跨函数/SQL 且能证明订阅、Context、取消和可变性等价的低复杂度改法，本阶段仅保留热点证据，不加入特定函数缓存、特定 SQL 分支或自定义 Reactor 操作符。基准使用同步预构造冷源和无界 demand，不能代表异步 I/O、背压/取消或长期 live heap；后续若提出生产候选须另做等价性测试及成对 A/B。

## 跨操作符热点复核（计划，2026-10-05）

目标：在前述同一 JAR 与环境下扩展覆盖面，检查高基数窗口 `count/sum/avg`、Top-N、DISTINCT 和多行 JOIN 的当前吞吐、瞬时分配及 JFR 热点，寻找能跨 SQL 场景复用且保持响应式语义的下一候选。

范围与步骤：复用 `ReactorQLBenchmark` 已有的 `profilingHighCardinalityAggregates`、`profilingHighCardinalityCount`、`profilingOrderByLimit`、`profilingDistinctRows`、`profilingMultiRowInnerJoin`。先核对各自 SQL、输入构造、结果消费和 setup 校验；然后用当前 JAR、JDK 17、512 MB/G1、单线程、2 forks、3×1s warmup、5×1s measurement、GC profiler 集中测量，最后短 JFR 按对象类和 CPU 栈定位。高基数、Top-N、JOIN 使用预构造输入；DISTINCT 使用 `Flux.range().map()`，其生成成本必须归给夹具而非 SQL。按各自输入行归一化，JOIN 的一对多输出不得与其他场景直接横比。

不做：不在诊断阶段修改生产代码、默认限制、Feature/Record 契约或新增操作符；不以 JFR 样本估算 live heap。若热点只有必要的精确状态或公开可变/异步契约成本，记录限制并停止该候选；若出现低复杂度通用候选，先写等价性风险与成对 A/B 门禁，再实施和集中验证。

Top-N 追加判别计划：当前 JFR 若确认 20,000 输入行中每行都物化结果 Map/Record，则在同一 `ReactorQLBenchmark` 增加原生 `profilingNativeTopN`，复用现有 100 项优先队列逻辑、预构造 `profilingOrderBySource()`、相同结果 Map 与 `ProfilingSubscriber`。setup 比较完整 SQL/原生 Top-N 输出；同新 JAR 成对测吞吐和 GC 分配，并用 JFR 核对结果 Map 是否只在最终 100 项生成。该手写固定查询只是性能上界；不将 SELECT 延后到 ORDER BY 前，因为自定义投影 Feature、异常、订阅与别名可能使通用重排不等价。

通用比较热路径候选（计划）：当前 Top-N JFR 的 CPU 叶子有 `CompareUtils.compare` 39/135、`Integer.equals` 21/135；源码在 `Objects.equals(source,target)` 已为 false 后又调用 `source.equals(target)`，对遵守 Java `equals` 契约的非空值重复判断。仅将第一处改为引用相等检查，保留 null、内容相等优先、同类 `Comparable` 和所有跨类型转换的原顺序；不改 Top-N、Filter、JOIN 或聚合的调用点，也不按类型/SQL 增加分支。先以当前同夹具 JMH/JFR 为 before，加入最小语义回归，阶段末统一运行定向及全量测试、同配置 after JMH/JFR；仅在结果等价且吞吐无稳定回退时保留。外部对象若违反 `equals` 一致性规范、让连续两次调用返回不同结果，原行为本就不稳定，不为其加入特殊兼容路径。

排序比较器第二处判别计划：`OrderBySupport.compareOrderValue` 先执行 `Objects.equals(left,right)`，随后对非空不等值进入 `CompareUtils.compare`，仍会重复一次内容相等判断。只将前者改为 `left == right`，null 排序与 `CompareUtils` 的内容相等判断保持原顺序；`OrderBySingleKeyStateTest` 已覆盖 ASC/DESC、NULLS、Top-N/全局排序和自定义异步键。以当前一次 `equals` 候选的 Top-N JMH 为基线，改动后完整测试并做同 JAR 配置的成对 SQL/原生对照；没有明确增益即撤回第二处，不扩大优化范围。

跨操作符结果：正式 before JAR SHA-256 为 `02ddf2de554cb5fe5e15c4a58e67cf1a655c6fdfae29769c2ef8a2a9f608dac1`，原始 `target/jmh-cross-operator-current-20261005.json`；短 JFR 位于 `target/jfr-cross-operator-current-20261005/`。JDK 17.0.18、512 MB/G1、单线程、3×1s warmup、5×1s measurement、2 forks、GC profiler 的输入行归一化结果如下：

| 查询 | 输入行/s（99.9% CI 半宽） | B/输入行 |
| --- | ---: | ---: |
| DISTINCT 20,000 行、1,024 个不同值 | 35,584,411 ± 1,142,751 | 176.350 |
| `_window(50000),key` 50,000 个键，count/sum/avg | 2,841,907 ± 86,528 | 1,189.012 |
| 同窗口/键，仅 count | 3,708,895 ± 98,161 | 957.012 |
| Top-N：20,000 行取 100 | 8,330,980 ± 369,560 | 176.148 |
| 一对多 INNER JOIN：20,000 左行产生 105,000 输出 | 791,678 ± 56,991 | 5,502.046 |

JFR 只定位：高基数多聚合 145 CPU / 614 allocation samples，叶子有 `HashMap.putVal` 23、`LinkedHashMap.linkNodeLast` 19、`GroupState.<init>` 10；仅 count 为 134 / 660，分配类以 `HashMap$Node[]` 212、`HashMap` 128 及分组状态/累加器为主。Top-N 为 135 / 660，CPU 叶子 `CompareUtils.compare` 39、优先队列下沉 22、`Integer.equals` 21，分配类集中在结果 Map/Record/`OrderedRecord`。JOIN 为 144 / 627，其中结果 Map/Record 类 598；其右源按当前冷源契约逐左行订阅，不能视为可无条件构建一次 hash table。DISTINCT 的 100 个 `Integer` 分配样本来自夹具 `Flux.range().map()`，不能归给 SQL。上述输入规模、输出基数和状态生命周期不同，不可横比“哪个操作符更慢”；JFR 样本及 GC B/op 都不证明 live heap。

Top-N 固定查询上界：`ReactorQLBenchmark.profilingNativeTopN` 与 SQL 共用预构造 20,000 行、100 项 Top-N、结果 `HashMap`、`ProfilingSubscriber`；setup 全量比较结果。新增基准仅改变 JMH，原始成对结果 `target/jmh-topn-native-paired-20261005.json` 为原生 `24,793,560±398,289` 输入行/s、`0.664 B/行`，SQL `8,520,159±45,178`、`176.147 B/行`。同 JAR 短 JFR `target/jfr-topn-native-paired-20261005/` 的原生分配样本主要是最终 100 个结果的 Map，而 SQL 对每个输入行物化 Record、结果 Map 和排序包装。原生仅代表该固定 SQL 在投影可延后时的上界；公开自定义 Feature 的求值、异常、别名、订阅与 Context 语义尚不允许通用地把 SELECT 移到 ORDER BY/LIMIT 之后，不引入该重排。

通用比较冗余的生产 A/B：保留 `CompareUtils.compare` 的 `source == target` 和 `OrderBySupport.compareOrderValue` 的 `left == right`，其余内容相等、null 顺序、类型转换和优先队列逻辑均未改。`CompareUtilsTest` 补充同类字符串顺序和 `BigDecimal` 比较相等但 `equals` 不等的回归。首处候选的旧版/候选可逆复测中，Top-N 旧版分别为 `8.520/8.101` M 输入行/s、`176.147/184.148 B/行`，候选分别为 `9.364/9.575` M、均约 `168.147 B/行`；原始 JSON 为 `target/jmh-topn-native-paired-20261005.json`、`target/jmh-topn-equals-once-after-20261005.json`、`target/jmh-topn-equals-twice-b2-20261005.json`、`target/jmh-topn-equals-once-a2-20261005.json`。原生负对照的吞吐在这些轮次也有明显漂移，因此不能将 SQL 差值全归因于该一行优化；候选没有稳定回退，分配量在两次候选轮次一致下降。

第二处候选的 A/B/A（首处保持候选）SQL Top-N 为 `11.402±0.505`、`9.541±0.109`、`11.050±0.490` M 输入行/s，三个轮次均约 `168.146–168.147 B/行`；原始 JSON 依次是 `target/jmh-topn-comparator-identity-after-20261005.json`、`target/jmh-topn-comparator-identity-b-20261005.json`、`target/jmh-topn-comparator-identity-a2-20261005.json`。原生负对照在三轮为 `36.976/40.786/43.177` M 行/s，仍有环境/JIT 漂移；故仅称此 Top-N 夹具观察到重复的吞吐提升，不向全部 SQL 外推固定百分比。第二处候选 JFR `target/jfr-topn-comparator-identity-after-20261005/` 为 131 CPU / 618 allocation samples，`CompareUtils.compare` CPU 叶子 30 条、优先队列下沉 29 条；前述旧 JFR 为 122 / 611，前者同名叶子 44 条。样本数只支持热点方向，不是精确 CPU 时间。最终基准源码 SHA-256 `713377c84a1b86b8f7aa4fb65d82ab80a2466af6f7fa80618f0f06d4fbcebad7`，两处生产源码 SHA-256 分别为 `af7072722d347e47ea12f6de4b8e6e8a0a8cfc1e7cc48f77fb6f2119c802add7`、`432baf1d6e6ee0e606be95595559c2e10884b329eec657a63d0c20c999120b79`，最终 JAR SHA-256 `177126f8cb459969ba7d0d8f7dd490e903faed3132a4de639e65108f3fa7045e`。同源码完整 `mvn -q -Pjmh package` 通过：435 tests，0 failures/errors/skipped；其后仅做可逆旧版/候选 JMH 并恢复到相同生产源码，`mvn -q -Pjmh -DskipTests package` 通过。`target/jmh-cross-operator-equals-once-20261005.json` 的 DISTINCT、高基数两组聚合及 JOIN 与 before 的吞吐置信区间均有重叠，分配量没有稳定上升；第二处只影响 ORDER BY，未另跑不经过排序的场景。两个一行优化都不改变 Reactor 操作符、背压、缓存、默认限制或 live-state 所有权。

## 默认单表原始 WHERE 前置过滤（计划，2026-10-05）

目标：宽投影与 DISTINCT 的 JFR 均显示每输入行 `DefaultReactorQLRecord` 分配；目前普通单表查询即使 WHERE 已编译成 `RawScalarFilter`，仍由 `FromTableFeature` 先把所有原始行包装成 Record 再过滤。对有选择性的常见 Map 行查询，优先过滤被拒绝行有望降低无用 Record 分配，且不改变命中行的结果 Map/Record 契约。

范围：先在 `WideSqlWorkloadBenchmark` 补一条 16 列直接投影的真实设备行控制查询，WHERE 只使用数值区间与布尔字段，和手写同源原生控制组全量比较输出、顺序、字段、类型及冷源订阅次数。统一构建后按既有 JDK 17、512 MB/G1、2 forks、3×1s warmup、5×1s measurement、GC profiler/JFR 得到 before。仅若 `rawWhere` 确认由内置默认单表路径支持，再在 `DefaultReactorQL` 对该结构复用现有 `RawScalarFilter`：Map 行先同步判断，被拒绝不建 Record；非 Map 行或不能直接读原始行时走原 Record 谓词；命中后仍交给现有投影、排序、去重、限制和 wrapper。自定义 From/Property/Filter Feature、row-info、JOIN、分组、checkpoint 均回退原路径。

不做：不新增公开 SPI、不改 SQL 函数/运算语义、默认限制、查询 Context、订阅次数或异步过滤顺序；不针对某个字段/SQL 文字分支，不用跨行可变缓存。测试须覆盖 Map 与非 Map 混合源、选择性过滤、别名、自定义 Feature 回退、错误、背压与取消；`Flux.handle` 最多每输入行发射一条，源仍按下游 demand 工作。仅在同源成对 after 降低分配、吞吐无稳定回退且全量测试通过时保留，JFR 样本不用于推断常驻堆。

### 当前源码真实 SQL 压测与候选筛选（2026-10-05）

`WideSqlWorkloadBenchmark` 新增纯数值/布尔 WHERE、16 列直接投影及等价原生 `Flux.handle` 控制组；setup 全量比较两个路径的结果、行顺序和冷源订阅次数。其余宽函数 SQL 含 16 列、算术/比较、字符串/JSON/日期函数与选择性过滤；`NestedSqlWorkloadBenchmark` 覆盖两层未关联聚合和逐行关联 lookup，并在 setup 核对值、结果结构及 lookup 订阅次数。所有热路径使用预构造输入、流式 Blackhole 消费，按输入行归一化。只改了 JMH 夹具，本轮未改生产代码。JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler；JAR SHA-256 `1c76185b788531c9e6fcf2631ea782bb153e7360e1cf49f6f65cb457725f6a25`，原始数据 `target/jmh-real-sql-current-20261005.json`。

| 查询/控制组 | 输入行/s（均值 ± JMH 误差） | 分配 B/输入行 |
| --- | ---: | ---: |
| 16 列直接投影，同 WHERE | 6,551,597 ± 277,878 | 388.818 |
| 手写原生直接投影，同 WHERE | 14,256,609 ± 644,592 | 316.811 |
| 16 列函数投影，两次 JSON 读取 | 1,579,082 ± 14,611 | 2,844.917 |
| 同结果函数投影，一次 JSON 读取 | 1,919,109 ± 40,621 | 2,066.094 |
| 纯数值/布尔 WHERE、16 列直接投影 | 6,399,478 ± 557,690 | 428.009 |
| 手写原生纯数值/布尔 WHERE、同投影 | 13,205,401 ± 928,017 | 396.003 |
| 两层未关联聚合，1,024 外层/1,024 lookup | 8,410 ± 376 | 732,609.389 |
| 关联 lookup，4,096 外层/256 lookup | 162,779 ± 4,532 | 28,360.218 |

两条子查询的 lookup 行数、聚合工作量和结果形态不同，不以两行吞吐直接排名。前者每次执行订阅 lookup 1,024 次，后者 4,096 次，均由 setup 验证。同 JAR 短 JFR（1×1s 预热、2×1s 测量、1 fork）在 `target/jfr-real-sql-current-new-20261005/`：16 列函数投影的 121 个 CPU/660 个 allocation samples 中，JSON/JSONPath 调用栈分别覆盖 48/351，日期栈为 10/70；两层聚合为 113/660，其中 Map/Record 栈覆盖 95/637，`DefaultReactorQLRecord.resultToRecord` 是 16 个 CPU 叶子；关联 lookup 为 121/660，其中 Map/Record 栈覆盖 92/550。纯数值 WHERE 为 140/660，`BinaryFilterFeature.test` 是 30 个 CPU 叶子，Record 类有 103 个 allocation samples。采样栈类别可能重叠，JFR 只作热点定位，不能换算字节、CPU 占比或 live heap。

候选筛选：同源纯 WHERE 的 SQL/原生分配差为约 32 B/输入行，且结果 Map 本身是两者共同的主要分配；在验证前不能把这 32 B 全归于可省的被拒绝行 Record。JSON 第二次解析的同结果控制组少约 779 B/输入行、吞吐高约 21.5%，但对任意函数/自定义 Feature 引入跨列缓存会改变可变参数、调用次数、异常和 Context 契约，不按 `json_get` 或 SQL 文本特调。两层聚合的重复订阅受目前保守的函数确定性规则约束；不凭 `sum` 名称缓存或重排。故本轮保留取证，不据此直接加入前置 WHERE、函数缓存、派生 Record 共享或自定义 Reactor 操作符；前置 WHERE 仍是待独立语义测试和同源 A/B 的候选，不宣称已优化。阶段性 `mvn -q -Pjmh package` 通过（435 tests，0 failures/errors/skipped），`git diff --check` 通过；构建后重新生成的同源码 JAR SHA-256 为 `121557a9e710e5d9fe7e6206bc9c0895d46e058572e616dc66c964d0c43f7757`，正式 JMH/JFR 仍归属上文压测时的 `1c76185…` JAR。

### 默认单表原始 WHERE 前置过滤实现与复测（2026-10-05）

`DefaultReactorQL.prepare` 仅在内置 `FromTableFeature`、单表、无 JOIN/分组/row-info/checkpoint，且已编译的 `RawScalarFilter` 声明接受当前表别名时，使用 `applyRawWhereBeforeRecord`。Map 行先同步判断，未命中时不构造 Record；命中行才构造 Record，并在同一个 `handle` 内完成同步投影。非 Map（含已有 Record）仍先按原路径构造/转换 Record，再用原 `scalarWhere` 判断；异步过滤、自定义 From、非 raw-capable 谓词仍使用原管线。没有新增 SQL 文本分支、公开 SPI、逐行共享状态或额外 Reactor 操作符。`RawWhereBeforeRecordTest` 覆盖 Map/非 Map/已有 Record 输入、别名、输出顺序、错误、背压/取消、Context 与自定义 From 回退；现有自定义 Property、异步 Feature、checkpoint 和窗口回归一并保持。

同一 JMH 夹具/JDK 17.0.18/512 MB G1/单线程/3×1s 预热/5×1s 测量/2 forks/GC profiler 的候选 JAR SHA-256 为 `971a8abb1f1fa667c5249ba71f34cd2624c9cd4995f2b34b6455b02d57bd08a3`，原始数据 `target/jmh-raw-where-before-record-after-20261005.json`；before 为上文 `target/jmh-real-sql-current-20261005.json`（JAR `1c76185…`）。纯数值/布尔 WHERE 16 列投影从 `6,399,478±557,690` 到 `6,806,769±511,316` 输入行/s，误差区间重叠，不申领稳定吞吐提升；GC 分配从 `428.009` 降到 `414.008 B/输入行`，少 `14.001 B`（约 3.3%）。相同原生控制组为 `13,205,401→13,149,201` 行/s、均约 `396.003 B/行`；宽度 SQL 控制组为 `6,551,597→6,544,851` 行/s、均约 `388.818 B/行`；宽函数 SQL 为 `1,579,082→1,588,732` 行/s、`2,844.917→2,850.317 B/行`；关联子查询为 `162,779→159,699` 行/s、均约 `28,360.218 B/行`。控制组的吞吐波动与候选误差均不支持向全部 SQL 外推固定提升或退化。短 JFR 原始文件 `target/jfr-raw-where-before-record-after-20261005/`：同查询 `DefaultReactorQLRecord` 分配类样本从 before `103/660` 降到 after `7/655`，只用于确认少建 Record 的方向，不换算 B/行或存活堆。

阶段性完整 `mvn -q -Pjmh package` 通过（439 tests，0 failures/errors/skipped），`git diff --check` 通过；生产源码 `DefaultReactorQL.java` SHA-256 `6f2db59efca606f2d7e74fd5250c12358d2a9edb1198187e012a32709a8b8871`，新增测试 SHA-256 `ae0a3e27c8d43ce3d9627eab82d2e61bc2edb32aaf5216db31bf70d936bccfd0`。完整构建重新生成的同源码 JAR SHA-256 为 `5f0039c001d1830b4d51545603cb98244d861774e2a8f8145528c24ae28d8f1b`。此优化降低瞬时分配，不证明高基数分组的常驻状态或长期 live heap 已下降；整体接近原生性能的目标仍未完成。

## 高基数增量聚合的原生边界（计划，2026-10-05）

目标：现有窗口化高基数 JFR 只说明通用 `GroupState`/Map/累加器是热点，尚无同样维护精确逐键状态、相同结果物化和响应式订阅边界的原生上界。新增独立 JMH 夹具，以 50,000 条预构造设备 Map 行、`_window(50000),key` 和 `count/sum/avg/max` 查询，对比 ReactorQL 与手写 Java 状态加 Reactor `collect` 的原生控制组；分别覆盖每键 1 条与 2 条输入。两路径均在窗口关闭后输出每键一行，setup 逐键校验所有值/类型/字段、结果数及冷源单次订阅；热路径只流式消费结果到 Blackhole，按输入行归一化。

范围：只新增 JMH 夹具并回填本节；使用同 JAR、JDK 17、512 MB/G1、单线程、2 forks、3×1s warmup、5×1s measurement、GC profiler 和短 JFR。先以原生控制组确定精确逐键状态及输出 Map 的合理下界，再判定剩余成本是否可由已有通用结构低复杂度地减少。不得把原生固定查询的字段/累加器组合搬入生产代码，也不改变默认分组限制、分组生命周期、背压/取消或公开 Feature 契约。GC B/输入行不代表存活堆，50,000 键的精确结果本身必然占用 O(键数) 状态；若差距来自必要的通用可扩展性，记录边界并停止该候选。

同 JAR 正式结果：新增 `HighCardinalityNativeBenchmark.java`，JAR SHA-256 `4c7914ebc4d2c7b7a69d0b16d67b6871cc1612165ec527edfe304cbccb7f097c`、夹具源码 SHA-256 `3c27fd6df60511dc3e2ceeba898f1904a1890dc8dcee04fbf4331a8307e822b3`，原始数据 `target/jmh-highcard-native-current-20261005.json`。setup 在 50,000 行、每键 1/2 次两种形态下逐键校验 `key/count/sum/avg/max` 的字段、类型和值及单次源订阅；smoke `target/jmh-highcard-native-smoke-20261005.json` 通过。正式环境为 JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler。

| 每键输入数 | SQL 输入行/s | SQL B/输入行 | 原生输入行/s | 原生 B/输入行 |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 2,841,239 ± 67,274 | 1,245 | 8,704,562 ± 661,728 | 413 |
| 2 | 4,084,430 ± 117,187 | 755 | 19,657,516 ± 602,524 | 206 |

短 JFR 在 `target/jfr-highcard-native-current-20261005/`：每键 1 次的 SQL 为 120 CPU/620 allocation samples，CPU 叶子以 `HashMap.putVal` 23、`GroupState.<init>` 10、`HashMap.resize` 12 为主；分配类包括 `Object[]` 66、累加器数组 32、`LinkedList` 32，以及 Map/Node。每键 2 次的 SQL 为 128/618，另有 `DefaultReactorQLRecord` 53、`LinkedList$Node` 24。原生对应 129/625 与 135/645，主要为精确逐键 `LinkedHashMap` 状态、最终结果 Map/Double 和其节点。样本不可换算字节或稳定 CPU 百分比，但与 GC B/行共同表明逐输入行 Record/分组键列表及逐键通用状态都有实际成本。原生是该固定四聚合的上界，不能直接替换可扩展聚合。

高基数原始分组候选（已实施并验证）：沿 `WindowedAggregateStage` 已有 `RawAccumulatorFactory` 扩展默认单表的原始行分组输入，**仅当**无时间窗口、无源记录保留、只有一个可直接读取原始行的分组维度、全部累加器支持同一来源且 WHERE 同步可原始判断时启用。单维门禁来自现有 `GroupFeature.writeGroupKey` 逐维可见性契约：第二个维度可能读取前一维刚写入的 `_group_by_key`，不能把多维统一提前到原始 Map 上求值。Map 行在现有 `SubscriptionState` 内直接计算键并更新原始累加器；非 Map/已有 Record 仍走原路径。保留每键精确状态、窗口计数/关闭、组键输出、取消清理、Context、默认限制和下游 demand；不增加跨行缓存、公开 SPI 或 SQL 名称分支。阶段门禁：测试覆盖窗口前/后分组键、每键多行、混合源、错误、取消、背压、自定义 Group/From/Property 回退及输出组键隔离；只有同源 A/B 分配下降且吞吐无稳定回退才保留。

### 当前源码真实 SQL 集中复测及热点（2026-10-05）

本轮复用已校验输出字段、样本值与类型、全量行顺序和冷源订阅次数的三类 JMH 夹具：`WideSqlWorkloadBenchmark` 的 65,536 行、16 列设备事件宽投影（含算术、比较、字符串、两次 JSON 读取、日期、`coalesce`）；`NestedSqlWorkloadBenchmark` 的两层未关联聚合与逐行关联 lookup；`HighCardinalityNativeBenchmark` 的 50,000 行精确窗口 `count/sum/avg/max` 及相同结果的原生状态控制组。嵌套查询及聚合另逐行/逐键核对结果；输入均预构造，热路径流式消费真实结果，未把输入创建或 `collectList` 计入 JMH 测量。聚合按输入行、子查询按外层行归一化；不同查询的工作量不能直接横比。

定向 `RawKeyedAggregateTest,WindowedAggregateStageTest,CollectListIncrementalTest` 通过；完整 `mvn -q -Pjmh package` 第二次通过，Surefire 报告合计 443 tests、0 failures/errors/skipped，`git diff --check` 通过。首次完整构建仅 `ReactorQLTest.testGroupByTimeHaving` 失败：该旧测试使用真实 `delayElements(500ms)` 与 `interval('1s')` 却固定断言 4 个窗口，隔离复跑通过；本轮未为它调整生产行为或放宽断言，时间边界波动仍需关注。当前生产源码 SHA-256 为 `WindowedAggregateStage.java` `ec32547a1c3598cf985ae63b9c24666e75c918ee445a8d12a4bb1c6a3c96ef9b`、`DefaultReactorQL.java` `f33dc010690df2140822258906326ea27c19506d0641d4da3d46c08f872d1967`；回归测试 `RawKeyedAggregateTest.java` 为 `fd3f6461cccee478db4ab579769e0df1d2c3fb576412f67d5dd4aa310d95d400`。正式 JAR SHA-256 `3001c0aeb1df32765bfe7563e1bb5201e18589c4030bdfbeb5297a3cf310a63f`。

同一 JAR 的正式 JMH：JDK 17.0.18、512 MB/G1、单线程、2 forks、3×1s warmup、5×1s measurement、`-prof gc`；原始 JSON 为 `target/jmh-realistic-current-20261005.json`。

| 查询 | 输入/外层行/s（均值 ± JMH 误差） | 分配 B/输入或外层行 |
| --- | ---: | ---: |
| 16 列直接投影 | 6,285,105 ± 117,840 | 388.818 |
| 16 列函数投影 | 1,583,432 ± 13,699 | 2,844.917 |
| 两层未关联聚合，1,024 外层/1,024 lookup | 8,612 ± 134 | 732,609.373 |
| 逐行关联 lookup，4,096 外层/256 lookup | 164,214 ± 7,677 | 28,360.217 |
| 高基数聚合，每键 1 条 | 3,120,091 ± 198,859 | 957.011 |
| 原生控制，每键 1 条 | 8,410,231 ± 1,153,414 | 412.944 |
| 高基数聚合，每键 2 条 | 4,857,276 ± 133,537 | 478.524 |
| 原生控制，每键 2 条 | 17,696,188 ± 5,019,427 | 206.478 |

与上文 `4c7914…` JAR 的同夹具高基数基线相比，当前原始分组候选的 SQL 分配分别从约 `1,245→957`、`755→479 B/输入行`（约少 23%/37%），吞吐从约 `2.84→3.12`、`4.08→4.86 M 输入行/s`；原生控制的吞吐误差区间与原基线重叠，分配量基本不变。宽投影及两类子查询与此前同夹具数据处于同一量级，未观察到稳定回退。因此保留这一通用候选，但不把固定四聚合的原生上界搬入生产；其仍需保留通用聚合器、分组状态和窗口语义。GC B/行是瞬时分配，不证明常驻堆降低。

同 JAR 短 JFR 位于 `target/jfr-realistic-current-20261005/`，只作热点定位：宽函数投影的 125 CPU / 642 allocation samples 中，分配类靠前的是 `byte[]` 97、`String` 62、`FixedArgumentList` 42 及 Map/节点，CPU 叶子包含 `CastUtils.castNumber`、字符串包含判断、函数求值与 `HashMap.putVal`；两层未关联聚合为 126/660，分配类中 `HashMap$Node` 268、`HashMap` 132、`DefaultReactorQLRecord` 80，CPU 叶子有 `HashMap.putVal` 19、`resize` 13、`resultToRecord` 11；关联 lookup 为 116/623，`HashMap$Node` 273、`HashMap` 178、`DefaultReactorQLRecord` 82，CPU 叶子包括属性/比较过滤与 Map 查找。未关联聚合每次执行仍订阅 lookup 1,024 次，故它的高 B/外层行主要体现重复子查询执行及结果物化，而非一次查询的常驻状态。高基数 SQL JFR 为 128/660 与 110/660，但录制中出现只应在夹具 setup 校验中执行的 `NativeState.result` 样本，不能据此计算测量热路径的 CPU 占比；正式 JMH 分配数据仍独立有效。JFR 样本数不可换算为字节、稳定 CPU 百分比或 live heap。

下一步可验证的通用方向：为表达式显式建模可缓存的确定性与副作用/Context 契约，再考虑两层未关联聚合的一次求值或宽投影的行内共享解析；在契约未证明前继续逐行执行，不按 `sum`、`json_get` 或 SQL 文本特调。精确高基数分组仍有逐键 Map/累加器状态及输出 Record 的必要成本，后续若优化必须同时量化 retained heap、取消后的释放与默认状态限制，不能只凭分配率判断。

## 纯函数子查询复用候选（计划，2026-10-05）

目标：上文三层未关联聚合每 1,024 个外层行重复订阅 lookup 1,024 次，主要由 `SubqueryCorrelationAnalyzer` 遇任何函数即禁用缓存引起。沿已有订阅级 `SubqueryMapper`/`SubscriptionContext` 缓存能力，只让显式声明“相同局部输入在本次订阅内可复用、无时间/外层行/Context 依赖或额外副作用”的 Feature 参与静态分析；未声明及自定义替换 Feature 继续逐行执行。先覆盖具有该契约的内置数值聚合，再递归检查其参数表达式，拒绝参数、外层列和未声明函数；不按函数名或 SQL 文本硬编码，也不建立跨订阅缓存。

范围：`Feature` 的保守默认能力、内置聚合 Feature 的显式声明、`SubqueryCorrelationAnalyzer` 与 `SelectFeature` 的调用，以及订阅次数/结果/取消/错误/Context 回归。复用原缓存的默认无限制行为和显式 `subquery.maxRows` 硬上限、下游 demand 及取消后重启；不新增常驻全局缓存、线程切换、自定义 Subscriber 或额外运维状态。以同一三层查询 JMH 的 before/after 和关联/宽投影负对照判定收益，阶段末集中验证；若语义不稳或收益不足，撤回本切片生产改动。JFR 样本只作定位，GC B/行不代表 live heap。

实施与验收：`Feature.isSubscriptionCacheSafe()` 默认为 `false`，只有当前内置 `count/sum/avg/max/min` 数值聚合实例显式声明安全；`SubqueryCorrelationAnalyzer` 在实际 metadata 中核对 Feature（含局部替换），再递归检查函数参数，未知函数、参数、外层列及未声明 Feature 仍回退逐行执行。旧分析入口保持全部函数不可缓存；实际 `SelectFeature` 才传入 metadata。缓存仍由已有 `SubscriptionContext.cacheMany` 按根订阅拥有，未新增全局状态或新的 Reactor 操作符。`SubqueryCacheTest` 验证三层聚合在每次根订阅只读取 lookup 一次、跨订阅不共享，且自定义未声明函数与替换 `count` Feature 仍逐外层行执行；分析器测试覆盖五种内置聚合、嵌套算术中的未知函数/外层列和旧入口。最终完整 `mvn -q -Pjmh package` 通过（446 tests，0 failures/errors/skipped），`git diff --check` 通过。最终 JAR SHA-256 `7a58fecedf2a4719717877e0ae06ff245d3bd3bd5ff6401c69cdb425bac0aa7a`；关键源码 SHA-256：`Feature.java` `769e6b488ff8c11017e2dad3c7b6341d9e0f7124fc302984213b77e6ea7a31d6`、`SubqueryCorrelationAnalyzer.java` `8622b10a91a816af491a9a8b0e7823d16aaf2f95d4a971a791eacf3329b9d7e0`、`SelectFeature.java` `464536510120cb4eb38044a6eaad3abb175d816e91ea311f0c5db13ed1b6b29c`、`NestedSqlWorkloadBenchmark.java` `c2d02fa67c2a405c0fdbc23fd1f1dc411f7c0c32ce4ad95f9376527afd4754c6`。正式 JMH 仍归属上述 `5b369e…` 的同生产/夹具源码 JAR；恢复生产与夹具源码后的 `2fb8a…` JAR 已通过短 smoke，随后只修改 SPI 注释与测试断言并完成上述最终完整构建。

正式同 JAR JMH（SHA-256 `5b369e20f831e996ad85ad9703d9a7e121443b8fb1c77529e3d65599d4c48548`）：JDK 17.0.18、512 MB/G1、单线程、2 forks、3×1s warmup、5×1s measurement、`-prof gc`；原始 JSON `target/jmh-subquery-pure-aggregate-after-20261005.json`。夹具对同一 SQL/数据分别设置默认缓存与 `subquery.cache=false`，setup 核对每次执行 lookup 订阅 1 次与 1,024 次，并逐行核对输出值/类型/字段与顺序；热路径仍流式消费结果。

| 查询 | 外层行/s（均值 ± JMH 误差） | 分配 B/外层行 |
| --- | ---: | ---: |
| 三层未关联 `sum`，默认缓存 | 4,180,992 ± 130,478 | 1,276.884 |
| 同 SQL，显式关闭缓存 | 8,415 ± 73 | 732,609.373 |
| 关联 lookup 负对照 | 149,009 ± 3,030 | 28,360.221 |
| 16 列函数投影负对照 | 1,603,791 ± 13,341 | 2,844.917 |

此约 497 倍的吞吐差和约 99.8% 的每外层行分配下降仅适用于夹具中可证明不关联、可共享的聚合子查询：它去掉每次 1,024 条外层输入上的重复 1,024 行 lookup 扫描，不代表所有 SQL 或原生计算路径获得同样提升。关闭缓存的同 JAR 控制组与此前未缓存基线同量级；宽函数路径分配不变。关联查询在此次正式轮次较旧轮次略低，但旧夹具 setup 不同；同夹具复测为 `151,356±1,028` 行/s、`28,360.220 B/行`（`target/jmh-correlated-control-repeat-20261005.json`）。可逆诊断把 `SelectFeature` 暂时改回旧资格入口并相应调整夹具订阅断言后，关联查询为 `152,206±4,153` 行/s、`28,360.220 B/行`（`target/jmh-correlated-control-cache-disabled-20261005.json`），与当前版本误差区间重叠。先前两次仅改入口、却未同步调整 JMH setup 断言的诊断启动没有产出分数，不纳入比较。随后已恢复正式入口和夹具期望，重新打包并在恢复的 JAR 上完成缓存查询短 smoke（`target/jmh-subquery-restored-smoke-20261005.json`）。

保留此通用优化。它不在堆中保存全部源行，只由已有缓存持有子查询输出（本查询为一行）；根订阅结束后状态可释放。`gc.alloc.rate.norm` 仍是瞬时分配指标，不证明 live heap 下降；默认 `subquery.maxRows` 行为保持不变。下一阶段若扩展至其它函数，应继续逐个证明 Feature 的输入/副作用契约并关注缓存结果基数，不得凭函数名称或测试 SQL 形状放宽分析器。

### 最终 JAR 的真实 SQL 复测与 JFR 热点（2026-10-05）

目标与范围：只复测既有 `WideSqlWorkloadBenchmark`、`NestedSqlWorkloadBenchmark`，不改生产代码或默认限制。宽场景每次输入 65,536 条预构造设备事件，SELECT 16 列并组合算术/比较、字符串、两次 JSONPath、日期与 `coalesce`；另有同 WHERE 的直接投影、少一次 JSON 读取但输出相同的控制组及同结果原生 Reactor 对照。嵌套场景包含三层 SELECT 的未关联 `sum`（lookup 1,024 行、外层 1,024 行）、相同 SQL 显式关闭订阅内缓存，以及逐行关联 lookup 负对照。夹具 setup 核对字段、值、类型、输出顺序和冷源订阅次数；输入创建与 oracle 不计入热路径，结果逐行交给 Blackhole。输入数据是模拟设备事件而非线上回放，基准不含上游 I/O、调度和真实字段分布；各分数按输入行或外层行归一化，不能把不同查询的工作量直接横比。

正式 JMH：最终 JAR SHA-256 `7a58fecedf2a4719717877e0ae06ff245d3bd3bd5ff6401c69cdb425bac0aa7a`；JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler。原始结果为 `target/jmh-realworld-final-20261005.json` 与 `target/jmh-realworld-native-final-20261005.json`。

| 场景 | 输入/外层行每秒（均值 ± JMH 误差） | 分配 B/输入或外层行 |
| --- | ---: | ---: |
| 宽 16 列，运算符/函数/两次 JSON 读取 | 1,585,086 ± 9,701 | 2,844.917 |
| 同结果宽函数查询，一次 JSON 读取 | 1,935,872 ± 28,481 | 2,060.694 |
| 同 WHERE 的 16 列直接投影 | 6,359,092 ± 311,577 | 388.818 |
| 同结果原生 Reactor 直接投影 | 14,165,867 ± 417,787 | 316.811 |
| 三层未关联聚合，默认缓存 | 4,280,233 ± 59,441 | 1,276.884 |
| 同 SQL，显式关闭缓存 | 8,331 ± 29 | 732,609.373 |
| 逐行关联 lookup 负对照 | 163,056 ± 3,189 | 28,360.217 |

短 JFR：同一 JAR，1×1s 预热、2×1s 测量、1 fork；原始录制在 `target/jfr-realworld-final-20261005/`，诊断分数在 `target/jmh-realworld-jfr-final-20261005.json`。六条录制的 CPU 样本均来自 JMH worker，按完整栈检查均未包含夹具 `setup`；因此以下定位不把 oracle 的 `collectList` 或输入构造归入热路径。宽双 JSON 场景 121 个 CPU / 622 个分配样本中，JSON 解析/规范化相关栈覆盖 37 / 328 个，日期转换 14 / 95 个；分配类包括 `HashMap$Node[]` 94、`byte[]` 83、`String` 51 和 `FixedArgumentList` 31。一次 JSON 控制组仍有 JSON 栈，但正式 GC 分配比双 JSON 少约 784 B/输入行、吞吐高约 22%；其 `jsonLevel` 已预置在输入，因此这只是隔离第二次解析成本，不是零成本生产改进。直接投影的 129 CPU / 660 分配样本中，结果 Record/Map 写入栈覆盖 34 / 353 个，函数参数装配 `FixedArgumentList` 分配类为 169 个；原生对照比 SQL 高约 2.23 倍吞吐，SQL 多约 72 B/输入行瞬时分配，此差距不是可直接领取的优化收益。

三层聚合默认缓存录制 105 CPU / 616 分配样本，`resultToRecord`、`setResult`、`newContainer` 等 Record/Map 栈覆盖 42 / 419 个；关闭缓存时为 134 / 660，覆盖 89 / 628 个，反映重复派生行物化。逐行关联为 112 / 618，Record/Map 栈覆盖 37 / 528 个，CPU 叶子还落在属性读取、`BinaryFilterFeature.test` 与 Map 查找。关联扫描和派生 Record 成本不能简单用订阅内缓存消除，因为结果依赖外层行。JFR 栈类别可能重叠、分配采样数有采样偏差，不换算 CPU 百分比、字节或 live heap；精确分配量只取正式 GC profiler。

候选顺序：先针对 JSON 文本的重复解析/规范化做跨函数、同一行输入契约调查，必须保留动态参数、可变文档、自定义 Feature、错误/Context/取消语义；未证明前不做按函数名或 SQL 文本的缓存。其次调查 `resultToRecord` 派生 Map 的复制/容量与别名可见性，只有在跨宽投影/子查询的等价测试及同源 A/B 中有收益才改动。`FixedArgumentList` 已是通用短参数容器，不能仅凭样本数再叠加缓存或自定义操作符；关联查询的逐行扫描应优先在数据源索引/查询下推边界解决，不能在无界流里无条件物化 lookup。此轮只完成压测与热点定位，没有申领新的生产优化或常驻堆收益。`git diff --check` 通过。

### JSON 输入形态的有界判别（计划，2026-10-05）

目标：区分重复 JSON 文本解析与后续 JSONPath/结果物化的成本，判断是否值得为通用的行内复用契约承担复杂度。范围仅在 `WideSqlWorkloadBenchmark` 增加同 SQL、同数据内容的预解析 Map 输入对照；原始字符串输入和现有结果 oracle 保持不变，不修改生产 Feature、SQL 编译器、缓存、默认限制或响应式链。setup 对两种输入完整比较输出字段、值、类型、顺序与一次冷源订阅；热路径均消费相同结果数。阶段末一次构建和同 JAR 成对 JMH/短 JFR；解析输入的准备成本排除在测量外，因此只代表上游本来提供结构化文档时的边界收益，不能算作零成本的自动优化。若 Map 规范化仍占主要成本或收益不明显，停止该候选；即使收益明显，也先证明可变文档与自定义 Feature 契约，不能直接加入跨列缓存。

结果：`WideSqlWorkloadBenchmark.wideProjectionWithParsedJsonInput` 只在 setup 中用现有 Jayway `JsonProvider` 把同一批 JSON 文本预解析为 Map；正式测量仍执行完全相同的 16 列 SQL。setup 全量比较两种输入的每行 Map 值、字段、类型、顺序，并各验证冷源只订阅一次。最终 JAR SHA-256 `7635d8ddcbc937be2c0bca2898360f810f7d14dc914dde98487b9e30a8890ea5`；`mvn -q -Pjmh package` 通过，Surefire 共 446 tests、0 failures/errors/skipped。JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler 的正式 JSON 为 `target/jmh-json-input-shape-final-20261005.json`：字符串输入 `1,478,631±17,288` 输入行/s、`2,844.917 B/行`；预解析 Map 输入 `1,346,022±15,754` 输入行/s、`2,406.904 B/行`。前一次同生产/热路径但 oracle 类型检查尚未补齐的 JAR 复测也同向（`1,485,623→1,371,723` 行/s，`2,844.917→2,406.904 B/行`），只作复现而不替代最终数据。

同一最终 JAR 短 JFR 在 `target/jfr-json-input-shape-final-20261005/`，诊断 JSON 为 `target/jmh-json-input-shape-jfr-final-20261005.json`。两条录制分别为 123/660 与 125/639 个 CPU/分配样本，均无 setup 栈；字符串输入的 `JsonValueSupport.normalizeText` 覆盖 19/209 个，Map 输入的 `normalizeMap` 覆盖 11/130 个。Map 形式消除了本查询热路径的文本解析，却仍需递归规范化并复制结构；短样本不足以把吞吐回退精确归因于该复制，也不能换算为 live heap。预解析成本已排除仍出现约 9% 吞吐回退，因此不把“上游先解析”当作通用吞吐优化，不增加行内或跨列缓存。该实验仅保留为输入形态负对照；下一步应优先寻找不改变可变输入、扩展 Feature 和订阅语义的其他跨 SQL 热点。`git diff --check` 通过。

## 单维高基数分组状态收敛（计划，2026-10-05）

目标：降低精确分组必须存在的 O(键数) 常驻状态中可避免的每键 `Object[]`，不牺牲吞吐。owning module 为 `WindowedAggregateStage` 的内部 `GroupState`，仅当分组维度数为 1 时直接保存该键值；0 维不保存数组，2+ 维继续复制维度数组。当前每个组均 `Arrays.copyOf(dimensionValues, dimensionValues.length)`，高基数 JFR 已观测到数组分配。先在现有 50,000 输入行、每键 1/2 条的 SQL/原生 JMH 上取同 JAR 基线，再替换内部表示；保持分组键可能本身是数组、别名/上游 `_group_by_key`、窗口关闭与取消释放、默认状态限制、精确输出字段/类型/顺序及自定义 Group/Accumulator 回退。阶段末集中跑聚合回归、完整测试、同配置 A/B 和差异检查；若分配未降或吞吐稳定回退，撤回候选。不做聚合函数名称或 SQL 文本特调，不增加公共 SPI、缓存或 Reactor 操作符。GC B/行只表示瞬时分配；常驻堆减少只能按每键少持有一份数组的状态拓扑推断，若需精确 live heap 字节仍要独立测量。

实施与功能验证：`SubscriptionState.snapshotGroupValues` 对单维直接返回该维的键对象，对零维返回 `null`，对多维继续复制；`GroupState` 按查询已知维度数读取该对象，不根据键对象类型判断，因此数组类型的业务键仍是一维值。输出 `_group_by_key` 继续创建独立可变 List，保留别名、上游组键与取消释放边界。`RawKeyedAggregateTest.shouldKeepArrayValuedSingleDimensionAsOneKey` 新增数组键身份/输出/组键断言；定向 `RawKeyedAggregateTest,WindowedAggregateStageTest,AggregationResourceLimitTest` 和完整 `mvn -q -Pjmh package` 均通过，完整 Surefire 为 447 tests、0 failures/errors/skipped。最终 JAR SHA-256 `d4f406cfa6f1d21efede2d779a74751cdedf510d3219c4699b7a3e2b00346983`。

正式 A/B：相同 `HighCardinalityNativeBenchmark`、JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler。before 为 `target/jmh-singlekey-state-before-20261005.json`（JAR `7635d8dd…`），after 为 `target/jmh-singlekey-state-after-20261005.json`（上文最终 JAR）；两者 setup 均逐键比较 SQL/原生结果值、字段、类型及一次源订阅，热路径按 50,000 输入行归一化。

| 每键输入 | SQL before 行/s | SQL after 行/s | SQL before→after B/行 | 原生 before→after 行/s / B/行 |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 3,168,371 ± 113,642 | 3,315,007 ± 59,977 | 957.011 → 909.011（少 48.000） | 9,182,977 → 9,176,564 / 412.944 → 412.944 |
| 2 | 4,854,082 ± 188,716 | 4,785,097 ± 279,793 | 478.525 → 454.525（少 24.000） | 19,552,507 → 19,252,109 / 206.478 → 206.478 |

SQL 两形态的分配均按每键约少 48 B，与每键少一份常驻单元素数组、输出少一个瞬时列表包装对象的方向一致；原生分配不变。吞吐误差区间在两形态均有重叠，只能确认未出现稳定回退，不能申领稳定提升。after 短 JFR 在 `target/jfr-singlekey-state-after-20261005/`：每键 1/2 条分别为 132/660、131/657 个 CPU/分配样本，均无 setup 栈，`snapshotGroupValues` 下没有采到数组分配；该短样本只辅助定位，不换算字节或常驻堆。保留此通用内部状态收敛；精确 live heap 仍未量测，按字段持有关系可确认每活跃单维组不再保留一份键数组。`git diff --check` 通过。

### 活跃分组 live heap 与取消释放探针（计划）

目标：补上前述高基数状态的直接驻留证据，并验证取消后可释放，不用 GC 分配率代替 live heap。只在 JMH 诊断源码增加独立入口，预构造 50,000 个键与相同四聚合 SQL，用未完成的第 50,001 行计数窗口保持 50,000 个组活跃；同一 JVM 分别在订阅前、活跃、取消后由外部 `jcmd GC.class_histogram` 取样。探针只用标准输入协调采样点，阻塞发生在诊断 main 线程而非 Reactor 链内；不改变生产代码、默认限制、算子或数据源。先核对每行被消费、没有提前输出、取消到达源，再比较 `GroupState` 与相关数组/累加器类的实例数和字节。若订阅受背压未消费完、探针自身保留状态或直方图无法区分阶段，判为无效而不宣称 live heap 结果；若有效，只报告此固定 5 万键场景，不能外推到任意键分布或并发数。

结果：`GroupHeapProbe` 在 JMH 源目录中以同一四聚合 SQL、`_window(50001),key` 持有 50,000 个精确单维组；探针显示 `consumed=50000, emitted=0`，取消信号到达源。`mvn -q -Pjmh -DskipTests package` 通过；生产 `WindowedAggregateStage.java` SHA-256 仍为 `28b32df07e0f3185d309cda80e14e6a07b427e9bd0bfceea954ef3e14d8359cb`，新诊断 JAR SHA-256 `bd4056b65f2cb42722a336960d3fdca9842e475572403767badf07bafed1cb9a`，上一阶段 447 项完整测试继续覆盖相同生产源码。第一次沙箱内探针与外部 `jcmd` 处于不同许可边界，附加失败；该尝试只验证了消费/取消前置，不计入堆数据。第二次在同一许可边界启动探针后，对同一 PID 83162 依次运行 `jcmd 83162 GC.class_histogram`，使用 JDK 17.0.18、512 MB/G1，三次取样均在显式 GC 后报告存活实例：

| 类 / 指标 | 订阅前 | 50,000 组活跃 | 取消后 |
| --- | ---: | ---: | ---: |
| `WindowedAggregateStage$GroupState` | 0 | 50,000 / 1,600,000 B | 0 |
| `Accumulator[]` | 0 | 50,000 / 1,600,000 B | 0 |
| `LinkedHashMap$Entry` | 未单独记录 | 50,009 / 2,000,360 B | 9 / 360 B |
| 普通 `Object[]` | 1,479 / 126,648 B | 1,545 / 129,320 B | 1,546 / 129,416 B |
| 全部存活对象浅大小合计 | 14,332,936 B | 26,116,440 B | 14,391,240 B |

活跃阶段还分别持有 50,000 个 `CountAggFeature$2` 和三个 `MapAggFeature` 累加器对象；取消后这些活跃组对象不再出现。整个堆直方图的活跃减订阅前为约 11.78 MB，取消后与基线仅差约 58 KB，但该差值包含 JIT/类加载等运行时变化，不是查询状态的精确 retained-size。关键结论是组状态确实驻留且取消后释放；普通 `Object[]` 仅净增 66 个，支持上一阶段“单维组不再每键持有一个数组”的结构判断。此探针不能给出旧版本的同条件 live-heap A/B，也不覆盖多维、并发或无界无限键；不得把 11.78 MB 全部归为优化收益。`git diff --check` 与新增未跟踪探针的空白检查通过。

### 无设置元数据的函数限制读取（计划，2026-10-05）

目标：减少内置受限字符串函数在默认 metadata、未配置任何 setting 时逐行重复读取四项默认上限的成本。当前真实 16 列函数 SQL 的 JFR 在 `DefaultReactorQLMetadata.functionLimits/intSetting` 采到 CPU 样本；同 JAR 正则基线 `target/jmh-default-limits-before-20261005.json` 为 14,421,750±528,583 输入行/s、712.027 B/行。owning module 仅 `DefaultReactorQLMetadata`，回归放在已有正则/字符串函数测试；不改变配置值、硬上限、自定义 metadata、SQL 文本、Reactor 操作符或默认限制。

步骤：仅在精确内置 metadata 且内部 settings 尚未创建时返回共享的不可变默认限制值；已有 setting 或子类仍逐次读取原配置。验证构建后首次 `setting` 生效、子类 `getSetting` 覆写、非法上限和正则行为，再集中运行完整测试、相同 512 MB/G1/双 fork 的宽函数与正则 A/B、宽直接投影负对照及短 JFR。若分配未下降、吞吐稳定回退或功能语义不等价，则撤回生产候选；GC B/行仅表示瞬时分配，不能宣称 live heap 收益。

结果：`DefaultReactorQLMetadata.functionLimits` 对精确内置、尚无 settings 的 metadata 返回共享的不可变默认值；一旦调用 `setting` 创建配置 Map，或 metadata 是子类，仍按原路径逐次读取和校验。`RegexPatternReuseTest` 新增构建后从默认值切到更严格/再放宽的限制、子类覆写 `getSetting` 的回归。完整 `mvn -q -Pjmh package` 通过（448 tests、0 failures/errors/skipped），`git diff --check` 通过；before/after JAR SHA-256 分别为 `bd4056b65f2cb42722a336960d3fdca9842e475572403767badf07bafed1cb9a`、`54678c3952f27e4c9dd8c800367b7dc2df3d2dd555e8986690b19c19d383e238`。

正式 JMH：相同 JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler。正则字面量 `profilingRegexpLikeLiteral` 从 `14,421,750±528,583` 到 `15,454,688±217,664` 输入行/s、`712.027→616.027 B/行`（少约 96 B/行）；16 列宽函数从 `1,458,325±24,500` 到 `1,468,153±15,576` 输入行/s、`2,844.917→2,830.517 B/输入行`（少约 14.4 B/输入行），吞吐误差区间重叠；宽直接投影负对照从 `6,030,579±101,843` 到 `5,950,302±159,921` 输入行/s，分配保持 `388.818 B/行`。原始 JSON 为 `target/jmh-default-limits-before-20261005.json`、`target/jmh-realistic-refresh-20261005.json` 和 `target/jmh-default-limits-after-20261005.json`。before 宽投影 JFR 的 `intSetting` 是 7/128 个 CPU 栈样本，after 宽函数 0/121、正则 0/98，录制在 `target/jfr-default-limits-after-20261005/`；短 JFR 只辅助确认路径变化，不用于换算吞吐、字节或 live heap。保留此无状态默认值复用，不扩展到有 setting 的 metadata 或 JSON 函数，后两者尚无同等证据。

### 单组长流五聚合的输入 payload 驻留检查（2026-10-05）

只运行既有 `HighCardinalityLiveHeapProbe`，不修改生产代码或探针。JAR SHA-256 `54678c3952f27e4c9dd8c800367b7dc2df3d2dd555e8986690b19c19d383e238`，JDK 17.0.18、512 MB/G1；分别以 `--keys=1 --values-per-key=50000/100000 --aggregate=five --hold-seconds=1` 运行默认快路及 `--compat` 负对照。SQL 为单键、保持窗口未关闭的 `count/sum/avg/min/max`，每行附带不进入聚合值/输出的 256 B payload；探针仅保留 payload 的弱引用，并在阶段读数前显式 GC。两种规模均确认输入全部被接受、无提前输出。

| 行数 | 默认快路：活跃/取消后 payload 存活数 | 兼容路径：活跃/取消后 payload 存活数 |
| ---: | ---: | ---: |
| 50,000 | 0 / 0 | 1 / 0 |
| 100,000 | 0 / 0 | 1 / 0 |

因此在这些固定场景中，默认标量聚合没有随输入行数保留无关 payload；兼容路径只保留最后源记录的 payload，取消后释放，与其可观察别名/结果上下文契约相符。探针自身持有每行一个 `WeakReference`，不同输入规模的 `heapUsed` 不能当作聚合状态斜率或真实 retained-size；本结果不证明任意自定义聚合、无限增长的 group key 或集合聚合均有界。精确多组聚合仍需要 O(活跃键数) 状态，不能通过删去 avg/max 的逐行历史来消除这一成本，因为默认快路本来就不保留逐行历史。

### 同步 Top-N 仅为入堆候选创建排序包装（计划，2026-10-05）

目标：减少同步排序键 Top-N 在拒绝大部分输入时逐行分配的 `OrderedRecord`，保留同一个 Reactor `collect` 订阅级小堆、排序规则和精确输出。owning module 为 `OrderBySupport`，仅影响可构建期证明同步的单键 `ORDER BY ... LIMIT`；异步键、多键、无 LIMIT、窗口排序和第三方未声明同步能力的 mapper 保留原路径。当前 JAR `54678c39…` 的短 JFR 中，单键 Top-N 有 118/612 个 worker allocation samples 为 `OrderedRecord`；正式降序输入 SQL 为 `11,966,351±222,625` 行/s、`168.146 B/行`，原生固定查询为 `42,592,880±979,547` 行/s、`0.664 B/行`，原始 JSON `target/jmh-topn-retained-only-before-20261005.json`。降序输入每行几乎都入堆，不应预期该候选在此组数据下降分配。

步骤：先在既有 JMH 增加预构造升序与确定性乱序输入，对同一 SQL、原生 Top-N、字段/类型/顺序与完整输出做 setup oracle；降序保持负对照。取三种分布同 JAR 正式 before。然后在单键同步路径中逐行求键并与堆顶比较，只有未满或优于当前最差值时创建既有 `OrderedRecord`；不缓存输入行、不改变每行求值次数、相等键不入堆规则或 `NULLS` 处理。测试覆盖 ASC/DESC、null、重复键、offset、已满后 mapper 错误、自定义同步 mapper、异步回退、request/取消/Context。阶段末集中完整测试、同配置 after JMH 和 JFR；只有升序/乱序分配下降且吞吐无稳定回退、降序负对照不稳定回退时保留，否则撤回生产候选。不改变默认资源限制、公开 SPI、Reactor 自定义操作符或输出 Map/Record 契约；GC B/行不是常驻堆。

结果：仅同步单键 Top-N 在现有 `collect` 小堆内逐行求键，拒绝候选不创建 `OrderedRecord`；异步、多键、无 LIMIT 仍使用原路径。升序、确定性乱序和降序查询在 JMH setup 中与原生 Top-N 全量核对结果、字段、类型和顺序。相同 JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler 的正式 before/after 分别为 `target/jmh-topn-admission-before-20261005.json`（JAR SHA-256 `658aebd119d8902a4b1c276f3dc851de1440274273ea2fb44d530e6b5fb39472`）与 `target/jmh-topn-admission-after-20261005.json`（JAR SHA-256 `95558172be95b98bee6256614f514c2bbe3a62422d434e6269bd9ea82519300d`）。

| 输入分布 | SQL before → after 输入行/s | SQL before → after B/输入行 | 原生 before → after 输入行/s |
| --- | ---: | ---: | ---: |
| 升序，多数候选被拒绝 | 39.08±2.34 → 43.12±1.54 M | 168.143 → 144.260 | 179.68 → 176.94 M |
| 确定性乱序 | 34.50±1.71 → 39.55±1.12 M | 168.143 → 144.891 | 170.18 → 170.43 M |
| 降序，候选持续入堆 | 11.71±0.80 → 12.09±0.62 M | 168.146 → 168.143 | 37.09 → 37.46 M |

升序和乱序的本夹具吞吐观测分别约 +10%/+15%，JMH 误差区间不重叠；瞬时分配分别少约 23.9/23.3 B/输入行。降序负对照吞吐区间重叠、分配基本不变，原生对照亦无稳定变化。短 JFR `target/jfr-topn-admission-after-20261005/` 中，升序查询 660 个 allocation samples 未采到 `OrderedRecord`，但 `HashMap`、桶数组、投影 lambda 与 `DefaultReactorQLRecord` 仍居前；采样结果仅支持热点方向，不能换算字节或 live heap。新增回归覆盖拒绝候选仍求键、堆满后错误、DESC/null、offset、Context、下游 demand、取消以及自定义同步/异步 mapper。最终完整 `mvn -q -Pjmh package` 在允许现有 `ReactorDebugAgent` 本机 attach 后通过（450 tests，0 failures/errors/skipped），`git diff --check` 通过；最终 JAR SHA-256 `5ec6eb8b10370cd55dd64dc68b0219c5c050b05479f1891927b176f522d51b1d`。受限沙箱下旧 `GroupByWindowTest` 的静态 agent 初始化会失败，单独测试与完整构建在允许本机 attach 后均通过；未为该环境差异修改生产代码或放宽测试。

### 最终源码的真实查询负载与热点复核（2026-10-05）

复用已在 setup 校验完整输出、字段/类型、顺序和冷源订阅次数的 `WideSqlWorkloadBenchmark` 与 `NestedSqlWorkloadBenchmark`。宽查询为 65,536 条预构造设备事件、16 列投影，函数版本包含算术、比较、字符串、两次 JSON 路径读取、日期与 `coalesce`；两层未关联子查询每次处理 1,024 外层行/1,024 lookup 行，关联查询为 4,096 外层行/256 lookup 行。热路径只流式消费结果，不计输入构造；吞吐按输入/外层行归一化。最终 JAR SHA-256 `5ec6eb8b10370cd55dd64dc68b0219c5c050b05479f1891927b176f522d51b1d`，JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler；正式 JSON `target/jmh-real-sql-final-20261005.json`。

| 查询 | 输入/外层行/s（均值 ± JMH 误差） | 分配 B/输入或外层行 |
| --- | ---: | ---: |
| 16 列直接投影 | 5.781±0.387 M | 388.818 |
| 16 列函数投影 | 1.471±0.055 M | 2,830.517 |
| 两层未关联 `sum`，订阅内复用 | 4.168±0.090 M | 1,276.869 |
| 同 SQL，显式关闭复用 | 8,189±344 | 732,593.406 |
| 逐行关联 lookup | 159,457±16,189 | 28,360.219 |

同一最终 JAR 的短 JFR 位于 `target/jfr-real-sql-final-20261005/`（1×1s 预热、2×1s 测量、1 fork，诊断 JSON `target/jmh-real-sql-final-jfr-20261005.json`）。宽函数查询 115 CPU/660 allocation samples，JSON 调用栈覆盖 19/155，日期栈 8/87，分配类以 `byte[]` 95、`String` 67、`FixedArgumentList` 39 及 Map/桶/节点为主。默认复用的两层查询为 104/615，CPU 叶子含 Map 写入/扩容及 Reactor `flatMap` drain，分配类含 `HashMap$Node` 151、`HashMap` 128、Record 57 和内部订阅者；关闭复用为 123/660，Map 节点 247、Map 128、EntryIterator 136。关联查询为 119/660，CPU 叶子含 `BinaryFilterFeature.test` 11、Map 查找/写入，分配类含 Map 节点 324、Map 174、Record 70。JFR 样本有采样偏差且调用栈类别可重叠，不作为 CPU 百分比、字节数或常驻堆证据。

结论：宽函数的 JSON/日期转换及结果物化、关联查询的逐外层行过滤/Map-Record 物化仍是主要开销；未关联聚合的重复 lookup 扫描已通过显式纯度契约下的订阅级复用消除。下一步若寻求跨查询通用优化，应先证明表达式行内复用的纯度/可变输入/自定义 Feature 契约，或为业务提供有界且有索引的关联源；不对 `json_get`、`sum` 名称或 SQL 文本特调，也不将可能无界的关联源隐式全量缓存。GC B/行是瞬时分配；这些数据不能证明长期 live heap 下降。

### 当前宽投影与同结果原生上界复核（2026-10-05）

为判断剩余通用空间，在最终源码 JAR `5ec6eb8b10370cd55dd64dc68b0219c5c050b05479f1891927b176f522d51b1d` 上成对运行 `WideSqlWorkloadBenchmark.wideProjectionWidthControl` 与 `nativeWideProjectionControl`。两者共用 65,536 条预构造 Map 行、相同选择性 WHERE、16 个结果列、逐行独立可变结果 Map 和同一流式消费端；setup 全量核对结果值、字段、顺序及单次冷源订阅。JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler 的原始 JSON 为 `target/jmh-wide-native-final-20261005.json`：SQL `5.966±0.160 M` 输入行/s、`388.818 B/行`，原生 `13.934±0.553 M`、`316.811 B/行`。原生是固定 SQL 的上界，不具备动态 Feature/Record 契约，不能直接替换通用执行路径。

同 JAR 短 JFR `target/jfr-wide-native-final-20261005/`（1×1s 预热、2×1s 测量、1 fork；诊断 JSON `target/jmh-wide-native-final-jfr-20261005.json`）中，SQL 的 660 个 allocation samples 主要为结果 Map 节点 234、`FixedArgumentList` 170、`DefaultReactorQLRecord` 132、桶数组 55 与 Map 49；原生 658 个样本主要为结果 Map 节点 493 与桶数组 163。SQL 的可变参数列表来自 WHERE 中通用 `str_contains(text,'beta')` 的标量函数契约，而不是 16 列直接投影；Record 仍承载别名、属性回退及扩展 Feature 语义。样本数不能换算为 72.007 B/行的具体来源或 live heap。

检查了一个看似通用的候选：让 `FunctionMapFeature.scalar` 自动传播 `RawScalarValueMapper`，使函数谓词能在创建 Record 前过滤。但当前公开 `scalar(...)` 允许自定义函数修改/保留可变参数及其 Map 值；无条件声明原始行只读能力会把未 opt-in 的扩展函数提前到 raw 阶段，并违反 `RawScalarValueMapper` 的输入行契约。`FilterFeature` 还需在函数谓词上保留 raw 能力，单改 mapper 不会触达现有前置过滤。故本轮不做自动传播、函数名白名单或跨行列表复用；若另立实现，须以显式能力契约覆盖内置与第三方回退，并先在同一宽 SQL、稀疏/非 Map 输入、别名、Context、异常、取消和原生负对照上证明收益与等价。当前无生产代码改动，`git diff --check` 通过。

### 业务形态复测与热点分层（2026-10-05）

在同一最终 JAR `5ec6eb8b10370cd55dd64dc68b0219c5c050b05479f1891927b176f522d51b1d` 上重跑 `src/jmh/java/org/jetlinks/reactor/ql/` 下既有、逐行核对结果与源订阅次数的三个夹具。输入为预构造的 65,536 条设备事件（16 列 SELECT、算术/比较/字符串/JSON/日期函数）、1,024 外层行和 1,024 lookup 行的三层 SELECT 未关联聚合、4,096 外层行和 256 lookup 行的关联查询，以及 50,000 行、每键 1/2 行的精确窗口四聚合。输入行构造与 `@Setup` 校验不计入热路径；每次 `Flux.fromArray` 发布和结果的流式 Blackhole 消费计入。JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler；原始结果为 `target/jmh-real-sql-user-20261005.json` 与 `target/jmh-real-sql-highcard-user-20261005.json`。宽投影/聚合按输入行、子查询按外层行归一化，不跨不同工作量直接比较吞吐。

| 查询 | 行/s（均值 ± JMH 误差） | 分配 B/行 |
| --- | ---: | ---: |
| 16 列直接投影 / 同结果原生控制 | 5.581±0.199 M / 13.513±0.716 M | 388.818 / 316.811 |
| 16 列函数投影，两次 JSON / 一次 JSON | 1.457±0.063 M / 1.744±0.109 M | 2,830.517 / 2,046.294 |
| 两次 JSON、输入预解析成 Map | 1.329±0.027 M | 2,392.503 |
| 三层 SELECT 未关联聚合，订阅内复用 / 显式关闭复用 | 4.158±0.209 M / 8,337±221 | 1,276.869 / 732,593.381 |
| 逐行关联 lookup | 155,915±17,376 | 28,360.219 |
| 四聚合每键 1 行，SQL / 同结果原生控制 | 3.194±0.312 M / 9.143±0.528 M | 909.011 / 412.944 |
| 四聚合每键 2 行，SQL / 同结果原生控制 | 4.685±0.403 M / 19.317±1.715 M | 454.525 / 206.478 |

同 JAR 短 JFR 位于 `target/jfr-real-sql-user-20261005/` 和 `target/jfr-real-sql-highcard-user-20261005/`（1×1s 预热、2×1s 测量、1 fork；仅定位）。宽函数查询 117 CPU/660 分配样本，分配类以 `byte[]` 105、`String` 83、`FunctionMapFeature$FixedArgumentList` 38 和 Map 桶/节点为主；JSON 路径栈在两次读取时覆盖 36 CPU/360 分配样本，单次读取时为 21/246。`src/main/java/org/jetlinks/reactor/ql/supports/map/JsonFunctionSupport.java` 的 `readPath` 每次调用 `JsonValueSupport.normalize`，因此同一文本行的两次读取各自执行规范化；但按行自动共享会触及输入 Map 可变性、自定义 Feature、副作用和 Context 契约，本轮不据此引入缓存。预解析 Map 使分配下降约 438 B/行却吞吐下降，说明深层 Map 规范化/复制仍有成本，不能把预解析当作无条件加速手段。

关联查询的 114 CPU/654 分配样本中，`String.equals`、`BinaryFilterFeature.test` 与 Map 查找/写入是 CPU 叶子热点，分配类主要是 `HashMap$Node` 328、`HashMap` 149、`DefaultReactorQLRecord` 81；未关联且可复用的查询主要是 Map/Record 与少量 `FlatMapInner`，关闭复用后 `EntryIterator` 和 Map 节点进一步增多。高基数 SQL 的 122 CPU/635 分配样本中有 Map 节点 164、Map 129、`GroupState` 44 及各类累加器，符合精确逐键状态的预期；CPU 栈另出现 4 个 `NativeState.result` 控制组样本，故该短 JFR 不用于计算 SQL 热路径的精确 CPU 占比。JFR 家族栈计数可重叠且不等于字节数；上表 GC B/行是瞬时分配，不是 live heap。已有独立 50,000 活跃键直方图探针在取消后观察到组状态释放，本轮未重测或外推其保留字节。

当前可优先评估的通用边界是函数参数容器、JSON 规范化的行内共享契约和关联查询的逐候选物化；先证明可变输入、扩展 Feature、背压/取消/错误/Context 等语义及成对 A/B 收益，再考虑生产改动。不得按函数名或 SQL 文本特调，也不隐式全量缓存可能无界的关联源。本轮只更新测量记录，未改生产代码或默认限制。

### 分组输出投影遍历分配 A/B（计划，2026-10-05）

目标：降低精确分组聚合的逐结果行瞬时分配，并检查吞吐。当前同 JAR 高基数 JFR 的 635 个分配样本中，33 个 `ArrayList$Itr` 由 `WindowedAggregateStage.GroupState.toRecord` 遍历内部只读 `projections` 列表产生；另有 37 个 `ArrayList` 来自对外隔离的 `_group_by_key` 输出，后者不能删除。基线为 `target/jmh-real-sql-highcard-user-20261005.json`，每键 1/2 行 SQL 分别为 3.194/4.685 M 输入行/s、909.011/454.525 B/输入行。

影响范围只限 `src/main/java/org/jetlinks/reactor/ql/WindowedAggregateStage.java` 的输出投影循环与本文档。将内部只读列表的增强 `for` 改为索引遍历；不改投影顺序、值、类型、组键隔离、窗口状态、默认限制、背压、取消、错误、Context 或扩展 Feature。阶段末集中运行聚合相关回归与完整测试，再以同 JDK/JVM/JMH 夹具、2 forks、GC profiler 比较 SQL 每键 1/2 行及原生负对照，必要时以短 JFR 确认迭代器分配是否消失。仅在结果等价、分配明确下降且吞吐无稳定回退时保留；否则撤回。不引入自定义操作符、缓存或 SQL/聚合函数特调。

结果：内部 `projections` 只读列表改为索引遍历，输出投影求值次数、顺序和 `setResult` 调用保持不变。完整 `mvn -q -Pjmh package` 通过（450 tests，0 failures/errors/skipped）；最终 JAR SHA-256 `faa01a3ed06bb9075135c88899d7cc21f4ed9a14fc5a7d998db4d0699e317a92`。同条件正式 after 为 `target/jmh-group-output-iterator-after-20261005.json`，before 为上节 `target/jmh-real-sql-highcard-user-20261005.json`：

| 每键输入行 | SQL before→after 行/s | SQL before→after B/输入行 | 原生 before→after B/输入行 |
| ---: | ---: | ---: | ---: |
| 1 | 3.194±0.312 → 3.415±0.087 M | 909.011 → 853.011 | 412.944 → 412.944 |
| 2 | 4.685±0.403 → 4.939±0.272 M | 454.525 → 426.524 | 206.478 → 206.478 |

两形态的 SQL 分配均按每个输出分组约少 56 B；吞吐误差区间重叠，只能确认无稳定回退，不宣称稳定吞吐提升。短 JFR `target/jfr-group-output-iterator-after-20261005/` 的 660 个分配样本中未采到 `ArrayList$Itr` 或 `UnmodifiableCollection` 迭代器（before 同路径为 33 个）；JFR 仅佐证热点消失，不用于换算 56 B。`_group_by_key` 仍在输出时创建隔离列表，精确活跃分组状态与默认限制未变。`git diff --check` 通过；本轮未测试 live heap A/B，因此不宣称常驻堆进一步下降。

### 分组结果 Map 容量提示 A/B（计划，2026-10-05）

目标：减少分组输出多列结果时默认 `HashMap(4)` 首次插入与扩容产生的桶数组，而非减少必要的结果字段、源记录或分组状态。上一阶段 JAR `faa01a3ed06bb9075135c88899d7cc21f4ed9a14fc5a7d998db4d0699e317a92` 的高基数 JFR 中，`DefaultReactorQLRecord.setResult` 路径有 81 个桶数组分配样本；同一 Record 已有仅对默认 Context 生效的 `setResult(name,value,expectedEntries)`，宽标量投影正在使用。

范围只限 `WindowedAggregateStage` 调用这一现有容量提示及本文档；按编译后的维度、聚合列和投影列数求上界，不根据 SQL 文本或函数名分支。自定义 Record/Context 仍调用原公开路径；即使首列为 null，也保持后续首个非 null 结果可提示容量。不得改变结果内容、字段顺序、`_group_by_key` 隔离、窗口状态、默认限制、背压、取消、错误或 Context。阶段末集中跑完整测试，并以同 JDK/JVM/JMH 的每键 1/2 行高基数 SQL、同结果原生负对照及短 JFR 做 A/B；若分配未明确下降或吞吐出现稳定回退则撤回。瞬时分配不等于活跃分组常驻堆。

结果：编译期为分组输出保存维度、聚合列和投影列数的容量上界，写结果时复用 `DefaultReactorQLRecord.setResult(name,value,expectedEntries)`。该方法只有默认 Context 且结果 Map 尚未创建时才预分配；自定义 Context/Record 仍由原公开 `setResult` 与 `newContainer` 路径处理，首个非 null 字段即使不是第一列也可提示。结果 Map、`_group_by_key` 和每键状态的所有权与生命周期均未改。完整 `mvn -q -Pjmh package` 通过（450 tests，0 failures/errors/skipped），最终 JAR SHA-256 `adc6c953329abf0d6e6cf84a9884a4f5cb3da269366a9436347a1e94c19d971e`。

同条件正式 after 为 `target/jmh-group-result-capacity-after-20261005.json`，before 为 `target/jmh-group-output-iterator-after-20261005.json`：

| 每键输入行 | SQL before→after 行/s | SQL before→after B/输入行 | 原生 before→after B/输入行 |
| ---: | ---: | ---: | ---: |
| 1 | 3.415±0.087 → 3.445±0.049 M | 853.011 → 821.011 | 412.944 → 412.944 |
| 2 | 4.939±0.272 → 5.061±0.199 M | 426.524 → 410.524 | 206.478 → 206.478 |

两种键分布均按每个输出分组约少 32 B；吞吐误差区间重叠，不能宣称稳定提升，但未观察到稳定回退。短 JFR `target/jfr-group-result-capacity-after-20261005/` 中，结果 `setResult` 路径的桶数组分配样本为 34（before 81）；样本数不换算为字节。二维/三维分组的快路与 Publisher 回退路径另以 `target/jmh-group-result-capacity-multidim-smoke-20261005.json` 完成同 JAR smoke，夹具 setup 逐键核对了输出。该收益属于结果物化的瞬时分配，未重测 live heap，不等同于 50,000 个活跃键状态的常驻堆下降。`git diff --check` 通过。

### 宽查询函数 WHERE 与原始行过滤判别（2026-10-05）

在最终 JAR `adc6c953329abf0d6e6cf84a9884a4f5cb3da269366a9436347a1e94c19d971e` 上复用 `WideSqlWorkloadBenchmark` 的 65,536 条预构造设备事件和 16 列结果 Map；每个 SQL 与其原生控制在 setup 核对结果、顺序与单次源订阅。两组分别是含 `str_contains(text,'beta')` 的 WHERE，以及仅有数值/布尔比较、能够在默认单表原始 Map 行上过滤的 WHERE；两组选择率不同，不能直接将其吞吐或 B/行相减解释为函数成本。JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler；同 JAR 原始数据为 `target/jmh-function-where-width-pair-20261005.json` 与 `target/jmh-function-where-discriminator-20261005.json`。

| WHERE 形态 | SQL / 同结果原生输入行/s | SQL / 原生 B/输入行 |
| --- | ---: | ---: |
| 函数 WHERE | 5.943 / 13.708 M | 388.818 / 316.811 |
| 原始行标量 WHERE | 6.024 / 12.887 M | 414.008 / 396.003 |

同 JAR 短 JFR `target/jfr-function-where-discriminator-20261005/` 中，函数 WHERE 为 145 CPU/626 分配样本，其中 `FunctionMapFeature$FixedArgumentList` 139、`DefaultReactorQLRecord` 138；原始行 WHERE 为 139/621，对应列表 0、Record 1，分配主要转为必要的结果 Map 节点与桶数组。这支持“先过滤、后包装”的通用价值，但不证明对自定义函数可自动传播 `RawScalarValueMapper`：当前 `FunctionMapFeature.scalar` 的公开回调可修改或保留可变参数列表，原始行能力又明确要求不保留/修改输入。未更改生产代码，不按函数名白名单或 SQL 文本特调。若后续实施，须以显式、可证明的能力契约覆盖内置与扩展回退，并在相同选择率夹具及非 Map、别名、错误、取消、Context 负对照上做 A/B；当前先转向不涉及该契约的操作符热点。

### JOIN 单异步 ON 编排 A/B（计划，2026-10-05）

目标：移除单个 ON 谓词在每个右候选上的一元素 `Flux.fromIterable(filters) → metadata.flatMap → all` 包装，同时保持冷谓词的订阅时求值、空 Mono 为 true、false/错误/取消和 Context 传播。影响范围为 `DefaultReactorQL.createJoin`、JOIN 定向测试和本文档；不改多谓词的求值顺序、JOIN 源、默认并发限制、输出 Map/Record、无界右源处理或公开 Feature/SPI。现有 JAR `adc6c953329abf0d6e6cf84a9884a4f5cb3da269366a9436347a1e94c19d971e` 的 `target/jmh-join-single-on-baseline-20261005.json`：异步 ON 0.161±0.002 M 左行/s、27,252 B/左行，同分布同步 ON 0.824±0.010 M、5,460 B/左行；短 JFR `target/jfr-join-single-on-baseline-20261005/` 中每候选 `MonoAll$AllSubscriber`、`FluxFlatMap$FlatMapMain` 均有 54 个分配样本，另有 15 个 `FluxIterable$IterableSubscription`。

实施只对**恰好一个谓词、默认 Metadata 类型且运行时未显式设置 `concurrency`**的路径使用惰性的单谓词 Flux 并保留 `all(Boolean::booleanValue)` 终止语义；自定义 Metadata（尤其 `flatMap` 覆写）、显式并发设置和多谓词继续走完全相同的原路径。先覆盖 INNER/LEFT/RIGHT、空/true/false/错误、冷源订阅次数、下游 request/取消与 Reactor Context，再集中运行完整测试和同 JAR/JDK/堆/JMH 夹具的异步 ON、同步负对照 A/B，短 JFR 只作定位。仅在契约等价、异步分配明确下降且吞吐无稳定回退时保留；否则撤回，不引入自定义 Subscriber 或按 SQL 文本/函数名分支。

结果：`DefaultReactorQL.createJoin` 的默认单谓词路径用 `Flux.defer` 推迟自定义谓词调用到订阅时，仍以 `all(Boolean::booleanValue)` 等待终止并保留空流为 true；运行时若显式设置 `concurrency`，或 Metadata 为子类/其他实现，仍走原 `metadata.flatMap(Flux.fromIterable(filters), ...)`。新 `SingleAsyncJoinOnTest` 对默认快路逐一验证 true/false/空流、INNER/LEFT/RIGHT、冷谓词订阅次数、Context、错误、订阅后取消；对 Metadata 覆写和无效显式并发设置验证回退。第一次完整测试的两个失败是测试 oracle/取消时序错误：比较表达式的内部空 Mono 会先变为 false，无法代表外层空谓词；零需求后立即取消也不能证明冷谓词已订阅。改为直接布尔 ON 函数及由响应式信号触发订阅后取消，没有修改生产代码迎合错误夹具。定向 4 项与最终完整 `mvn -q -Pjmh package` 均通过，完整汇总 454 tests、0 failures/errors/skipped。最终基准 JAR SHA-256 `23b1b4f01c7d39b6396d49beedfc6a1feed6e94f073ce0ea9896b1d343df193e`。

正式 before/after 分别为 `target/jmh-join-single-on-baseline-20261005.json` / `target/jmh-join-single-on-after-20261005.json`；同一 20,000 左行、21 右行夹具覆盖每左行 0/1/4/16 个匹配，setup 核对 105,000 个输出及 20,000 次右源、420,000 次异步谓词订阅。JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler，按左输入行归一化：

| 查询 | before→after 左行/s（均值 ± JMH 误差） | before→after B/左行 |
| --- | ---: | ---: |
| 单异步 ON INNER JOIN | 0.161±0.002 → 0.239±0.003 M | 27,252.072 → 19,356.062 |
| 同分布同步 ON 负对照 | 0.824±0.010 → 0.828±0.050 M | 5,460.045 → 5,460.045 |

异步 ON 在本夹具观测吞吐约 +49%、每左输入行少约 7,896 B（约 29%）；吞吐误差区间不重叠，同步负对照未出现稳定变化。短 JFR `target/jfr-join-single-on-baseline-20261005/` 与 `target/jfr-join-single-on-after-20261005/` 中，before 的 `FluxFlatMap$FlatMapMain` 54、`FluxFlatMap` 21、`FluxIterable$IterableSubscription` 15 个分配样本在 after 的前列不再出现；`MonoAll` 保留。JFR 样本不是字节数或 live heap 证据；此改动减少逐候选瞬时编排对象，不改变右源是否有界或精确 JOIN 状态。`git diff --check` 通过。

### 最新 JAR 的真实 SQL 宽列与多层子查询复测（2026-10-05）

目标是按实际使用形态重新确认 CPU 与瞬时分配热点，不改生产逻辑。使用已逐行核对结果、类型、顺序和源订阅次数的 `WideSqlWorkloadBenchmark` 与 `NestedSqlWorkloadBenchmark`：预构造 65,536 行设备事件上执行带选择性 WHERE 的 16 列投影（算术、比较、字符串、JSON、日期函数），以及 1,024 外层行/1,024 lookup 行的三层未关联聚合与 4,096 外层行/256 lookup 行的相关 lookup。这些是代表性合成数据，不是线上流量录制；不包含真实数据库、网络或调度成本。输入构造和 setup 校验不计入热路径；`Flux.fromArray` 发布、查询执行及流式 Blackhole 消费计入。此轮测量对应最终 JAR SHA-256 `23b1b4f01c7d39b6396d49beedfc6a1feed6e94f073ce0ea9896b1d343df193e`，JDK 17.0.18、单线程、512 MB/G1、3×1s 预热、5×1s 测量、2 forks、GC profiler。原始数据：`target/jmh-real-sql-current-20261005.json`、`target/jmh-real-sql-current-controls-20261005.json`。

| 查询形态 | 输入/外层行/s（均值 ± JMH 误差） | 分配 B/输入或外层行 |
| --- | ---: | ---: |
| 16 列直接投影 / 同结果原生 `handle` | 5.622±0.656 M / 13.282±1.082 M | 388.818 / 316.811 |
| 16 列函数投影：两次 JSON / 一次 JSON | 1.465±0.018 M / 1.757±0.045 M | 2,830.517 / 2,046.294 |
| 两次 JSON、输入预解析成 Map | 1.350±0.027 M | 2,392.503 |
| 三层未关联聚合：订阅内复用 / 显式关闭复用 | 4.193±0.031 M / 8,792±478 | 1,276.837 / 732,561.373 |
| 逐行相关 lookup | 153,140±2,940 | 28,360.220 |
| 16 列纯标量 WHERE / 同结果原生 `handle` | 5.655±0.481 M / 12.878±0.320 M | 414.008 / 396.003 |

宽函数查询与一次 JSON 对照使用相同 WHERE、输出列和值；少一次 JSON 读取在本夹具降低约 784 B/输入行、吞吐约高 20%。预解析输入虽然少约 438 B/输入行，吞吐却更低，不能把“预解析”当成通用加速建议。纯标量 WHERE 的选择率不同于含 `str_contains` 的 WHERE，因此不得把这两组直接相减当作函数成本。三层未关联查询缓存命中只订阅 lookup 一次；关闭复用则每外层行重新扫描 1,024 行。相关查询按语义每外层行订阅并扫描 256 个 lookup 候选，主要是查询工作量差异，不能与未关联缓存路径直接比较。

同 JAR 短 JFR（1×1s 预热、2×1s 测量、1 fork）保存在 `target/jfr-real-sql-current-20261005/` 与 `target/jfr-real-sql-current-controls-20261005/`；诊断 JSON 为 `target/jmh-real-sql-current-jfr-20261005.json` 和 `target/jmh-real-sql-current-controls-jfr-20261005.json`。两次 JSON 的 123 个 CPU 样本中，43 个调用栈包含 JSON 处理；660 个分配样本中，211 个包含 JSON 文本 parser、142 个包含 JSONPath。一次 JSON 分别为 9/132 个 CPU parser 栈、151/614 个分配 parser 栈；预解析 Map 输入的 parser 分配栈为 0，但 `JsonValueSupport` 深层规范化仍有 139/660 个分配栈样本。`JsonFunctionSupport.readPath` 每次调用 `JsonValueSupport.normalize`，文本路径可重复解析，预解析 Map 路径则会复制嵌套容器。16 列直接投影的 JFR 主要为 `HashMap$Node`、结果桶数组、`DefaultReactorQLRecord` 与函数 WHERE 的 `FixedArgumentList`；对应 CPU 叶子包括 `String.equals`、`BinaryFilterFeature.test` 和 Map 读写。三层复用路径仍出现 `FluxFlatMap`/`MonoDefer`、Record 和 Map 物化；相关 lookup 的 CPU 叶子集中在 `String.equals`、Map 读取及谓词测试，分配类中 Map 节点 288、Map 132、Record 86（613 个带栈分配样本）。

热点优先级：先区分重复 JSON 文本解析/规范化与不可避免的结果物化；再评估普通函数 WHERE 的参数容器与早过滤契约；相关 lookup 若要接近索引查询性能，应由有界/可索引的来源提供查找能力，不能对任意 `Publisher` 静默建无界索引或缓存。任何行内共享或原始行提前求值都须先明确输入可变性、扩展 Feature、副作用、Context、错误/取消语义，再做通用 A/B；不按 SQL 文本或函数名特调。JFR 栈家族计数会重叠，不能换算 B/行；GC B/行是瞬时分配，**不是常驻堆**。本轮没有生产代码改动，也没有 live heap A/B。

### 混合条件操作符与宽投影诊断（计划，2026-10-05）

目标：补足代表性宽查询中 `CASE`、`BETWEEN`、`LIKE`、`IN`、`IS NULL`、`OR`、`CAST` 的组合取证，判断 JFR 是否出现超出必要表达式求值和结果物化的可复用热点。只在 `WideSqlWorkloadBenchmark` 复用既有 65,536 条预构造事件，新增一条 16 列混合表达式 SQL 与同结果原生控制；在 setup 按全部输出行核对值、类型、顺序、结果列及源单次订阅。保留现有函数宽查询作为负对照，不改生产代码、默认限制、响应式信号路径或现有基准。

阶段末集中构建 JMH，按同 JDK 17/512 MB/G1/单线程/3×1s 预热/5×1s 测量/2 forks 测吞吐与 GC B/输入行，再以短 JFR 排除夹具 setup 和消费端干扰后定位 CPU/分配栈。SQL/原生必须处理相同输入与结果；若解析、值类型或订阅前置不等价，先修正夹具，不能把无效分数当证据。JFR 样本不换算字节或 live heap；若只看到必要的 Map/Record、标量表达式和过滤成本，不据此添加 SQL 特判或自定义 Reactor 操作符。

取证结果：新增的混合查询、仅将 `status in ('online','unknown')` 改成在此夹具上同结果的 `status = 'online'` 对照、原生控制均在 setup 逐行核对结果/类型/顺序及单次源订阅。JDK 17.0.18、512 MB/G1、3×1s 预热、5×1s 测量、2 forks、GC profiler；首轮 SQL/原生为 1.063±0.016 / 18.311±0.788 M 输入行/s、4,290.786 / 140.705 B/输入行（`target/jmh-operator-mix-current-20261005.json`）。同源码差分轮次的 `IN` / 等价 `=` 为约 1.06 / 4.118 M 输入行/s、4,298.786 / 704.102 B/行（`target/jmh-operator-mix-in-discriminator-20261005.json`）；两次 SQL 测量绝对值有轻微 JMH 漂移，以同轮差分判断热点。短 JFR `target/jfr-operator-mix-current-20261005/` 中，`IN` 查询的 628 个分配样本以 `Object[]` 166、`FluxFlatMap$FlatMapMain` 45、`MonoZip$ZipInner` 28、`FluxDefaultIfEmpty` subscriber 23 等编排对象为主，而同结果原生的 660 个样本以结果 Map 节点 388、桶数组 101 为主；JFR 只定位，不从样本数换算字节。

### 常量列表 `IN` 的通用单值快路（计划，2026-10-05）

根因是 `InFilter` 即使左右均为同步标量、右侧为编译期常量，也逐行构造 `Flux.fromIterable → flatMap → cache → any`。目标是在不新增 SPI、Subscriber、缓存或 SQL 文本/函数名分支的前提下，消除这类表达式的逐行内部 Publisher 编排。影响范围仅 `InFilter`、其行为测试、上述同结果 JMH 与本文档。构建期只有默认标量能力有效、左 mapper 实现 `ScalarValueMapper`，且所有右 mapper 声明为非容器/非 Publisher 的常量时，才以现有 `CompareUtils.equals(right,left)` 按原列表顺序比较；通过 `Mono.fromSupplier` 保持比较、异常与终止信号在订阅时发生。左值 mapper 仍在原先的谓词调用时求值。运行时左值若为 `Iterable`、`Publisher` 或单入口 `Map`，以及所有非标量、子查询、自定义异步 Feature、checkpoint 形态，均走原有展平和订阅路径；`NOT IN` 沿用现有布尔取反语义。

测试目标：普通数值/字符串、`NOT IN`、null、可展平的左集合/单入口 Map、冷 Publisher 的 Context/取消/错误、关联子查询 `IN` 和元数据回退；不因新路径删除右侧真实 Publisher 的订阅，也不提前比较可能有副作用的值。阶段末集中运行定向和全量测试，再以同 JAR/JDK/JMH 成对比较 `IN`、等价 `=` 与原生控制，短 JFR 验证 `IN` 内部编排样本下降。只有功能等价、分配明显下降且吞吐没有稳定回退时保留，否则撤回；GC B/行不代表 live heap。无新增常驻状态，不需要 MBean 或逐行 Trace span。

实施中发现独立的响应式生命周期风险：原 `doPredicate` 为重用左源使用 `Flux.cache()`；独立 Reactor 3.4.34 探针确认冷 `Flux.never()` 的下游取消不会取消左源（`CACHED_CANCELLED=false`）。同源 `replay().refCount(1)` 在源完成后先后两次订阅仅订阅上游一次，且最后订阅者取消时能向未完成上游传播取消。将这一处缓存改为可断开的订阅内 replay，保留同一谓词内多个右值对左值的重放；不改匹配布尔规则、结果缓存范围或默认限制。补取消及多右值单次订阅测试，并用异步左值/子查询负对照检查性能；若重放或错误时序不等价则撤回。该换法解决取消后的源释放，不把任意未终止双无界流声称为有界内存算法。

### 真实查询复测前的 `IN` 路径收敛（计划，2026-10-05）

前述通用 `replay().refCount(1)` 替换虽修复取消传播，却令动态右值 `IN` 从 `1.036 M` 降至 `0.480 M` 输入行/s，分配从 `4,330.787` 增至 `5,824.104 B/行`（`target/jmh-in-cache-a-20261005.json`、`target/jmh-in-refcount-b1-20261005.json`）；因此不能全量替换后直接交付。进入本阶段时，`doPredicate` 曾暂回到旧 `cache()`，取消测试尚不通过；该临时状态已由下面的分流实现取代。

目标与范围：仅在现有 `ScalarValueMapper` 契约已证明左侧单值、运行时值非容器/Publisher 时，将动态右值流直接与该值比较，省去左源缓存/重放；其余左侧多值、Publisher、异步/扩展 Mapper 保持原展平比较并使用可取消的订阅内重放。沿用右侧冷源的订阅、错误、Context 和 `NOT IN` 语义，不按 SQL/函数名分支，不引入自定义 Subscriber、跨行状态或无界缓存。修改限 `InFilter`、契约测试及本文档；阶段末集中完成全量测试、常量/动态 `IN` 与等价 `=`/原生控制的同条件 JMH，再以已有 16 列混合函数查询和三层/关联子查询 JMH、短 JFR 找当前 CPU/分配热点。若功能不等价或关键负对照稳定回退，撤回候选并报告遗留取消风险，不以放松断言换性能。

结果：`InFilter` 对已声明标量能力且运行时左值为普通单值的动态右值直接在冷右值流上比较；左值为空时仍订阅右侧并传播错误。多值/Publisher 左值仍走展平和订阅内 `replay().refCount(1)`，取消未完成上游不再被 `cache()` 阻断。新增 `NOT IN`/空值/右侧错误与右侧取消测试，并保留 Context、多值、左侧取消、子查询和 Metadata 回退用例。`mvn -q -Pjmh package` 通过：460 tests、0 failures/errors/skipped；`git diff --check` 通过。测试及下面所有新测量对应 JAR SHA-256 `3192e314b4ea4a8c0355d87972e38dc5fd61e55cec8505194e24a86c4a54b93b`，未改变默认限制。

### 16 列业务查询与多层子查询复测（2026-10-05）

夹具 `WideSqlWorkloadBenchmark` 用 65,536 条预构造设备事件，16 列混合 SQL 包括算术、`CASE`、`BETWEEN`、`LIKE`、`IN`、`IS NULL`、`OR`、`CAST` 与字符串函数；函数 SQL 包括 JSON/日期函数。`NestedSqlWorkloadBenchmark` 使用 1,024 外层/1,024 lookup 行的三层未关联聚合，以及 4,096 外层/256 lookup 行的相关 lookup。`@Setup` 全量核对 SQL/控制结果、值类型、顺序和源订阅次数；数据构造不计入热路径。数据为代表性合成输入，并非线上流量录制，不含数据库、网络或调度耗时。JDK 17.0.18、单线程、512 MB/G1、3×1s 预热、5×1s 测量、2 forks、GC profiler；原始 JSON 为 `target/jmh-real-sql-in-scalar-20261005.json` 和 `target/jmh-nested-sql-in-scalar-20261005.json`。

| 查询形态 | 输入/外层行/s（均值 ± JMH 误差） | 分配 B/输入或外层行 |
| --- | ---: | ---: |
| 16 列混合条件、常量 `IN` | 2.069±0.111 M | 1,920.115 |
| 同结果动态右值 `IN` | 1.529±0.052 M | 2,501.449 |
| 同结果 `=` 对照 | 4.124±0.160 M | 704.102 |
| 混合查询同结果原生控制 | 18.137±2.286 M | 140.705 |
| 16 列函数，两次 JSON / 一次 JSON | 1.463±0.031 / 1.769±0.036 M | 2,841.317 / 2,057.094 |
| 16 列直接投影 | 6.121±0.121 M | 388.818 |
| 三层未关联聚合，订阅内复用 / 显式关闭 | 4.132±0.200 M / 8,277±21 | 1,276.837 / 732,561.373 |
| 相关 lookup | 146,233±1,932 | 28,360.222 |

同配置的旧 `cache()` 动态 `IN` 为 1.036±0.013 M 行/s、4,330.787 B/行，整体替换为 `replay().refCount(1)` 为 0.480±0.010 M、5,824.104 B/行。本次按标量能力分流后，动态 `IN` 相对旧 `cache()` 约 +48% 吞吐、-42% 瞬时分配，且常量 `IN` 与 `=` 对照未出现稳定回退。三层/相关查询和上一版同口径结果接近，不能把跨轮微小差异归因为本次修改。`=` 仅在本夹具给定数据上与 `IN` 同结果，不代表 SQL 一般可互换。

同 JAR 短 JFR 使用 1×1s 预热、2×1s 测量、1 fork，原始文件在 `target/jfr-real-sql-in-scalar-20261005/`，诊断 JSON 为 `target/jmh-real-sql-in-scalar-jfr-20261005.json`。混合常量 `IN` 的 118 CPU/660 分配样本中，`MonoZip$ZipInner` 68、`ZipCoordinator` 47、正则 `Matcher` 40；`[I` 71 个分配样本的代表性栈为 `CastFeature.normalizeType → String.replaceAll`。`CastFeature.createMapper` 已在构建期规范化类型，但 `castValue` 再逐行规范化，这个重复工作是下一步可证伪的通用候选；`LIKE` 的 Matcher 是逐行匹配所需，不能仅凭样本删除。动态 `IN` 的 96 CPU/660 分配样本中 `MonoZip$ZipInner` 46、`Matcher` 33，`InFilter` 相关栈仍可见但不再为左标量建立 replay。函数宽查询的 110 CPU/660 分配样本中，JSON 路径家族覆盖 30 CPU/351 分配样本，分配类以 `byte[]` 105、`String` 74、`FixedArgumentList` 42 为主；两次 JSON 与一次 JSON 的成对分配差支持重复解析/规范化成本，但不授权对可变输入隐式共享。

三层复用查询的 113 CPU/608 分配样本主要可见 `FluxFlatMap` 调度及结果物化：`HashMap$Node` 155、`HashMap` 124、`DefaultReactorQLRecord` 43、`FlatMapInner` 24。相关 lookup 的 127 CPU/642 分配样本中，CPU 叶子为 `HashMap.getNode` 19、`String.equals` 15、`HashMap.putVal` 13、`BinaryFilterFeature.test` 13；分配类为 Map 节点 308、Map 162、Record 74。相关查找若要消除逐外层行候选扫描，应由显式有界/可索引来源提供索引，不能对任意 Publisher 静默建立无界缓存。JFR 栈家族会重叠、样本数不等于字节；JMH GC B/行是**瞬时分配**，本轮没有 live-heap A/B，不能声称常驻堆下降。

### `CAST` 编译期类型复用 A/B（计划，2026-10-05）

目标：消除所有已编译 `CAST` 表达式逐行重复执行类型字符串规范化，而不改变转换语义。依据为上节混合宽查询 JFR：71/660 个 `[I` 分配样本的代表性栈经过 `CastFeature.normalizeType → String.replaceAll`；源码中 `createMapper` 已取得规范化类型，但 `castValue` 每次再次规范化。影响范围仅 `CastFeature`、定向行为测试及本文档；不改 SQL 解析、公开 `castValue(Object,String)` 的任意入参兼容性、标量/异步 Publisher 边界、异常/空值/Context/取消语义或默认限制。

实施：让编译后 mapper 调用只接受已规范化类型的私有转换方法，公开 `castValue` 仍先规范化再委托；不增加缓存、类型枚举、特殊 SQL/类型名分支或自定义操作符。阶段末集中运行完整测试与同 JDK/JVM/JMH 配置下的 16 列混合查询、同结果原生控制和不含 `CAST` 的负对照；短 JFR 验证逐行 `CastFeature.normalizeType` 栈是否消失。仅当结果等价、分配明确下降且吞吐无稳定回退时保留，否则撤回。GC B/行不能证明 live heap 下降。

结果：`CastFeature.createMapper` 的标量和异步路径均复用构建期规范化类型，公开 `castValue(Object,String)` 仍对任意传入类型名执行原规范化后转换。新增直接调用与异步 Context 测试，已有 SQL 测试覆盖 `BIGINT`、`DOUBLE PRECISION` 等类型。完整 `mvn -q -Pjmh package` 通过（462 tests，0 failures/errors/skipped），JAR SHA-256 `4f9693ac418606792eccbe32f786399821d564ed62f585ee8c9a389992571752`，`git diff --check` 通过。

同一 65,536 行、16 列混合查询夹具，JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler 的 before/after 原始结果为 `target/jmh-real-sql-in-scalar-20261005.json` / `target/jmh-cast-normalized-after-20261005.json`：

| 查询 | before→after 输入行/s（均值 ± JMH 误差） | before→after B/输入行 |
| --- | ---: | ---: |
| 混合条件常量 `IN`（含两个 `CAST`） | 2.069±0.111 → 2.247±0.033 M | 1,920.115 → 1,696.699 |
| 同结果 `=`（含两个 `CAST`） | 4.124±0.160 → 4.611±0.068 M | 704.102 → 480.686 |
| 无 `CAST` 的宽投影负对照 | 6.121±0.121 → 6.210±0.174 M | 388.818 → 388.818 |
| 同结果原生控制 | 18.137±2.286 → 18.199±0.345 M | 140.705 → 140.705 |

两条含 `CAST` 查询均少分配约 223.416 B/输入行，观测吞吐约 +9% / +12%；两组吞吐误差区间不重叠，但这仍是给定夹具的收益，不能推广为所有 SQL 固定提升。短 JFR `target/jfr-cast-normalized-after-20261005/`（诊断 JSON `target/jmh-cast-normalized-after-jfr-20261005.json`）中，`CastFeature.normalizeType` 栈从 before 的 6/118 CPU、79/660 分配样本降为 after 的 0/116、0/660；after 仍有 54 个 `[I` 样本，代表性栈来自 `LIKE` 的逐行 `Pattern.matcher`，不能把所有正则分配归因于 `CAST`。JFR 样本数不换算为字节；本轮没有 live-heap A/B。

### 同步与异步条件混合时的逻辑编排（计划，2026-10-05）

最新混合查询 JFR 的 `MonoZip` 对象分配栈明确落在 `AndFilter.createPredicate` 的混合条件回退；同样的双订阅模式也存在于 `OrFilter`。目标是在**恰好一侧明确实现 `ScalarFilter`、另一侧保持 Publisher** 时，不再逐行构造针对同步常量 Mono 的 `zip`、协调器和订阅者。影响范围限 `AndFilter`、`OrFilter`、行为测试、本文档；现有双同步路径与双异步 `zip` 路径不变，不引入自定义 Subscriber、短路、跨行状态或特定 SQL/函数分支。

约束：继续按表达式左右顺序调用谓词，即使同步值已决定布尔结果也必须调用并订阅另一侧，保留异常、空 Mono、Reactor Context、取消和 `AND`/`OR` 既有布尔语义。`ScalarFilter.test` 是该能力的同步权威路径，不能把未声明能力的自定义 Filter 当标量。先覆盖同步侧在左/右、冷异步源、空/错误/取消、两侧副作用次数与 Context，再集中跑全量测试；以当前 JAR `4f9693ac418606792eccbe32f786399821d564ed62f585ee8c9a389992571752` 的同结果混合查询和无异步 `=`/原生负对照作正式 JMH before，短 JFR 验证 `AndFilter → Mono.zip` 栈。仅在功能等价、分配下降、吞吐无稳定回退时保留，否则撤回。

结果：`AndFilter` 和 `OrFilter` 在一侧为 `ScalarFilter` 时同步求值该侧，仍按原左右顺序调用两侧谓词，并在结果 Publisher 上订阅异步侧、处理空流；双同步能力和双异步 `Mono.zip` 路径未改。新 `MixedScalarLogicalFilterTest` 覆盖左右顺序、同步侧已决定结果仍订阅异步侧、空流、错误、取消与 Reactor Context。完整 `mvn -q -Pjmh package` 通过（465 tests，0 failures/errors/skipped），JAR SHA-256 `6f6e976db13e3642a1a7ddd71aeee3b5c0ceaaa75a0427903fa463171d321484`，`git diff --check` 通过。

正式 A/B 使用同一宽 SQL 夹具、JDK 17.0.18、单线程、512 MB/G1、3×1s 预热、5×1s 测量、2 forks、GC profiler；before 为上一节 `target/jmh-cast-normalized-after-20261005.json`，after 为 `target/jmh-mixed-logical-after-20261005.json`：

| 查询 | before→after 输入行/s（均值 ± JMH 误差） | before→after B/输入行 |
| --- | ---: | ---: |
| 16 列混合条件、常量 `IN` | 2.247±0.033 → 2.811±0.048 M | 1,696.699 → 1,024.698 |
| 同结果纯同步 `=` 负对照 | 4.611±0.068 → 4.636±0.189 M | 480.686 → 480.686 |
| 无 `CAST` 的宽投影控制 | 6.210±0.174 → 5.771±0.514 M | 388.818 → 388.818 |
| 同结果原生控制 | 18.199±0.345 → 18.507±0.728 M | 140.705 → 140.705 |

混合 `IN` 查询在此夹具观测吞吐约 +25%、每输入行少约 672 B 瞬时分配，吞吐误差区间不重叠；三组控制的分配未变、吞吐误差区间重叠，不能把控制波动申领为提升或回退。动态 `IN` 的 after 为 1.778±0.027 M 行/s、1,606.033 B/行；本阶段没有同一 `CAST` 基线 JAR 的对应正式测量，不将它的跨阶段差异归因为本次逻辑条件改动。短 JFR `target/jfr-mixed-logical-after-20261005/`（诊断 JSON `target/jmh-mixed-logical-after-jfr-20261005.json`）中，混合 SQL 的 `AndFilter → Mono.zip` 对象分配样本从 before 41/660 降为 after 0/656，`MonoZip$ZipInner` 从 76 降为 0；`MonoDefaultIfEmpty` 等空流语义相关对象仍保留。`OrFilter` 只完成等价行为验证，未对其单独申领吞吐收益。JFR 样本数不等于字节，JMH B/行不是 live heap。

### 当前代码的高基数存活堆与子查询物化复核（2026-10-05）

这是取证，不改生产代码或默认限制。三层未关联子查询的短 JFR 中，33 个 `HashMap$EntryIterator` 分配样本全部落在 `DefaultReactorQLRecord.resultToRecord` 的来源别名与结果 Map 复制路径。`DefaultReactorQLRecordTest` 已验证转换后来源继续修改结果或别名时，派生记录必须隔离；直接共享 Map 会破坏契约，因此不为消除迭代器而引入共享可变状态或复制延迟层。`FluxFlatMap$FlatMapInner` 的 24 个样本只有订阅栈、无足够的所有权归因，不能据此删除响应式边界。

在当前 JAR SHA-256 `6f6e976db13e3642a1a7ddd71aeee3b5c0ceaaa75a0427903fa463171d321484` 上运行既有 `HighCardinalityLiveHeapProbe`：JDK 17.0.18、512 MB/G1、未关闭计数窗口、每键一行、10,000/50,000 活跃键；每阶段显式 GC 后取 `MemoryMXBean` 读数，结果订阅者不收集行。融合路径与 `--compat` 负对照都处理相同输入；`open-active-keys` 阶段输出均为 0。

| 聚合 / 活跃键 | 融合路径 post-GC heapUsed | 兼容路径 post-GC heapUsed |
| --- | ---: | ---: |
| `count` / 10,000 | 5,645,848 B | 27,288,528 B |
| `count` / 50,000 | 12,371,224 B | 120,190,752 B |
| `count/sum/avg/min/max` / 10,000 | 7,083,600 B | 62,145,592 B |
| `count/sum/avg/min/max` / 50,000 | 19,492,616 B | 293,898,968 B |

八个进程在取消后均回到约 4.2 MB 基线。另在同一权限边界对五聚合、50,000 键融合路径的独立 PID 75344 执行 `jcmd GC.class_histogram`：50,000 个 `GroupState`（1.6 MB）、50,000 个 `Accumulator[]`（2.0 MB）、50,000 个 `LinkedHashMap$Entry`（2.0 MB），以及五个标量聚合对应的 `CountAggFeature$2` 50,000、`MapAggFeature$1` 50,000、`$2` 50,000、`$3` 100,000。源码显示 `sum/avg` 只保留数值与计数，`min/max` 各保留一个当前值，没有按输入行驻留历史记录。精确的 50,000 活跃键、五个独立聚合和稳定输出顺序仍要求 O(keys) 状态；旧的“合并分组状态数组”候选已实测撤回，不因直方图重启。上述 heapUsed 是受控进程读数，包含探针、查询计划等其他堆对象基线，不外推为通用每键配额；这次未做新的代码 A/B，不能申领新的堆下降。

### 代表性宽查询与多层子查询复跑（2026-10-05）

仅复跑现有夹具并定位热点，未改生产代码、默认限制或 SQL 语义。当前基准 JAR SHA-256 为 `6f6e976db13e3642a1a7ddd71aeee3b5c0ceaaa75a0427903fa463171d321484`，且 `src/main/java`、`src/jmh/java` 均无比该 JAR 更新的文件。`WideSqlWorkloadBenchmark` 的 65,536 行设备事件上，16 列混合查询包含 `CASE`、`BETWEEN`、`LIKE`、`IN`、`OR`、`CAST`、算术和字符串函数；另一个 16 列函数查询包含两次 JSONPath 和日期函数。`NestedSqlWorkloadBenchmark` 包含三层未关联聚合、同 SQL 关闭订阅内复用的控制组，以及逐行关联 lookup。夹具 setup 全量核对结果和源订阅次数；输入数据是代表性合成事件，并非线上回放，且不包含数据库/网络 I/O。

JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler；JMH 原始数据为 `target/jmh-real-sql-user-refresh-20261005.json` 和 `target/jmh-real-sql-user-refresh-controls-20261005.json`。吞吐均按输入或外层行计，B/行是瞬时分配，不是存活堆。

| 查询形态 | 输入或外层行/s（均值 ± JMH 误差） | 分配 B/行 |
| --- | ---: | ---: |
| 16 列混合运算符/函数 | 2.702±0.091 M | 1,024.698 |
| 同结果原生固定逻辑 | 16.154±0.637 M | 140.705 |
| 16 列双 JSON/日期函数 | 1.474±0.021 M | 2,884.518 |
| 同结果单 JSON 输入控制 | 1.720±0.099 M | 2,067.895 |
| 16 列直接投影 / 同结果原生固定投影 | 5.284±0.253 / 12.051±0.568 M | 388.818 / 316.811 |
| 三层未关联聚合，默认复用 / 显式关闭 | 4.217±0.091 M / 8,310±285 | 1,276.869 / 732,561.389 |
| 逐行关联 lookup | 155,540±14,336 | 28,360.219 |

同 JAR 的短 JFR 为 1×1s 预热、2×1s 测量、1 fork，位于 `target/jfr-real-sql-user-refresh-20261005/`，诊断结果为 `target/jmh-real-sql-user-refresh-jfr-20261005.json`。worker 栈均无 setup/结果 oracle。双 JSON 查询的 112 个 CPU、615 个分配样本中，JSON 解析/路径/规范化栈分别覆盖 37/321 个，日期栈 7/74 个；`FixedArgumentList` 分配类为 37 个。少一次 JSON 的同结果控制组少约 816.6 B/输入行，但 `jsonLevel` 已在输入中预置，不能将这部分当作免费自动优化。混合查询的 96 个 CPU、658 个分配样本中，`DefaultIfEmpty` 相关订阅/操作符对象 122 个，`LIKE` 所需 `Matcher` 及其整数数组分别为 47/91 个；`[I` 的代表性栈明确经过 `LikeFilter`，不是先前已修正的 `CAST` 重复规范化。

三层默认复用查询的 637 个分配样本中，Map 节点 192、Map 92、Record 42、`HashMap$EntryIterator` 39、`FluxFlatMap$FlatMapInner` 23；关闭复用后 Record/Map 物化显著增加。关联 lookup 的 108 个 CPU 样本叶子以 Map 写入/扩容、字符串比较、属性查找和 `BinaryFilterFeature.test` 为主，659 个分配样本中 Map 节点 343、Map 155、Record 79。热点归因仅限采样：类别可重叠，不把样本数换算成 CPU 百分比、字节或常驻堆。派生 Record 复制具有来源变更隔离契约；`defaultIfEmpty` 保留空 Publisher 的布尔结果，不能仅凭对象数删除。下一步若优化，应先用同结果控制组验证通用的 JSON 行内复用或函数参数容器契约，不按 SQL/函数名特调；关联 lookup 的候选扫描需要显式有界/可索引的数据源，不能对任意 Publisher 隐式建无界索引。

### 定长双参数标量函数的参数容器消除（计划，2026-10-05）

目标：在普通内置双参数函数的同步标量路径减少逐行 `FixedArgumentList`，覆盖宽查询 WHERE 和其它同类函数，提升吞吐、降低瞬时分配。依据是同结果直接宽投影 SQL/原生约 5.284/12.051 M 输入行/s、388.818/316.811 B/行；既有短 JFR 的 SQL 路径采到 `FixedArgumentList` 170/660 个分配样本，函数 WHERE 使用公开 List 回调。此差距不全由 List 引起，须通过 A/B 证明收益。

owning module 为 `FunctionMapFeature` 与默认内置函数注册；只新增显式固定双参数标量回调，不改现有 `scalar(..., List)` 的可变/保留参数契约，也不按 SQL 文本、函数名或参数值自动分支。将能直接证明恰好两个参数且行为只依赖两个值的内置函数统一迁移到新入口；空值仍按既有 `defaultValue` 或“忽略空参数”规则处理，无法走直接路径时保留 List 路径。异步参数、distinct/unique、checkpoint、自定义 Feature、错误/Context/取消均保留原 Publisher 链路。不新增跨行缓存、状态或自定义 Subscriber。

步骤：先补固定双参数的直接值调用及 List 兼容回退，再迁移适用的默认内置函数；测试覆盖非空、空值/默认值、异步回退、自定义可变 List 回调与多订阅；阶段末一次完整构建，按同 JDK/JVM/JMH 配置复测直接宽投影、原生对照、16 列函数查询和多层子查询负对照，并用 JFR 确认容器样本变化。只有功能等价、瞬时分配下降且吞吐无稳定回退时保留；否则撤回。GC B/行不能证明常驻堆收益。

结果：`FunctionMapFeature.scalar2` 是显式双值回调；普通 `scalar(..., List)` 的每次调用仍创建独立可变 List。固定两个参数的数值幂、左右截取、前后缀判断、包含与位置查询等内置函数统一改用该入口；缺失参数回原 List 索引/异常规则，默认占位值、异步参数、distinct/unique 与 checkpoint 保留原行为。新增测试逐项核对直接值、默认/缺失参数、异步 Context/取消，并复用既有自定义可变 List 测试。最终 `mvn -q -Pjmh package` 通过（467 tests，0 failures/errors），修正测试断言后定向 12 tests 再通过；`git diff --check` 通过。性能 JAR SHA-256 为 `b105e0409a7efb907acb7268ba6d994118b8b145b557432a2ad150415dac8fe8`。

同 JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler；before 为上节同一夹具 `target/jmh-real-sql-user-refresh-20261005.json`、`target/jmh-real-sql-user-refresh-controls-20261005.json`，after 为 `target/jmh-scalar2-after-20261005.json`、`target/jmh-scalar2-nested-control-20261005.json`。所有 JMH setup 继续全量核对结果与订阅次数。

| 查询 | before→after 输入或外层行/s（均值 ± JMH 误差） | before→after B/行 |
| --- | ---: | ---: |
| 16 列直接宽投影，双参数函数 WHERE | 5.284±0.253→6.734±0.034 M | 388.818→348.818 |
| 16 列函数宽查询 | 1.474±0.021→1.534±0.032 M | 2,884.518→2,783.317 |
| 16 列混合操作符负对照 | 2.702±0.091→2.753±0.055 M | 1,024.698→1,024.698 |
| 同结果原生直接宽投影 | 12.051±0.568→12.557±0.738 M | 316.811→316.811 |
| 三层未关联聚合负对照 | 4.217±0.091→4.207±0.017 M | 1,276.869→1,276.837 |
| 相关 lookup 负对照 | 0.156±0.014→0.153±0.010 M | 28,360.219→28,360.220 |

直接宽投影在该夹具观测吞吐约 +27%、少 40 B/输入行；函数宽查询约 +4%、少 101.2 B/输入行，二者吞吐误差区间不重叠。直接宽投影与固定原生实现的吞吐差由约 2.28 倍收敛到约 1.86 倍，但原生不承担动态 Feature/Record 契约，不能把剩余差距全部视作可消除开销。三个负对照无稳定回退或可归因分配变化。短 JFR 位于 `target/jfr-scalar2-after-20261005/`（1×1s 预热、2×1s 测量、1 fork）：直接宽投影 623 个 worker 分配样本中 `FixedArgumentList` 为 0；函数宽查询仍有 26/659 个，说明其它函数仍使用 List 契约。JFR 只定位对象类别，不换算 B/行或 live heap。保留此按参数能力而非 SQL 名称的优化；它不增加跨行状态，未测新的存活堆 A/B。

### 双参数纯函数 WHERE 的原始行前置过滤（计划，2026-10-05）

目标：在默认单表 Map 源上，让可证明不修改/保留输入值的固定双参数函数组成 `RawScalarValueMapper`，再由通用函数谓词形成 `RawScalarFilter`，使被 `WHERE` 拒绝的行不创建 Record。当前同结果 16 列直接投影为 6.734±0.034 M 输入行/s、348.818 B/行；同 JAR 短 JFR 仍有 66/623 个 `DefaultReactorQLRecord` 分配样本。依据仅支持尝试和 A/B，不预先申领收益。

owning module 为 `FunctionMapFeature` 的新 `scalar2` 契约、`FilterFeature` 函数谓词适配及 `RawWhereBeforeRecordTest`；不改旧 `scalar(..., List)`、自定义异步 Feature、JOIN/子查询/窗口的来源边界，也不按 SQL 文本、函数名称或数据值特判。`scalar2` 明确要求回调不修改/保留输入参数值；只有两侧都声明 `RawScalarValueMapper`、metadata 允许标量快路且未启用 checkpoint 时才传递原始行能力。非 Map 行仍在 `DefaultReactorQL` 现有边界先包装 Record；row.index/elapsed、别名不匹配、自定义 FROM/属性 Feature 保持回退。无跨行缓存、线程本地状态、自定义 Subscriber 或新默认限制；不新增常驻内存、MBean 或逐行 Trace span。

先补 Map/非 Map、别名、空值、错误、需求/取消/Context、自定义 List 函数与自定义 FROM 的行为测试，再实现通用能力组合。阶段末集中跑完整测试和 JMH：直接宽投影、同结果原生、16 列函数查询、混合运算符及多层子查询负对照；短 JFR 验证 Record 分配样本方向。只有行为等价、每输入行分配明显下降且吞吐无稳定回退时保留。GC B/行不证明 live heap 下降。

结果：`scalar2` 回调契约明确不修改或保留输入参数值；当两个参数映射器均声明 `RawScalarValueMapper` 且 metadata 允许时，函数映射器组合出同等的原始行能力。`FilterFeature` 对此类函数谓词组合 `RawScalarFilter`，由现有 `DefaultReactorQL` 单表 Map `handle` 在包装 Record 前执行。普通 `scalar(..., List)`、异步、checkpoint、不匹配别名及自定义 FROM 均保留原路径；未新增状态、缓存或低层 Reactor 操作符。`RawWhereBeforeRecordTest` 新增了 Map/非 Map、匹配与不匹配别名、空值、Map/非 Map 全局计数聚合、需求/取消、Context、源与函数错误、自定义可变 List 函数及自定义 FROM 覆盖。首次完整构建仅因新增测试把 `start(ReactorQLContext)` 的 `ReactorQLRecord` 误断言成 Map 而编译失败，修正测试类型后 `mvn -q -Pjmh package` 通过（470 tests、0 failures/errors）；随后补充的边界断言以定向 8 tests 再通过，`git diff --check` 通过。性能 JAR SHA-256 为 `5337b09e9d77c5afac73db71b490a329bcb4ddbc063bf60bf277d64151efb564`。

JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler 的 before 为 `target/jmh-scalar2-after-20261005.json`、`target/jmh-scalar2-nested-control-20261005.json`，after 为 `target/jmh-raw-scalar2-after-20261005.json`、`target/jmh-raw-scalar2-nested-control-20261005.json`；每个夹具 setup 仍核对相同结果及订阅次数。

| 查询 | before→after 输入或外层行/s（均值 ± JMH 误差） | before→after B/行 |
| --- | ---: | ---: |
| 16 列直接宽投影、函数 WHERE | 6.734±0.034→6.756±0.330 M | 348.818→331.217 |
| 16 列函数宽查询 | 1.534±0.032→1.559±0.019 M | 2,783.317→2,765.716 |
| 同结果原生直接宽投影 | 12.557±0.738→12.256±0.958 M | 316.811→316.811 |
| 16 列混合操作符负对照 | 2.753±0.055→2.745±0.039 M | 1,024.698→1,024.698 |
| 三层未关联聚合负对照 | 4.207±0.017→4.145±0.098 M | 1,276.837→1,276.837 |
| 相关 lookup 负对照 | 0.153±0.010→0.162±0.001 M | 28,360.220→28,360.218 |

两条函数 WHERE 宽查询各少约 17.6 B/输入行瞬时分配；所有吞吐误差区间与 before 均重叠，不能申领稳定吞吐提升或回退。直接宽投影相对固定原生对照的分配差从约 32.0 缩至 14.4 B/行，原生对照仍不承担动态 Feature/Record 契约。短 JFR `target/jfr-scalar2-after-20261005/` 与 `target/jfr-raw-scalar2-after-20261005/`（1×1s 预热、2×1s 测量、1 fork）中，直接宽投影 `DefaultReactorQLRecord` 分配样本为 66/623→3/652；只作为被拒行提前过滤的方向证据，不把样本数换算字节或 live heap。按既定门槛保留此通用能力组合；本轮未测存活堆 A/B，也未改变高基数聚合的 O(活跃键数) 状态。

### 当前构建的真实形态 SQL 压测与 JFR 热点（2026-10-05）

仅复测现有夹具并定位热点，不改生产代码或默认限制。性能 JAR SHA-256 仍为 `5337b09e9d77c5afac73db71b490a329bcb4ddbc063bf60bf277d64151efb564`，`src/main/java` 和 `src/jmh/java` 均无比该 JAR 更新的文件。65,536 条预构造设备事件上的两种 16 列 SELECT 分别覆盖混合 `CASE/BETWEEN/LIKE/IN/OR/CAST`、算术/字符串函数，以及双 JSONPath/日期函数；子查询包括三层未关联聚合、同 SQL 关闭订阅内复用的控制组、逐外层行关联 lookup。另测 50,000 行、每键两行的窗口 `count/sum/avg/max`。全部夹具在 setup 核对结果与源订阅次数，输入构造、数据库和网络 I/O 不计入热路径；这是代表性合成负载，不是线上回放。

JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler；原始 JSON 为 `target/jmh-real-sql-current-20261005.json` 和 `target/jmh-real-sql-highcard-current-20261005.json`。吞吐按输入行或外层行归一化；不同查询工作量不能横向直接排名，B/行只表示瞬时分配，不是 live heap。

| 查询形态 | 输入或外层行/s（均值 ± JMH 误差） | 分配 B/行 |
| --- | ---: | ---: |
| 16 列混合运算符/函数 | 2.760±0.080 M | 1,024.698 |
| 同结果固定原生混合逻辑 | 17.484±0.671 M | 140.705 |
| 16 列双 JSONPath/日期函数 | 1.572±0.071 M | 2,765.716 |
| 16 列直接投影 | 7.171±0.337 M | 331.217 |
| 三层未关联聚合，默认复用 / 关闭复用 | 4.238±0.009 M / 8,494±132 | 1,276.837 / 732,561.373 |
| 逐行关联 lookup | 154,482±1,456 | 28,360.220 |
| 高基数窗口四聚合 / 固定原生控制 | 5.197±0.239 M / 18.789±1.813 M | 410.524 / 206.478 |

同 JAR 的短 JFR（1×1s 预热、2×1s 测量、1 fork）位于 `target/jfr-real-sql-current-20261005/` 和 `target/jfr-real-sql-highcard-current-20261005/`。宽混合查询的 104 个 worker CPU、649 个分配样本中，`LIKE`/正则调用栈覆盖 22/135 个；80 个 `int[]` 分配样本均经过 `LikeFilter`，`DefaultIfEmpty` 类对象合计 125 个。宽函数查询 122/660 个样本中，JSON/JSONPath 相关栈覆盖 35/361 个，仍有 33 个 `FixedArgumentList` 分配类样本。三层默认复用查询 120/658 个样本中，Map/Record 调用栈或对象类分别覆盖 63/464 个；关联 lookup 为 117/628，对应 67/609 个。四聚合 JFR 的 CPU 栈还采到原生 oracle 的 `NativeState.result`，因此不拿该短录制计算 SQL 聚合 CPU 占比；其 Map、每键累加器和输出物化对象仅作方向线索。前四条 worker 栈未出现 setup/结果 oracle。JFR 类别可重叠且有采样偏差，样本数不能换算 CPU 百分比、字节或存活堆。

下一步候选按证据优先级为：先研究保持现有正则语义的通用字面量 `LIKE` 快路，并用换行、正则元字符、动态参数差分验证；JSON 同行复用须先解决可变输入和自定义 Feature 契约；`DefaultIfEmpty` 不能仅凭对象数删除空 Publisher 语义。关联查询若要减少重复扫描，需要业务显式提供有界/可索引 lookup，不能对任意 Publisher 隐式建立无界索引。此处仅提出待验证候选，未申领新的性能改进。

### 字面量 LIKE 通用快路（计划，2026-10-05）

目标：在保持当前 `LIKE` 的 Java 正则语义下，减少普通字面量模式逐行创建 `Matcher` 和整数数组的成本。owning module 仅 `LikeFilter`，回归位于其过滤测试；现有同 JAR 16 列混合查询基线为 `2.760±0.080 M` 输入行/s、`1,024.698 B/行`，短 JFR 有 135/649 个 `LIKE` 调用栈分配样本。模式分类只依据通用语法形态：无正则元字符的精确值、单个边缘 `%` 或两端各一个 `%`，其余全部保留原 `Pattern` 路径；仍在创建谓词时编译原正则，以维持非法模式错误时机。`%` 当前映射为 Java `.*`，它默认不能跨 `\n/\r/U+0085/U+2028/U+2029`，因此包含这些行终止符的输入使用原正则判定，不直接用 `startsWith/endsWith/contains`；动态参数、异步 mapper、空值、`NOT LIKE` 和扩展 Feature 保持现有行为。

先补当前正则 oracle 的差分测试（边缘 `%`、无 `%`、多重/内部 `%`、正则元字符、下划线、空值、五类行终止符、动态模式与 `NOT LIKE`），再实现最小编译期分类。阶段末一次完整构建；以相同 JDK/堆/JMH 设置对比混合 16 列查询、同结果原生控制、宽函数和三层子查询负对照，短 JFR 验证 `Matcher`/`int[]` 样本方向。仅在结果等价、分配下降且吞吐无稳定回退时保留。不得按 SQL 文本或业务字段名特调，不增加公共 SPI、自定义 Subscriber、跨行缓存或默认限制；GC B/行不证明存活堆收益。

结果：`LikeFilter` 仍在创建谓词时编译原 `Pattern`，只对可证明简单的字面量模式选择 `equals/startsWith/endsWith/contains`；匹配候选命中后才检查行终止符，含行终止符或复杂正则语法继续由原 `Pattern` 判定。右侧动态模式、异步路径、空值和 `NOT LIKE` 沿用原语义；无 Reactor 操作符、公共 SPI 或跨行状态变化。`LikeFilterTest` 新增正则 oracle 差分和逐行动态模式回归。最终 `mvn -q -Pjmh package` 通过（473 tests，0 failures/errors），`git diff --check` 通过。正式 A/B 使用 JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler；before JAR 为 `5337b09e9d77c5afac73db71b490a329bcb4ddbc063bf60bf277d64151efb564`，after 顺序调整的测量 JAR 为 `6c1dd5a0f62d488c0570172d13971724ba099829f38d3a00a5d7c0d924498468`。最终全测试重打包 JAR 为 `76763d03878a3abc63a969da3fbf0230582f151b793f49beef50fbbee800e772`，重打包前后生产/JMH 源码未再变更。原始 JSON 为 `target/jmh-real-sql-current-20261005.json`、`target/jmh-like-literal-after-20261005.json` 和 `target/jmh-like-literal-order-after-20261005.json`。

| 查询 | before→after 输入或外层行/s（均值 ± JMH 误差） | before→after B/行 |
| --- | ---: | ---: |
| 16 列混合运算符/函数 | 2.760±0.080→2.789±0.029 M | 1,024.698→792.963 |
| 同结果固定原生混合逻辑 | 17.484±0.671→17.846±0.629 M | 140.705→140.705 |
| 16 列双 JSONPath/日期函数 | 1.572±0.071→1.554±0.029 M | 2,765.716→2,765.716 |
| 三层未关联聚合 | 4.238±0.009→4.221±0.061 M | 1,276.837→1,276.837 |
| 逐行关联 lookup | 154,482±1,456→157,914±12,654 | 28,360.220→28,360.219 |

混合查询每输入行少约 231.7 B 瞬时分配；吞吐区间与 before 重叠，因此不宣称稳定提速。所有负对照吞吐区间重叠、分配基本不变。首版快路短 JFR `target/jfr-like-literal-after-20261005/` 显示 `LikeFilter.hasLineTerminator` 为 14/125 个 CPU 叶子样本，促使将低成本匹配提前；最终短 JFR `target/jfr-like-literal-order-after-20261005/` 中该叶子为 5/120，623 个 worker 分配样本未采到 `Matcher`、`int[]` 或 `LikeFilter` 栈（before 混合查询为 649 个分配样本，`LIKE` 栈 135，`int[]` 80）。这仅支持热点方向，不换算字节或 CPU 百分比；仍有 `DefaultIfEmpty` 类对象 138 个样本，不能不顾空 Publisher 语义删除。按预定门槛保留此通用快路；本阶段未做新的 live-heap A/B，不申领常驻堆下降。

剩余 `MonoDefaultIfEmpty` 的创建栈落在 `AndFilter` 混合同步/异步谓词分支；对应订阅者样本只显示订阅链，不能再按类名推断是哪一个叶子表达式迫使回退。下一阶段先识别该叶子的 mapper 能力与空流契约，再决定是否存在可通用消除的操作符；不在这里删除 `defaultIfEmpty(false)`。

### 常量 IN 与同步组合边界诊断（2026-10-05）

仅使用当前 JAR `76763d03878a3abc63a969da3fbf0230582f151b793f49beef50fbbee800e772` 的现有同结果夹具取证，未改生产代码。`operatorMixWideProjection` 与 `operatorMixWithoutIn` 只将 `status in ('online','unknown')` 换为 `status = 'online'`；setup 已逐行核对本批事件输出和单次冷源订阅。该等价只针对夹具数据，不是 SQL 语义等价或可自动重写依据。JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler，原始结果 `target/jmh-in-vs-equals-like-current-20261005.json`：常量 `IN` 为 `2.889±0.065 M` 输入行/s、`792.963 B/行`；等值条件为 `5.066±0.045 M`、`248.951 B/行`。不同谓词能力触发不同编排，不能把约 544 B/行差额全部归因于 `DefaultIfEmpty`。

短 JFR `target/jfr-like-literal-order-after-20261005/` 与 `target/jfr-operator-mix-equals-current-20261005/` 只用于定位：`IN` 路径的 623 个 worker 分配样本中有 `MonoDefaultIfEmpty` 55、`DefaultIfEmptySubscriber` 83、`MonoMap` 44、`MonoSupplier` 15，以及 `InFilter`/`AndFilter` 的逐行 lambda；同结果等值路径以输出 Map/Record 为主，未采到这些 `DefaultIfEmpty` 类对象。源码判别：`InFilter.scalarCandidates` 可预编译右侧常量，但左侧属性运行时仍可能是 `Iterable`、`Publisher` 或单字段 Map，需要展开与取消语义，所以返回普通响应式谓词而非 `ScalarFilter`；嵌套 `AndFilter` 因此使用混合分支并以 `defaultIfEmpty(false)` 兼容任意可能为空的子谓词。不能将常量 `IN` 一概标记成同步，也不能凭当前 JFR 直接删除空流兜底。若未来引入“必发一个布尔值”等额外能力契约，应先以多个操作符和自定义 Feature 的空流/错误/取消反例证明通用收益，避免为该 SQL 增加复杂 SPI；当前暂停该候选，转向其它低复杂度热点。

### 必发布尔谓词的组合兜底收敛（计划，2026-10-05）

复核上述取证后，候选收敛为“正常完成时必发一个布尔值”这一内部能力，而不是把 `IN` 误称同步。`InFilter` 的内置三条路径最终均以 `Mono.fromSupplier`、`Flux.any` 或 `doPredicate(...).any` 发出布尔值；任意左侧 `Iterable/Publisher/Map` 仍按原路径订阅、展开和取消。`AndFilter/OrFilter` 的异步组合若已对可能为空的子谓词补过一次 `false`，其结果本身也必发一个布尔值；外层无需重复追加 `defaultIfEmpty(false)`。目标是在不改变非空/空流、错误、订阅和顺序语义时减少多层布尔组合的 Reactor 对象，尤其是当前 16 列混合查询中 JFR 可见的 `DefaultIfEmpty` 族。

owning module 仅 `supports/filter`：加一个包内能力标记，内置 `InFilter` 只在精确原类而非可重写子类时声明；`AndFilter/OrFilter` 按能力有条件保留空流兜底并传播结果能力。自定义 Feature、未声明能力的异步谓词、子类覆写、动态/错误/取消路径保持兼容。不新增公共 SPI、自定义 Subscriber、线程本地状态、缓存、默认限制或 SQL 文本特调。先补嵌套 AND/OR 的空源、错误、取消、Context、两侧求值顺序和 `IN` 的集合/Publisher 反例，再实现；阶段末一次完整构建，同 JDK/JMH 配置 A/B 当前 `IN`、同结果 `=`、宽函数和子查询负对照，短 JFR 复核对象来源。只有行为等价、分配显著下降、吞吐无稳定回退才保留，否则撤回。瞬时分配不等于 live heap。

结果：新增仅包内可见的 `TotalBooleanPredicate`，契约仅承诺正常完成时发出一个布尔值，不声称同步或保证及时完成。内置 `InFilter` 的三条路径通过 `fromSupplier/any` 满足该契约；可重写 `asFlux/doPredicate` 的子类不声明它。`AndFilter/OrFilter` 对未声明能力的子谓词仍执行原空流兜底，已兜底的组合结果继续传播该能力。未更改 Publisher 参数订阅、展开、取消、错误、Context 或两侧求值顺序，也未增加公开 SPI、自定义 Reactor 操作符或跨行状态。`MixedScalarLogicalFilterTest` 与 `InFilterScalarTest` 新增嵌套空流、错误、取消、Context、Publisher 左值和可覆写子类反例；最终 `mvn -q -Pjmh package` 通过（476 tests、0 failures/errors），`git diff --check` 通过。after 性能 JAR SHA-256 为 `e00a73e1e74cd0eade04f9b92175f87f9a28a86a18ab73c12ad3f128e5952552`。

同 JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler；before 是 JAR `76763d03878a3abc63a969da3fbf0230582f151b793f49beef50fbbee800e772` 的 `target/jmh-in-vs-equals-like-current-20261005.json`，after 为 `target/jmh-total-bool-after-20261005.json`。宽函数/子查询的 before 取前节同源码相关夹具结果；它们不经过本次改动的 `IN` 路径。夹具 setup 均核对全部输出字段、值、顺序和源订阅次数；宽混合查询另核对每列结果类型。

| 查询 | before→after 输入或外层行/s（均值 ± JMH 误差） | before→after B/行 |
| --- | ---: | ---: |
| 16 列混合操作符、常量 `IN` | 2.889±0.065→3.137±0.036 M | 792.963→624.963 |
| 同结果 `=` 负对照 | 5.066±0.045→4.948±0.088 M | 248.951→248.951 |
| 16 列双 JSONPath/日期函数 | 1.554±0.029→1.580±0.042 M | 2,765.716→2,765.716 |
| 三层未关联聚合 | 4.221±0.061→4.194±0.037 M | 1,276.837→1,276.837 |
| 逐行关联 lookup | 157,914±12,654→158,714±6,003 | 28,360.219→28,360.219 |

混合 `IN` 夹具观测吞吐约 +8.6%、瞬时分配少约 168 B/输入行（约 21.2%）；吞吐误差区间不重叠。其它查询的吞吐区间重叠、分配基本不变，不能外推固定收益或回退。after 短 JFR `target/jfr-total-bool-after-20261005/`（1×1s 预热、2×1s 测量、1 fork）在 633 个 worker 分配样本中未采到 `DefaultIfEmpty` 类对象；before 的相同混合查询为 623 个样本中 138 个。剩余热点以 `MonoMapFuseable`、条件订阅者、逐行 lambda 与输出 Map/Record 为主。JFR 样本只确认对象类别方向，不换算分配字节、CPU 百分比或 live heap；本阶段未测常驻堆 A/B，按预定门槛保留此内部能力。

### 固定三参数纯标量函数直调（计划，2026-10-05）

目标：减少通用固定三参数同步函数每行创建的 `FixedArgumentList`，同时避免改动公开的可变 List 回调契约。当前宽函数 SQL 约 `1.580±0.042 M` 输入行/s、`2,765.716 B/行`；既有同函数路径短 JFR 在 660 个分配样本中采到 33 个 `FixedArgumentList`，不足以预先断言吞吐收益。owning module 为 `FunctionMapFeature` 和内置函数注册/实现 `DefaultReactorQLMetadata`；测试在 `FunctionMapFeatureCompatibilityTest` 及现有函数回归。

新增与现有 `scalar2` 对称的显式三值回调，只在恰好三个参数均为 `ScalarValueMapper`、非 checkpoint、非 `distinct/unique` 时直调；参数为 null 且无默认值时仍构造独立 List 交给原回调，以保留跳过缺参、索引/异常规则；有默认值则逐位置替换。两参数调用、异步 Publisher、自定义可变 List 回调和扩展 Feature 均走原路径。首批迁移现有固定三参数调用频繁的字符串/日期内置函数，并在原 List 方法中只抽取同一计算内核，不按 SQL 文本或业务字段特调。不新增逐行缓存、自定义 Reactor 操作符、公共查询计划状态或资源限制。先补三参数值/空值/默认值、两参数回退、异步 Context/取消和自定义可变 List 测试；阶段末集中完整构建，同配置 JMH 对比宽函数、直接宽投影、混合操作符、多层子查询负对照和短 JFR。只有功能等价、分配下降且吞吐无稳定回退时保留；B/行不是 live heap。

结果：`scalar3` 仅在固定三参数均为同步标量、且无 `distinct/unique` 与 checkpoint 时调用三值回调；缺参、可选参数、异步及自定义 List 回调仍保留原路径。迁移 `replace`、`substring`、`split_part`、`date_add`、`date_sub`、`date_diff`，复用原计算内核。新增兼容测试覆盖缺参/默认值、可选第三参数、异步 Context 与取消。最终 `mvn -q -Pjmh package` 通过，Surefire 汇总 478 tests、0 failures/errors/skipped；性能 JAR SHA-256 为 `448e428eeb458ea7e2188d4f1db46a0924cf2427878973ed67e6432f189b5767`。

代表性场景均复用预构造输入，setup 核对结果、类型、顺序与源订阅次数，热路径流式消费结果；数据为合成设备事件/lookup，不含数据库、网络或调度成本。JDK 17.0.18、单线程、512 MB/G1、3×1s 预热、5×1s 测量、2 forks、GC profiler；吞吐按输入或外层行归一化，B/行是瞬时分配。原始 JSON：`target/jmh-real-sql-three-arg-after-20261005.json`。与上一阶段 JAR `e00a73e1e74cd0eade04f9b92175f87f9a28a86a18ab73c12ad3f128e5952552` 的 `target/jmh-total-bool-after-20261005.json` 比较：

| 查询形态 | before→after 输入或外层行/s | before→after B/行 |
| --- | ---: | ---: |
| 16 列双 JSONPath/日期函数 | 1.580±0.042→1.645±0.031 M | 2,765.716→2,657.713 |
| 16 列混合运算符/函数 | 3.137±0.036→3.047±0.111 M | 624.963→618.616 |
| 三层未关联聚合，默认复用 | 4.194±0.037→4.158±0.197 M | 1,276.837→1,276.837 |
| 逐行相关 lookup | 0.159±0.006→0.150±0.009 M | 28,360.219→28,360.221 |

同一新 JAR 的 16 列直接投影为 6.719±0.719 M 行/s、331.217 B/行，同结果原生投影为 13.594±0.718 M、316.811 B/行；混合查询的同结果原生固定逻辑为 18.228±0.728 M、140.705 B/行。关闭三层未关联子查询的订阅内复用后仅 8,571±283 外层行/s、732,561.373 B/行，说明复用边界对多层查询至关重要，但该有限夹具不能外推无界流的 live heap。宽函数分配少约 108 B/输入行（约 3.9%），吞吐点估计高约 4.1%，但本次与基线 JMH 误差区间仍略有交叠，不声称稳定提速；负对照无可归因的稳定回退。保留三参数直调的依据是功能等价、明确的分配下降及未观察到稳定吞吐回退，而不是仅看吞吐点估计。

短 JFR（1×1s 预热、2×1s 测量、1 fork）在 `target/jfr-real-sql-three-arg-after-20261005/`，诊断 JSON 为 `target/jmh-real-sql-three-arg-jfr-20261005.json`。宽函数查询的 worker 分配样本中 `FixedArgumentList` 从此前同查询 33/660 降为 5/658；本轮 128 个 CPU 与 658 个分配样本的调用栈中，JSON/JSONPath 相关分别覆盖 20/164，日期相关 15/107，Map/Record 相关 32/180（类别重叠）；`pow` 的 `Math.pow`/数值转换入口也是 CPU 叶子热点，属真实函数计算。混合查询的 659 个分配样本以 `HashMap$Node`、`FluxMapFuseable$MapFuseableConditionalSubscriber` 各 101、`MonoMapFuseable` 71 为主，未采到 `FixedArgumentList`。三层默认复用的 649 个分配样本以 Map 节点 170、Map 108、Record 51 及 Reactor 内部订阅者为主；逐行相关 lookup 的 652 个分配样本中 Map 节点 302、Map 186、Record 72，CPU 叶子集中于字符串比较、属性查找与 Map 写入。JFR 运行中一次 JVM 报出 sampler 警告，但所有目标录制仍有 104–128 个 worker CPU、649–659 个 worker 分配样本；因此只作方向性热点定位，不换算 CPU 百分比、精确字节或常驻堆。

后续候选按通用契约审视：同一行多次 JSONPath 读取可研究解析/规范化复用，但必须先证明输入 Map 可变性、自定义 Feature 与 Context 兼容；逐行相关 lookup 对通用 Publisher 必须重新订阅/扫描，若来源本身可提供有界索引或 JOIN 能力，应从来源契约入手，不能在引擎内部隐式无界建索引；三层查询的 Map/Record 复制承担别名与变更隔离，不能仅凭样本删除。当前不新增特调、跨行缓存、自定义 Subscriber 或操作符；未做新的 live-heap A/B，故不申领常驻堆下降。

### 混合同步/异步 AND 的标量组合（计划，2026-10-05）

目标：消除连续 `AND` 中多个同步条件围绕一个异步谓词时逐层产生的 `Mono.map`，但保持每一侧在装配结果 Publisher 时都求值、异步源照常订阅，以及空流兜底、错误、取消和 Context 语义。当前混合 16 列 SQL 的短 JFR 在 659 个分配样本中有 71 个 `MonoMapFuseable`，其中 25/25/21 个分别来自 `AndFilter` 三层混合组合，另有 101 个 `MapFuseableConditionalSubscriber` 由这些 `Mono.map` 的订阅链创建；这是跨 `AND` 表达式形态的共有编排成本，而不是特定 SQL 或字段。前节同 JAR 正式基线为 3.047±0.111 M 输入行/s、618.616 B/行。

owning module 仅 `AndFilter`、`MixedScalarLogicalFilterTest`、现有 JMH 和本文档。只在已存在的 `ScalarFilter` 与异步谓词混合分支，将查询构建期已知的相邻标量条件顺序组合到一个内部谓词；逐行仍先按原顺序执行左侧/右侧标量求值和异步 `apply`，只对最终异步布尔结果做一次 `map`，并沿用 `TotalBooleanPredicate.defaultFalseIfNeeded`。两个异步子树仍用原 `Mono.zip`，全同步路径不变。不得短路跳过副作用、改变空流/错误时序、缓存结果、引入新公共 SPI 或自定义 Subscriber。若左/右嵌套次序、取消或 Context 无法证明等价则撤回。

先补左右嵌套与标量/异步 `apply` 调用顺序、空流、错误、取消、Context 测试；阶段末集中运行完整构建及同配置 JMH（混合宽查询为收益场景，16 列函数、直接宽投影和三层子查询为负对照），短 JFR 核对 `AndFilter` 的 `MonoMapFuseable` 来源。只有分配下降、吞吐无稳定回退且功能等价才保留；B/行不代表 live heap。

结果：`AndFilter.MixedScalarAnd` 仅为已识别的标量/异步混合 `AND` 组合相邻标量判断；逐行按原顺序先执行前置标量、异步 `apply`、后置标量，均不短路，然后对异步布尔结果做一次 `Mono.map`。两个异步子树仍用 `Mono.zip`，全同步分支、未知自定义 Feature 和已有 `TotalBooleanPredicate` 空流边界不变。新增左右嵌套测试记录 `trace_a → async.apply → trace_b → trace_c → async.subscribe`，即使 `trace_a/trace_b` 为 false 也继续调用；既有空流、错误、Context、取消及 Publisher 左值测试均通过。完整 `mvn -q -Pjmh package` 通过，479 tests、0 failures/errors/skipped；`git diff --check` 通过。当前性能 JAR SHA-256 为 `45aef823e73c9add9ca4828c1d2c84bc114e81a3ce6720d16a5c5fdd59df120a`。

同 JDK 17.0.18、单线程、512 MB/G1、3×1s 预热、5×1s 测量、2 forks、GC profiler；before 为上一节 JAR `448e428eeb458ea7e2188d4f1db46a0924cf2427878973ed67e6432f189b5767` 的 `target/jmh-real-sql-three-arg-after-20261005.json`，after 为 `target/jmh-mixed-and-fusion-after-20261005.json`。夹具 setup 对 SQL 与控制组逐行核对值、类型、顺序和源订阅次数；合成输入不含数据库/网络 I/O，B/行表示瞬时分配。

| 场景 | before→after 输入或外层行/s（均值 ± JMH 误差） | before→after B/行 |
| --- | ---: | ---: |
| 16 列混合运算符/函数 | 3.047±0.111→3.863±0.107 M | 618.616→474.615 |
| 16 列双 JSONPath/日期函数 | 1.645±0.031→1.646±0.023 M | 2,657.713→2,657.713 |
| 16 列直接投影 | 6.719±0.719→7.188±0.160 M | 331.217→331.217 |
| 同结果原生混合逻辑 | 18.228±0.728→18.441±0.584 M | 140.705→140.705 |
| 三层未关联聚合 | 4.158±0.197→4.285±0.025 M | 1,276.837→1,276.837 |

混合查询点估计吞吐约 +26.8%，分配少 144 B/输入行（约 23.3%），JMH 误差区间不重叠；其它场景的分配不变、吞吐区间重叠，不申领稳定提升或回退。短 JFR `target/jfr-mixed-and-fusion-after-20261005/`（1×1s 预热、2×1s 测量、1 fork，诊断 JSON `target/jmh-mixed-and-fusion-jfr-20261005.json`）中，`MonoMapFuseable` 分配类样本从 71/659 降到 23/640，后者均来自 `MixedScalarAnd.apply` 的单层 `map`；`MapFuseableConditionalSubscriber` 从 101 降到 40。样本只用于确认对象来源和方向，不能换算精确字节、CPU 百分比或 live heap。按既定门槛保留；未增加跨行状态、缓存、自定义 Subscriber 或 SQL 特调。

### 当前 JAR 的高基数存活堆复核与 `IN` 延迟比较边界（2026-10-05）

继续用 JAR `45aef823e73c9add9ca4828c1d2c84bc114e81a3ce6720d16a5c5fdd59df120a` 和既有 `HighCardinalityLiveHeapProbe`，JDK 17.0.18、512 MB/G1、open 窗口、每键一行、显式 GC 后读取 `MemoryMXBean`，结果订阅者不收集行；`--compat` 作为相同输入的负对照。两种模式在 `open-active-keys` 阶段都尚未输出。当前 post-GC `heapUsed` 为：

| 聚合 / 活跃键 | 融合路径 | 兼容路径 |
| --- | ---: | ---: |
| `count` / 10,000 | 5,652,960 B | 27,248,904 B |
| `count` / 50,000 | 12,378,960 B | 120,147,784 B |
| `count/sum/avg/min/max` / 10,000 | 7,091,464 B | 62,281,880 B |
| `count/sum/avg/min/max` / 50,000 | 19,502,304 B | 293,939,368 B |

八个进程取消后均回到约 4.2 MB；融合路径从 10,000 到 50,000 键的斜率约为 `count` 168 B/新增键、五聚合 310 B/新增键，与前次 JAR 同口径结果接近。这证明本轮行级优化未见该探针下的常驻堆回退，不代表所有 SQL 或生产负载的 live heap 均已最小化。当前混合查询 JFR 还有 `InFilter` 的 `MonoSupplier`/lambda 样本，但现有 `IN` 契约要求常量候选比较在订阅时执行；直接改为 `Mono.just(立即比较)` 会移动用户值 `equals` 的副作用与异常时机，因此不为少几个对象破坏懒执行。

### 混合同步/异步 OR 的真实 SQL 取证（计划，2026-10-05）

目标：确认连续 `OR` 中标量条件围绕一个可异步的 `IN` 时是否也产生多个逐行 `Mono.map`，并仅在真实成本显著且语义可保持时复用 `AND` 的通用组合办法。先扩展 `WideSqlWorkloadBenchmark`：同一批 65,536 个设备事件和同一 16 列投影，用 `(status in ('online','unknown') or battery > 75 or signal < -70)` 代表状态/电量/信号的告警放行条件；setup 全量核对 SQL/原生等价输出、类型、顺序、单次源订阅及三分支覆盖，热路径不收集结果。原有混合 `AND` 查询、直接投影、函数宽查询和原生控制作为负对照；不改生产代码、默认限制或 `OR` 语义。

阶段一集中构建并录同配置 JMH/JFR 基线，只有 `OrFilter` 的嵌套 `Mono.map` 构成稳定热点才进入生产 A/B。若进入阶段二，只在已识别的 `ScalarFilter` 与异步谓词混合时按原顺序组合标量求值，仍订阅异步侧、保留空流 false、错误、取消、Context 和两异步 `Mono.zip`；不短路副作用、不新增公共 SPI、缓存或自定义 Subscriber。新增左右嵌套调用顺序与信号测试，阶段末一次完整构建、同配置成对 JMH 与短 JFR。只有功能等价、分配下降、吞吐无稳定回退才保留。

阶段一结果：JMH 夹具 setup 已核对全部输出行、16 列字段/值/类型、顺序、源单次订阅以及状态、电量、信号三个 OR 分支；`mvn -q -Pjmh -DskipTests package` 通过。基线 JAR SHA-256 为 `de89f5ed0bea313f72a9dc69a37010abcfe9a0e9a65167d6167ae2ebd858f008`。JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler 的 SQL 为 2.687±0.023 M 输入行/s、678.660 B/行；同结果原生控制为 15.986±0.689 M、190.605 B/行。原始数据 `target/jmh-mixed-or-baseline-20261005.json`。短 JFR `target/jfr-mixed-or-baseline-20261005/`（1×1s 预热、2×1s 测量、1 fork）有 101 个 worker CPU、658 个分配样本；`MonoMapFuseable` 67 个，其中 20+19 个分别直接由嵌套 `OrFilter` 的两层 `Mono.map` 装配，另有 28 个来自外层混合 `AND` 的必要组合。`MapFuseableConditionalSubscriber` 为 100 个样本。该诊断足以进入通用 OR 标量组合 A/B，但样本不能换算字节或 live heap。

阶段二结果：`OrFilter.MixedScalarOr` 只在已识别的标量/异步混合 OR 中，将相邻标量条件按原调用顺序组合；即使前置标量为 true 也继续调用后置标量与异步 `apply`/订阅。异步空流仍先按 false 处理，再与标量结果 OR；两异步子树的 `Mono.zip`、全同步/RawScalarFilter、未知自定义 Feature、错误、Context、取消及默认限制不变。新测试记录两种左右嵌套的 `trace_a → async.apply → trace_b → trace_c → async.subscribe`，既有混合 OR 空流、错误、取消和 Context 反例继续通过。完整 `mvn -q -Pjmh package` 通过（480 tests、0 failures/errors/skipped）；`git diff --check` 通过。after JAR SHA-256 为 `2f619755077d2c8e766b3593f97b066ec6e18f99b8633cf2983e7c5bf432445e`。

同 JDK/JVM/JMH 配置的成对结果位于 `target/jmh-mixed-or-baseline-20261005.json` 与 `target/jmh-mixed-or-after-20261005.json`；后者的负对照 before 取上一节同生产实现的 `target/jmh-mixed-and-fusion-after-20261005.json`：

| 场景 | before→after 输入或外层行/s（均值 ± JMH 误差） | before→after B/行 |
| --- | ---: | ---: |
| 16 列混合 OR SQL | 2.687±0.023→2.857±0.060 M | 678.660→606.660 |
| 同结果原生混合 OR | 15.986±0.689→16.712±0.537 M | 190.605→190.605 |
| 原混合 AND SQL | 3.863±0.107→3.903±0.037 M | 474.615→474.615 |
| 16 列双 JSONPath/日期函数 | 1.646±0.023→1.640±0.021 M | 2,657.713→2,657.713 |
| 三层未关联聚合 | 4.285±0.025→4.248±0.073 M | 1,276.837→1,276.837 |

混合 OR 点估计吞吐约 +6.3%，瞬时分配少 72 B/输入行（约 10.6%），JMH 误差区间不重叠；负对照的分配不变、吞吐区间重叠。after 短 JFR `target/jfr-mixed-or-after-20261005/`（诊断 JSON `target/jmh-mixed-or-after-jfr-20261005.json`）中，两个独立 `OrFilter` `Mono.map` 装配点收敛为 `MixedScalarOr.apply` 的一个点；worker 样本中 `MonoMapFuseable` 为 67/658→57/660，`MapFuseableConditionalSubscriber` 为 100→63。采样数只验证对象来源和方向，精确分配以 JMH GC profiler 为准；无新的 live-heap A/B。按既定门槛保留，不引入特调、缓存或手写 Reactor 操作符。

### 跨场景 JFR 复核与 DISTINCT 夹具隔离（计划，2026-10-05）

继续在同一最新 JAR `2f619755077d2c8e766b3593f97b066ec6e18f99b8633cf2983e7c5bf432445e` 上复用已有 profiling 入口，短 JFR `target/jfr-global-sweep-20261005/` 覆盖高基数聚合、DISTINCT、Top-N、普通投影和相关子查询；这些 1×1s 预热、2×1s 测量、1 fork 的分数只用于诊断，不当作正式吞吐。高基数聚合分配集中于输出 Map、逐键状态和必需的标量累加器；Top-N CPU 集中于 `CompareUtils.compare`/`PriorityQueue`，候选持续改进时的 `OrderedRecord` 是精确排序所需；普通投影以输出 Map 和 Record 为主。相关子查询还可见实际订阅与 Map 物化，不因样本数删除冷 Publisher 边界。

DISTINCT 的 629 个 worker 分配样本中 `Integer` 达 159 个，但当前 profiling 入口复用 `Flux.range(...).map(index -> index & 1023)`，即热路径含输入装箱，不能把这些样本归因于引擎。只扩展 `ReactorQLBenchmark` 的 JFR-only DISTINCT 输入为 setup 预构造 `Integer[]`/`Flux.fromArray`，保留正式 `distinctRows` 入口不变；setup 比较两种输入的完整 SQL 输出值、类型和数量，不对没有 `ORDER BY` 的 DISTINCT 强加行顺序契约，热路径仍仅流式消费引用。阶段末集中构建并重录 DISTINCT JFR，判断剩余对象归属；本切片不改生产、默认限制或 SQL 语义。若剩余仅为精确 DISTINCT 的 key 状态与输出 Map，则停止，不为某一夹具增加缓存或自定义操作符。

复核结果：上述预构造输入已经在 `ReactorQLBenchmark.profilingDistinctRows` 中落地，setup 对原输入与预构造输入的 1,024 个输出做集合、字段和类型核对。用当前 JAR `0dedc87eaca7350e074524a7516c7999938a5fc0d314f9fd1d4065722800bc7f` 复录 `target/jfr-distinct-prebuilt-current-20261005/`（1×1s 预热、2×1s 测量、1 fork）；604 个 worker 分配样本中没有 `Integer`，主要为 `DefaultReactorQLRecord` 239、结果 Map 桶数组 123、节点 119、Map 117。89 个 worker CPU 样本的叶子以 Map 写入/扩容、哈希和比较为主。该采样只说明原先的输入装箱误归因已排除；要在投影前去重会改变自定义列求值/错误/副作用时机，不能为当前夹具重排 SQL 阶段。停止 DISTINCT 微调，不申领吞吐或常驻堆收益，也不增加原始行专用路径。

### 真实宽查询与多层子查询复测（2026-10-05）

目标：用常见设备事件 SQL 复核宽 SELECT、运算符/函数混合及多层子查询的当前成本，区分可优化的引擎编排与必需的计算、投影和数据源订阅。范围仅为 `WideSqlWorkloadBenchmark`、`NestedSqlWorkloadBenchmark` 的既有夹具与本次诊断；不改变生产代码、默认限制、SQL/响应式语义，也不按样本 SQL 特调。65,536 个预构造事件行覆盖 16 列直接投影、16 列运算符/字符串函数和 16 列双 JSONPath/日期函数；多层未关联聚合含派生表，相关 lookup 对照每外层行重新扫描来源。setup 检查源订阅、输出行数/顺序及代表性字段值和类型；运算符混合夹具逐输出行核对所有列。热路径以 Blackhole 消费输出引用，不收集结果或生成源行。

同一 JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler；性能 JAR SHA-256 `b44204e1bb6b4e66719866d5c3b87243a35b061f85c11e4332ac13c9e8beeee5`。原始结果：`target/jmh-real-sql-current-20261005.json`。下表吞吐按输入行或外层行归一化，B/行是瞬时分配，不是 live heap：

| 场景 | 行/s（均值 ± JMH 误差） | B/行 |
| --- | ---: | ---: |
| 16 列直接 SQL 投影 | 7.221±0.173 M | 331.217 |
| 16 列运算符/字符串函数 SQL | 3.824±0.133 M | 474.615 |
| 同结果原生 Reactor 控制 | 18.459±0.589 M | 140.705 |
| 16 列双 JSONPath/日期函数，JSON 文本输入 | 1.655±0.022 M | 2,657.713 |
| 同 SQL，预解析 Map 输入 | 1.548±0.019 M | 2,219.699 |
| 多层未关联聚合，默认订阅内复用 | 4.156±0.287 M | 1,276.837 |
| 同 SQL，显式关闭复用的对照 | 8,443±161 | 732,561.373 |
| 逐行相关 lookup | 148,697±6,127 | 28,360.221 |

JFR 使用同一 JAR 的 1×1s 预热、2×1s 测量、1 fork，见 `target/jfr-real-sql-current-20261005/` 和诊断 JSON `target/jmh-real-sql-current-jfr-20261005.json`、`target/jmh-real-sql-uncached-jfr-20261005.json`。双 JSON 文本查询的 worker 分配样本中，输出 `setResult` 相关 87/637、日期解析/格式化相关至少 49/637、JSON 路径读取/文本规范化至少 36/637；`pow` 计算入口为 14/105 CPU 叶子样本。预解析 Map 的 655 个分配样本中，`JsonValueSupport.normalizeMap` 及逐项复制关联 149 个，输出 `setResult` 113 个；预解析输入减少分配约 438 B/行，却没有提高吞吐，不能仅把解析缓存当作收益。运算符混合查询的 630 个分配样本中，结果写入 172 个、异步 WHERE 装配/执行约 139 个，仍反映 `IN` 懒比较和混合条件的真实 Publisher 边界。多层默认复用的 658 个样本中，`resultToRecord` 相关 197 个、容器建立 98 个；逐行相关 lookup 的 659 个样本中，具名记录绑定/复制与容器建立占主要部分。关闭复用的 JFR 同样集中于 `resultToRecord` 的 Map/Record 复制；它还实际重复执行 1,024 行 lookup 聚合，不能把对照差距归咎于单个操作符。

优先级结论：已存在的订阅内未关联子查询复用是本组最大收益，必须保留；相关 lookup 在通用冷 Publisher 上逐外层行扫描是当前契约下的真实成本，若业务源提供有界索引/查询下推能力，应在来源边界做独立方案，而不是引擎隐式建无界索引。下一可证伪候选是派生表 `resultToRecord` 的复制成本，但它承担别名和结果隔离，先做可变输入/自定义 Record/多订阅的语义审计与独立 A/B；目前不改生产。JFR 样本只用于定位来源，不能转换成 CPU 百分比、精确字节或常驻堆收益。本轮 `git diff --check` 通过；此前同 JAR 的 `mvn -q -Pjmh package` 已通过（480 tests），本轮无生产或夹具代码改动。

### JSON Map 规范化容器容量 A/B（计划，2026-10-05）

目标：降低 Java Map 输入执行通用 JSON 函数时，`JsonValueSupport.normalizeMap` 每层复制到默认 16 桶 `LinkedHashMap` 的过量桶数组和扩容成本。真实 16 列双 JSONPath/日期函数的预解析 Map 输入为 1.548 M 行/s、2,219.699 B/行，短 JFR 的 655 个分配样本里 `normalizeMap`/逐项复制关联 149 个；这是 JSON 规范化入口的通用成本，不限定 SQL 名称、字段或列数。Owning module 为 `JsonValueSupport`、既有 JSON 功能测试和本文档。

上一候选“混合投影容量提示”因需要按同步列数和内置 Record/Context 选择路径、预测收益较小，未写生产代码即撤回；`resultToRecord` 的可变结果/别名隔离也不直接删。当前方案仅在已验证容器大小上限的 `normalizeMap` 内用 Map 已知的 `size()` 为新 `LinkedHashMap` 指定足够初始容量，类似同文件 `normalizeCollection` 已按大小创建 `ArrayList`；不增加类型判断、查询缓存、结果共享、操作符或新 SPI。保持递归规范化、键字符串化、插入顺序、资源限制、可变输入隔离及错误时机。

阶段末集中执行 JSON 定向测试与完整 JMH 构建；同 JDK 17.0.18、512 MB/G1、3×1s 预热、5×1s 测量、2 forks、GC profiler，对上一节同 JAR 的预解析 Map 宽函数作收益 A/B，JSON 文本宽函数、直接宽投影、运算符混合和三层子查询作负对照。短 JFR 核对桶数组来源。只有完整功能通过、B/行下降且吞吐无稳定回退才保留；若收益很小或有负面影响则撤回。B/行不等于常驻堆，且该切片不改变 JSON 文本解析次数。

结果：仅将 `JsonValueSupport.normalizeMap` 的新 `LinkedHashMap` 初始容量按已知且受资源上限约束的源 Map 大小计算；递归复制、键字符串化、插入顺序和资源校验不变。`mvn -q -Pjmh package` 通过（480 tests、0 failures/errors），after 性能 JAR SHA-256 为 `bd7ab6c70dcca18a1e1c88699ed83fb6028d83da99ec8e0538def2a8e0ee72fd`。同配置正式 A/B 原始数据为 `target/jmh-real-sql-current-20261005.json`（before JAR `b44204e1bb6b4e66719866d5c3b87243a35b061f85c11e4332ac13c9e8beeee5`）和 `target/jmh-json-map-capacity-after-20261005.json`：

| 场景 | before→after 行/s（均值 ± JMH 误差） | before→after B/行 |
| --- | ---: | ---: |
| 预解析 Map 输入、16 列双 JSONPath/日期函数 | 1.548±0.019→1.542±0.046 M | 2,219.699→2,075.695 |
| JSON 文本输入、同 SQL | 1.655±0.022→1.662±0.015 M | 2,657.713→2,657.713 |
| 16 列直接投影 | 7.221±0.173→7.202±0.106 M | 331.217→331.217 |
| 16 列混合运算符/字符串函数 | 3.824±0.133→3.849±0.100 M | 474.615→474.615 |
| 三层未关联聚合 | 4.156±0.287→4.202±0.045 M | 1,276.837→1,276.837 |

预解析 Map 查询的瞬时分配少约 144 B/输入行（约 6.5%），没有证据证明吞吐变化；负对照的分配不变、吞吐误差区间重叠。短 JFR `target/jfr-json-map-capacity-after-20261005/`（诊断 JSON `target/jmh-json-map-capacity-after-jfr-20261005.json`）中，`normalizeMap` 关联的 `HashMap$Node[]` 分配样本由前次同查询 79/655 降到 15/659；该采样只定位桶数组来源，不能换算精确 B/行。`git diff --check` 通过。按既定门槛保留，不声明 live heap 或 JSON 文本输入收益；后续高收益方向仍应以真实 SQL 的 JFR/JMH 证据选择，而不是扩展这类容量阈值分支。

### 16 列直接投影与原生边界复核（2026-10-05）

目标：判断同结果直接宽投影约两倍的 SQL/原生吞吐差是否仍含可低复杂度移除的 Reactor 操作符或分配热点。范围只包括既有 `WideSqlWorkloadBenchmark` 四个 SQL/原生同结果入口及本文档；不改生产、JMH 夹具、默认限制或输出语义。先核对 JAR 身份，再用当前 JAR 录 JFR 和正式 JMH，避免把此前目录里属于较早源码阶段的 `FixedArgumentList` 样本误当作当前热点。

当前性能 JAR 为 `bd7ab6c70dcca18a1e1c88699ed83fb6028d83da99ec8e0538def2a8e0ee72fd`；同 JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler 的正式结果在 `target/jmh-wide-direct-current-20261005.json`。每对 SQL/原生均复用 65,536 条预构造设备事件、相同 WHERE 和 16 列输出；setup 已逐行核对同结果、顺序与源订阅，热路径流式消费输出引用：

| 同结果查询 | SQL 行/s（均值 ± JMH 误差） | 原生行/s（均值 ± JMH 误差） | SQL / 原生 B/输入行 |
| --- | ---: | ---: | ---: |
| 含字符串函数的 WHERE + 16 列投影 | 7.354±0.457 M | 13.938±0.657 M | 331.217 / 316.811 |
| 可在原始 Map 上过滤的数值/布尔 WHERE + 16 列投影 | 6.696±0.181 M | 13.082±0.287 M | 414.008 / 396.003 |

同 JAR 短 JFR `target/jfr-wide-direct-current-20261005/`（诊断 JSON `target/jmh-wide-direct-current-jfr-20261005.json`）中，两条 SQL 的 worker 分配样本分别为 631/638 个，主要是最终 `HashMap$Node` 511/506、桶数组 111/126；Record 分配只有 5/1，`FixedArgumentList` 均为 0。原生对照同样以结果 Map 节点和桶数组为主。SQL 相对原生多约 14–18 B/输入行，但绝大部分输出分配是同结果必需的 Map 物化，不能靠删 Record 或逐行列表解决约 1.9 倍的吞吐差。JFR 中部分内联 lambda 的叶子方法名/源码行号与本查询 SQL 不一致，未将其当成特定函数 CPU 归因；样本数不转换为 CPU 百分比或 B/行。

结论：当前无证据支持为这两条宽投影增加原始行投影专用路径、公共 SPI 或低层自定义操作符。那会改变结果列引用前面投影列、空值回退、自定义 Feature、别名和扩展 Record 等通用契约，复杂度明显高于本轮证据可支持的收益。后续先用独立的无 WHERE 同源宽投影和异步/自定义属性负对照区分属性求值与结果写入成本，再决定是否值得做通用执行段改造；不为追逐原生数字牺牲功能性。`git diff --check` 通过。本轮无生产或夹具代码修改。

### 无 WHERE 宽投影成本隔离（计划，2026-10-05）

目标：用相同的 65,536 条预构造设备事件和相同 16 列输出，隔离 SELECT 投影自身相对原生 Reactor `map` 的成本，判断下一步是否存在值得实施的通用高收益点。范围限于 `WideSqlWorkloadBenchmark` 和本节；不改生产代码、默认限制、结果语义或 Reactor 订阅边界，也不因单条 SQL 建立专用执行路径。

先添加无 WHERE 的 SQL/原生成对入口；setup 对全部输出行核对列集合、值、类型、顺序与各一次源订阅，热路径只消费结果引用。阶段末集中构建，录正式 JMH+GC 与短 JFR，并和现有两组带 WHERE 的同结果基准比较。如果剩余开销以必要的结果容器和属性访问为主，停止该方向；只有找到跨查询可复用、语义边界明确且分配/吞吐收益显著的热点，才另做最小生产改动和异步/自定义 Feature 负对照。

阶段一结果：完整 `mvn -q -Pjmh package` 通过（480 tests）；JAR SHA-256 `a1ccd1ac1339ab238b5ff4efe7f9b20b095de73e58f0677c49eb3f3d735da2a3`。同 JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler，SQL 无 WHERE 投影为 5.083±0.357 M 行/s、736.009 B/行；同结果原生 `map` 为 9.592±0.074 M 行/s、704.003 B/行。原始结果 `target/jmh-no-where-wide-20261005.json`。短 JFR `target/jfr-no-where-wide-20261005/` 中，SQL 的 659 个 worker 分配样本为结果 Map 节点/桶数组 656 个、Record 2 个、Map 1 个；原生 632 个样本全部是结果 Map 节点/桶数组。SQL 的 127 个 worker CPU 叶子样本中 `LinkedHashMap$LinkedHashIterator.nextNode` 为 11 个，来自标量投影按列遍历编译期 `mappers`；原生未见该遍历。采样不能换算精确 CPU 百分比或 B/行，32 B/行差额也不应全部归因于迭代器。

阶段二仅在现有标量投影分支，将编译期有序 `mappers` 固定为列数组，逐行按同一顺序执行；异步列、聚合、扁平映射、扩展 Record 的 `setResult` 和 `$this` 展开语义不变，不增加新的快路判断、SPI 或跨行状态。阶段末集中跑完整构建、无 WHERE/带 WHERE/函数/多层子查询的同配置成对 JMH，以及短 JFR。仅在功能等价、吞吐有稳定改善且无分配/负对照回退时保留，否则撤回。

阶段二结果：数组化后的完整 `mvn -q -Pjmh package` 通过（480 tests），JAR SHA-256 `6e77ba6d0e422c0c433f8e918d3a6b703d04f1f625515bfeadd2039cfe4bb350`。同配置 A/B `target/jmh-no-where-wide-20261005.json` → `target/jmh-no-where-wide-array-after-20261005.json`：SQL 5.083±0.357→5.211±0.143 M 行/s，原生 9.592±0.074→10.028±0.157 M 行/s；SQL 分配均为 736.009 B/行，原生均为 704.003 B/行。SQL/原生吞吐比从约 0.530 降到约 0.520，SQL 绝对值变化不能排除环境漂移；没有达到稳定收益门槛。已撤回数组化生产改动，不继续跑负对照或 after JFR。无 WHERE 基准夹具保留供后续分辨投影成本；前一版已通过的生产代码与 480 个测试不因这次试验改变。下一轮应以更大真实热点为前提，避免沿投影循环继续微调。

撤回后 `mvn -q -Pjmh -DskipTests package` 与 `git diff --check` 均通过，当前性能 JAR SHA-256 为 `0dedc87eaca7350e074524a7516c7999938a5fc0d314f9fd1d4065722800bc7f`；最终源码仅保留本节基准夹具/记录，不包含数组化生产改动。工作树没有 `.trellis/spec`，因此无该目录的规范或日志需要回填。

### 同步日期参数的响应式包装成本（计划，2026-10-05）

目标：在不重排 SELECT 列、不删除异步列边界的前提下，判断 `date_format`/`format_datetime`/`dateformat` 对同步日期表达式的 `Mono.from(mapper.apply(record)).map(...)` 是否还有可通用收敛的逐行装配成本。owning module 为 `DateFormatFeature`；验证覆盖其同步空值/错误/副作用时机、异步 Publisher/Context/取消，以及真实 16 列宽函数 SQL 和无关查询负对照。不修改时间格式/时区校验、默认限制、公共 SPI、其他函数或跨行状态。

当前 JAR `0dedc87eaca7350e074524a7516c7999938a5fc0d314f9fd1d4065722800bc7f` 的短 JFR `target/jfr-wide-functions-current-20261005/`：JSON 文本/预解析 Map 两组 16 列宽函数查询都还采到 `JsonPathFunctionMapFeature` 的逐行参数 `ArrayList`（约 7/14 个 worker 分配样本）、JSONPath 内部 `ArrayList`（约 8/7 个）以及解析/规范化和最终结果容器；这些列表不是本候选目标。`DateFormatFeature` 当前始终返回普通 Publisher，因此整条宽投影使用异步列编排，即便日期输入映射器是 `ScalarValueMapper`。先对当前 JAR 录正式 JMH+GC 基线，再只在同步日期输入分支以单个冷 `Mono` 完成取值和格式化，保持空流、异常和取消的响应式信号；普通异步输入仍沿原路径。阶段末集中运行功能测试和同配置成对 JMH/JFR。仅在显著降低分配、吞吐无稳定回退且所有信号/顺序测试通过时保留，否则撤回。

结果：同步日期参数分支使用一个冷 `Mono.fromSupplier` 完成取值与格式化，不将日期列改成 `ScalarValueMapper`，因此混合 SELECT 的既有列调用顺序、空值/错误、Context、取消及异步参数回退保持不变。新增 `DateFormatFeatureTest` 覆盖这些边界。完整 `mvn -q -Pjmh package` 通过（483 tests、0 failures/errors/skipped），`git diff --check` 通过；after JAR SHA-256 为 `975502a30fd4109cd6e80091f47af51a21cd9d8cd6529cf0d0f91b2f3919d354`。

同 JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler 的 before/after 数据位于 `target/jmh-date-format-wrapper-before-20261005.json` / `target/jmh-date-format-wrapper-after-20261005.json`。按输入行或外层行归一化：

| 场景 | before→after 行/s（均值 ± JMH 误差） | before→after B/行 |
| --- | ---: | ---: |
| 16 列双 JSONPath/日期函数，JSON 文本 | 1.696±0.022→1.703±0.045 M | 2,657.713→2,632.512 |
| 同 SQL，预解析 Map 输入 | 1.620±0.061→1.648±0.060 M | 2,075.695→2,050.494 |
| 无 WHERE 16 列直接投影 | 4.971±0.062→5.079±0.213 M | 736.009→736.009 |
| 三层未关联聚合，订阅内复用 | 4.343±0.107→4.295±0.015 M | 1,276.837→1,276.837 |
| 同 SQL，显式关闭复用 | 8,755±187→8,568±141 | 732,561.373→732,561.373 |

两种宽函数输入形态均少约 25.2 B/输入行（约 1% 的瞬时分配），吞吐区间重叠，不申领稳定提速；负对照分配不变且吞吐区间重叠。短 JFR `target/jfr-date-format-wrapper-after-20261005/` 的日期函数栈及 `MonoMapFuseable` 样本方向与少一层逐行映射一致，但样本不能换算字节或 CPU 百分比，精确分配以 GC profiler 为准。本项收益不大；因实现只在既有同步参数能力上收敛一个函数内部的操作符，不增加 SPI、特定 SQL 分支或状态，并有明确分配下降且无稳定回退，予以保留。不声明常驻堆收益。

2026-10-06 共享错误边界复核更新：上述验收未覆盖 `onErrorContinue` 的字段级继续行为。日期格式化单 Supplier 合并会丢弃原本应保留的空字段行，因此该形式不再保留；现已以冷参数读取 + 原生 map 恢复原错误边界。当前结论与成本见本文“冷 Supplier 合并的共享错误边界复核”，不能继续将此历史微优化当作功能等价的已完成项。

### JSON 访问操作符的同步文档包装成本（计划，2026-10-05）

目标：评估 JSON `->`、`->>`、`#>`、`#>>` 在同步文档表达式上的逐行 `Mono.fromDirect(...).flatMap(...)` 成本，仍保持每列冷 Publisher、JSON 资源限制/异常、列调用顺序、空值、Context 和取消。owning module 为 `JsonOperatorMapFeature`，验证夹具沿用 `WideSqlWorkloadBenchmark` 的 65,536 条预构造设备事件，新增两次 JSON 操作符读取的 16 列 SQL、JSON 文本/预解析 Map 输入和同结果原生控制；setup 全量核对列、值、类型、顺序和各一次源订阅。热路径只消费结果引用，不收集或构造输入。

先以当前 JAR `975502a30fd4109cd6e80091f47af51a21cd9d8cd6529cf0d0f91b2f3919d354` 录同配置 JMH+GC/JFR 基线。若确定逐行 Publisher 装配是可观成本，只在 `ScalarValueMapper` 且默认 metadata 已确认支持标量快路时，用单个冷 `Mono` 同步求值；普通异步文档、扩展 metadata wrapper 和 checkpoint 保留原 Publisher 路径，不将 JSON 列提升成标量投影，也不跨列缓存/共享可变 JSON。补空值、错误、异步 Context/取消及自定义 Feature 回归；阶段末集中完整构建与同配置 SQL/原生、宽函数和子查询负对照 A/B。只有明确降低分配、吞吐无稳定回退且信号/顺序等价才保留，不因某个 JSON 路径或输入类型特调。

阶段一：JMH 夹具 setup 已核对 65,536 条预构造事件的 16 列 JSON 操作符 SQL（文本/预解析 Map）与原生控制的全部值、类型、顺序及各一次源订阅。`mvn -q -Pjmh -DskipTests package` 通过，基线 JAR SHA-256 为 `c90762b597f35f4eb4f97b0b59cfece6006cb00add34564c4206b44853b2d12b`。同 JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler，JSON 文本 SQL 为 1.096±0.015 M 输入行/s、5,109.169 B/行；预解析 Map SQL 为 1.341±0.024 M、3,816.015 B/行；同结果原生控制为 8.111±0.428 M、800.003 B/行；无 WHERE 直接投影负对照为 5.017±0.059 M、736.009 B/行。原始数据 `target/jmh-json-operator-wrapper-before-20261005.json`。短 JFR `target/jfr-json-operator-wrapper-before-20261005/` 的文本/Map 查询分别有 660/654 个 worker 分配样本；两者仍可见 JSON 解析或 Map 规范化、最终结果 Map，以及逐行 `MonoZip`/`MonoFlatMap`/`FluxFlatMap` 编排，Map 查询中的 `MonoFlatMap` 为 14 个样本。采样只支持进入最小 A/B，不把 SQL/原生差距全部归于这些 Mono 对象；两个 JSON 读取本身仍会各自解析/规范化，不在本切片共享文档。

阶段二：仅在已有 `ScalarValueMapper` 契约、默认 metadata 允许快路且未开启 checkpoint 时，`JsonOperatorMapFeature` 用一个冷 `Mono.fromSupplier` 合并文档取值与 JSON 读取；结果仍为逐列 Publisher，仍经过原表达式 wrapper。普通异步参数沿原路径，两个分支复用同一个纯同步读取方法；没有缓存文档、改变 JSON 限制或提升为同步投影。新增 `JsonOperatorMapFeatureTest` 覆盖同步空值/缺失/错误、冷求值、真实 SQL 混合列顺序、异步 Context/取消、自定义 metadata wrapper 和 checkpoint 回退。完整 `mvn -q -Pjmh package` 通过（489 tests、0 failures/errors/skipped），`git diff --check` 通过。正式 A/B 使用的 after JAR SHA-256 为 `fc3e5a34a1791f262acdaf312539bbb969e69c87ec9c79bd4f867fd71456f20c`；增加 checkpoint 测试后的打包 SHA-256 为 `ad73d1d401da7c7b784c85b363d6520e235c82ce855ed9dcb21fdc79b80c2c06`。此后仅清理未使用测试 import，定向测试通过，生产代码及 JMH 夹具未再变化。

同配置正式数据 `target/jmh-json-operator-wrapper-before-20261005.json` → `target/jmh-json-operator-wrapper-after-20261005.json`：JSON 文本 SQL 为 1.096±0.015→1.091±0.007 M 行/s、5,109.169→5,061.169 B/行；预解析 Map SQL 为 1.341±0.024→1.340±0.020 M 行/s、3,816.015→3,768.015 B/行。两组均少 48 B/输入行（分别约 0.9% 和 1.3%），吞吐误差区间重叠，不申领稳定提速。同结果原生控制为 8.111±0.428→8.540±0.117 M 行/s、800.003→800.003 B/行；无 WHERE 直接投影为 5.017±0.059→4.996±0.061 M 行/s、736.009→736.009 B/行。宽 JSONPath/日期函数负对照的分配保持 2,632.512 / 2,050.494 B/行，三层未关联聚合为 1,276.837 B/外层行，均未因本改动改变分配。短 JFR `target/jfr-json-operator-wrapper-after-20261005/` 的预解析 Map worker 分配样本中，`MonoFlatMap` 从基线 14/654 降到 0/629；样本只定位对象来源，不能换算字节或 CPU 比例。此项分配收益可测但幅度有限；因局部实现简单、通用且没有可确认的吞吐回退，予以保留，不宣称 live heap 改善。更高收益方向仍需新的跨场景 JFR 证据，不扩展成特定 JSON 路径或输入形态的缓存/专用执行器。

### 按键独立计数窗口的高基数排空（计划，2026-10-05）

目标：检验大量前缀键在源完成时同时形成未满窗口，`WindowedAggregateStage.SubscriptionState.detachedWindows` 逐个 `ArrayList.remove(window)` 是否造成平方级搬移；若成立，在不改变分组、窗口关闭顺序、背压/取消、默认资源限制或 `ClosedGroupWindow` 所有权的前提下消除该成本。范围限 `WindowedAggregateStage`、同模块 JMH/测试及本节；不引入自定义 Subscriber/哈希表、隐式缓存或 SQL 文本特判。

先用 50,000 个预构造唯一键、`group by key,_window(2)` 建立真实 SQL 基线，setup 全量核对输出、顺序和源订阅；同一输入的 `_window(50000),key` 作负对照。短 JFR 检查 `ArrayList.remove`/数组搬移是否出现在排空栈，正式 JMH+GC 记录吞吐和瞬时分配。若热点成立，优先以 FIFO 跟踪关闭窗口并保留非 FIFO 关闭回退，确保源完成、正常排空、下游取消和错误都准确释放状态；补多前缀、取消/错误、重复订阅测试。阶段末一次完整构建与同配置 A/B，并复测全局聚合和混合 SQL 负对照。只有功能等价、显著改善目标吞吐且无稳定负对照回退才保留，否则撤回。JFR 样本不换算 CPU 百分比，GC B/行不等于常驻堆。

结果：`ReactorQLBenchmark.profilingPerKeyWindowCount` 使用 50,000 个预构造唯一键，setup 逐行验证输出值、类型、顺序及一次源订阅；同输入的 `profilingHighCardinalityCount` 保留为窗口在前的负对照。基线 JAR SHA-256 为 `eb54f1309c48320c7415a3f155e2300fb934e942d4dd8fef2d0c1deeaf4a1c62`。短 JFR `target/jfr-per-key-window-before-20261005/` 的目标查询只有 39 个 worker CPU 样本，未直接采到 `ArrayList.remove`，不能由样本声称其 CPU 占比；代码路径上 `finish()` 会按顺序 detach 50,000 个未满窗口，原 `closed()` 对跟踪 `ArrayList` 每次按对象删除，构成平方级搬移风险。正式同 JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler 的原始结果为 `target/jmh-per-key-window-before-20261005.json`。

仅将订阅内关闭窗口跟踪由 `ArrayList` 改为 `ArrayDeque`：正常串行排空从队首 O(1) 移除，非 FIFO 关闭仍按对象回退移除；`finish()` 的输出顺序、窗口状态所有者、下游 demand、取消及错误清理未改。`WindowedAggregateStageTest` 新增 32 个未满前缀窗口的按需首条、完整顺序、重复订阅、Context、取消和错误验证。完整 `mvn -q -Pjmh package` 通过（490 tests、0 failures/errors/skipped），after 性能 JAR SHA-256 为 `663e5bd8e5ece5194d7c7494e7bf1afe03536e4ab6e5eacad655a6ddd01905a6`；之后仅增加诊断探针的 `--per-key` 模式及调整 import 顺序，最终完整 `mvn -q -Pjmh package` 再次通过（490 tests），生产逻辑未再变化。

同配置正式 A/B `target/jmh-per-key-window-before-20261005.json` → `target/jmh-per-key-window-after-20261005.json`：按键独立窗口 0.549±0.003→2.317±0.208 M 输入行/s（约 4.2 倍），1,167.176→1,167.473 B/行；窗口在前的负对照 4.551±0.173→5.210±0.267 M 输入行/s，565.010→565.011 B/行。负对照同时提速表明存在环境漂移，但幅度远小于目标查询的变化；目标分配未降低，不能申领瞬时分配或常驻堆收益。after 短 JFR 在 `target/jfr-per-key-window-after-20261005/`，121 个 worker CPU 样本主要落在分组状态、HashMap 和输出构造；仅作热点方向证据。额外正式负对照 `target/jmh-per-key-window-negative-controls-20261005.json` 测得全局聚合 37.921±0.282 M 输入行/s、约 0.002 B/行，16 列混合 SQL 3.915±0.104 M、474.615 B/行；两条不进入本次窗口关闭路径，且没有同 JAR 的 before 成对数据，不据此宣称其吞吐变化。

`HighCardinalityLiveHeapProbe --closed --per-key --count` 在相同 512 MB/G1 下按需仅请求首条：10,000/50,000 键等待 demand 时分别约 8.26/25.35 MB，取消后各回到约 4.23 MB；只验证 after 的状态释放，不与旧实现作 live-heap A/B，也不宣称驻留堆降低。该改动保持每个活跃键必需的累加器与 O(活跃键数) 状态，仅消除高基数窗口排空的非必要平方级操作，予以保留。

### 单组前缀的常驻容器开销（计划，2026-10-06）

目标：在高基数按键窗口中降低每个活跃前缀的堆占用，同时保持吞吐、输出顺序、按需物化和取消/错误释放。owning module 为 `WindowedAggregateStage`，验证沿用真实 SQL 的按键窗口 JMH、窗口在前的负对照及 `HighCardinalityLiveHeapProbe`。不按 SQL 文本、键类型或基数特调，不改变默认活跃键限制、聚合累加器或响应式边界，不引入自定义 Subscriber。

当前执行计划在 `windowPosition == dimensions.size()` 时每个前缀至多对应一个 `GroupState`，但 `PrefixState` 仍创建 `LinkedHashMap`、条目和桶数组；现有 JFR 在这些构造点采到分配样本。仅对这个由维度结构保证的单组前缀改用字段保存状态，多组前缀继续使用原有有序 Map。审计 `finish()`、`ClosedGroupWindow` 的迭代/按需输出、状态预算和 `clear()`，补首条 demand、完整顺序、HAVING 过滤、取消/错误、重复订阅与预算回归。阶段末集中完整构建，并在同配置下对目标及负对照做成对 JMH/GC、对 10,000/50,000 键做 live-heap A/B。只有正确性通过、常驻堆明确下降且吞吐无稳定回退才保留；若需要复杂迭代协议或多层模式判断，则撤回此候选。

结果：`PrefixState` 仅在存在后续分组维度时创建有序 Map；由分组维度结构保证单组的前缀直接保存一个 `GroupState`。`ClosedGroupWindow` 沿用 `Flux.fromIterable(...).handle(...)` 的按需输出，单组释放和多组迭代分别处理，没有新增 Subscriber、SQL 文本特判或默认限制变化。新增测试覆盖单组前缀 HAVING 过滤、逐条 demand/顺序和活跃状态预算；原有按键窗口测试继续覆盖取消、错误、Context 与重复订阅。完整 `mvn -q -Pjmh package` 在允许 `ReactorDebugAgent` JVM 自附加的环境中通过（492 tests、0 failures/errors/skipped）；受限沙箱中仅 `GroupByWindowTest` 因其静态 agent 初始化失败，单独在相同自附加环境重跑通过。性能 JAR SHA-256 为 `a0cc3571e51a444d07b401cd4925e89ce7c346a15191b7c44602d446bd2e4599`。

同 JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler，`target/jmh-per-key-window-after-20261005.json` → `target/jmh-single-prefix-after-20261006.json`：50,000 键按键窗口 **2.317±0.208→3.269±0.152 M 输入行/s**，**1,167.473→975.472 B/行**；窗口在前负对照 5.210±0.267→4.864±0.231 M 行/s，565.011→565.011 B/行，吞吐误差区间重叠。目标吞吐约提升 41%、逐行分配减少约 192 B，负对照没有可确认的稳定回退。

同配置 `HighCardinalityLiveHeapProbe --closed --per-key --count` 的改动前→后：10,000 键等待 demand 8.258→6.338 MB，50,000 键 25.399→15.750 MB；取消后都约 4.225 MB。跨 10,000→50,000 键的 live-heap 斜率约 429→235 B/键，下降约 193 B/键，与 GC profiler 的分配方向吻合；这些数值是整进程存活堆而非单类精确大小。功能、吞吐、瞬时分配及常驻堆均满足保留门槛，保留此最小结构优化。

### 当前跨场景热点复核与单组窗口排空（计划，2026-10-06）

目标：从当前性能 JAR 而非历史采样寻找可通用删除的逐行/逐窗口成本。范围为 `WindowedAggregateStage` 的关闭窗口排空、既有 JMH/测试及本节；不更改 SQL 规划条件、默认状态限制、聚合算法、多组迭代顺序或响应式背压/取消边界，不引入自定义 Subscriber。

当前 JAR `a0cc3571e51a444d07b401cd4925e89ce7c346a15191b7c44602d446bd2e4599` 的短 JFR 位于 `target/jfr-global-current-20261006/` 和 `target/jfr-global-highcard-current-20261006/`。宽函数旧采样中的 `FixedArgumentList` 已由现有双参数标量路径消除；当前 JFR 的宽函数 CPU 叶子主要是 `Math.pow`、JSON 解析和结果 Map，不能以旧采样重复改。当前 16 列直接投影与同结果原生正式对照见 `target/jmh-wide-native-current-20261006.json`：SQL 7.314±0.144 M 行/s、331.217 B/行，原生 14.015±0.443 M 行/s、316.811 B/行；剩余差距主要在通用记录/字段/结果写入，不能只靠删 Reactor 算子解决。多层未关联子查询 JFR 的 657 个 worker 分配样本中 224 个经过 `resultToRecord`，但该方法必须保留来源别名与可变结果隔离，本切片不删除复制。相关子查询的逐外层行扫描不能隐式替换为无界索引。

高基数按键窗口的当前 JFR 仍显示每个已关闭的单组窗口经 `Flux.fromIterable` 产生 `FluxIterable` 订阅/Spliterator；这是当前 `PrefixState.singleGroup` 已证明只有一个结果时的纯编排成本。先用当前 JAR 对按键窗口、窗口在前负对照及多聚合高基数查询录正式 JMH+GC 基线；随后仅在单组窗口的 `drainWindow` 使用 Reactor `Flux.just(group)`，多组仍用原 `Flux.fromIterable(window)`，继续统一经过 `handle(window::emit).doFinally(window::close)`。阶段末集中完整测试，并以同配置正式 JMH/JFR 与 live-heap 探针验证。只有语义等价、目标吞吐或分配有明确收益且负对照无稳定回退才保留；若收益不明确或增加复杂性，则撤回。

结果：仅在执行计划已保证单组的关闭窗口使用 `Flux.just(singleGroup)`，多组仍由 `Flux.fromIterable(window)` 按原顺序迭代；两条路径共享 `handle` 的按需转换和 `doFinally` 的取消/错误释放。没有新增订阅类型或修改窗口状态所有权。完整 `mvn -q -Pjmh package` 通过（492 tests、0 failures/errors/skipped），after 性能 JAR SHA-256 为 `40e8dd5dc481eecc651c4a701e6e25ec1c9a8276435afb1b3d53874c358f0d45`。

同 JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler，`target/jmh-single-window-direct-before-20261006.json` → `target/jmh-single-window-direct-after-20261006.json`：50,000 键按键窗口 3.143±0.198→3.050±0.122 M 输入行/s，975.472→903.472 B/行（每行少约 72 B，吞吐误差区间重叠，不申领提速）。窗口在前的 count 负对照 4.475±0.281→4.536±0.122 M 行/s、565.011→565.011 B/行；多聚合高基数负对照 3.578±0.074→3.482±0.070 M 行/s、765.012→765.012 B/行，均无可确认的分配变化或稳定吞吐回退。短 JFR 的目标查询分配样本中原有 `FluxIterable$IterableSubscriptionConditional`/`IteratorSpliterator` 各 36/34 个（总 626）在 after 652 个样本中不再出现，改为 `ScalarSubscription` 等同步单值装配；样本不作为精确字节或 CPU 百分比依据。10,000/50,000 键等待 demand 的 live heap 分别约 6.337/15.749 MB，取消后约 4.224 MB，与上一阶段约 6.338/15.750 MB 一致；不声明常驻堆进一步下降。因实现只增加一个由单组结构保证的选择、瞬时分配明确下降且功能和负对照通过，予以保留。

### 通用等值比较的直接类型边界（计划，2026-10-06）

目标：评估同步 WHERE/HAVING、`IN` 和其他通用等值调用中，`CompareUtils.equals` 对同为布尔值或同为字符串的参数仍进入完整 `compare()` 多类型流程的 CPU 成本。owning module 为 `CompareUtils`，验证覆盖真实 16 列宽投影与混合运算符 SQL，并用多层子查询、高基数聚合负对照；不改变跨类型数字/时间/枚举转换、异常转 false 的既有语义，不加 SQL 文本或字面量特判，也不新增响应式操作符。

当前 JAR 的 `target/jfr-record-current-20261006/` 短采样把大量 `@Setup` 工作计入了 CPU 统计；即使用启动延迟录制，`target/jfr-width-steady-delayed-20261006.jfr` 的部分 CPU 栈仍把没有 `pow` 的宽投影归到 `pow` lambda，不能把这些叶子计数当成目标函数耗时。可用的证据仅是等值条件确实调用 `CompareUtils.equals` 的栈与源码路径；收益必须由正式 JMH A/B 决定。先以当前 JAR 录同配置正式基线，然后只对同为 `Boolean` 或同为 `String` 的值在 `equals` 入口直接比较，其他类型仍由原 `compare()` 执行。补跨类型字符串、布尔、`BigDecimal` 数值语义测试；阶段末集中全量构建与同配置 JMH/GC。只有目标 SQL 吞吐出现可确认收益、分配和负对照不回退，才保留此候选，否则撤回。

结果：候选实现与跨类型语义测试曾通过完整 `mvn -q -Pjmh package`（492 tests）；正式同配置 `target/jmh-equals-direct-before-20261006.json` → `target/jmh-equals-direct-after-20261006.json` 测得混合运算符查询 3.905±0.021→4.030±0.035 M 行/s，16 列宽投影 7.271±0.311→7.356±0.084 M 行/s，后者误差区间重叠；两者分配分别保持 474.615/331.217 B/行。多层未关联子查询 4.302±0.026→4.231±0.068 M 行/s，分配约 1,276.6 B/行；无关高基数 count 4.406±0.138→4.850±0.231 M 行/s，分配 565.011 B/行，表明并行的机器/运行时波动显著。等值快路未证明跨场景稳定高收益，且增加了类型分支，按门槛撤回生产与测试改动；最终源码继续使用原通用 `compare()` 语义，不申领吞吐或堆收益。JFR 中与 SQL 不相符的 `pow` 栈不用于优化归因，未来短采样需用源码和测量区间双重核验。

撤回后完整 `mvn -q -Pjmh package` 再次通过（492 tests、0 failures/errors/skipped），`git diff --check` 通过；最终性能 JAR SHA-256 为 `5eb3f82b7d572b537316812bd0ac12b41766c2bb14a8d9c348a99bc72eea7983`。上一阶段的单组窗口改动未被撤回，本节没有保留新的生产代码或测试修改。

### 测量区间内的宽投影 JFR 与原生上界（2026-10-06）

目标：排除 `WideSqlWorkloadBenchmark.@Setup` 对短 JFR 的污染，定位当前真实 16 列 SQL 与同结果原生实现的剩余成本；本节只更新证据，不修改生产代码、执行计划或默认配置。使用性能 JAR `5eb3f82b7d572b537316812bd0ac12b41766c2bb14a8d9c348a99bc72eea7983`，在 JMH 完成 warmup、进入 120 秒 measurement 后，分别向 SQL/原生 fork 附加 12 秒 JFR；录制文件为 `target/jfr-wide-measurement-attached-20261006.jfr` 与 `target/jfr-native-wide-measurement-attached-20261006.jfr`。两份文件均已完整写出，停止长时间测量仅发生在录制完成之后，因此未用被中断的 JMH 分数作为吞吐证据。

本次 measurement-only SQL JFR 的 867 个 worker CPU 样本中，叶子 `HashMap.getNode` 328、`PropertyMapFeature` 标量属性映射 132、`BinaryFilterFeature.test` 87、`LinkedHashMap.afterNodeInsertion` 77、`HashMap.putVal/put` 合计 116；此前与 SQL 不符的 `pow` 叶子为 0。原生控制的 800 个 worker 样本主要落在结果 Map 写入（`putVal/put` 合计 596）、`FluxHandleFuseable.onNext` 89 和源 Map 读取 `getNode` 53。两次样本量及吞吐不同，不能把样本数之比当作精确 CPU 时间比；源码确认两者均对预构造 `LinkedHashMap` 输入输出相同 16 列，SQL 额外经过通用属性、条件和 Record 边界。对应分配样本 SQL 3558 个中结果 Map 节点/桶数组 3557 个；原生也以结果 Map 为主，不支持删除必要结果容器。

同 JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler 的同 JAR 正式成对结果见 `target/jmh-wide-native-paired-current-20261006.json`：

| 查询 | SQL / 原生输入行/s（均值 ± JMH 误差） | SQL / 原生 B/输入行 |
| --- | ---: | ---: |
| 含数值、字符串、布尔 WHERE 的 16 列投影 | 7.127±0.214 / 13.553±0.703 M | 331.217 / 316.811 |
| 无 WHERE 的同 16 列投影 | 4.889±0.183 / 9.979±0.105 M | 736.009 / 704.003 |

有 WHERE 查询的筛选比例是 29,492/65,536≈0.4500；SQL 比原生多约 14.406 B/输入行，折合约 32.01 B/输出行，与无 WHERE 的约 32.007 B/输出行差额一致。源码中 SQL 路径每个输出保留一层 `DefaultReactorQLRecord`，原生对照直接输出 Map；这解释多余分配的量级，但不证明全部吞吐差都来自该对象。已尝试且撤回的投影数组化、等值类型分支没有稳定跨场景高收益；若要消除 Record 层，需要重新证明别名、前列引用、`*`、自定义 Feature、异步列、ORDER/DISTINCT 与下游 Record 语义，当前不以专用 SQL 快路或缓存硬做。下一候选必须基于测量区间内的跨场景证据，而非短 JFR 的 setup 栈。

### JSON 结构校验的遍历分配（计划，2026-10-06）

目标：仅消除已解析 JSON 结构校验中逐层创建 Map `values()` 视图及迭代器的成本，不改变解析、递归深度/容器大小上限、异常、结果或响应式边界。owning module 为 `JsonValueSupport`；不做跨列 JSON 缓存、路径特判或自定义操作符。当前 JAR `5eb3f82b7d572b537316812bd0ac12b41766c2bb14a8d9c348a99bc72eea7983` 的测量期宽函数 JFR `target/jfr-wide-functions-measurement-attached-20261006.jfr` 在已解析 JSON 校验栈采到 `LinkedHashMap$LinkedValues` 33+25 个分配样本；整体仍以 JSON parser/JSONPath、日期处理和必要结果 Map 为主。采样只表明存在可删对象，不证明总收益。

先以当前 JAR 对 JSON 文本/预解析 Map 的 16 列宽函数、同结果直接宽投影和三层子查询录正式 JMH+GC 基线；随后只将 `assertJsonStructure` 中 Map 子项遍历改为 `Map.forEach`，保持原递归调用顺序。现有 JSON 深度、容器、非法文本、动态路径及宽查询结果测试作行为验证；阶段末统一完整构建和同配置 A/B。只有总分配出现明确下降且吞吐无稳定回退、实现仍局限一个遍历边界时保留；若收益仅是采样噪声则撤回。此处不申领 live-heap 收益。

结果：仅把 `JsonValueSupport.assertJsonStructure` 的 Map `values()` 遍历改为 `Map.forEach`，其余解析、JSONPath、资源限制与响应式链未改。第一次完整 `mvn -q -Pjmh package` 的唯一失败是既有真实时钟窗口测试 `testGroupByTimeWindow` 在 200 ms 输入/500 ms 分窗边界得到第二窗 4.0 而非 3.5；未修改该测试或窗口实现，定向复测通过，随后完整构建通过（492 tests、0 failures/errors/skipped）。after 基准 JAR SHA-256 为 `2f2b2c319e46151284d7e76971f872df3623df635415f6e26e079aef21eb710f`。

同 JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler，正式 `target/jmh-json-structure-validation-before-20261006.json` → `target/jmh-json-structure-validation-after-20261006.json`：JSON 文本 16 列宽函数为 1.667±0.016→1.711±0.029 M 输入行/s、2632.512→2610.911 B/行，少约 21.6 B/行（约 0.8%）；吞吐误差区间几乎相接，不认定稳定提速。预解析 Map 形态为 1.616±0.024→1.613±0.018 M、2050.494→2050.494 B/行；直接宽投影 7.378±0.152→7.258±0.316 M、331.217→331.217 B/行；三层未关联子查询 4.348±0.070→4.265±0.046 M、1276.611→1276.611 B/外层行。负对照未出现可确认的稳定回退。此项只减少解析后校验的短命遍历对象，保留简洁改动；不申领常驻堆收益，也不是新的高收益执行路径。

### 运算符混合查询的测量期复核（2026-10-06）

以同一 after JAR 的 `WideSqlWorkloadBenchmark.operatorMixWideProjection` 完成 warmup 后附加 12 秒 JFR，文件为 `target/jfr-operator-mix-measurement-attached-20261006.jfr`；录制完成后才中止长迭代，不采用中断测量的 JMH 分数。816 个 worker CPU 样本的主要叶子包括 `InFilter.needsFlattening` 50、输入 Map 查找/属性回退、`BinaryFilterFeature.test`、字面量 `LIKE` 的行终止符判断及结果 Map 写入。3,559 个 worker 分配样本仍有 `MonoHandleFuseable` 323、`MonoMapFuseable` 166、`MonoSupplier` 113 及相关 Subscriber/lambda，来源于常量列表 `IN` 在运行时仍须兼容左侧 `Iterable`/`Publisher`/单入口 Map 的展开与冷订阅；输出 Map 节点/桶数组及 Record 也占主要部分。`CASE` 的 Map entry 迭代器有 72 个样本，但单独替换其编译期容器收益上界较小，且先前普通投影数组化未证明稳定提速。本轮不为某条 SQL 的普通标量输入删除 `IN` 的异步边界，不增加“通常是标量”的软类型承诺、缓存或自定义 Subscriber。样本不换算 CPU 百分比、字节或常驻堆；下一高收益候选需先在多个谓词/真实值形态上证明可复用的同步能力契约，当前证据不足以改生产路径。

### 高基数多聚合的测量期热点复核（2026-10-06）

目标：复核当前每键实时累加的五聚合路径是否仍有可通用删除、且收益足以抵消复杂度的逐行成本。本节只取证，不改聚合状态、默认活跃键限制或响应式信号。使用同一性能 JAR `5eb3f82b7d572b537316812bd0ac12b41766c2bb14a8d9c348a99bc72eea7983`；`ReactorQLBenchmark.profilingHighCardinalityAggregates` 在 50,000 条预构造、唯一键输入上完成 warmup 后附加 12 秒 JFR，文件为 `target/jfr-highcard-measurement-attached-20261006.jfr`。录制完成后才中止长迭代，不采用中断测量的 JMH 分数。此前同 JAR 正式基准 `target/jmh-single-window-direct-after-20261006.json` 的该查询为 3.482±0.070 M 输入行/s、765.012 B/行。

773 个测量期 worker CPU 样本中，主要叶子是分组 `LinkedHashMap` 插入/扩容、`GroupState` 与累加器创建、实时 `addRaw` 和输出 Map 写入；3,521 个 worker 分配样本中，结果 `HashMap$Node` 697、来源键记录 Map 380、结果 Map 351、聚合结果 `Double` 347、输出 `DefaultReactorQLRecord` 327，另有分组条目和累加器。这些是精确高基数、多个实时聚合和可变结果 Map 的当前所有权成本；采样数不能换算字节、CPU 百分比或常驻堆。`GroupState` 仍只保留每键累加器和键，不缓存输入行。为了减少这些对象而改用共享可变空记录、固定容量巨型 Map、聚合专用状态布局或额外操作符，会改变公开 Record/Feature 语义或引入常驻内存/复杂度；当前证据不支持实施。JOIN 的候选行与多层子查询的派生记录也各有别名和隔离契约，不能将三者的 Map 样本合并当作一个可删除热点。本阶段没有新的生产改动或可申领的性能收益；后续只在出现跨场景、测量期证据和可验证的简洁通用方案时再实施。

### 多候选 JOIN 的当前状态与优化边界（2026-10-06）

目标：在已保留“单异步 ON 编排收敛”之后，判断同步多候选 INNER JOIN 是否仍有无需新语义或复杂状态的高收益操作符热点。本节只诊断并更新现有记录，不修改生产代码、右源订阅次数、默认资源限制或 JOIN 语义。当前性能 JAR SHA-256 为 `5eb3f82b7d572b537316812bd0ac12b41766c2bb14a8d9c348a99bc72eea7983`。

`ReactorQLBenchmark.profilingMultiRowInnerJoin` 使用 20,000 个预构造左行、21 个右行；每左行 0/1/4/16 个匹配，setup 校验 105,000 个结果及每左行一次右源订阅。JMH warmup 完成并进入 measurement 后附加 12 秒 JFR，录制完整写出为 `target/jfr-join-measurement-attached-20261006.jfr`，然后才中止长迭代，不用中断迭代的分数。另以同 JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler 正式测得 **0.846±0.005 M 左行/s、5,502.045 B/左行**，原始数据为 `target/jmh-multirow-join-current-20261006.json`。

测量期 JFR 的 3,510 个 worker 分配样本中，`HashMap$Node` 1,474、`DefaultReactorQLRecord` 676、HashMap 桶数组 674、Map 本身 493；`FluxFlatMap$FlatMapInner` 为 55。813 个 worker CPU 样本的叶子以属性映射 222、`HashMap.putVal` 173、等值/比较及 Record 创建为主。样本定位对象来源，不换算精确字节、retained heap 或 CPU 占比。源码中 ON 必须读取左右来源别名，因而当前路径在判断候选前建立组合 Record；要显著消除失败候选的 Map/Record，需引入受完整别名、结果隔离、异步 ON、取消/背压约束的新双来源视图，而不是简单删除一个 `flatMap`。右源在该 SQL 下逐左行重订阅符合当前冷数据源契约，不能未经明确有界/索引契约而隐式缓存或哈希化。现阶段不为此加入对象池、SQL 特判或复杂 Record 表示，后续若来源 API 提供有界索引/查询下推，再单独以接口契约和多 JOIN 形态验证。

### 多维原始行分组的通用扩展（计划，2026-10-06）

目标：对已有增量聚合、直接表源与默认属性读取能力所覆盖的多维分组，避免逐输入行创建 `ReactorQLRecord` 及中间分组键列表；保留当前按键累加器、窗口、活跃键限制和按 demand 输出。当前 `WindowedAggregateStage.supportsRawKeyed` 仅允许一个维度，而同阶段的复合键查找/存储、窗口前缀和输出已支持多个维度。这是表达式能力与执行器之间的结构性缺口，不针对某个 SQL 文本、键类型或行数。Owning module 为 `WindowedAggregateStage`，测试与 JMH 分别放在现有 `RawKeyedAggregateTest` 和 `src/jmh/`。

先建立同一查询的高基数与重复复合键 JMH/GC 基线，并用测量期 JFR确认 Record/键列表是逐行成本。仅当所有维度都具有 `RawScalarValueMapper` 且接受该源、每个维度不依赖前一维写入的 `_group_by_key` 时进入原始行路径；多维情况下还要求分组列与属性 mapper 为内置实现，避免旧扩展依赖维度间 Record 变化。否则维持现有 Record 路径。原始行按现有维度顺序分别计算窗口前缀和后缀；后缀为空时仍推进该前缀窗口计数，前缀为空则跳过，与现有 `SubscriptionState.add` 一致。非 Map 行继续原 Record 路径；自定义 FROM/属性/分组 Feature、checkpoint 与需要保留最后原始行的投影不改变。无跨行值缓存、无自定义 Subscriber、无新增资源配置或默认限制。

验证覆盖无窗口、窗口在首/中/末、重复与唯一复合键、空维度、`_group_by_key` 依赖回退、混合 Map/Record 源、HAVING、请求/取消/错误、Context、输出顺序、组键隔离与预算。阶段末统一完整构建和 `git diff --check`，同 JDK/堆/夹具做正式 JMH/GC A/B；单维分组、全局聚合、宽投影和多层子查询作负对照。若多维路径未明确改善吞吐或分配，或任一边界不能等价，则撤回生产扩展。B/输入行不是常驻堆，不申领没有 live-heap A/B 支持的常驻收益。

基线：新增 `CompositeKeyAggregateBenchmark`，同一 50,000 条预构造 Map 输入执行 `group by product,device` 的四项增量聚合，分别验证 50,000 个唯一复合键和 256 个重复复合键的输出数量及总计数。性能 JAR SHA-256 `f3012cee9da7c5800ae36820a41c31e828d34fb0d8cbf4d0250b37218f7907d7`；JDK 17.0.18、512 MB/G1、3×1s 预热、5×1s 测量、2 forks、GC profiler 的 `target/jmh-composite-raw-before-20261006.json`：唯一键 **2.930±0.067 M 输入行/s、1373.021 B/行**；重复键 **11.641±1.891 M、373.288 B/行**。后者 fork 间波动较大，after 需同轮负对照或 A/B/A 核对吞吐，分配以同夹具 GC profiler 为准。测量区间附加 JFR `target/jfr-composite-repeated-before-20261006.jfr` 有 852 个 worker CPU、3550 个分配样本；分配类含 `LinkedList` 755、其节点 420、`DefaultReactorQLRecord` 415，指向每行分组键写入与 Record 包装。样本不能换算字节、CPU 占比或常驻堆。

实施：只把已有 `RawScalarValueMapper` 的逐维读取扩展到多个内置普通列；先算窗口前缀，后算剩余维度，保留 null 键和窗口计数的原顺序。多维时要求内置 `GroupByValueFeature` 与 `PropertyMapFeature`，排除 `_group_by_key` 和复杂/自定义分组表达式；自定义 FROM、属性 Feature、扩展 Record、非 Map 行及单维路径保持原有回退。没有新增 Publisher 操作符、缓存、配置或活跃状态。`RawKeyedAggregateTest` 新增无窗口与窗口首/中/末、HAVING、null 维度、混合 Map/Record、组键隔离、冷源 Context/取消/错误、资源预算和扩展 Feature 回退测试；与默认属性 Feature 的 Record 路径逐条比较值和顺序。最终 `mvn -q -Pjmh package` 通过（495 tests、0 failures/errors/skipped），`git diff --check` 与新增文件空白检查通过；最终 JAR SHA-256 `9d7795dc579e5e03ba9fddda429545cb9ba5eb906aaf1bf0dcc2183e30589b5b`。

同配置最终 A/B 为 `target/jmh-composite-raw-before-20261006.json` → `target/jmh-composite-raw-final-20261006.json`：唯一键 **2.930±0.067→3.749±0.045 M 输入行/s**、**1373.021→981.019 B/行**（约 +28.0% 吞吐、-28.6% 瞬时分配）；重复键 **11.641±1.891→24.220±0.586 M**、**373.288→5.164 B/行**（约 +108% 吞吐、-98.6% 瞬时分配）。两种吞吐误差区间不重叠，且最终复测与首次 after 值接近；重复键最终每次只物化 256 组输出，其分配按 50,000 输入行摊销。`target/jmh-composite-negative-final-20261006.json` 中，单维高基数、全局聚合、无 WHERE 宽投影、三层子查询的瞬时分配分别为 765.012、约 0、736.009、576.144 B/行，均与各自先前同夹具记录一致；吞吐存在跨轮波动，不据此申领无关场景收益或断言微小回退。此优化主要去掉逐输入行 Record/键链表，未改变每键精确累加状态；常驻堆结果见下，不把某个复合键负载的倍率外推到全部 SQL。

常驻堆补充验证计划：Record 路径的 `GroupState.lastGroupKeys` 可能为每个活跃复合键保存最后一条 `_group_by_key` 链表，原始行路径不需要该列表。仅扩展已有 `HighCardinalityLiveHeapProbe` 的诊断参数，以相同最终 JAR、同一无界未完成源、10,000/50,000 唯一复合键分别比较内置原始行路径和通过等价自定义属性 Feature 强制的融合 Record 路径。等待全部键进入后触发 GC、读取 live heap 与类直方图，取消后再次核对释放。若斜率差稳定且直方图证实链表归属，再申领该负载的常驻堆收益；否则只保留已证明的瞬时分配和吞吐结果。探针不进入生产模块，不修改默认限制或结果语义。

常驻堆结果：仅扩展 JMH 源下已有探针的 `--composite`/`--record` 参数，`mvn -q -Pjmh -DskipTests package` 通过；探针 JAR SHA-256 `9fe729a7895f80f6151eb3a081697745923a9633e2a6b8d6b73e80f32c745f23`，正式性能测量之后的生产改动只有解释性注释，没有执行逻辑变化。JDK 17.0.18、512 MB/G1、每键一条输入、源接 `Flux.never()`、全部键进入后 5 次 GC 的 live heap：10,000 键原始行/Record 路径约 **6.372/7.170 MB**，50,000 键约 **16.001/19.995 MB**；两点斜率分别约 241/321 B/新增键，差约 **80 B/键**。50,000 键的同源 post-GC 差额约 **4.0 MB（20%）**，取消后四组均回到约 4.22 MB。额外暂停直方图中 Record 路径有 50,000 个 `LinkedList`（1.6 MB）与 100,000 个节点（2.4 MB），原始行路径没有这些类的存活实例；两边均保留 50,000 个 `GroupState`。这与每键不再保留末行组键链表的所有权变化吻合。该结论限于探针的活跃复合键与相同结果查询，不声称所有 SQL 的堆占用均下降，也不把瞬时 B/行当作 live heap。

### 完整复合键的分组维度快照共享（计划，2026-10-06）

目标：减少精确多维分组每个活跃键的常驻堆与首次建组分配，同时保持吞吐。owning module 为 `WindowedAggregateStage`；不更改默认活跃键限制、输出、背压/取消、Feature 选择或聚合方式。当前完整复合键的 `CompositeKey` 已持有一份只在建组时复制的维度数组，`GroupState.snapshotGroupValues()` 又复制一份。仅当后缀键或窗口前缀键恰好覆盖**全部**维度时，让状态直接共享该键的私有 final 数组；窗口处于维度中间时继续独立快照。输出 `_group_by_key` 仍生成独立 List，不暴露索引数组。不得引入多套累加器布局、逐行类型特判、缓存或自定义 Subscriber。

现有 `CompositeKeyAggregateBenchmark` 的同 JAR 基线（SHA-256 `9fe729a7895f80f6151eb3a081697745923a9633e2a6b8d6b73e80f32c745f23`）及 `HighCardinalityLiveHeapProbe --composite` 作为 before；本阶段统一跑完整测试、复合键 JMH/GC 与 10,000/50,000 活跃键 live-heap after，另以重复键、单维、全局聚合和窗口中段为负对照。验收为功能与取消/隔离等价、每键 live-heap 斜率和首次建组分配明确下降，吞吐无稳定回退。若收益未达到一份数组的预期或需增加复杂状态表示则撤回。JFR 数组样本混有累加器数组，不把所有 `Object[]` 归因于此候选。

结果：`WindowedAggregateStage.SubscriptionState.snapshotGroupValues` 仅在新分组索引键覆盖全部维度时复用 `CompositeKey.values`；窗口位于维度之间仍复制，单维仍按原方式直接保留值。完整键数组只由索引键创建后读，输出组键另建 List。现有 `RawKeyedAggregateTest` 对无窗口及窗口首/中/末、Record 回退、组键隔离、请求/取消、错误与预算的回归均通过；完整 `mvn -q -Pjmh package` 为 **495 tests、0 failures/errors/skipped**，`git diff --check` 通过。after JAR SHA-256 `fdf1d04501ca29d3570a6b6a3bfe4eac672f28cc1ebd325942c567d55830e18b`。

同 JDK 17.0.18、512 MB/G1、3×1s 预热、5×1s 测量、2 forks、GC profiler，`target/jmh-composite-raw-final-20261006.json` → `target/jmh-composite-shared-values-after-20261006.json`：50,000 个唯一复合键 **3.749±0.045→3.807±0.043 M 输入行/s、981.019→957.019 B/行**；256 个重复复合键 **24.220±0.586→23.593±0.434 M 行/s、5.164→5.041 B/行**。两项吞吐误差区间均有重叠，不能申领稳定提速，也未证明稳定回退；分配的 -24 B/唯一键和按 256/50,000 摊销的 -0.123 B/重复输入行与删除一份双元素数组一致。`target/jmh-composite-negative-final-20261006.json` → `target/jmh-composite-shared-values-negative-20261006.json`：单维五聚合、全局聚合、三层子查询的分配仍分别为 765.012、约 0、576.144 B/输入行，吞吐误差区间重叠；窗口中段由功能回归覆盖，不声称其性能变化。

活跃键堆使用同一个未完成窗口、每键一条输入、`--composite --count` 与五次 GC 口径：10,000 键约 **6.372→6.130 MB**，50,000 键约 **16.001→14.774 MB**，两点斜率约 **241→216 B/新增键**；50,000 键约少 **1.23 MB（7.7%）**。取消后均回到约 4.22 MB。探针默认五聚合的 after 数字没有同口径 before，不用于收益计算。保留这一处通用状态共享；收益仅针对活跃完整复合键，不外推到单维、窗口中段或所有 SQL。

### 嵌套属性分段的通用成本复核（计划，2026-10-06）

目标：判断多列设备事件查询中，`DefaultPropertyFeature` 对嵌套 Map 路径的逐行 `Pattern.split` 是否是值得消除的跨 SQL 热点；owning module 是属性访问实现，不碰 FROM、Record、Reactor 操作符或 Feature 扩展契约。先新增真实形态的宽嵌套属性 JMH 夹具，预构造多字段 `payload`/`meta` Map，包含筛选、多列投影及同结果原生控制；setup 校验值、顺序和订阅次数。基于当前 JAR 获取同配置 JMH/GC 基线及测量区间内 JFR，确认分段/属性访问的 CPU 与分配来源。若样本与分配显示结构性收益上界足够，再只在默认属性实现中用等价的低分配分段方式替换正则分段，保持直接含点键优先、空段、嵌套解析、数组/集合、自定义子类覆写和错误语义；否则只记录诊断，不改生产。阶段末统一完整测试、同配置 A/B 与宽投影、分组、子查询负对照。禁止按列名/SQL 文本缓存或特调，不增加常驻状态。

基线与热点：`NestedPropertyBenchmark` 以 65,536 条预构造设备事件、14 个嵌套/普通列及嵌套属性筛选，setup 验证 SQL/原生值和顺序相同、SQL 源仅订阅一次。JDK 17.0.18、512 MB/G1、3×1s 预热、5×1s 测量、2 forks、GC profiler 的 `target/jmh-nested-property-before-20261006.json`：SQL **2.603±0.043 M 行/s、1925.714 B/输入行**；同结果原生 **25.593±3.364 M 行/s、159.968 B/行**。这只是全链差额，不归因到单个方法。当前性能 JAR `target/reactor-ql-1.0.21-SNAPSHOT-benchmarks.jar` 在编入该夹具后、改生产代码前的测量区间 JFR 为 `target/jfr-nested-property-measurement-20261006.jfr`：worker CPU 744 个样本中 `Pattern$Start.match`/`Pattern.split` 叶子分别 181/67，另有默认属性访问和 Map 查找；3465 个 worker 分配样本中 `Matcher.<init>` 1421、`Pattern.split` 378、`ArrayList.grow` 766，均沿默认属性分段栈。录制延迟 8 秒开始、持续 12 秒，3×1 秒预热后进入 30 秒 measurement；采样不换算字节或 CPU 百分比。该证据支持只替换 `splitDot(..., 2)` 的通用分段实现，其他 limit 保留正则语义和 protected 覆写入口。

实施与验证：`DefaultPropertyFeature.splitDot` 对唯一的逐行调用形态（`limit=2`）改用 `indexOf('.')` 和两段 substring，保留该 protected 方法及其他 limit 的原正则实现。直接含点键查找仍先于嵌套分段；没有路径缓存、列名分支、额外状态或 Reactor 链改动。`DefaultPropertyFeatureTest` 对空串、前/后空段、连续点、多层点与旧 `Pattern.split` 作等价断言，原有嵌套 Map/集合、含点键和转换测试也通过。完整 `mvn -q -Pjmh package` 为 **496 tests、0 failures/errors/skipped**，`git diff --check` 和新增文件空白检查通过。before/after JAR SHA-256 分别为 `6805a733cca4ab4c32626bfc2764a96792ace0eb5123e89e692da99c01a8388f` / `1eca18d6e5e0233d1252e34d5c5244ab93d8c055d0e70cec59219e7246e9be54`。

同配置 `target/jmh-nested-property-before-20261006.json` → `target/jmh-nested-property-after-20261006.json`：SQL **2.603±0.043→4.255±0.090 M 输入行/s（约 +63.5%）**、**1925.714→707.897 B/行（约 −63.2%）**；原生对照 **25.593±3.364→28.276±2.274 M 行/s**、分配约 **159.968 B/行**不变。SQL 吞吐区间不重叠，但仍远低于原生，不能把总差距归给正则。`target/jmh-nested-property-negative-after-20261006.json` 与同夹具先前基线相比，平铺 16 列、单维高基数五聚合和三层子查询分配分别保持 736.009、765.012、576.144 B/行，吞吐误差区间均重叠。after 测量期 JFR `target/jfr-nested-property-after-20261006.jfr` 的 829 个 worker CPU 与 3517 个分配样本中不再出现 `Pattern.split`/`Matcher.<init>` 叶子；剩余路径片段 String/数组、Map 读取与结果 Map 为主要成本。短 JFR 不用于计算精确 CPU 占比或分配字节。保留此通用改动；若继续减少片段分配，应先证明编译期属性路径与含点键优先、自定义 PropertyFeature/覆写、动态输入及别名语义等价，不能引入全局跨行缓存或只服务该夹具的分支。

### 固定嵌套属性路径的构建期准备（计划，2026-10-06）

目标：继续减少上一阶段 JFR 中固定路径逐行 `String.substring`/数组分配，向同结果原生计算靠近。当前 after JAR SHA-256 `1eca18d6e5e0233d1252e34d5c5244ab93d8c055d0e70cec59219e7246e9be54`；`target/jfr-nested-property-after-20261006.jfr` 的 3517 个 worker 分配样本中 `Arrays.copyOfRange` 1040、`StringLatin1.newString` 1003、`DefaultPropertyFeature.splitDot` 648，主要来自按固定属性名逐行分段。此为采样归因，不换算总字节。owning module 为内置属性 Feature 与表达式 mapper；不改 Reactor 操作符、Record、FROM 或默认限制。

仅内置 `DefaultPropertyFeature.GLOBAL` 的编译期固定路径可准备首段和每层剩余后缀，运行时仍先尝试当前层的完整含点键，再逐层读取，保持缺失/null、集合索引、`this`/`$`、多层含点键、别名及输出顺序。动态属性调用、带类型转换的路径和所有自定义/派生 `PropertyFeature` 保持原方法及覆写时机，不设全局或跨行缓存。新增动态/预备路径等价测试、真实 SQL 含点键和自定义 Feature 反例；阶段末统一完整构建、同配置嵌套 SQL/原生 JMH+GC 及平铺宽投影、高基数聚合、三层子查询负对照。若收益不足或需双套复杂解析状态，撤回候选，不以单一夹具牺牲通用性。

结果：`DefaultPropertyFeature.preparePropertyValue` 仅在构建期为固定、无类型转换的含点路径准备首段和剩余后缀；运行时仍先查完整路径键、再逐层查后缀。`PropertyMapFeature` 只在属性 Feature 恰为内置单例时使用，其他扩展每行继续调用原 `getProperty`；无点路径和带 `::` 转换的路径仍由原动态入口执行。新增动态/准备路径在同一与不同 Map 行上的值、null、多层含点键、集合索引、`this` 与转换等价测试，以及真实 SQL 直接含点键优先和自定义覆写反例。完整 `mvn -q -Pjmh package` **498 tests、0 failures/errors/skipped**；`git diff --check` 与相关文件空白检查通过。after JAR SHA-256 `33ce5c5c634b18aaf12307e6581302755f46f344503c0cf455fbfcf8f3705ae5`。

同 JDK 17.0.18、512 MB/G1、3×1s 预热、5×1s 测量、2 forks、GC profiler，`target/jmh-nested-property-after-20261006.json` → `target/jmh-prepared-property-after-20261006.json`：嵌套 SQL **4.255±0.090→7.601±0.261 M 输入行/s（约 +78.7%）**、**707.897→191.974 B/行（约 −72.9%）**；原生对照 **28.276±2.274→29.094±1.861 M 行/s**、约 **159.968 B/行**不变。SQL 与原生的分配差约 **32.006 B/行**，符合保留结果 Record 的量级，不等于证明全部吞吐差均由 Record 造成；SQL 吞吐仍仅约原生的 26%。`target/jmh-nested-property-negative-after-20261006.json` → `target/jmh-prepared-property-negative-after-20261006.json`：平铺 16 列、高基数五聚合、三层子查询的分配分别保持 736.009、765.012、576.144 B/行；后两项吞吐误差区间重叠，平铺 16 列均值上升但无同轮原生配对，不单独申领该负载提速。

同测量区间附加的 `target/jfr-prepared-property-after-20261006.jfr` 有 830 个 worker CPU、3444 个分配样本；分配叶子主要为必要结果 Map 节点 2279、Record 创建 774、Map 扩容 303，不再采到路径 `splitDot`/`StringLatin1.newString`/`Arrays.copyOfRange`。JFR 样本不换算精确字节或 CPU 占比。保留这处查询级固定路径准备，不跨行缓存输入、不新增常驻键索引或 Reactor 操作符；未测量编译后查询对象的 retained heap，不申领常驻堆绝对下降。后续若追求原生吞吐，须分别证明属性访问和结果 Record 边界的可观察语义，不能为该 14 列夹具直接绕开公开扩展/别名契约。

### 固定含点键的首次 Map 读取（计划，2026-10-06）

目标：检验准备路径中首次完整键读取的 CPU 开销能否用一个等价的直接 Map 读取减少；当前 JAR SHA-256 `33ce5c5c634b18aaf12307e6581302755f46f344503c0cf455fbfcf8f3705ae5`，`target/jfr-prepared-property-after-20261006.jfr` 的 830 个 worker CPU 样本中，`DefaultPropertyFeature.doGetProperty0` 为 313 个叶子，仍是属性访问主要路径。样本不能证明全部叶子可删，故仅实验一处有严格语义依据的分支。owning module 为 `DefaultPropertyFeature.preparePropertyValue`；不改动态属性、扩展 Feature、Reactor 操作符或默认配置。

准备路径只在无转换且含点时生成，完整属性名不可能是 `this`、`$` 或 Map 虚拟属性名。当来源是 Map，可用一次 `Map.get(完整键)` 替代 `doGetProperty0` 的通用判断；命中时结果不变，null 时仍按原路径解析嵌套值；非 Map 来源仍走原实现。补直接键为 null 与 POJO 根来源等价测试。阶段末统一完整测试、同配置嵌套 SQL/原生 JMH+GC 和宽投影/高基数/子查询负对照；只在吞吐有明确收益、分配与负对照不回退且代码保持单一条件时保留，否则撤回。

结果：仅实验性把准备路径的首次完整键读取在 Map 来源改为 `Map.get`，补上直接含点键为 null 时继续嵌套读取及非 Map POJO 来源等价测试；全量构建一度通过（498 tests）。同 JDK 17.0.18、512 MB/G1、3×1s 预热、5×1s 测量、2 forks、GC profiler，`target/jmh-prepared-property-after-20261006.json` 的 SQL/原生分别为 **7.601±0.261/29.094±1.861 M 行/s**，实验版 `target/jmh-dotted-map-direct-after-20261006.json` 为 **7.942±0.138/28.927±1.510 M**；SQL 分配两轮均为 **191.974 B/行**。SQL 吞吐误差区间略有重叠，提升未被确认。回切原实现后同配置 `target/jmh-dotted-map-direct-reverted-20261006.json` 为 **7.425±0.249/27.149±0.728 M**，SQL/原生比值实验版约 0.2745、回切版约 0.2735，说明绝对值变化与机器/运行时漂移相当。按门槛撤回这处生产分支，不申领吞吐或内存收益；保留语义测试，最终源码继续使用通用 `doGetProperty0`。本候选不再追加负对照，因为没有保留执行改动；上一阶段已验证的构建期路径准备不受影响。

回切后最终 `mvn -q -Pjmh package` 再次通过（498 tests、0 failures/errors/skipped），`git diff --check` 与相关文件空白检查通过；最终 JAR SHA-256 `129401138918add5c42aebbdb53fa97dad5b3548ff566d09f3c24cd31c21830c`。相对于上一阶段保留的准备路径，生产执行逻辑无新增改动。

### 同步过滤器的原始行能力传播（计划，2026-10-06）

目标：减少默认单表 Map 查询中被 WHERE 拒绝的行的 Record/响应式操作符分配，不改变 SQL 语义或扩展点。owning module 是 `BetweenFilter`、`LikeFilter`、`FilterFeature` 的 `IS NULL` 分支及其回归测试；只沿用现有 `RawScalarValueMapper`/`RawScalarFilter` 能力，`AND`/`OR` 与默认单表 FROM 的原始行路径不改。当前同 JAR 正式基线 `target/jmh-operator-mix-current-20261006.json`：无 `IN` 的 16 列混合查询为 4.967±0.045 M 输入行/s、242.603 B/行，同结果原生控制约 18.795±0.738 M、140.705 B/行。其测量期 JFR `target/jfr-operator-mix-without-in-current-20261006.jfr` 中仍有 Record 创建、Map 查找、二元比较和 LIKE 的 CPU/分配样本；样本不换算为精确成本。

仅在三个操作符的全部输入 mapper 已声明原始行同步能力、元数据显式允许同步快路且未开启 checkpoint 时，创建带有原有 Record 谓词的 `RawScalarFilter`；原始行谓词保持从左到右求值、null、异常、`NOT`、正则匹配和来源别名判断。默认单表 Map 行可在创建 Record 前筛选；非 Map、扩展 Feature/元数据、异步 mapper、checkpoint、Context、背压和取消保留现有路径。补构建期能力/别名、与 Record 等价、非 Map/扩展回退和订阅契约测试。阶段末集中运行完整构建、同配置 JMH/GC A/B（混合查询和原生控制），并复核宽投影、高基数分组、多层子查询负对照；只在语义测试通过、分配下降且吞吐无稳定回退时保留，不引入通用新抽象或针对单条 SQL 的分支。

结果：三个内置谓词仅在已有 mapper 声明 raw 能力且元数据明确允许时向上传播；普通 Record 谓词保留，`AND`/`OR` 按原有求值顺序组合，非 Map 与自定义 PropertyFeature/元数据仍回退。不增加跨行状态、缓存、默认限制或 Reactor 操作符。新增测试核对 raw/Record 结果、别名、否定、动态 `LIKE`、null、非 Map、自定义属性/元数据和 checkpoint；已有 raw WHERE 测试覆盖需求、取消、Context 与错误传播。沙箱内 `ReactorDebugAgent` 不能初始化，8 个既有窗口示例测试失败；允许本机 agent attach 后完整 `mvn -q -Pjmh package` 通过，**502 tests、0 failures/errors/skipped**，`git diff --check` 通过。最终 JAR SHA-256 为 `f959c548691c34ddb7307900a30b9bdf06ecec1430efa74b473b75772324bef4`。

同 JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler，`target/jmh-operator-mix-current-20261006.json` → 最终源码/JAR 的 `target/jmh-raw-filter-final-20261006.json`：无 `IN` 的 16 列混合查询 **4.967±0.045→5.868±0.245 M 输入行/s（约 +18.1%）**、**242.603→157.214 B/行（约 −35.2%）**。同结果原生控制 **18.795±0.738→17.812±1.032 M**、约 **140.705 B/行**；SQL/原生吞吐比约 26.4%→32.9%，仍不是原生性能。此前实验 JAR 的 `target/jmh-raw-filter-after-20261006.json` 为 **5.949±0.076 M、157.214 B/行**，方向一致。含 `IN` 查询保持原有异步展开路径，最终分配 **474.615 B/行**不变，吞吐 **3.894±0.050→3.832±0.038 M**，区间重叠，不声称该路径收益。

负对照 `target/jmh-raw-filter-negative-20261006.json`：无 WHERE 宽投影、高基数聚合、多层子查询的分配分别为 **736.009、765.012、576.144 B/行**，与 `target/jmh-prepared-property-negative-after-20261006.json` 的对应值一致；聚合和子查询吞吐误差区间与旧轮重叠。无 WHERE 宽投影旧轮 **5.260±0.034 M**，本轮 **5.026±0.073 M**、复测 `target/jmh-raw-filter-repeat-20261006.json` **5.062±0.075 M**，但该路径没有 WHERE，完全不执行本次新增代码；原生控制从首轮 18.339 降至复测 17.641 M，存在运行环境/测量漂移，不能从跨轮差值认定功能无关代码的吞吐回退。复测含 `IN` **3.768±0.033 M**、分配仍为 **474.615 B/行**。这轮只申领无 `IN` 且三个谓词均具备 raw 能力的查询收益；GC B/行表示瞬时分配，不等于 retained heap。负对照之后仅补充查询构建期元数据 opt-in 检查，默认元数据的逐行执行路径不变；最终 JAR 另行复测了关键混合查询和原生控制。

### 原始 Map 普通键 null 回退的重复读取（计划，2026-10-06）

目标：在已验证的默认单表 raw WHERE 路径中，消除普通简单键值为 null 时对同一个 Map 的重复属性读取；owning module 仅内置 `PropertyMapFeature`，不改变扩展 `PropertyFeature`、异步过滤器或默认限制。最终上一阶段 JAR `f959c548691c34ddb7307900a30b9bdf06ecec1430efa74b473b75772324bef4` 的 12 秒测量期 JFR 为 `target/jfr-raw-filter-after-20261006.jfr`：808 个 worker CPU 样本中，`DefaultPropertyFeature.getPropertyValue` 叶子 130 个，主要在 `PropertyMapFeature.applyRaw` 的 null 回退；结果 Record/Map 与数值计算仍是主要分配来源。含 `IN` 的对应 JFR `target/jfr-in-after-raw-filter-20261006.jfr` 有 798 个 worker CPU、3536 个分配样本，逐行 `Mono`/`Flux` 与订阅对象仍多；由于左值可为 Iterable/Publisher/单字段 Map，不为该夹具建立“通常是标量”的同步分支或新增接口。样本不换算 CPU 百分比、精确字节或存活堆。

仅对内置默认属性 Feature 的简单 Map 键，在构建期判定 null 后是否还可能按现有规则得到值：Map 虚拟键（`size`/`empty`/`keys`/`values`/`entries` 及 `$` 形式）和再次清理后名称变化的引号键仍执行原回退；其余普通无点、无类型转换键 null 后直接返回 null。保留首次 `Map.get`、非 null 值优先、自定义 Map 的值语义、非 Map Record 路径和查询构建/订阅时机。补虚拟键被显式置 null、引号键、普通缺失/null、动态属性与自定义 Feature 等价测试。阶段末集中完整构建，并以同配置无 `IN` 混合查询、含 `IN`、原生控制及宽投影/高基数/多层子查询负对照做 JMH/GC A/B；只有分配不增加、吞吐稳定改善且实现保持单一局部判断才保留，否则撤回。

结果：曾实验性增加构建期 null 回退判定，并补普通 null 键单次读取及虚拟键/引号键测试；完整测试与 JMH 打包通过。正式同配置 `target/jmh-raw-filter-final-20261006.json` → `target/jmh-raw-null-after-20261006.json`：无 `IN` 查询 **5.868±0.245→6.290±0.164 M 行/s**、分配均约 **157.213 B/行**；同结果原生控制 **17.812±1.032→19.047±0.700 M 行/s**，约同步上升 7%，SQL/原生比值约 **0.3295→0.3302**。含 `IN` 查询 **3.832±0.038→3.783±0.145 M**、分配均约 **474.615 B/行**。没有证据把无 `IN` 的绝对吞吐增量归因于该分支，也没有分配收益；按门槛已撤回所有实验性生产与测试代码，不继续为此候选跑负对照或申领性能收益。撤回后完整 `mvn -q -Pjmh package` 为 **502 tests、0 failures/errors/skipped**，`git diff --check` 通过，最终 JAR SHA-256 `dda294a1eee8fb20e27a646a5d8bfb02ba4cf4863993e54b1b7cd6c0e63de1a6`；已有 raw WHERE 能力和默认行为不受影响。

### 字面量 LIKE 已匹配片段的行终止符扫描（计划，2026-10-06）

目标：降低普通前缀/后缀 `LIKE` 在内置字面量快路上的逐行字符扫描，保持当前 Java 正则语义。owning module 仅 `LikeFilter` 及其差分测试，不改变动态模式、自定义过滤 Feature、Reactor 边界或默认限制。当前已撤回 null 实验的源码/JAR SHA-256 为 `dda294a1eee8fb20e27a646a5d8bfb02ba4cf4863993e54b1b7cd6c0e63de1a6`；同源码上一 JAR 的无 `IN` 混合查询测量期 JFR `target/jfr-raw-filter-after-20261006.jfr` 在 808 个 worker CPU 样本中有 `LikeFilter.hasLineTerminator` 102 个叶子，主要关联前缀模式。该 JFR 只定位，不将样本换算为 CPU 百分比。

`createLiteralMatcher` 已在构建期将含行终止符的模式退回原 `Pattern`，因此简单前缀/后缀匹配成功后，该已匹配字面量片段可证明没有行终止符；只扫描其余片段即可，`%` 当前映射的 `.*` 仍不得跨五类行终止符。内部/多重 `%`、正则元字符、动态模式和 null/`NOT LIKE` 保持原路径。补长前缀/后缀、五类行终止符和原正则 oracle 等价测试；阶段末统一完整构建、同配置无 `IN` 混合查询/原生控制 A/B，含 `IN`、宽投影、高基数聚合、多层子查询负对照。仅在吞吐有超过同轮控制漂移的稳定改善、分配不增加且实现维持一个小范围扫描函数时保留，否则撤回。

结果：实验性只扫描已匹配前缀后的剩余部分或已匹配后缀前的部分；五类行终止符、长字面量及正则 oracle 差分测试和完整构建均通过。正式同配置 `target/jmh-raw-filter-final-20261006.json` → `target/jmh-like-scan-after-20261006.json`：无 `IN` 查询 **5.868±0.245→6.093±0.101 M 行/s**、分配约 **157.213 B/行**不变；同结果原生控制 **17.812±1.032→18.427±0.532 M**，SQL/原生比值约 **0.3295→0.3307**，收益与同轮环境漂移相当。含 `IN` 查询 **3.832±0.038→3.797±0.035 M**、分配约 **474.615 B/行**不变。未达到稳定吞吐改进门槛，已撤回生产与新增测试改动，不继续跑负对照，也不申领性能收益。撤回后完整 `mvn -q -Pjmh package` 为 **502 tests、0 failures/errors/skipped**，`git diff --check` 通过，最终 JAR SHA-256 `23dbb3d05a5082dcfba70eaa2c8b3134aa5a6bf9bdebd006af6b215cc56f6d48`。

### 混合同步/异步 AND/OR 的布尔映射函数复用（计划，2026-10-06）

目标：降低跨 SQL 形态的混合布尔过滤器每输入行捕获 lambda 的分配，而不改变任何 `Mono` 订阅/操作符边界。owning module 为 `AndFilter.MixedScalarAnd` 与 `OrFilter.MixedScalarOr`，使用现有 `MixedScalarLogicalFilterTest` 的求值顺序、空流、错误、Context 和取消契约；不引入新 SPI、手写 Subscriber、缓存或默认限制。最终当前 JAR SHA-256 `23dbb3d05a5082dcfba70eaa2c8b3134aa5a6bf9bdebd006af6b215cc56f6d48` 的含 `IN` 混合查询测量期 JFR `target/jfr-in-after-raw-filter-20261006.jfr` 中，3536 个 worker 分配样本包括 `MixedScalarAnd` 每行捕获 lambda 124 个、`MonoMapFuseable` 166 个；旧混合 OR JFR 亦采到逐行 `Mono.map` 对象。JFR 只定位来源，不换算精确字节或存活堆。

保持前置标量→异步 `apply`→后置标量→异步订阅的现有顺序，仍对异步结果调用一次 `Mono.map` 并维持原空流补 false 位置；只将已求出的标量真值折叠为无捕获的 identity/常量布尔函数，保留异步源的订阅、错误、取消与 Context。既有测试必须全部通过。当前 JAR 同配置 JMH/GC 基线 `target/jmh-mixed-boolean-mapper-before-20261006.json`：混合 AND **3.816±0.023 M 行/s、474.615 B/行**，混合 OR **2.875±0.093 M、606.660 B/行**；两者各有同轮原生控制。阶段末完整构建、相同 JMH/GC A/B，宽投影、高基数聚合、多层子查询负对照；只在至少一个混合场景稳定减分配、另一个不回退，且吞吐无稳定回退时保留。未证实则撤回，不把采样数当收益。

结果：`MixedScalarAnd`/`MixedScalarOr` 分别复用无捕获的 identity/常量布尔函数，仍保留原位一次 `Mono.map`、空流默认值、前后同步求值及异步订阅。`MixedScalarLogicalFilterTest` 已覆盖左右顺序、两种布尔结果、空流、错误、Context、取消和嵌套调用次序；完整 `mvn -q -Pjmh package` **502 tests、0 failures/errors/skipped**，`git diff --check` 通过。after JAR SHA-256 `059fcc0749094dd238867084d11b76d67695a13dbbe00a6824ba08cdcdebe683`。

同 JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler，`target/jmh-mixed-boolean-mapper-before-20261006.json` → `target/jmh-mixed-boolean-mapper-after-20261006.json`：混合 `AND` **3.816±0.023→3.781±0.070 M 输入行/s**、**474.615→458.615 B/行（−16.000 B，约 −3.4%）**；混合 `OR` **2.875±0.093→2.945±0.019 M**、**606.660→574.660 B/行（−32.000 B，约 −5.3%）**。两条 SQL 的吞吐误差区间均重叠，不申领稳定提速或回退；各自同轮原生控制约 **17.965→18.041 M** 与 **15.896→16.136 M**，分配分别保持 **140.705/190.605 B/行**。负对照 `target/jmh-mixed-boolean-mapper-negative-20261006.json` 的无 WHERE 宽投影、高基数五聚合、三层子查询分配分别为 **736.009、765.011、576.144 B/行**，与先前同夹具结果一致，吞吐区间重叠。按计划保留这一通用低复杂度减分配改动；GC B/行是瞬时分配，不证明活跃键或查询状态的 retained heap 下降。

### 混合同步/异步宽投影的结果容器预估容量（计划，2026-10-06）

目标：消除已知宽投影列数下结果 `HashMap` 的逐行扩容，降低瞬时分配并争取提升吞吐。owning module 为 `DefaultReactorQL.createMapper`，复用 `DefaultReactorQLRecord.setResult(name, value, expectedEntries)` 现有边界；不改变默认资源限制、列求值/订阅顺序、空列语义、星号展开、自定义 Record/Context 容器或 JSON/日期函数行为。当前 JAR `059fcc0749094dd238867084d11b76d67695a13dbbe00a6824ba08cdcdebe683` 的真实 16 列双 JSONPath/日期函数测量期 JFR `target/jfr-wide-functions-current-20261006.jfr` 中，worker 的 `HashMap$Node[]` 扩容样本有 225 个经 `DefaultReactorQLRecord.setResult`，另 289 个经 JSON 解析器；仅前者是本候选目标。采样不等于 B/行或 retained heap。

实施：在无星号且列数已知的投影编译阶段统一计算现有容量提示，混合投影的同步/异步列写入均走该提示；只允许内置默认 Record/Context 首次创建结果 Map 时使用，其他路径继续公开 `setResult`。增加混合投影多列、空异步列、星号覆盖及自定义容器回归测试。阶段末集中完整构建和同配置 JMH/GC A/B：真实宽函数、已解析 JSON 输入、普通宽投影为目标/对照，高基数聚合与多层子查询为负对照。仅在功能测试通过、目标分配下降且无稳定吞吐回退时保留；否则撤回。不做跨列缓存、JSONPath 特判、自定义 Reactor 操作符或新的配置。

结果：`DefaultReactorQL.createMapper` 统一识别无星号宽投影，但仅混合分支的编译期 `ProjectionColumn` 保存容量提示；纯同步原写入路径不变。第一次把所有写入抽到同一 helper 后，多层子查询负对照增加约 8 B/行，已撤回该逐行 helper 形态并复测恢复。最终 `mvn -q -Pjmh package` **503 tests、0 failures/errors/skipped**，`git diff --check` 通过；最终性能 JAR SHA-256 `27ab6298a6263256e69a5a2e6ea7c4992dc4166480909d475db75d0e09dd411b`。

同 JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler 的正式结果：`target/jmh-mixed-result-capacity-before-20261006.json` → `target/jmh-mixed-result-capacity-final-20261006.json`，双 JSONPath/日期函数宽投影 **1.636±0.087→1.738±0.019 M 输入行/s、2610.912→2517.309 B/行（−93.603 B，约 −3.6%）**；预解析 JSON 同查询 **1.567±0.084→1.621±0.049 M、2050.494→1978.492 B/行（−72.002 B，约 −3.5%）**。吞吐误差区间略有重叠，只确认分配下降，不申领稳定吞吐增幅。纯同步普通宽投影 **6.884±0.503→6.451±0.472 M、331.217 B/行不变**，吞吐区间重叠。负对照 `target/jmh-mixed-boolean-mapper-negative-20261006.json` → `target/jmh-mixed-result-capacity-negative-final-20261006.json`：多层子查询 **7.524±0.134→7.582±0.101 M、576.144 B/行不变**；高基数聚合 **3.810±0.391→3.973±0.140 M、765.011 B/行不变**。短诊断 JFR `target/jfr-mixed-result-capacity-final-20261006/` 仍采到结果 Map 的初始桶数组分配，不把它解释为扩容未消除；JFR 样本数不能换算 B/行或 retained heap。保留该低复杂度通用改动。

### 双参数异步 JSON 函数的有序装配（计划，2026-10-06）

目标：减少 JSON 函数两个动态参数中至少一个为异步 Publisher 时，每输入行的通用 `Flux.fromIterable + concatMap + collectList` 编排成本；owning module 为 `JsonPathFunctionMapFeature`。当前 JAR `27ab6298a6263256e69a5a2e6ea7c4992dc4166480909d475db75d0e09dd411b` 的冷异步 JSON 查询短 JFR `target/jfr-cold-json-followup-20261006/` 在 615 个分配样本中采到 `FluxIterable$IterableSubscription` 33、`FluxConcatMap$ConcatMapInner` 30、`FluxConcatMapNoPrefetch` 28、`MonoCollectList` 后备数组 28 等；采样只定位热点。日期 `LocalDateTime` 转换不能直接绕过 `Timestamp.valueOf`：实测夏令时重叠和历史日期的时区解释不同，不纳入本试验。

实施：只在已有通用 JSON 函数恰好两个参数且无法走标量快路时，尝试用 Reactor 原生的串行 `Flux.concat` 组合两个延迟参数 Publisher，仍用 `Mono.fromDirect`、逐参数 `defaultIfEmpty(EMPTY)` 与最终 `collectList` 保留多值参数、订阅顺序、空值占位、错误、取消、Context 和 wrapper；其他参数个数保留原路径。测试必须包含两个异步参数的顺序/Context、左右空流、多值 Publisher、错误和取消。阶段末集中完整构建，同配置正式 JMH/GC 比较冷异步 JSON、同步 JSON、真实宽函数及负对照。只有分配显著下降且吞吐无稳定回退、兼容测试全过才保留；不写自定义 Subscriber、跨行缓存或 SQL 形态特判。

结果：原生 `Flux.concat` 两参数试验的多值、空参数、顺序、Context、错误和取消测试通过，完整构建 505 tests 通过，但正式 JMH/GC `target/jmh-cold-json-concat-before-20261006.json` → `target/jmh-cold-json-concat-after-20261006.json` 显示冷异步 JSON **2.091±0.020→1.873±0.071 M 输入行/s**，分配仅 **2968.039→2948.039 B/行（−20 B）**；同步 JSON 负对照 **3.309±0.016→3.247±0.027 M、2088.028 B/行不变**，真实宽函数 **1.743±0.022→1.736±0.046 M、2517.309 B/行不变**。异步目标吞吐出现明确回退且减分配很小，已撤回生产实现；保留多值/取消测试作为既有契约的回归覆盖。不能以操作符个数减少代替吞吐和堆分配证据。

撤回后最终 `mvn -q -Pjmh package` 为 **505 tests、0 failures/errors/skipped**，`git diff --check` 通过；性能 JAR SHA-256 `79ee01a3791a7b64af03d170ad34d88e4db37d7979a694eb7bd837d69ebc8219`。本轮未保留新的生产快路，仍以此前混合宽投影容量优化为当前有效生产改动。

### 当前 JAR 的高基数常驻堆斜率复核（2026-10-06）

复用 `HighCardinalityLiveHeapProbe`，不新增探针或修改生产代码。JAR SHA-256 `79ee01a3791a7b64af03d170ad34d88e4db37d7979a694eb7bd837d69ebc8219`；JDK 17.0.18、512 MB/G1、open 计数窗口、每键一行、惰性输入、下游不收集结果，阶段标记前显式 GC。四组均完整接收输入、输出 0 行；post-GC `heapUsed`：`count` 10k/50k 键分别为 **5,656,144 / 12,380,400 B**，五标量聚合分别为 **7,094,064 / 19,501,992 B**。40k 键差分斜率约为 **168.1 / 310.2 B/键**，与旧 JAR 同口径的约 168/310 B/键一致。取消后四组分别为 **4,218,592 / 4,220,136 / 4,236,352 / 4,237,400 B**，未见当前改动引入活跃键驻留回退。

当前 JAR 的高基数聚合短 JFR `target/jfr-highcard-global-followup-20261006/` 在 660 个分配样本中采到结果 Map 节点/容器、每键 `GroupState`、`LinkedHashMap` 条目与内置累加器；它只能定位分配，不能给出 retained-size。本次外部 `jcmd GC.class_histogram` 附加因沙箱权限/进程附加边界失败，未取得新的类直方图；此前同探针的类直方图已核对精确键索引与累加器数量。当前精确分组仍需要 O(活跃键数) 状态，保留默认限制行为；不为了压测键分布改写顺序、近似聚合或隐式驱逐。若要更低的无限高基数常驻内存，必须另有明确的窗口/键生命周期或外部状态契约，不能宣称 `avg/max` 的单组 O(1) 状态意味着所有分组可 O(1) 驻留。

### 当前普通宽投影热点的高收益门槛复核（2026-10-06）

复核同一性能 JAR `79ee01a3791a7b64af03d170ad34d88e4db37d7979a694eb7bd837d69ebc8219` 的 16 列普通投影。正式成对 JMH/GC `target/jmh-wide-native-paired-current-20261006.json` 中，有 WHERE 的 SQL/等价原生为 **7.127±0.214/13.553±0.703 M 输入行/s**、**331.217/316.811 B/输入行**；无 WHERE 为 **4.889±0.183/9.979±0.105 M**、**736.009/704.003 B/行**。按有 WHERE 的 29,492/65,536 输出比例换算，两类 SQL 都约多 **32 B/输出行**，对应仍需保留的默认 `ReactorQLRecord` 包装量级；这不是吞吐差距的完整归因。此前测量区间专门附加的 `target/jfr-wide-measurement-attached-20261006.jfr` 中，输出 Map 写入、源 Map 读取与默认属性映射占主要 CPU 样本，结果 Map 节点/桶数组占主要分配样本。当前短 JMH profiler `target/jfr-wide-native-current-followup-20261006/` 还出现映射到 `pow` 的样本，但被测 `controlSql` 不含 `pow`，且 setup 已核对 SQL 与原生逐行结果；仅凭这份样本不能改变函数语义或断言 `pow` 是该 SQL 的可优化热点。

结论：当前宽投影的结果 Map、原始属性读取与 Record/Feature 扩展边界都是真实工作；此前投影数组化、普通 Map 直接读、等值类型分支等局部试验未给出稳定的跨场景收益。为了守住“不特调、不过度设计”的门槛，本阶段不再增加 SQL/列形态专用快路、自定义 Subscriber 或隐式缓存，也不把操作符数量减少当作性能收益；没有新的生产代码改动。已有高收益改动（固定嵌套属性准备、同步原始行过滤、多维原始行增量分组）仍保留，以各自成对 JMH/GC 与功能回归为验收依据。下一候选须先在当前 JAR 的测量区间证明一个跨查询的可删除对象/算法步骤，并给出扩展 Feature、背压/取消和默认限制的等价边界，再进入实现和集中验证。

### 固定文本修剪正则的编译成本（计划，2026-10-06）

目标：评估常用文本清洗查询中 `ltrim`/`rtrim` 每行 `String.replaceAll` 重新编译固定 `^\\s+`/`\\s+$` 正则的吞吐与瞬时分配成本。owning module 为 `DefaultReactorQLMetadata` 的两个内置标量函数，JMH 夹具和函数语义测试分别放在现有 `src/jmh/`、`ReactorQLTest`；不改 `trim`、其他函数、Reactor 操作符、Feature SPI、默认限制或跨行查询状态。先以当前生产 JAR 加入真实多列文本清洗夹具，预建不同行首/行尾空白、无空白和 Unicode 文本，setup 校验 SQL 与原生 Java 正则的结果、顺序和订阅次数；录正式 JMH/GC 基线和测量区间 JFR。只有 JFR 明确显示这两个固定正则的编译/匹配成本，才把它们改为静态预编译的不可变 `Pattern`，逐行只创建 `Matcher`，不手写不完全等价的空白扫描器。以 Java 原 `replaceAll` 作 oracle 覆盖 ASCII `\\s`、行终止符、Unicode、空串和 null 边界；阶段末统一跑完整测试、同配置目标/原生对照 A/B 与普通宽投影、高基数聚合、多层子查询负对照。保留门槛是目标查询分配明确下降、吞吐无稳定回退、跨数据形态语义一致；否则撤回生产候选。不申领 retained heap 改善，因为两个 Pattern 是进程级小常量。

基线：`StringTrimBenchmark` 预构造 65,536 个 Map 输入，投影 `id`、两种修剪、`upper(name)` 和 `length(text)`；setup 逐行验证与 Java 原生 `String.replaceAll` 的结果、类型、顺序及源恰好一次订阅。JAR SHA-256 为 `e163c4060c3a9a5624504af66075c3d55cbb8951875ba6e3be8461c4d6954f2f`。JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler 的 `target/jmh-string-trim-before-20261006.json`：SQL **4.148±0.040 M 输入行/s、2041.414 B/行**，同结果 Java 正则控制 **4.954±0.073 M、2009.407 B/行**。控制同样逐行编译正则，因此不是优化后的原生上界。短诊断 JFR `target/jfr-string-trim-before-20261006/.../profile.jfr` 的 550 个 CPU 与 2716 个分配样本中，SQL benchmark 栈内有 `Pattern.compile` CPU 46 个叶子、分配 1071 个叶子；分配栈明确经 `String.replaceAll` 回到 `DefaultReactorQLMetadata` 的 `ltrim`/`rtrim` 第 175/176 行。这是结构性、非 SQL 文本特调的固定正则重复编译，足以进入预编译 Pattern 的单一低复杂度试验；样本不换算 CPU 百分比、字节或存活堆。

实施与验收：仅在 `DefaultReactorQLMetadata` 为两条固定正则各保存一个静态不可变 `Pattern`，逐行仍调用 `matcher(...).replaceAll("")`；这与原 `String.replaceAll` 的 Java 正则实现保持同一表达式、替换和空白定义，没有增加查询级缓存、条件分支、操作符或默认配置。`ReactorQLTest.testLeftAndRightTrimKeepJavaRegexSemantics` 逐值与旧实现比较空串、六类 ASCII 空白、内嵌空白、Unicode 空白、数字及 null 省略。完整 `mvn -q -Pjmh package` 为 **506 tests、0 failures/errors/skipped**；最终 JMH 夹具扩充同结果的预编译 Java 对照后仅重打包，最终 JAR SHA-256 `58c694eb21cbfdf4e090568d0c40e5934cce7e191a574840346498bf5eb55d0d`。`git diff --check` 与新夹具空白检查通过。

同配置正式 A/B `target/jmh-string-trim-before-20261006.json` → `target/jmh-string-trim-after-20261006.json`：SQL **4.148±0.040→5.462±0.418 M 输入行/s（约 +31.7%）**、**2041.414→1041.413 B/行（约 −49.0%）**。两轮吞吐误差区间不重叠；未优化 Java 控制从 4.954±0.073 降至 4.595±0.035 M、分配 2009.407→1973.405 B/行，说明存在环境/JIT 漂移，但不能解释 SQL 的反向大幅改善。最终夹具的同轮复测 `target/jmh-string-trim-prepared-native-20261006.json` 中，SQL **5.324±0.602 M、1041.413 B/行**；同结果、同样复用预编译 Pattern 的 Java 实现 **6.788±0.034 M、991.406 B/行**，SQL 约达到该原生对照的 78% 吞吐、每输入行多约 50 B 瞬时分配。不要把这个比值外推到其他 SQL 或称已完全达到原生性能。

after 短 JFR `target/jfr-string-trim-after-20261006/.../profile.jfr` 的 SQL benchmark 栈不再出现 `Pattern.compile` CPU/分配叶子，剩余主要是逐行 `Matcher`、必要结果 Map 与 Record。负对照 `target/jmh-string-trim-negative-20261006.json`：无 WHERE 普通宽投影、高基数五聚合、三层子查询分别为 **736.009、765.011、576.144 B/输入行**，与旧同夹具记录一致；吞吐误差区间与已有基线重叠，未确认无关场景回退。本项保留：收益来源是两个内置函数通用的固定正则编译消除，而不是某种 SQL、列名或输入分布的专用快路。Pattern 常驻空间极小但尚无 live-heap 测量，不申领常驻堆下降。

### 普通日期文本转换中的时钟读取（计划，2026-10-06）

目标：确认 `CastUtils.castDate(String)` 在没有当前时间占位符的普通日期文本上无条件调用 `LocalDateTime.now()`，是否构成跨日期 SQL 的真实 CPU/分配热点。owning module 为通用日期转换工具，基准覆盖内置 `date_format` 的真实日期列查询及相同转换/输出的 Java 控制；不改日期格式、默认时区/夏令时解释、异常、Feature/Publisher 选择或资源上限。先新增预建 65,536 行的普通日期文本夹具，setup 核对全量结果、类型和一次源订阅，再对当前生产 JAR 录同配置 JMH/GC 与测量期 JFR。只有 JFR 确认无条件时钟读取有足够成本，才尝试在发现 `yyyy/MM/dd/hh/mm/ss` 任一模板占位符时再取一次当前时间，并继续用同一个快照替换全部占位符；无占位符时保留原解析与回退顺序。用普通日期、模板组合、边界和异常回归验证，阶段末完整测试及同配置 A/B、宽投影/聚合/子查询负对照。若收益小于波动或语义风险高则不改生产；不引入跨行缓存或按 SQL 形态分支。

基线与计划收敛：`DateStringCastBenchmark` 在 65,536 个预建 Map 行上执行 `date_format(eventTime,'yyyy-MM-dd HH:mm:ss')`，Java 控制复用相同 `CastUtils.castDate` 与 formatter；setup 核对全量输出与一次源订阅。未改生产代码的基线 JAR SHA-256 `46bb7da8c3fb4ab056097f332d2515a6a32b343bc7450662b529cdb4f0178ea2`；同 JDK 17.0.18、512 MB/G1、3×1s 预热、5×1s 测量、2 forks、GC profiler 的 `target/jmh-date-string-before-20261006.json` 为 SQL **2.804±0.027 M 输入行/s、2400.013±38.247 B/行**，同转换 Java 控制 **3.623±0.063 M、2064.004 B/行**。测量 JFR `target/jfr-date-string-before-20261006/.../profile.jfr` 中有 2733 个 worker 分配样本：`Pattern.compile` 922 个叶子经 `StringUtils.isNumber → isInt → String.matches` 回到 `CastUtils.castDate:248`；`LocalDateTime.ofEpochSecond` 516、`LocalTime.create` 346 个叶子经无条件 `LocalDateTime.now()` 回到 `castDate:256`。CPU 仅 24 个样本，不据此估 CPU 占比；分配栈足以证明两处重复工作。调整本节试验范围为同一 `CastUtils.castDate(String)` 中的这两处：把当前依赖实现的有符号整数/小数完整匹配规则合并成一个静态预编译 `Pattern`，保持小数字符串随后 `Long.parseLong` 的原异常时机；仅遇当前时间模板占位符才读取一次时钟，仍用同一个时间快照完成所有替换。不得把普通日期文本、数字文本或某个 SQL 写成专用分支；功能差分必须覆盖 `StringUtils.isNumber` 对字符串的正反例、普通日期、模板组合、溢出/小数异常、时区/夏令时及原回退顺序。只在完整测试、目标同配置 A/B 和负对照均通过且收益明确时保留；否则撤回候选。

实施与验收：`CastUtils.castDate(String)` 仅将旧依赖对字符串使用的 `[-+]?\\d+` 与 `[-+]?\\d+\\.\\d+` 完整匹配合并为同语义、静态预编译的 `[-+]?\\d+(?:\\.\\d+)?`；仍由 `Long.parseLong` 处理匹配的数字，因此小数和溢出字符串继续在原位置抛 `NumberFormatException`。普通日期文本不再读取无用时钟；模板在检测到任一占位符后才取得一次 `LocalDateTime.now()`，六种替换顺序与单快照保持不变。没有引入跨行输入缓存、SQL/列名分支、Reactor 操作符或默认配置。`CastUtilsTest` 新增整数、小数/溢出异常、非数字回退和 `yyyy` 模板回归；现有日期格式、LocalDateTime/Date 转换测试继续通过。完整 `mvn -q -Pjmh package` 为 **507 tests、0 failures/errors/skipped**；最后仅把 JMH 夹具的日期数字格式固定为 `Locale.ROOT` 并重打包，最终 JAR SHA-256 `e9ac9ef46a0feea60341c37b2d36c299d884d6c2653124220ff69377c93d05a1`。`git diff --check` 通过。

同配置正式 `target/jmh-date-string-before-20261006.json` → 最终夹具 `target/jmh-date-string-final-20261006.json`：日期 SQL **2.804±0.027→3.711±0.026 M 输入行/s（约 +32.3%）**、**2400.013→1272.013 B/行（约 −47.0%）**；相同通用转换的 Java 控制 **3.623±0.063→5.257±0.099 M**、**2064.004→936.004 B/行**。初次 after `target/jmh-date-string-after-20261006.json` 的 SQL **3.724±0.021 M、1272.013 B/行**，方向和最终复测一致；两轮 SQL 吞吐误差区间均与 before 不重叠。after JFR `target/jfr-date-string-after-20261006/.../profile.jfr` 的 SQL worker 栈不再采到 `Pattern.compile` 分配叶子，也没有经 `LocalDateTime.now()` 的分配样本；仍有每行 `Matcher`、实际日期解析、必要结果 Map 和 Publisher 对象，不把 JFR 样本换算字节或 live heap。

负对照 `target/jmh-date-string-negative-20261006.json` 的普通无 WHERE 宽投影、高基数五聚合、三层子查询分配分别为 **736.009、765.012、576.144 B/输入行**，与上阶段同夹具一致；吞吐误差区间重叠，未确认无关场景回退。保留本项通用日期字符串转换优化。它降低瞬时分配与 GC 压力，但静态 Pattern 占用少量常驻空间，本阶段未测目标查询 live heap，不能宣称 retained heap 下降，也不能以单个日期查询代表所有 SQL 已接近原生。

### 中段窗口单后缀分组的常驻状态（计划，2026-10-06）

目标：验证 `group by deviceId,_window(n),type` 在每个前缀只有一个后缀键时，`PrefixState` 每键预建 `LinkedHashMap` 的常驻堆和首次建组成本；owning module 是 `WindowedAggregateStage`。先扩展现有诊断夹具，测一后缀与多后缀的 10,000/50,000 活跃前缀 live heap、取消后释放、JMH/GC 及 JFR；用当前性能 JAR 作为 before。若 Map 确为主要冗余且收益明显，只在该通用状态容器中内联首个键/组，遇第二个不同键时升级为保持插入顺序的 Map。保持键精确相等、窗口计数、默认限制、输出顺序、背压、取消、错误和自定义 Feature 回退；不做 SQL 文本识别、跨行缓存、新聚合架构或自定义订阅器。验证一/多后缀、重复键、窗口关闭及负对照，阶段末集中完整测试与同配置 A/B。若收益不清楚或需扩大状态复杂度，撤回候选。

基线与热点：新增 `MiddleWindowAggregateBenchmark` 在同 50,000 预建输入行/25,000 前缀下覆盖每前缀一个后缀、两个后缀及无后缀；setup 校验输出组数和总输入计数。JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler 的 `target/jmh-middle-window-before-20261006.json`：单后缀 **6.004±0.288 M 行/s、725.844 B/行**，双后缀 **3.707±0.167 M、1137.845 B/行**，无后缀 **7.974±0.109 M、549.844 B/行**。单后缀测量期 `target/jfr-middle-window-before-20261006/.../profile.jfr` 的 928 个分配样本中，154 个经过 `PrefixState.getGroup`，其中 Map 桶数组/Entry 分别 58/42 个；193 个 CPU 样本中 12 个经过该方法。采样只用于归因，不换算精确 CPU 或分配字节。

实施与验收：`PrefixState` 首个后缀键/组直接存字段，重复键按原等值语义复用；第二个不同键才升级为 `LinkedHashMap`，先放首组再放新组。窗口末尾的既有单组路径不变。统一的 `groupCount`、迭代、排空和关闭继续负责预算扣减与取消释放，不新增 Publisher/Subscriber、输入缓存或默认限制。新增测试覆盖复合后缀重复键→升级→再命中、首次出现顺序、原始行/Record/兼容路径一致、资源上限及关闭窗口未排空时取消。完整 `mvn -q -Pjmh package` **509 tests、0 failures/errors/skipped**；`git diff --check` 和本节相关未跟踪文件空白检查通过。最后仅将单组键比较的方向与 `HashMap.get(lookup)` 一致，最终性能 JAR SHA-256 `730b0a7f33aea1d1920379feed2d94e5fe98bc0900712f94321142ff1c81cdad`。

同配置正式 A/B：基线 `target/jmh-middle-window-before-20261006.json` → 最终 JAR `target/jmh-middle-window-final-one-20261006.json`，单后缀 **6.004±0.288→7.251±0.474 M 输入行/s（约 +20.8%）**、**725.844→589.844 B/行（−136 B，约 −18.7%）**；吞吐误差区间不重叠。比较方向调整前的 `target/jmh-middle-window-after-20261006.json` 为 **7.376±0.534 M、589.844 B/行**，与最终复测方向一致。同一 after 测量中，双后缀 **3.707±0.167→3.647±0.201 M、1137.845 B/行不变**，无后缀 **7.974±0.109→7.978±0.079 M、549.844 B/行不变**，吞吐误差区间均重叠；最终键比较方向调整不影响这些标准整数键的匹配。单后缀 after JFR 的 930 个分配样本中，经 `getGroup` 的 Map 桶数组/Entry 均为 0，支持已消除逐前缀 Map 分配；JFR 是该微调前的同实现结构录制，不申领精确 CPU 百分比。

`HighCardinalityLiveHeapProbe --middle-window --count` 在每键一行、输入源保持开放、5 次 GC 的相同口径下：10,000/50,000 活跃前缀从 **8.062/24.414 MB→6.302/15.587 MB**，两点斜率约 **409→232 B/新增前缀**；50,000 键约少 **8.83 MB（36.2%）**。双后缀 10,000/50,000 前缀约 **9.747/32.806→9.751/32.842 MB**，微小差异不申领收益也未见显著退化。四组取消后均约 4.22 MB，表明状态可释放。该结论限于中段窗口单后缀/高基数形态，不把瞬时分配或 JFR 样本当成所有查询的常驻堆收益。保留这一处通用状态容器优化。

### 异步 WHERE 原生过滤操作符的低复杂度试验（计划，2026-10-06）

目标：判断所有异步 WHERE 的 `concatMap(predicate.mapNotNull(...))` 是否可用 Reactor 原生 `filterWhen(predicate, 1)` 表达同一有序过滤，减少逐行 Mono 包装与订阅分配，而不引入自定义操作符。当前 JAR `730b0a7f33aea1d1920379feed2d94e5fe98bc0900712f94321142ff1c81cdad` 的混合运算符 SQL 短 JFR `target/jfr-operator-mix-deep-20261006/.../profile.jfr` 记录到 `FluxConcatMap` 排空 CPU 叶子，且 203 个 worker 分配样本的栈经过 `DefaultReactorQL.createWhere` 的逐行 `Mono.mapNotNull`；该数字只是热点方向，不能换算性能收益。owning module 仅 `DefaultReactorQL.createWhere`，不改谓词、Feature SPI、同步/raw 快路或默认并发与资源限制。

先用当前 JAR 对固定列表 IN、动态 IN、无 IN 同投影、宽函数查询录 JMH/GC 基线。实验使用 `filterWhen(..., 1)` 保持一次只评估一个谓词、顺序与背压；补/复用空谓词、错误、Context、请求及取消测试。阶段末完整构建和相同 JMH/GC A/B，若目标吞吐/分配无明确收益、负对照回退或任何语义/需求变化，则撤回该单行替换。禁止新状态层、专用 SQL 分支或假设输入通常为标量。

结论：当前 JAR 的正式基线 `target/jmh-async-where-filterwhen-before-20261006.json` 中，固定 IN、动态 IN、无 IN 混合投影和宽函数分别为 **3.792±0.027/2.508±0.051/5.685±0.188/1.763±0.011 M 输入行/s**，逐行分配 **458.615/1039.949/157.214/2517.309 B**。仅替换异步 WHERE 为 `filterWhen(predicate, 1)` 后，完整测试出现两项真实契约回归：`SubqueryCacheTest.shouldFailBeforeCachingUnboundedSubqueryResult` 的原 `ReactorQLException` 被 `CompositeException` 包装；`ReactorQLTest.testErrorFunc` 的 checkpoint + `onErrorContinue` 从完成变为 `CompositeException` 错误。失败发生在正式 after JMH 之前，已立即撤回该生产替换，未改测试、错误处理或谓词实现，也不申领任何性能收益。这证明原生操作符名称更贴近过滤语义仍不足以替代当前的错误传播契约；若要解决 `IN` 主导的逐行编排成本，需要独立的通用能力或更强证据，不能用软特调或逐场景错误兜底。

### 日期文本数字判别的逐行 Matcher（计划，2026-10-06）

目标：减少通用 `CastUtils.castDate(String)` 对每个普通日期文本创建数字正则 `Matcher` 的成本，不改变日期格式、数字异常、回退、时区或当前时间占位符语义。owning module 为 `CastUtils`；不触及 SQL 编译、Reactor 链或默认限制。撤回上一节操作符实验后的完整 `mvn -q -Pjmh package` 恢复 **509 tests、0 failures/errors/skipped**，当前基线 JAR SHA-256 `821dc53f2b08865e805014666c191cb304042dcb6f77ef7b47f84724a86a43a6`。`DateStringCastBenchmark` 当前同配置正式 JMH/GC `target/jmh-date-numeric-scan-before-20261006.json`：日期 SQL **3.706±0.040 M 输入行/s、1272.013 B/行**，同转换 Java 对照 **5.184±0.018 M、936.004 B/行**。目标查询短 JFR `target/jfr-date-numeric-scan-before-20261006/.../profile.jfr` 的 1248 个 worker 分配样本中，`Pattern.matcher`/`Matcher.<init>` 叶子分别 36/72；此为可删对象的归因，不换算字节或 CPU 占比。

仅以一个无状态字符扫描替换 `[-+]?\\d+(?:\\.\\d+)?` 的匹配，保持 ASCII 数字、可选符号、整数与小数的完整匹配规则，以及原 `Long.parseLong` 对小数/溢出的异常时机；其他日期解析与回退顺序原样保留。新增边界/Unicode/异常等价测试，阶段末集中完整测试、同配置日期 SQL/Java JMH+GC A/B 与宽投影、高基数聚合、多层子查询负对照。只有目标分配和吞吐有明确收益、语义等价且负对照无稳定回退才保留；不做日期 SQL 或数据分布特调，不增加缓存和状态。

实施与验收：`CastUtils.isNumericDateText` 只扫描可选符号、ASCII 整数位和可选的小数位，保留 `Long.parseLong`、普通日期与模板解析的原顺序；删除静态 Pattern 与逐行 Matcher，没有额外状态或 SQL/列名判断。`CastUtilsTest` 对长度 0–4、由数字/符号/小数点/非数字/阿拉伯及全角数字组成的 **4,681** 个字符串，与旧 Java 正则逐项比较；既有数字、小数/溢出异常、日期/时区、模板回归继续通过。完整 `mvn -q -Pjmh package` **510 tests、0 failures/errors/skipped**，after JAR SHA-256 `35e30d674c427536c0f0d7a81a1103942c1abfc7901d9d99d75b2353e6af4e01`。

同 JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler 的 `target/jmh-date-numeric-scan-before-20261006.json` → `target/jmh-date-numeric-scan-after-20261006.json`：真实日期 SQL **3.706±0.040→4.288±0.043 M 输入行/s（约 +15.7%）**、**1272.013→1064.012 B/行（−208 B，约 −16.4%）**；同结果、同转换的 Java 对照 **5.184±0.018→6.623±0.421 M 行/s**、**936.004→728.003 B/行**。SQL 吞吐误差区间不重叠。after JFR `target/jfr-date-numeric-scan-after-20261006/.../profile.jfr` 中，经 `CastUtils.castDate` 的正则 Matcher/内部整数数组分配样本从 36/72 降为 0/0；采样不换算字节或精确 CPU 占比。

负对照 `target/jmh-date-numeric-scan-negative-20261006.json` 中无日期转换的普通宽投影、高基数五聚合和三层未关联子查询分别为 **736.009、765.012、1276.611 B/输入行**，与既有同夹具基线一致；吞吐分别为 **4.792±0.421、3.706±0.388、4.298±0.124 M 行/s**，与最近旧版本同夹具测量区间重叠。它们不是当前生产 JAR 的严格成对 before，故只作为无分配变化/未见明确回退的旁证。保留这处通用转换优化；它降低短命分配和 GC 压力，不据此申领日期查询的常驻堆绝对下降，也不把单一查询收益外推到所有 SQL。

### 文本两侧修剪的正则匹配成本（计划，2026-10-06）

目标：减少内置 `ltrim/rtrim` 在预编译固定正则后仍逐行创建两个 `Matcher` 及内部数组的成本，保持 Java 默认 `^\\s+` 与 `\\s+$` 的**完整输出语义**。owning module 仅 `DefaultReactorQLMetadata` 两个内置函数；不改公开 regex 函数、自定义 Feature、SQL 编译、响应式链或默认限制。当前性能 JAR SHA-256 `35e30d674c427536c0f0d7a81a1103942c1abfc7901d9d99d75b2353e6af4e01`；当前 `StringTrimBenchmark` 的正式 JMH/GC `target/jmh-trim-scan-before-20261006.json`：文本 SQL **5.416±0.244 M 输入行/s、1041.413 B/行**，同结果预编译 Java 正则 **6.988±0.090 M、973.404 B/行**。目标 SQL 短 JFR `target/jfr-trim-scan-before-20261006/.../profile.jfr` 中 `Matcher.<init>` 的整数数组 277、`Pattern.matcher` 的 Matcher 174、`Matcher.<init>` 的内部集合数组 39 个 worker 分配样本，另有正则替换的 StringBuilder；采样不换算字节或精确 CPU 占比。

尝试只用无状态字符边界扫描实现两条固定 Java 正则：`\\s` 保持默认 ASCII 空白六字符；`rtrim` 还必须保留 `$` 在**最终** NEL/U+2028/U+2029 行终止符之前匹配并仅移除其前方 ASCII 空白的行为。用旧 Java `replaceAll` 对包含空白、普通文本、Unicode 空白与行终止符的系统组合做 SQL 输出差分，并保留现有 null、数字及多列查询回归。阶段末集中完整测试、同配置文本 SQL/预编译 Java JMH+GC A/B 和宽投影、分组、子查询负对照；仅在语义完全一致、目标分配显著下降且吞吐无稳定回退时保留。禁止按输入分布或 SQL 文本分支，不引入缓存、自定义操作符或额外常驻状态。

实施与验收：`DefaultReactorQLMetadata` 的 `ltrim/rtrim` 仅替换两条固定正则的内部实现；ASCII `\\s` 边界和最终 Unicode 行终止符前 `$` 的行为由短的无状态扫描器保持。其他 `regexp_*` 仍走原正则安全限制/缓存，不更改 Feature SPI 或输入行状态。`ReactorQLTest.testLeftAndRightTrimKeepJavaRegexSemantics` 将 null、数字、CRLF 和 Unicode 样例，与长度 0–4、8 字符字母表组成的 **4,681** 个字符串一起，经真实 SQL 输出逐项对照旧 Java `replaceAll`；完整 `mvn -q -Pjmh package` 为 **510 tests、0 failures/errors/skipped**。after 性能 JAR SHA-256 `c07a1daa910567d2ffcb5e4f1402bd750017e49804ac40b8b553dc5c9fe9ea47`。

同 JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler 的 `target/jmh-trim-scan-before-20261006.json` → `target/jmh-trim-scan-after-20261006.json`：文本 SQL **5.416±0.244→8.519±0.793 M 输入行/s（约 +57.3%）**、**1041.413→438.899 B/行（−602.514 B，约 −57.9%）**，吞吐误差区间不重叠。未修改的预编译 Java 正则对照 **6.988±0.090→6.463±0.174 M、973.404→1009.407 B/行**，显示两轮存在环境/JIT 漂移，不能把该对照变化归因于生产实现；其方向也不能解释 SQL 的大幅提升。after JFR `target/jfr-trim-scan-after-20261006/.../profile.jfr` 中经内置文本函数的 `Matcher`、其整数数组与替换 `StringBuilder` 分配样本从 **174/277/54** 降为 **0/0/0**；采样不换算精确字节或 CPU 占比。

严格相关的负对照是同生产 JAR 的 `target/jmh-date-numeric-scan-negative-20261006.json` → `target/jmh-trim-scan-negative-20261006.json`：普通宽投影 **736.009→736.009 B/行**、高基数五聚合 **765.012→765.011 B/行**、三层未关联子查询 **1276.611→1276.611 B/行**；吞吐误差区间均重叠。保留这一处跨输入形态等价的通用文本函数优化。它减少每行短命对象与 GC 压力，静态 Pattern 也被移除，但没有目标查询的 live-heap A/B，不声称常驻堆绝对下降或所有 SQL 均获得同等收益。

### 无 LIMIT 全局排序的逐行序号包装（计划，2026-10-06）

目标：移除 `OrderBySupport.limitSortRows` 为检查全局排序输入行数上限而对每行调用 `.index()` 创建的 `Tuple2`，降低通用全局排序的短命分配及 GC 压力。owning module 仅 `OrderBySupport`；不改 Top-N、窗口排序、排序键、默认 `orderBy.maxRows`、错误类型和提示、背压或取消语义，不增加自定义 Subscriber 或 SQL 形态判断。当前 JAR `c07a1daa910567d2ffcb5e4f1402bd750017e49804ac40b8b553dc5c9fe9ea47` 的 5,000 行排序 JFR `target/jfr-global-order-index-20261006/.../profile.jfr` 中，worker 分配样本的 `Tuple2` 为 227 个，是最高的一类；采样仅定位热点，不换算精确字节。正式 JMH/GC 基线 `target/jmh-global-order-index-before-20261006.json`：全局排序 **20.393±0.071 M 输入行/s、278.541 B/行**，Top-N 负对照 **10.915±0.440 M 输入行/s、200.020 B/行**。

实施：在现有每订阅 `Flux.defer` 边界内，为 `limitSortRows` 的单次流建立局部计数，仅用一个 `handle` 检查并传递输入 Record；第 `maxRows + 1` 行仍抛原 `resourceLimit`，恰好等于上限可完成。复用/补充上限边界、重复订阅、请求/取消和上游错误回归。阶段末集中完整构建、同配置全局排序和 Top-N 的 JMH/GC A/B；仅在语义测试全过、目标分配下降且吞吐无稳定回退时保留。若收益不明确或兼容性退化，撤回候选。

结果：`limitSortRows` 在调用方现有的每订阅 `Flux.defer` 中仅保留一个局部计数器和 `handle`，删除 `.index()` 与逐行 `Tuple2`；默认上限及第 `maxRows + 1` 行的错误保持不变。`ReactorQLTest.testGlobalOrderByLimitStateIsPerSubscription` 覆盖同一输出流重复订阅、分段请求、恰好到上限时完成、上游原异常和取消；既有 `testOrderByMaxRows` 覆盖超限。完整 `mvn -q -Pjmh package` 为 **511 tests、0 failures/errors/skipped**，`git diff --check` 通过。after 性能 JAR SHA-256 为 `14ead8bd5846df4cedb9e3bfbc1c03296d0abbf50af7bba2faf6ffb111c4a0c8`。

严格同配置的 JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler：`target/jmh-global-order-index-before-20261006.json` → `target/jmh-global-order-index-after-strict-20261006.json`，无 LIMIT 全局排序 **20.393±0.071→21.811±0.476 M 输入行/s（约 +7.0%）**、**278.541→231.147 B/行（−47.394 B，约 −17.0%）**；吞吐误差区间不重叠。Top-N 负对照 **10.915±0.440→10.728±0.515 M 行/s、200.020 B/行不变**，吞吐误差区间重叠。另一次 after 测量虽重复传入与基准类相同的 JVM 参数，仍得到 21.290±0.426 M、231.147 B/行；正式比较仅采用未重复参数的 strict 结果。保留这一处通用且低复杂度的分配削减；它减少短命分配和 GC 压力，但未测 live heap，不申领常驻堆绝对下降或其他 SQL 的同等收益。

### UNIQUE 聚合重复值的频次状态（计划，2026-10-06）

目标：检验 `StatefulAggregationSupport.frequencies` 对每个重复输入持续生成 `Long` 计数并写回 `LinkedHashMap`，是否是重复事件流的高收益 CPU/堆热点。此私有状态只被 `countUnique`、`collectUnique` 和 `uniqueValues` 消费，三者均只观察“次数恰好为 1”；不修改 DISTINCT、普通 count、功能函数、公开 Feature SPI、默认 `aggregate.maxCollectionSize`、输出顺序或响应式信号。先用 65,536 个预建整数事件（255 个高重复键和 256 个单次键）建立真实 `count(unique this)` 基准及 `count(distinct this)` 负对照，setup 校验 256/511 的 SQL 结果，再录正式 JMH/GC 基线和 JFR。仅在 JFR 确认重复计数装箱/Map 更新是目标热点时，才把私有频次状态饱和为一次/至少两次：第 1 次仍按原上限检查并插入，第 2 次改为共享状态，以后只读不写；首次出现顺序、错误、取消及 N/N+1 资源边界保持原样。

阶段末针对上述三个消费者补单次/重复/空源/上限/多订阅等价测试，集中完整构建及同配置 `unique` 目标、`distinct` 负对照 JMH/GC A/B；如分配或吞吐收益不明确、或语义/响应式契约退化，则撤回生产候选。此优化不需要新的操作符、缓存、自定义 Subscriber 或按 SQL/键值特调；精确 UNIQUE 仍必须为每个活跃不同键保留有界状态，不能宣称 O(1) 常驻内存。

基线与热点：新增 `UniqueAggregateBenchmark`，预建 65,536 个引用复用的整数输入，255 个键各重复 256 次、另 256 个键只出现一次；setup 验证 `count(unique this)=256`、`count(distinct this)=511`，输入构造和装箱不进入热路径。当前 JAR SHA-256 为 `a75cba4db009163927461938589971fc8b44537d676c77b38cdb7ee422f1d8a1`。正式 JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler 的 `target/jmh-unique-frequency-before-20261006.json`：UNIQUE **79.594±0.574 M 输入行/s、44.518 B/行**，DISTINCT 负对照 **118.625±12.513 M、32.467 B/行**。短 JFR `target/jfr-unique-before-20261006/.../profile.jfr` 的 worker 分配样本中 `Long` 有 187 个，全部栈经过 `StatefulAggregationSupport.frequencies`；同录制中的必要输入 Record 为 427 个样本。JFR 只定位频次装箱，不能将 UNIQUE/DISTINCT 的完整差额归因于它，也不能由样本换算精确字节或 live heap。

实施与验收：`StatefulAggregationSupport.frequencies` 的 `LinkedHashMap` 仍按首次出现顺序和相同键相等语义保存精确键，第 1 次插入 1，第 2 次置为 2，以后不再改写私有计数；三个消费者只判定 `==1`，结果不变。没有新缓存、操作符、Subscriber、SQL/类型分支或默认上限。`AggregationResourceLimitTest` 增加重复超过 Long 缓存范围、三种 UNIQUE 消费者、输出顺序、空源、恰好上限/超限、同一 Publisher 重复订阅、上游错误和取消回归。完整 `mvn -q -Pjmh package` **513 tests、0 failures/errors/skipped**；after 性能 JAR SHA-256 `02f460308005a96b718139441817ac6d27b2d7a9e27a00031a82714164c9ec04`。

同配置正式 `target/jmh-unique-frequency-before-20261006.json` → `target/jmh-unique-frequency-after-20261006.json`：UNIQUE **79.594±0.574→102.111±4.389 M 输入行/s（点估计约 +28.3%）**、**44.518→32.472 B/行（−12.046 B，约 −27.1%）**。after 短 JFR `target/jfr-unique-after-20261006/.../profile.jfr` 未再采到 `Long` 分配，频次栈只剩真实新键的 Map 条目/桶样本；JFR 只验证对象来源方向。DISTINCT 负对照分配维持 **32.467 B/行**，但吞吐为 **118.625±12.513→95.902±4.452 M**，不经本次修改的代码路径出现测量漂移；同一 after JAR 单独复测 `target/jmh-unique-distinct-repeat-20261006.json` 回到 **112.005±4.503 M**，与 before 区间重叠。因此不把 UNIQUE 的 +28.3% 点估计解释为稳定、可普遍外推的净吞吐收益；分配下降和未见目标吞吐回退是较强结论。未做 live-heap A/B，不申领常驻堆绝对下降；精确 UNIQUE 的键状态仍随不同键数增长。

### 精确 UNIQUE/DISTINCT 分组回退的开放窗口驻留（取证计划，2026-10-06）

目标：在当前 JAR 上量化 `count(unique value)` 与 `count(distinct value)` 因 `CountAggFeature.createAccumulator` 返回空而进入兼容 `GroupedFlux` 路径时，开放窗口、高基数分组的常驻状态；同时复用上节 JFR 对全局 UNIQUE 逐行 Record 包装的归因。owning 范围先仅为既有 `HighCardinalityLiveHeapProbe` 的两个 SQL 模式与本记录，不修改生产执行器、默认限制或输出语义。用同样的惰性 Map 输入、每键一行、open 窗口、下游不收集结果及取消后 GC，测 10k/50k 活跃键，与当前融合 `count` 控制组比较；`UNIQUE`/`DISTINCT` 的集合语义仍需逐键精确状态，不能把两条不同 SQL 的堆差全部解释为可删除内存。

只有测得明显额外驻留，且源码证实可在现有 `IncrementalValueAggMapFeature` 契约内通用保持标量/异步 Feature 回退、每订阅和每窗口隔离、上限 N/N+1、错误/取消与输出时机，才考虑生产 A/B。候选应基于声明的标量/原始行能力，而非 SQL 文本、键分布或默认情况下“通常是 Map”的假设；如需引入新的状态框架、隐式淘汰或为单个基准定制类型，停止实现。若进入生产阶段，再补全相应真实 SQL 功能和同配置吞吐、分配、live-heap 对照。

开放窗口取证：扩展现有探针的 `unique-count`/`distinct-count` 两个 SQL 模式；当前 JAR SHA-256 `769d54585c7593de1a580876052c36cf67ce3dd5c42b69d8f2d829754aae64e7`。相同 JDK 17.0.18、512 MB/G1、每键一行、惰性 Map 输入、全部输入已接收、输出 0 行且下游不收集、五次显式 GC 的 `heapUsed`：普通融合 `count` 在 10k/50k 键为 **5,656,800 / 12,381,264 B**；精确 `count(unique score)` 为 **29,513,680 / 131,453,392 B**，`count(distinct score)` 为 **29,647,696 / 132,082,544 B**。三类取消后都约 4.22–4.27 MB。此差异是同形态活跃订阅的 retained-heap 观察，不是泄漏，也不能把全部差额预先算作可消除对象；精确集合键状态本身不可省。源码确认两种精确计数目前在 `CountAggFeature.createAccumulator` 返回空，故回退到兼容 `GroupedFlux`；前节全局 UNIQUE JFR 在消除频次 `Long` 后仍有 637 个逐行 `DefaultReactorQLRecord` 分配样本，说明非融合的行包装也是候选成本。

下一试验：先在现有 JMH 加真实 `deviceId` 分组的 `count(unique score)`/`count(distinct score)` 两个预建 Map 输入入口，setup 核对组数、值、订阅次数；录当前同配置 JMH/GC 与短 JFR，确认兼容 `GroupedFlux`/Record 是测量热点。若成立，只在内置 `CountAggFeature` 的单参数 mapper 显式为 `ScalarValueMapper` 时提供现有 `IncrementalValueAggMapFeature.AccumulatorFactory`；`RawScalarValueMapper` 且来源别名被接受时再开放原始行入口，其余仍用原 Publisher 实现。每组增量状态只保留精确 DISTINCT Set 或 UNIQUE 一次/重复频次及既有 `aggregate.maxCollectionSize` 边界；复用相同容量错误函数，不改变默认无界行为。补全空输入、null、多值/异步及自定义 Feature 回退、分组/窗口、N/N+1、请求/取消/错误/Context、多订阅和结果可变性测试。阶段末完整构建、同配置全局/分组 JMH+GC 与 10k/50k live-heap A/B，另以普通 count/五聚合和不支持标量的兼容路径为负对照；任何语义回退或稳定跨场景吞吐损失都撤回生产候选。不创建新执行器、自定义 Subscriber 或独立的双轨状态框架。

实施与验收：`CountAggFeature` 仅对已声明同步单值能力的精确计数提供现有每订阅/每组累加器；原始行仍须通过 `RawScalarValueMapper.acceptsSource`，异步或多值自定义函数继续使用原 Publisher 聚合。`StatefulAggregationSupport` 的 DISTINCT 插入、UNIQUE 饱和频次和容量错误由两条路径共用；默认无界、恰好上限/第 N+1 个新键报错、null 省略及精确键集合不变。没有新增操作符、订阅器、SQL/类型特调、隐式淘汰或缓存。新增回归覆盖分组/窗口隔离、重复订阅、显式上限、多值函数回退，以及混合原始行/Record 输入的缺失值和 Context；既有空源、上游错误与取消测试继续通过。最终 `mvn -q -Pjmh package` 为 **517 tests、0 failures/errors/skipped**，`git diff --check` 通过。正式性能测量用 JAR SHA-256 `4d8eb87d4fce4f981bbb606f6b4d9434cdc0b1e36a4858f80bc81695725680cb`；最后只补注释及测试并重打包，最终 JAR SHA-256 `406f0ba6ec66b7e5348fe13b37e28ff9906f642c5d4d06d41dd065345ecbe67b`，生产执行逻辑未再变。

同 JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler 的 10k 键、每键一行真实 SQL：`target/jmh-keyed-exact-count-before-20261006.json` → `target/jmh-keyed-exact-count-after-20261006.json`，DISTINCT **0.040±0.001→8.372±0.256 M 输入行/s**、**3759.764→749.323 B/行**；UNIQUE **0.036±0.006→6.663±0.218 M 行/s**、**4019.789→989.324 B/行**。setup 验证 10k 个输出组、每组结果 1、一次源订阅；改进主要来自绕开兼容 `GroupedFlux` 的大量分组/排空状态，不能外推至所有键分布。全局 65,536 行重复值查询的同配置 after 为 DISTINCT **146.635±3.505 M 行/s、0.458 B/行**，UNIQUE **129.938±5.426 M 行/s、0.462 B/行**；改动前最近同夹具分别约 95.902 M 与 102.111 M、32.467/32.472 B/行，但原 DISTINCT 基线有已记录的吞吐漂移，主要可信结论是分配大幅下降且目标无回退。

相同开放窗口探针在 10k/50k 活跃键、全部输入已接收、输出 0 行、下游不收集、显式 GC 后的整进程 `heapUsed`：UNIQUE **29.514/131.453→7.566/21.896 MB**，DISTINCT **29.648/132.083→7.726/22.693 MB**；取消后四组均约 **4.23 MB**。这是同形态 post-GC 活跃状态 A/B，不是精确对象 retained-size，也不改变精确计数随活跃不同键数增长的事实。普通无 WHERE 宽投影、三层未关联子查询的分配保持 **736.009/1276.611 B/行**，吞吐误差区间与既有同夹具基线重叠；普通高基数五聚合保持 **765.011–765.012 B/行**，首次吞吐偏低，同逻辑重打包 JAR 复测 **3.839±0.292 M 行/s** 与此前 **3.958±0.131 M** 区间重叠，未确认稳定回退。该通用、低新增复杂度优化予以保留；继续坚持窗口/键生命周期是无界高基数精确状态的外部约束，不能以 `avg/max` 的单组 O(1) 推断整体 O(1) 常驻堆。

### 单聚合分组的累加器容器（计划，2026-10-06）

目标：减少所有融合单聚合查询每个活跃组的固定累加器数组，同时保持多聚合及扩展 Feature 语义。owning module 仅 `WindowedAggregateStage.GroupState`；不改聚合算法、Feature SPI、分组键、默认上限、背压、窗口输出时机或取消清理。当前 JAR SHA-256 `406f0ba6ec66b7e5348fe13b37e28ff9906f642c5d4d06d41dd065345ecbe67b` 的单聚合 50k 唯一键短 JFR `target/jfr-single-accumulator-before-20261006/.../profile.jfr` 在 614 个分配样本中有 37 个累加器数组样本，均经 `GroupState.<init>`；另有 31 个 `GroupState` 样本。样本只定位可删除的逐组数组，不换算精确字节或 CPU 占比。同 JDK 17.0.18、512 MB/G1、3×1s 预热、5×1s 测量、2 forks、GC profiler 的正式基线 `target/jmh-single-accumulator-before-20261006.json`：50k 键单 COUNT **4.789±0.545 M 输入行/s、565.011 B/行**，五聚合负对照 **3.760±0.119 M、765.011 B/行**。相同开放窗口、每键一行、显式 GC 的 10k/50k count live heap 为 **5,656,864/12,382,624 B**，取消后约 4.22 MB。

方案：仅在现有 `GroupState` 内把第一个累加器保存在字段，剩余累加器只在多聚合时装入数组；所有输入仍逐个调用同一累加器，结果依原列序输出。单聚合不再创建数组，多聚合只有容器布局变化，不新建执行器、状态框架或 SQL/类型分支。先补单聚合空值结果、混合多个聚合、不同窗口与取消的功能回归，再于阶段末集中完整构建；用同配置 50k 键 count/五聚合 JMH+GC、10k/50k 活跃键 live-heap A/B 和宽投影/子查询负对照验证。目标为单聚合明确减少分配和常驻堆、吞吐不稳定回退，且多聚合/无关场景无稳定损失；否则撤回生产候选。

实施与结果：`GroupState` 内联第一个累加器，仅多聚合分配其余累加器数组；原 `hasResult`、列序、原始行/Record 分支和每订阅窗口生命周期保持不变。现有回归已覆盖单聚合空/null、多聚合、窗口、延迟结果、取消与自定义增量 Feature，无需为容器形态写测试专用分支。完整 `mvn -q -Pjmh package` **517 tests、0 failures/errors/skipped**；after 性能 JAR SHA-256 `15bfd293dc71b8bac0b823d6deaf1c8f5b6bbad012db07179958f8683dfae741`。同配置 `target/jmh-single-accumulator-before-20261006.json` → `target/jmh-single-accumulator-after-20261006.json`：50k 键单 COUNT **4.789±0.545→4.767±0.308 M 输入行/s**、**565.011→549.011 B/行（−16 B）**；吞吐区间重叠。五聚合负对照 **3.760±0.119→3.906±0.144 M 行/s**、**765.011 B/行不变**，未见回退。目标短 JFR 中经 `GroupState.<init>` 的累加器数组分配样本 **37→0**，只用于确认分配来源，不换算精确字节或 CPU 占比。

同一 512 MB/G1 开放窗口、每键一行、下游不收集结果、显式 GC 的 live heap：单 COUNT 在 10k/50k 活跃键从 **5,656,864/12,382,624→5,496,936/11,594,640 B**，50k 约少 **0.79 MB**，与逐组省 16 B 一致；取消后均约 4.22 MB。50k 精确 UNIQUE/DISTINCT 分别从 **21,896,016/22,692,808→21,099,720/21,897,344 B**，约各少 0.80 MB；五聚合的 post-GC 数值在既有基线附近，未见同样省幅。受影响的精确分组 JMH/GC `target/jmh-single-accumulator-affected-after-20261006.json` 为 DISTINCT **8.822±0.056 M 行/s、733.322 B/行**，UNIQUE **7.386±0.073 M、973.323 B/行**，比改动前各少 16 B/行且未见吞吐损失；三层未关联子查询 **4.349±0.063 M、1276.595 B/行**，普通宽投影 `target/jmh-single-accumulator-wide-after-20261006.json` **4.793±0.087 M、736.009 B/行**，与既有同夹具基线相比没有稳定回退。保留这一处通用容器优化；它降低的是每组固定开销，不改变高基数精确聚合的 O(活跃键数) 本质，也不把 JFR 样本推断为 live-heap 绝对量。

### 精确 DISTINCT 新键查找的判别与通用优化（计划，2026-10-06）

目标：检验 `StatefulAggregationSupport.addDistinct` 对每个新键先 `contains` 再 `add` 的重复哈希查找是否是可见 CPU 成本。owning 范围限精确集合聚合与既有 JMH；不改变 DISTINCT/UNIQUE 语义、默认无界限制、上限 N/N+1、顺序、响应式信号或扩展 Feature。现有 keyed JMH 每组仅一行，DISTINCT/UNIQUE 均得 1，不能判别两种路径；名为 `keyedDistinctCount` 的长 JFR 同时采到 `addUnique` 和 `addDistinct`，故该录制不能单独授权生产改动。先增加每组两条等值事件的诊断输入，断言 DISTINCT=1、UNIQUE=0、恰好组数及一次源订阅；保留原基准作为负对照。

实施顺序：阶段末集中构建并记录可判别输入的 JFR 与正式 JMH/GC 基线，确认调用路径和成本。若热点成立，将 `addDistinct` 改成未满容量时由 `Set.add` 一次完成查重/插入，满容量时先判断已有键再沿用原错误边界；对只返回基数的 `count(distinct ...)` 使用无顺序负担的 `HashSet`，而集合输出继续保持 `LinkedHashSet` 的首次出现顺序。这些都是容器语义层面的规则，不按 SQL 文本、类型或键分布分支。复用既有资源边界、重复值、窗口和取消测试，补足必要的边界断言；同配置 A/B 覆盖新键与重复键、全局与分组 DISTINCT、UNIQUE 负对照，并测高基数开放窗口堆。只有功能等价、目标吞吐改善且分配/常驻堆无回退时保留；否则撤回生产候选。不新增操作符、Subscriber、跨行缓存或状态框架。

判别和取舍：`UniqueAggregateBenchmark` 增加每键两条等值事件，setup 对真实 SQL 严格断言 DISTINCT=1、UNIQUE=0、10k 输出组和一次源订阅，同时保留原每键一行基准。此前长 JFR 及本次短 JFR 包含 setup 的 UNIQUE/DISTINCT 校验样本，因此不能用混合样本计算测量期各算法 CPU 占比；只把样本用于确认集合查找/节点分配存在，性能决策以同配置 JMH/GC 和开放窗口堆 A/B 为准。改动前 JAR SHA-256 `334061c2049ff0e71f9799064fe885d06a57d7ece57cb3a5732950750ef2e51b`，`target/jmh-exact-distinct-repeat-before-20261006.json` 的全局高重复 DISTINCT 为 **149.322±3.848 M 行/s、0.458 B/行**，重复分组为 **14.991±0.762 M 行/s、366.662 B/行**；UNIQUE 负对照 **11.889±0.162 M 行/s、494.662 B/行**。同 JAR、50k 活跃键开放窗口 post-GC `heapUsed=21,899,352 B`，取消后 `4,229,072 B`。

实验同时替换计数容器并减少新键查找时，全局 DISTINCT 降至 **138.351±7.632 M 行/s**，单独复测 **142.619±0.932 M 行/s**；保留新键查找改写、撤回 Publisher 容器替换后仍为 **137.575±3.947 M 行/s**。重复值热路径的 `Set.add` 并不比 `contains` 更省 CPU，因此彻底撤回 `addDistinct` 改写及 Publisher 容器替换，不按键分布增设分支。最终仅在内置精确 `COUNT` 累加器中使用 `HashSet`：COUNT 只返回基数，其他保序集合操作保持原实现。

最终 `mvn -q -Pjmh package` **517 tests、0 failures/errors/skipped**，`git diff --check` 通过；第一次受限沙箱构建的现有 `GroupByWindowTest` 在 `ReactorDebugAgent.init()` 自附加处失败，本机完整复测通过，不修改测试。最终 JAR SHA-256 `40814177996d34c328e357809d4eb5e351c1302e84c279c1c3a6e8c3562718e7`。同配置 `target/jmh-exact-distinct-final-20261006.json`：全局高重复 DISTINCT **159.201±5.744 M 行/s、0.395 B/行**，单独复测 `target/jmh-exact-distinct-final-global-repeat-20261006.json` **157.870±0.579 M 行/s、0.395 B/行**；重复分组 **15.555±0.467 M 行/s、358.662 B/行**。全局吞吐点估计提高约 **5.7–6.6%**，分组吞吐区间与基线重叠，不宣称稳定提升；分组分配减少 **8 B/输入行**。50k 活跃键最终 post-GC 堆 **21,093,808 B**，较同配置基线少 **805,544 B**，取消后 **4,228,824 B**；10k 活跃键最终为 **7,405,848 B**，取消后 **4,227,944 B**。这些是整进程活跃堆读数，不是单类 retained size；精确 DISTINCT 仍需按不同键保留状态。最终没有新增操作符、缓存、默认上限或响应式边界。

### 精确 UNIQUE 计数的频次容器（计划，2026-10-06）

目标：降低 `COUNT(UNIQUE value)` 对大量活跃不同值的每键固定条目，以及每次分组输出的临时计数开销。它只观察恰好一次的数量，而 `collect_unique`/`uniqueValues` 输出具体值，仍须保留原有首次出现顺序。范围仅 `CountAggFeature.ExactCountAccumulator` 及 `StatefulAggregationSupport.countUnique`/`uniqueCount` 的计数边界；不改共享 `addUnique` 的饱和频次规则、null、默认资源上限、输出时机、背压/取消、扩展 Feature 和其他 SQL。先用当前 JAR 的真实重复分组/全局 UNIQUE JMH+GC、短 JFR 与 10k/50k 活跃键开放窗口堆建立基线。若热点成立，仅让计数使用不保序 `HashMap`，并以普通循环统计一次频次；具体值输出继续使用 `LinkedHashMap`。复用已有重复值、空值、N/N+1、窗口隔离、多订阅和取消测试；集中完整构建后同配置复测目标与 DISTINCT/五聚合负对照。验收以功能等价、分配/常驻堆降低且吞吐无稳定回退为准；若需新的状态框架或数据分布分支则停止。不新增自定义 Reactor 操作符或缓存。

JFR 判别：当前 JAR SHA-256 `40814177996d34c328e357809d4eb5e351c1302e84c279c1c3a6e8c3562718e7` 的重复分组 UNIQUE 短 JFR `target/jfr-unique-count-container-before-20261006/.../profile.jfr` 中有 642 个分配样本；`LinkedHashMap$Entry` 52 个，Stream `ReferencePipeline$Head` 39、`ReduceOps$5` 38、`ReferencePipeline$2` 35，另有 `LinkedHashMap$LinkedValueIterator` 31。`LinkedHashMap` 条目样本也包含分组索引，不能全部归于频次容器；但 `uniqueCount` 栈直接出现 values/迭代器/Stream 管线，确认逐组可删除对象。短 JFR 含 setup，不拿样本算测量期 CPU 百分比或精确 B/行，正式取舍仍看同配置 JMH/GC。

实施与结果：`COUNT(UNIQUE ...)` 的 Publisher 与每组增量路径都改用 `HashMap` 保存饱和频次；`collect_unique`/`uniqueValues` 继续通过 `LinkedHashMap` 保序。共用 `uniqueCount` 以普通循环代替每次创建 Stream 管线，不增加长期字段或改变 `addUnique` 的相等性、上限和首次/重复规则。当前阶段没有新增 SQL/类型/键分布分支或自定义操作符。完整 `mvn -q -Pjmh package` **517 tests、0 failures/errors/skipped**，`git diff --check` 通过；最终性能 JAR SHA-256 `2a3aeae6ff118b318569b0484ba4fe1ee71beb948e63dfb3ed42e56e119b1e47`。

同 JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler 的 `target/jmh-unique-count-container-before-20261006.json` → `target/jmh-unique-count-container-after-20261006.json`：全局 65,536 行重复 UNIQUE **151.086±15.597→148.026±0.732 M 输入行/s**、**0.462→0.395 B/行**（吞吐区间重叠）；10k 键每键一行 UNIQUE **7.209±0.073→7.590±0.074 M 行/s**、**973.323→717.323 B/行**；10k 键每键两条等值行 UNIQUE **11.911±0.108→13.517±0.639 M 行/s**、**494.662→358.662 B/行**。同套 DISTINCT 负对照 **15.544±0.192→14.997±0.835 M 行/s**、**358.662 B/行不变**；同 JAR 单独复测 `target/jmh-unique-count-container-distinct-isolated-20261006.json` 为 **15.215±0.417 M 行/s、358.662 B/行**，与基线区间重叠，未确认稳定旁路回退。不把其他不同输入形态的五聚合分数混作同口径 A/B。

相同开放窗口、每键一行、输入已全部接收且尚无输出、下游不收集并显式 GC 的整进程活跃堆：UNIQUE 计数在 10k/50k 键为 **7,405,392/21,095,032→7,246,064/20,293,608 B**，即 50k 键约少 **0.80 MB**、约 16 B/活跃组；取消后 before/after 均约 **4.23 MB**。after 短 JFR `target/jfr-unique-count-container-after-20261006/.../profile.jfr` 不再采到计数路径的 Stream `ReferencePipeline`/`ReduceOps` 或 `LinkedValues` 分配，仍存在结果/分组索引所需的容器。JFR 用于确认来源，JMH 分配和 post-GC 堆才是量化证据；精确 UNIQUE 仍需要随不同活跃值增长的状态。保留这处低复杂度通用优化。

### Publisher 精确 DISTINCT 计数的容器语义（计划，2026-10-06）

目标：检验 `StatefulAggregationSupport.countDistinct` 为只输出基数的 Publisher 聚合保留首次出现顺序，是否造成可删除的哈希条目/链表内存。同步标量 `COUNT(DISTINCT ...)` 已由 `CountAggFeature.ExactCountAccumulator` 使用 `HashSet`，本次目标是 `distinct_count(...)` 及非标量 `COUNT(DISTINCT ...)` 回退路径，不拿前者融合基准替代。`collectSet`、`distinctValues`、`collect_list(distinct ...)` 等输出具体值的路径必须继续保序。范围仅 `StatefulAggregationSupport.countDistinct` 与真实 SQL JMH，不改 `addDistinct` 的查重/上限规则、默认无界设置、响应式订阅/取消、扩展 Feature、其他聚合或执行器。

先给既有基准增加预构造的重复值和全唯一值输入，并在 setup 断言 `distinct_count` 结果、类型和一次源订阅；录当前 JAR 的 JFR、同配置 JMH/GC 基线。若证实 `LinkedHashSet` 条目来自该计数路径，只在 `countDistinct` 使用 `HashSet`，保留同一 `Flux.collect` 和容量错误函数。阶段末集中完整构建、资源上限/重复值/错误/取消回归，以及同配置两种基数分布和融合 COUNT、具体值输出负对照；只在分配或驻留内存下降、吞吐无稳定回退且语义等价时保留。不按值类型、SQL 文本或分布设快路，不新增操作符、Subscriber、缓存或状态框架。

取证与实施：`UniqueAggregateBenchmark` 新增 `distinct_count(this)` 的 65,536 行预建引用重复值/全唯一值入口；setup 分别断言精确 511/65,536、`Long` 值和一次源订阅。基线性能 JAR SHA-256 `90816e774a47887ab92c66df4abe66b926894dd5275c432f8f59f932c34fde48`。全唯一值短 JFR `target/jfr-publisher-distinct-container-before-20261006/.../profile.jfr` 的 660 个分配样本中，`LinkedHashMap$Entry` 287 个，样本栈直接经过 `StatefulAggregationSupport.addDistinct`；另有必需的 Record 240 个及哈希桶数组 132 个。JFR 仅用于对象来源，不把样本比例当作精确字节或 CPU 占比。生产改动只让 `countDistinct` 自己的 `Flux.collect` 使用 `HashSet`；`collectSet` 及所有具体值输出仍用 `LinkedHashSet`，`addDistinct`、错误边界和每订阅生命周期不变。

同 JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler 的 `target/jmh-publisher-distinct-container-before-20261006.json` → `target/jmh-publisher-distinct-container-after-20261006.json`：全唯一值 **50.485±1.567→58.319±0.830 M 输入行/s**、**88.032→80.032 B/行**，点估计吞吐约 +15.5%、分配少 8 B/新值；高重复值 **125.160±4.704→127.885±0.650 M 行/s**、**32.466→32.404 B/行**，吞吐区间重叠。融合 `COUNT(DISTINCT ...)` 负对照 **151.705±4.226→155.252±4.868 M 行/s**、**0.395 B/行不变**。after 短 JFR `target/jfr-publisher-distinct-container-after-20261006/.../profile.jfr` 未再采到 `LinkedHashMap$Entry`，仍有 `HashMap$Node`、Record 和桶数组，符合只移除保序条目的范围。

完整 `mvn -q -Pjmh package` **517 tests、0 failures/errors/skipped**，`git diff --check` 通过；现有真实 SQL 测试覆盖 `distinct_count` 的重复值、显式资源上限与 `collect_list(distinct ...)` 的保序输出。最终性能 JAR SHA-256 `6c7c43abae774523d67927c2da4a232d824b518bd50f5246a1129cf44525a22b`。本次量化的是吞吐与每输入行分配，未做独立 post-GC 活跃堆 A/B，故不申领常驻堆绝对下降；精确 DISTINCT 仍随不同值数线性保留键状态。保留这一处通用、低复杂度的计数容器优化。

### 派生结果 Map 的复制遍历（计划，2026-10-06）

目标：减少多层子查询 `DefaultReactorQLRecord.resultToRecord` 的临时遍历对象，继续生成独立、可变的结果副本并保留来源别名。既有 `target/jfr-nested-current-20261006/.../profile.jfr` 的 660 个分配样本中，结果 Map 复制产生 `HashMap$EntryIterator` 18 个、`HashMap$EntrySet` 9 个；来源别名容器复制另有同类样本，不能将其全部算作本次收益。当前生产 JAR SHA-256 `6c7c43abae774523d67927c2da4a232d824b518bd50f5246a1129cf44525a22b` 的这段实现仍为 `new HashMap<>(results)`，可复用其热点归属证据。

范围仅该方法的标准 HashMap 结果副本：预估容量后通过 `results.forEach` 写入，避免 copy constructor 创建 entrySet/迭代器；保留原大小对应的容量策略。Context 提供的具名记录容器仍使用原 `putAll`，输出别名碰撞、空结果、结果可变性、订阅和响应式信号均不变。现有测试覆盖来源修改后的副本隔离、隐式/显式别名和自定义 Context；补足副本修改不影响来源的双向隔离断言。先录当前 JAR 的两层聚合、关闭复用、三层子查询及关联 lookup 对照的同配置 JMH/GC 基线，再集中完整构建和同配置 A/B。只有实际分配下降、功能等价且吞吐无稳定回退时保留；否则撤回生产候选。不共享结果 Map，不增加执行器、缓存、类型/SQL 特调或自定义操作符。

结果与取舍：候选完整 `mvn -q -Pjmh package` 为 **517 tests、0 failures/errors/skipped**，JAR SHA-256 `40e300f54380349dcf10cce6c0025c2599fbc983d3901691cb9c9879a0473363`。同 JDK 17.0.18、512 MB/G1、3×1s 预热、5×1s 测量、2 forks、GC profiler 的 `target/jmh-derived-result-copy-before-20261006.json` → `target/jmh-derived-result-copy-after-20261006.json`：两层未关联聚合 **4.442±0.009→4.304±0.051 M 外层行/s**、**1276.626→1260.579 B/外层行**；关闭复用 **8918±77→8676±134 外层行/s**、**732313.373→715913.373 B/外层行**。两项吞吐均下降约 3%，分配改善不满足保留门槛。三层子查询 **6.550±0.162→6.549±0.060 M 行/s**、**592.147→592.145 B/行**，无实际收益；关联 lookup 对照吞吐区间重叠。撤回 `forEach` 生产候选，恢复原 Map copy constructor；保留双向可变副本隔离的两条测试断言。不进一步增加类型分支或复制框架。

### 流式 DISTINCT 已见键的容器（计划，2026-10-06）

目标：降低 `StatefulAggregationSupport.distinctValues` 在 `sum/avg/take(... DISTINCT ...)` 等通用聚合修饰符中，精确已见键集合的固定条目开销。该集合只做查重，不被遍历；第一条新值直接向下游发出，因此输出顺序由源事件和发出时机决定。范围仅该方法的每订阅内部 Set；具体值终态集合 `collectSet` 的保序容器继续保留。先给既有 JMH 增加真实 `sum(distinct this)` 的预建重复值/全唯一值入口，严格断言聚合值、类型及一次源订阅；用恢复后的生产 JAR 录 JFR 和同配置 JMH/GC 基线。若证实保序条目分配来自流式查重，只把 seen Set 改为 `HashSet`，保持同一 `handle`、contains/add 顺序、容量错误及取消传播。复用流式 DISTINCT 提前 take、上限、顺序、错误和多订阅测试，阶段末集中完整构建与同配置 A/B。验收为分配下降、吞吐无稳定回退和语义等价；不引入缓存、操作符、数据分布/类型分支或近似去重。

取证与实施：`UniqueAggregateBenchmark` 增加 `sum(distinct this)` 的 65,536 行预建引用全唯一/重复输入，setup 分别严格校验 `Double` 总和 2,147,450,880/130,305 及一次源订阅。baseline JAR SHA-256 `af5a45cb3f285e68c9cb16317bac03767e7aff1b40557b3b3167709713fdfc00`；短 JFR `target/jfr-streaming-distinct-before-20261006/.../profile.jfr` 的 601 个 worker 分配样本中，254 个 `LinkedHashMap$Entry` 栈直接经过 `distinctValues`。生产改动仅用 `HashSet` 保存不被遍历的已见键；`collectSet` 仍用 `LinkedHashSet`，每订阅 `Flux.defer`、原 `handle`、查重/容量检查顺序、默认限制和取消传播均不变。流式回归以 `take(distinct val,3)`、容量 3、输入 `3,1,3,2,4` 后接未完成源，校验按首次出现顺序输出 `3,1,2`、提前完成并取消上游，而非等待源完成或消费第四个新值。

完整 `mvn -q -Pjmh package` **517 tests、0 failures/errors/skipped**，`git diff --check` 与本阶段新增文件尾随空白检查通过。after 性能 JAR SHA-256 `6c03d570ba0767fd1be30dab0452262be0c6262f7ee2ac0e709f6a421d597b1e`。同 JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler 的 `target/jmh-streaming-distinct-before-20261006.json` → `target/jmh-streaming-distinct-after-20261006.json`：全唯一值 **44.272±0.799→45.590±9.267 M 输入行/s**、**88.034→80.034 B/行（约 −9.1%，−8 B/新键）**；高重复值 **103.898±2.485→105.259±0.974 M 行/s**、**32.468→32.405 B/行**。未改的 `publisherDistinctAllUnique` 负对照 **52.610±5.590→51.733±7.896 M 行/s**、**80.032 B/行不变**。两轮吞吐区间均重叠；全唯一目标和负对照均出现 fork 间波动，不据点估计申领稳定提速。

同配置独立复测 `target/jmh-streaming-distinct-isolated-after-20261006.json` 的全唯一目标为 **51.486±0.389 M 行/s、80.034 B/行**，负对照为 **51.925±8.735 M、80.032 B/行**；没有确认稳定吞吐回退，但不将这次目标分数外推为通用提速倍率。after 短 JFR `target/jfr-streaming-distinct-after-20261006/.../profile.jfr` 的 629 个 worker 分配样本中，流式查重路径的保序条目为 0、普通 `HashMap$Node` 为 236，仍存在 Record 和桶数组。JFR 样本只核验对象来源，不换算精确字节、CPU 占比或常驻堆。本阶段没有独立 live-heap A/B，故只申领逐新键分配下降；精确 DISTINCT 仍必须保留不同键状态，不能以普通 AVG/MAX 的 O(1) 单组累加器推断其无需驻留状态。保留这处低复杂度通用优化，不继续为小额遍历分配引入新执行框架。

### 单次多路径 JSON 函数的重复文档规范化（计划，2026-10-06）

目标：减少 `json_extract(document,path...)` 和 `json_contains_path(document,mode,path...)` 在单次函数调用内，按每条路径重复解析/复制同一个文档的 CPU 与短命对象。owning module 仅 `JsonFunctionSupport` 及现有 JSON 测试；不共享不同函数/列或不同行的文档，不新增缓存、配置、Reactor 操作符或 JSONPath 自研解析器。当前 JAR `6c03d570ba0767fd1be30dab0452262be0c6262f7ee2ac0e709f6a421d597b1e` 的测量区间 JFR `target/jfr-wide-functions-refresh-20261006.jfr` 显示宽函数查询仍在 JSON 解析/路径求值、日期转换与结果 Map 写入花费 CPU；3558 个 worker 分配样本中，有 125 个 `JSONParserString`、215 个解析器建立的 `LinkedHashMap`。样本只定位来源，不能据此外推多路径函数收益。源码中两种多路径函数的循环均调用 `readPath`，而其每次先 `normalize(document)`，可用独立诊断负载判别重复成本。

先增加 `JsonMultiPathBenchmark`，使用预构造设备事件与相同 JSON 文本/预解析 Map，参数覆盖 1/4/8 条固定路径；setup 全量校验值、结果类型、顺序、行数和一次源订阅。阶段末打包诊断夹具，录多路径目标的测量期 JFR及同配置 JMH/GC 基线。若热点成立，分离现有文档规范化与路径读取，在每次多路径函数调用的局部变量中仅规范化一次，再沿原 JSONPath 实现逐路径求值；每项提取结果仍单独规范化，保留可变结果隔离。单路径、动态路径、缺失/null/非法文档、无路径、one/all 短路、资源错误、异步参数和取消/Context 的语义均需保留。补齐边界回归后集中完整构建，按相同夹具做 A/B，单路径与既有宽函数/操作符查询作负对照。验收为多路径明确减少分配并提升吞吐、单路径无稳定回退且功能等价；若需要扩展 Feature 假设、跨函数复用或复杂状态表示，则撤回候选。

取证与实现：基线 JAR SHA-256 `1f0abcd431d3e33872523a3b743768ffd259cd1b9f4a993758ec4c2c790d25d1`。`JsonMultiPathBenchmark` 对 16,384 条预构造事件执行序号、多路径提取和 all 路径存在检查，消费结果而不收集输出；两种输入的结果与直接 JSONPath oracle 全量比较。8 路径文本目标的测量期 JFR `target/jfr-json-multipath-before-20261006.jfr` 有 742 个 worker CPU 与 3602 个分配样本；主要 CPU 叶子是 JSONParser 的 `readObject`/`readMain`/`readString`。使用 `jfr print --stack-depth 128` 查看完整录制栈，解析器 `LinkedHashMap$Entry` 样本中，279/412 个分别经过 `jsonExtract`/`jsonContainsPath`，确认重复解析位于两种函数内部，而非只在 setup 或其他 SQL。采样不换算 CPU 百分比或精确字节。

生产实现仅在 `JsonFunctionSupport` 分离原规范化/读取边界；多路径函数以局部变量保存本次规范化的文档，再顺序读取各路径。每个提取结果继续独立规范化，重复/root 路径结果仍可分别修改，不改变来源文档。单路径 `readPath` 保留直接规范化与原异常捕获边界；缺失文档的异常仅按原 `PathNotFoundException`/`IllegalArgumentException` 规则处理，资源错误继续传播。公开 factory 允许自定义参数范围，无路径扩展仍不读取文档；one/all 的动态路径校验继续按原短路顺序发生。没有跨列/行复用、额外状态字段、值类型/SQL 分支、新操作符或缓存，参数 Publisher 装配及响应式信号未改。新增 4 个回归覆盖文本/Map、动态/null/重复/root/缺失路径、可变结果隔离、非法文档、规范化异常、短路错误和资源限额及无路径扩展；原异步参数、Context、取消和 JSON 资源测试一并通过。

完整阶段首次构建只在已有真实时钟 `GroupByWindowTest.testGroupByTimeWindow` 遇到 200 ms 输入/500 ms 分窗边界的 3.5→4.0 波动；未修改窗口或测试。随后完整复测及最终 `mvn -q -Pjmh package` 均通过。本轮 49 份新生成报告为 **519 tests、0 failures/errors/skipped**；旧 `TEST-org.jetlinks.reactor.ql.Benchmarks.xml` 的 2 个历史用例不计入本轮。`git diff --check` 和新增 JMH 文件尾随空白检查通过。最终性能 JAR SHA-256 `ba675f73f6dcb1bbe4e4e09218d12cdf4564d3df39f7d09ea439bfc96989e335`。

同 JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler 的 `target/jmh-json-multipath-before-20261006.json` → `target/jmh-json-multipath-final-20261006.json`：

| 输入 / 路径数 | 吞吐，M 输入行/s（before→final） | 分配，B/输入行（before→final） |
| --- | --- | --- |
| Map / 1 | 1.910±0.087→1.997±0.042 | 3444.049→3484.049 |
| Map / 4 | 0.521±0.005→0.993±0.039 | 12592.057→7416.052 |
| Map / 8 | 0.2665±0.0009→0.5850±0.0093 | 24608.067→12400.056 |
| 文本 / 1 | 0.9487±0.0126→1.0245±0.0088 | 6536.052→6504.052 |
| 文本 / 4 | 0.2536±0.0055→0.6728±0.0043 | 25072.069→10480.055 |
| 文本 / 8 | 0.1285±0.0042→0.4467±0.0065 | 49568.089→15520.059 |

8 路径文本吞吐约 **3.48 倍**、分配 **−68.7%**；Map 约 **2.19 倍**、分配 **−49.6%**，两种多路径吞吐区间均不重叠。4 路径也有明确收益，不是只改善一个参数。单路径 Map 吞吐区间重叠，但分配约多 **40 B/行（+1.2%）**；不同 fork 分配水平也有波动。初轮该项多 108 B/行，恢复单路径的直接规范化入口后缩小；不再为消除此小额代价增加 arity/类型特判，保留这一量化取舍，不能声称所有 JSON 输入的分配都下降。

既有宽查询负对照补录使用生产实现与候选前相同的基线源码，JAR SHA-256 `2d9948cbe69e58f3570099ef09adaaadb89d0743ae3c5c6aa8c7a6fbec55a905`；`target/jmh-json-multipath-negative-before-20261006.json` → 最终同轮结果：16 列宽函数 **1.803±0.012→1.790±0.033 M 行/s**、**2528.109 B/行不变**；混合运算符宽查询 **4.168±0.125→4.095±0.112 M**、**458.615 B/行不变**。吞吐区间重叠，未确认稳定旁路回退。初轮 `target/jmh-json-multipath-after-20261006.json`/`negative-after` 是已收敛候选，不作为最终制品的验收结果。

最终测量期 JFR `target/jfr-json-multipath-final-20261006.jfr` 有 739 个 worker CPU、3537 个分配样本；两函数解析条目栈样本为 5/74，剩余 CPU 叶子主要是数组复制、JSONPath 属性遍历和路径字符串组合，解析仍存在但已不再按每条路径重复执行。样本只核验热点来源变化，量化收益以正式 JMH/GC 为准。保留这个单次函数内的通用算法优化；它降低瞬时堆分配，没有跨行驻留缓存，也没有 live-heap A/B，不申领常驻堆绝对下降或所有 SQL 的同等倍率。

### 常用日期字段函数的逐值映射成本（取证计划，2026-10-06）

目标：判断 `year/month/day_of_month/day_of_year/day_of_week/hour/minute/second` 的同步字段计算仍经逐行 Publisher 参数装配，是否是常见多列日期 SQL 的高收益热点。owning module 为 `FunctionMapFeature` 和内置日期函数注册；本阶段先加 `DateFieldBenchmark`，以预建 LocalDateTime/日期文本事件、1/8 个日期列与同计算次数的原生 Java 对照，校验完整结果、类型、行顺序和一次源订阅。用当前生产代码录同配置 JMH/GC 与测量期 JFR，只有明确的引擎装配热点才进入实现。

候选必须复用现有标量能力与原生 `map`，不改变日期转换、参数个数、distinct/unique、空值、异常、副作用时机、多值/异步参数、Context、取消及自定义 Feature/metadata 的回退。不做跨列日期缓存、日期类型/SQL 文本分派、自定义订阅器或动态同步/异步双轨框架。若不能以简短通用逐值映射适配器保留全部语义，或收益仅是微小分配，停止该候选。阶段末集中构建；真实宽函数、混合运算符、多层子查询作负对照，只有功能等价、目标明显改善且无稳定旁路回退才保留。

当前基线 JAR SHA-256 `94c0fd980dfee20455703ad947cf0069d123f60931fb1bdeb5e6725142050986`，测量期 `target/jfr-date-field-before-20261006.jfr` 的 983 个 worker 分配样本里，有 169 个经过 `FunctionMapFeature`；叶子包含 FluxMap/MonoNext/MonoDefaultIfEmpty 订阅器、参数流及 MonoZip 装配。正式基线 `target/jmh-date-field-before-20261006.json` 中 8 列 LocalDateTime SQL 为 **1.540±0.058 M 行/s、3338.345 B/行**，同计算次数 Java 对照 **10.347±0.110 M、474.307 B/行**。采样仅做归因，不换算 CPU 占比或分配字节。

实施边界收敛：逐值函数仅对显式 `ScalarValueMapper` 参数简化冷取值，返回值仍是普通 Publisher，不提升到标量投影，不重排列或改变 zipDelayError。单 Supplier 合并计算的初候选虽然通过初轮完整 524 tests，却在新增的旧实现差分测试中使 `onErrorContinue` 从保留空字段行变为丢弃整行，已停止该形式。下一候选保留原生 `Mono.map` 的逐值错误边界，只以 `Mono.fromSupplier` 替代同步参数的 defer/just/Flux 适配；公开 mapper 字段被修改时继续原 Publisher 参数流。没有手写订阅器、异常兜底或 SQL/日期类型分支。

最终实施：`FunctionMapFeature.map` 仅声明单参数的同步逐值转换，在已有 `apply(record,mappers)` 中对显式同步参数使用冷 `Mono.fromSupplier(...).map(...)`；未修改旧构造器、protected apply 调用链、其他标量工厂或 `math.*` 的 Publisher/集合展开。八个内置日期字段函数使用此通用工厂，日期转换本身不变。新增 6 个回归覆盖冷求值/default/空值/null 回调及错误、公开 mapper 编译后替换和恢复、多值/异步/Context/请求/取消、参数个数、distinct/unique、混合列的多错误组合，以及与旧 Flux.map 的 `onErrorContinue` 输出逐项差分。最终完整 `mvn -q -Pjmh package` 为 **525 tests、0 failures/errors/skipped**（49 份当前报告，排除旧 Benchmarks 报告的 2 项）。`git diff --check` 通过。

同 JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler：`target/jmh-date-field-before-20261006.json` → `target/jmh-date-field-after-20261006.json`：

| 日期输入 / 日期列数 | 吞吐，M 输入行/s（before→after） | 分配，B/输入行（before→after） |
| --- | --- | --- |
| LocalDateTime / 1 | 7.452±0.009→8.314±0.013 | 640.058→600.057 |
| 文本 / 1 | 6.536±0.180→7.545±0.206 | 712.051→660.050 |
| LocalDateTime / 8 | 1.540±0.058→1.519±0.147 | 3338.345→3018.345 |
| 文本 / 8 | 1.307±0.046→1.290±0.025 | 3914.287→3402.287 |

单日期列吞吐分别约 **+11.6%/+15.4%**，区间不重叠；8 列未确认吞吐提升，点估计略低但区间重叠。8 列分配分别约 **−9.6%/−13.1%**（每行少 320/512 B）；单列文本 after 两个 fork 的分配有约 24 B 差异，不把均值当成所有输入的精确节省量。相同转换次数的 Java 对照分配不变，1/8 列 LocalDateTime 吞吐为 38.828±1.680→39.115±0.194 / 10.347±0.110→10.481±0.035 M，文本为 25.216±1.720→24.862±0.449 / 5.306±0.120→5.612±0.376 M；区间均重叠，未确认通用环境提速。

负对照基线通过临时仅撤销本轮两处生产改动重建，JAR SHA-256 `1e62401007b294853be5a291deb4fe36054543ed884fd9721b999fede6a695f6`；原始数据 `target/jmh-date-field-negative-before-20261006.json`。16 列宽函数 **1.723±0.019→1.756±0.051 M**、**2517.309 B/行不变**；混合运算符 **3.707±0.124→3.869±0.247 M**、**458.615 B/行不变**，区间重叠。多层子查询初轮 **4.374±0.069→4.200±0.045 M**，区间不重叠，不能直接忽略；新增 3 forks 独立复测 `target/jmh-date-field-nested-repeat-after-20261006.json` 为 **4.331±0.085 M、1276.606±0.016 B/行**，与 before **1276.595 B/行**及吞吐区间重叠，未确认稳定旁路回退；保留初轮差异，不申领子查询提速。

after 测量期 JFR `target/jfr-date-field-after-20261006.jfr` 有 239 个 worker CPU / 1150 个分配样本；函数参数路径不再出现 before 的 `Flux.defer`/`Flux.map` 分配叶子（41/57），改为 MonoSupplier/MonoMapFuseable，列组合 MonoZip/占位订阅器和结果 Map 仍是主要成本。样本只定位来源，精确分配以正式 GC profiler 为准。性能与完整测试 JAR 为 `0b9c37b9e1fa09497cd09e611d777579badc63fda576bbebc93352a37e214a0c`；负对照完成后恢复并重打包的最终 JAR 为 `0efd50bff997513940ba063678dfebb42bb7134199a316da9f90b147efab7635`，核对 **504 个 ReactorQL/JMH class 条目完全相同**，复用仍有效的 525 项完整测试证据。其后仅将工厂注释中的“订阅时”准确表述为“按下游需求冷取值”，无执行逻辑变化。

保留这一低复杂度、通用的参数适配优化；不继续为消除列组合的剩余成本将函数提升到标量投影或引入新的双轨执行器。两个 Feature 回调引用是注册级固定成本，不新增每行/每组驻留状态或缓存。当前证据仅证明目标查询减少瞬时堆分配，没有 live-heap A/B，不能声称常驻堆绝对下降、8 列吞吐提速或所有 SQL 已接近原生。

### 冷 Supplier 合并的共享错误边界复核（取证计划，2026-10-06）

目标：将上一节发现的 `onErrorContinue` 边界风险，完整核对到现有 `DateFormatFeature`、`JsonOperatorMapFeature` 两处同类 Supplier 合并，而不是只修日期字段的新候选。owning module 为上述函数适配器与信号回归测试。用同一 SQL、数据、显式设置，对照默认优化 metadata 和声明不支持同步快路的真实原 Publisher 路径，逐项比较正常/空值/错误继续后的行、字段、源订阅与错误次数。日期格式化三个别名、JSON 文本/对象操作符与 PostgreSQL 路径是共享边界的兄弟覆盖。

先复现，再决定：若 Supplier 移动函数计算导致丢整行，撤回或以原生 map/flatMap 恢复原逐值错误边界；不手写异常恢复、不按错误或 SQL 增加分支、不扩大资源限制。一次阶段集中完整测试，并用已保存的当前基线 JAR `0efd50bff997513940ba063678dfebb42bb7134199a316da9f90b147efab7635` 做宽函数/JSON 操作符/日期格式化的同配置 JMH+GC 回归，明确保留功能性所付出的成本。未证明等价的既有微优化不继续申领已完成。

差分结果：默认优化的 `date_format` 对正常/非法/正常日期原本输出 2 行，而保留的真实 Publisher 路径输出 3 行（中间 `{id=2}` 无格式化字段）；错误处理次数及来源订阅均为一，判定为生产信号边界回归。`dateformat/format_datetime` 是同一实现的兄弟覆盖。JSON `->/->>/#>/#>>` 在显式资源错误与普通 Map 键规范化错误两组差分中均等价，因此不将 map 的结论机械扩到其原 flatMap 路径，不修改 `JsonOperatorMapFeature`。

修正仅在 `DateFormatFeature` 恢复原生 `Mono.map` 的格式化阶段，参数仍冷取值；不可变格式化回调在查询构建期创建一次，不跨行缓存日期或结果。没有 catch/按错误类型兜底、新订阅器、新 SPI 或查询特判。`PublisherFunctionErrorBoundaryTest` 新增 3 项回归覆盖三个日期别名、null/缺失、八组 JSON 操作符错误差分，比较行、字段、错误数及一次来源订阅。完整 `mvn -q -Pjmh package` **528 tests、0 failures/errors/skipped**（50 份当前报告，排除旧 Benchmarks 2 项），最终 JAR SHA-256 `39ad731f8d9cdbce5de461308b324684e8e29783e91408280cbe33c7ca9b6bb5`。其后仅补充同等行数的类职责注释与文档，无执行逻辑变化。

同 JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler：`target/jmh-publisher-error-boundary-before-20261006.json` → `target/jmh-publisher-error-boundary-after-20261006.json`：

| 场景 | 吞吐，M 输入行/s（before→after） | 分配，B/输入行（before→after） |
| --- | --- | --- |
| 单日期格式化 SQL | 4.351±0.039→4.089±0.029 | 1064.012→1112.013 |
| 同转换的原生日期对照 | 6.814±0.186→6.841±0.057 | 728.003→728.003 |
| 16 列双 JSONPath/日期宽函数 | 1.686±0.128→1.696±0.028 | 2517.309→2582.111 |
| 未修改的 JSON 操作符宽查询 | 1.207±0.015→1.215±0.011 | 4805.168→4805.168 |

保留功能修正，明确接受原生错误边界的成本：单日期 SQL 吞吐约 **−6.0%**、分配多 **48 B/行（+4.5%）**，区间不重叠；宽函数多 **64.802 B/输入行（+2.6%）**，吞吐区间重叠。对照分配不变且吞吐区间重叠，不能把此轮申领为性能提升。之前日期数字扫描及其他通用优化未撤销；不通过上下文私有键、异常类型判断或更多特殊路径追回这一微优化。常驻堆未录 A/B，不申领常驻内存收益。

根因与防复发（Trellis 未配置，回填本原始文档，不创建 spec/template 或自动提交）：类别为隐含假设和测试覆盖缺口——将“都是冷 Mono”误当作“错误边界也相同”，普通值/错误终止测试未覆盖下游继续后的行是否被保留。预防以真实保留路径的差分回归和原生 map 为准；JSON 的通过反例约束修复范围。下一阶段还需有界核对既有标量投影及复合增量聚合的值级/行级错误边界，不能以本节 528 项测试声称所有表达式和聚合已证明功能等价。

本轮关系操作 JFR `target/jfr-relational-refresh-20261006/` 只作当前状态取证：JOIN 为 263 个 worker CPU / 1211 个分配样本，仍以组合 Record/Map、属性和等值比较为主；INTERSECT/UNION/EXCEPT 各有 1258/1260/1143 个分配样本，经过 `resultToRecord` 的分别 787/776/664 个。`HashMap.resize` 栈包括初始桶数组建立，不能全部当成可删扩容。未发现可不触碰别名隔离、精确集合语义和冷来源订阅的简单高收益候选；不重复已撤回的 Map forEach 复制，也不加缓存哈希、对象池或双来源视图。

### 标量函数与复合聚合错误范围差分

目标是在继续性能改动前补齐共享边界证据：以正常/非法/正常/空值输入对照真实保留 Publisher 路径，核对一元数值函数及 SUM/AVG/COUNT 的值、终止信号、错误次数和一次源订阅；交换聚合 SELECT 顺序，观察失败是否影响其他累加器。对照必须同时关闭 `aggregate.fastPath`，并用普通 Function 暴露原属性 mapper 的同一 `apply` 实现，消除标量 marker 后才能强制消费者进入保留路径。仅设置 `supportsScalarFastPath=false` 并不会自动移除这些 mapper 的 marker，初次据此所得的“函数通过”属于无效对照，不作为功能等价证据。范围仅限函数标量适配和增量聚合 owner，不引入 SQL/异常类型特判、Context 私有键、新缓存或双轨框架。当前性能基线为上一节 528 项测试对应制品，功能语义优先于追回微优化。

有效差分复现：一元 `abs(value)` 在非法值上原路径输出 `{id=2}`，标量组合丢整行；`sum(abs(value)),avg(abs(value)),count(*)` 原 COUNT 为 4，标量组合为 3；直接 `sum(value),avg(value)` 的转换失败在原生 map 可以逐值继续，但 raw collect 中直接抛出会终止查询。处理不是按 SQL 或异常类型兜底：`SingleParameterFunctionMapFeature` 统一恢复冷参数 Supplier 后的原生 map，不再宣称该计算为 ScalarValueMapper；SUM/AVG 共用 `NumericAccumulator`，仅在原数值转换 map 范围调用公开 `Operators.onNextError`。增量 SPI 新增兼容缺省的 Context 重载，返回可空终止错误，无新的订阅器、Context 私有键或错误策略实现。归约/资源错误不能写入 `handle.error`，因为 Reactor 3.4.34 会再次恢复它；停止输入并取消上游后，由后段的原生 Flux/Mono.error 传递终止信号。错误存储只在每订阅，分组、窗口和 raw-source 单次扫描结构保留，不退回高基数 GroupedFlux，也不收集输入行或增加每组字段。原生 global 结果继续在源完成后只计算一次，零需求时不发射；独立 before/after 探针证明原 collect.flatMap 本来也是提前计算，未据错误的“零计算”假设增加执行层。

`ScalarAggregateErrorBoundaryTest` 的 6 项覆盖六个一元函数、两种 SELECT 顺序、空值、原始数值/函数参数聚合、显式 ORDER BY 的分组/窗口、限定转换异常的失败回调终止与取消，以及 Number.doubleValue 归约失败不能被非限定 onErrorContinue 恢复。`WindowedAggregateStageTest` 补充 global 的结果计数、零需求不发射、取消及单次计算合同。非抛出型 onErrorContinue 逐项比较数据、错误次数及一次订阅；两个原优化测试只改为断言一元函数现在返回普通 Publisher，原结果断言全部保留。无 ORDER BY 的分组完成次序原路径并不相同，不能拿这类 fixture 的 List 顺序当 SQL 排序合同。未限定异常类型且自身抛异常的回调，可能被旧层叠 Publisher 再次恢复；本阶段不承诺该类失败副作用次数完全相同，不为复制这种行为建立新框架。独立只读审查的终止边界发现经修正接受；其需求计算疑点经实际基线反例撤回。嵌套标量参数如 `abs(-value)` 仍须真实 Publisher 差分，不能用本阶段直接参数的通过结果宣称所有表达式等价。

完整离线 `mvn -o -q -Pjmh -DtrimStackTrace=false package` **535 tests、0 failures/errors/skipped**，51 份当前报告（仍排除旧 Benchmarks 2 项）。测试用 ReactorDebugAgent 动态 attach 被沙箱阻止，正式验证在允许本地 JVM attach 的环境使用原测试配置完成；全局启动 javaagent 的诊断尝试会改写被测链的 fusion/checkpoint/identity，不能作为等价环境，已弃用且未改测试以迁就该环境。正式日志 `target/scalar-aggregate-boundary-final-build-20261006.log`，当前 JAR SHA-256 `201acf56810b0e62110bf78cb069d9042b031e7f42141229058a6803c9e75c83`；before 制品 `target/scalar-aggregate-boundary-before-20261006-benchmarks.jar` SHA-256 `39ad731f8d9cdbce5de461308b324684e8e29783e91408280cbe33c7ca9b6bb5`。

同 JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler，`target/jmh-scalar-aggregate-boundary-before-20261006.json` → `target/jmh-scalar-aggregate-boundary-after-20261006.json`：

| 场景 | 吞吐，M 输入行/s（before→after） | 分配，B/输入行（before→after） |
| --- | --- | --- |
| 高基数五聚合 SQL（每键一行） | 3.437±0.110→3.394±0.074 | 829.012→829.012 |
| 同结果高基数原生对照 | 7.734±0.680→7.827±0.581 | 412.944→412.944 |
| 全局多聚合 SQL | 36.983±0.667→35.958±0.230 | 15.999743→15.999826 |
| 同结果全局原生对照 | 128.393±0.737→129.421±0.361 | 15.998670→15.998670 |
| 未修改的宽列投影对照 | 6.268±0.107→6.762±0.211 | 331.217→331.217 |
| 16 列双 JSONPath/日期宽函数 | 1.684±0.038→1.615±0.051 | 2582.111→2744.115 |

本阶段为功能修正，不申领新性能收益。高基数吞吐区间重叠、分配不变；全局 SQL 均值约 −2.8%。宽函数增加 **162.004 B/输入行（+6.27%）**，吞吐区间重叠，且未修改宽列对照约 +7.9% 表明测试存在环境漂移，不把宽函数 −4.1% 均值直接归因为实现回退。没有 live-heap A/B，不声称常驻堆下降。后续先对当前制品刷新全局聚合与宽函数测量期 JFR；只有热点能支持通用、低复杂度且保留响应式边界的优化才实施，不为追回上述成本建立特殊路径。

### 当前热点筛选与有符号参数边界

当前制品测量期 JFR（JDK 17.0.18、512 MB/G1、单线程、2×1s 预热、3×2s 测量、1 fork）位于 `target/jfr-scalar-aggregate-boundary-20261006/`，诊断结果为 `target/jmh-scalar-aggregate-boundary-jfr-20261006.json`。全局聚合 305 个 worker CPU 样本，未采到 worker 分配样本，主要为 GroupState.addRaw、数值累加及属性读取；不据零分配样本声称零分配或零驻留。宽函数 360 个 CPU / 1836 个分配样本，JSON normalizeText 为 66 / 602 个 inclusive、readNormalizedPath 为 379 个分配样本；这些非互斥采样计数不能相加或换算精确百分比／字节。JSON 文本解析与第三方 JSONPath 求值仍是主成本，不采用跨列缓存或绕开 JSON 规范化／资源限制来追回差距。聚合的重复属性读取不能在没有纯函数契约的情况下合并，之前已拒绝该方向，本轮不新增 SPI。

边界判别已证实嵌套 `abs(-value)` 的 signed 标量计算在 Supplier 中抛错，错误恢复收到 null 而非原非法参数；真实保留 Publisher 的数值转换 map 能恢复该值并保留 id 列。目标为恢复所有 SignedExpression 的原生参数转换／符号运算边界，不按 SQL、类型或异常特判，也不实现自定义错误框架。范围为 `ValueMapFeature` 的 signed mapper 和 `ScalarAggregateErrorBoundaryTest`，先移除不等价标量组合，再覆盖符号种类、嵌套投影／聚合与 SELECT 顺序，阶段末集中完整构建与受影响场景的正式 A/B。此为功能修正，不预先承诺性能提升。

首轮完整构建 536 项测试通过，但真实混合 OR（含固定负数字面量）正式 A/B 由 2.759±0.037 降至 1.783±0.020 M 输入行/s，分配由 650.229 增至 1482.230 B/输入行。这是功能修正把固定字面量也变成逐行 Publisher 的实质成本，不当作可交付性能结果。首轮制品 SHA-256 `c94787e6da27a961f6d335016eeb00021d5efa8a96e4a41998553f5bb69d12c1`，JFR `target/jfr-signed-or-unfolded-20261006/` 的 372 个 worker CPU / 1848 个分配样本中，CPU 叶子 FluxConcatMap.drain 50 个、经 OrFilter 原生组合的分配样本 497 个，SignedExpression mapper 的分配栈 77 个。该定向取证与 A/B 一起支持消除编译期已知的数值字面量运算，而不是取消动态表达式的错误边界。

下一步仅做标准数值字面量常量折叠：LongValue / DoubleValue 的值在现有 visitor 已于构建期读取，符号运算无源依赖、可观察副作用或失败；构建时直接产生同类型常量 mapper。该区分依据 SQL AST 的字面量语义，不判断逐行数据类型、不识别特定数值／SQL、不捕获异常以选择快路，也不新增 SPI 或优化器框架。字符串、参数、属性、函数及其他动态表达式保留原生转换和符号 map，包括非法字符串字面量的错误仍在订阅时恢复。覆盖整数／浮点类型、负零、三种符号、普通值／错误的动态表达式、延迟失败和背压；阶段末统一构建，并与最初及未折叠功能修正制品对照真实混合 OR、宽函数和三层子查询。

最终实现仅恢复 SignedExpression 动态参数的原生两段 map，以及新增 7 行构建期数值字面量折叠；没有新增类／SPI／执行框架。`ScalarAggregateErrorBoundaryTest` 共 10 项，新增三种符号的直接／嵌套参数及聚合差分、两种 SELECT 顺序、数值字面量类型／负零、非法字符串字面量错误时机和按需过滤。初版需求夹具在恰好请求四个输出后无法检查来源末尾的两个被过滤输入，属于普通有限源的需求行为而非生产死锁；夹具补一个请求并限定五秒验证超时，未改生产背压实现。完整离线 `mvn -o -q -Pjmh -DtrimStackTrace=false package` **539 tests、0 failures/errors/skipped**，51 份当前报告（排除旧 Benchmarks 报告）。正式日志 `target/signed-literal-fold-final-build-recheck-20261006.log`，最终 JAR SHA-256 `45369b6cc036adac07272485f8ab1d7f8a714473220837a028c479519ef4c6f4`，`git diff --check` 通过。

相同 JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler；原有效基线 `target/jmh-nested-signed-boundary-before-20261006.json` → 最终 `target/jmh-signed-literal-fold-after-20261006.json`：

| 场景 | 吞吐，M 输入行/s（原基线→最终） | 分配，B/输入行（原基线→最终） |
| --- | --- | --- |
| 16 列混合 OR（含负数字面量） | 2.759±0.037→2.788±0.072 | 650.229→650.229 |
| 16 列混合运算符对照 | 3.517±0.075→3.543±0.047 | 514.472→514.472 |
| 16 列宽函数对照 | 1.631±0.063→1.645±0.038 | 2744.115→2744.115 |
| 三层子查询对照 | 7.604±0.276→7.529±0.104 | 576.144→576.144 |

所有最终对照吞吐区间重叠、分配不变。本阶段消除语义回归并守住真实查询基线，**不申领新的性能提升**。相对未折叠中间版本追回的混合 OR 832.001 B/行和吞吐不能算相对原基线的优化收益；宽函数中间版本的轻微分配差异未在最终版复现，不归因于此改动。上述夹具包含负数字面量，但未直接量化大量动态 signed 列的吞吐；动态参数恢复原生边界有成本，不能由负对照声称所有 signed SQL 性能相同。没有 live-heap A/B，不声称常驻堆下降。仍未证明 Binary/Cast/FunctionMap.scalar 等其他标量组合的全面错误等价，不能以本阶段测试数代替后续边界证据。

最终短 JFR `target/jfr-signed-literal-fold-after-20261006/`（与未折叠版同录制配置）为 374 个 worker CPU / 1855 个分配样本。SignedExpression mapper 的分配栈由 77 个降为 0，`OrFilter.lambda$createPredicate$2` 的原生 OR 组合分配栈由 497 个降为 0；剩余 MixedScalarOr、IN 参数展开、结果 Record/Map 与普通运算仍存在。FluxConcatMap.drain 仍有 80 个 CPU 叶子，不能声称所有中间操作符已合并或总体 CPU 样本下降。样本只支持被移除成本的归因，字节和吞吐以正式 A/B 为准。IN 的左值允许 Iterable/Publisher/单字段 Map 等语义已在前阶段分析，不据该夹具通常为标量而新增专用同步分支。此阶段不改默认资源限制、不增加输入行驻留、不提交或推送。

### CAST 标量组合错误范围核对

目标：在继续性能变更前，用真实保留 Publisher oracle 对照 CAST 的转换错误边界；范围仅 `CastFeature` 与现有错误差分测试，保持所有转换类型、空值、公开 API 和订阅语义。对照沿用关闭聚合快路及移除属性 mapper marker 的方式，覆盖 long/double/decimal 的直接投影、嵌套一元函数、两种 SELECT 顺序和独立 SUM/AVG/COUNT；必要时补原生异步参数及取消证据。未复现前不改生产实现。若证实标量内联扩大错误范围，统一恢复原生 map，不用异常／SQL／类型特判、Context 私有键或新执行层补偿；保留已有构建期类型规范化。阶段末集中完整构建，对含 CAST 的真实 16 列混合查询及未改宽函数／三层子查询做同配置 A/B，如有明显成本先 JFR 归因，再决定有无通用低复杂度优化，不申领功能修正为性能收益。

有效判别日志 `target/cast-boundary-discriminator-20261006.log`：12 项定向测试中的两个新测试均失败，long/double/decimal 直接 CAST 的优化错误恢复收到 `{id=2}` 而非非法参数；聚合中优化路径以 TypeCastException／NumberFormatException 终止，原保留 map 可逐值恢复并保留 COUNT。按共享转换 owner 删除不等价标量内联，统一恢复 `Mono.from(parameter.apply(record)).map(castNormalizedValue)`；没有 Supplier 包装、类型／异常分支、新缓存、Context 键或 SPI。构建期类型规范化及公开 `castValue` 的直接调用行为不变。结构测试只将 CAST 列改为普通 Publisher 期望，原结果断言不删；扩展差分覆盖分组／计数窗口，并增加限定转换异常的失败回调终止与一次取消测试。既有 CastFeatureNormalizationTest 的异步参数／Context 合同作为本阶段回归验证的一部分，不复制新的测试替身。

功能修正制品 SHA-256 `3cf57e3d522e0495db9deac3d091684ac7daeceeeaee921cea2ea69c9eb6b7f0`，完整构建 `target/cast-boundary-final-build-20261006.log` 为 **542 tests、0 failures/errors/skipped**。其测量期 JFR `target/jfr-cast-boundary-after-20261006/` 为 385 个 worker CPU / 1816 个分配样本，主要仍为 IN 参数链与必要 Record/Map。CAST 栈有转换结果 Long 48、MonoMapFuseable 23、MonoJust 13、捕获固定 type 的 CastFeature lambda 11 个分配样本；转换类型 switch 只有 2 个 inclusive CPU 样本，不据此重写转换分派或新增专用类型快路。

仅试验一个两行通用分配优化：将只捕获构建期固定 type 的转换 Function 复用到查询级，再交给原生 map；参考已有 DateFormatFeature 的查询级无状态转换函数做法。Function 不捕获输入行、不缓存结果、不提前执行转换，同一值级错误／需求／Context 边界不变。候选仅针对 JFR 中逐行捕获 lambda 分配，不推断大幅吞吐收益；保留原生 MonoJust／MonoMap 及必要数值结果。阶段末集中验证，再与功能正确的原生 CAST 制品和最初基线对照，只有分配收益实测且无功能／显著吞吐回退才保留。

本阶段完整验证再次遇到旧 `GroupByWindowTest.testGroupByTimeWindow` 的 3.5→4.0 波动（同一方法／期望的既有记录见本文件前述投影、分组键和其他阶段）。该例没有 CAST，200 ms 延迟输入的第 5 个值接近 1000 ms 的窗口边界，真实线程调度决定其归属，原夹具却要求固定分区。按响应式测试最佳实践将此例的查询与来源组装放入官方 `StepVerifier.withVirtualTime` Supplier，以虚拟时钟推进 1200 ms；窗口 500 ms、输入延迟 200 ms、六个输入以及精确 1.5/3.5/5.5、完成信号断言均保留，补五秒墙钟超时。只修测试时钟，不调生产窗口／调度器、输入值、资源上限或性能夹具。根因类别 D/E（覆盖缺口／隐含时序假设）；不再以重复跑到绿作为该例的长期稳定性证据。Trellis 未配置，结论回填当前 owning 文档和代码注释，不创建 spec/template 或自动提交。

最终完整离线 `mvn -o -q -Pjmh -DtrimStackTrace=false package` **542 tests、0 failures/errors/skipped**，51 份报告（排除旧 Benchmarks）。日志 `target/cast-conversion-hoist-final-build-recheck-20261006.log`，最终性能制品 SHA-256 `e851a3898089c4aff383a81e644b0fb60500f578f175f982e0e06b226dd24fce`，`git diff --check` 通过。保存的原基线 `target/cast-boundary-before-20261006-benchmarks.jar` 为 `45369b6cc036adac07272485f8ab1d7f8a714473220837a028c479519ef4c6f4`；功能正确但未提取 Function 的 `target/cast-boundary-native-before-hoist-20261006-benchmarks.jar` 为上述 `3cf57e3d…`。

同 JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler：`target/jmh-cast-boundary-before-20261006.json`（原基线）→ `target/jmh-cast-boundary-after-20261006.json`（原生 map）→ `target/jmh-cast-conversion-hoist-after-20261006.json`（最终）：

| 场景 | 吞吐，M 输入行/s（原基线→原生 map→最终） | 分配，B/输入行（原基线→原生 map→最终） |
| --- | --- | --- |
| 16 列混合 OR（两列 CAST） | 2.818±0.050→2.668±0.035→2.693±0.021 | 650.229→796.209→789.339 |
| 16 列混合运算符（两列 CAST） | 3.559±0.107→3.473±0.024→3.435±0.042 | 514.472→622.372→617.294 |
| 未改宽函数对照 | 1.625±0.014→1.645±0.034→1.641±0.013 | 2733.315±17.212→2744.115→2733.315±17.212 |
| 三层子查询对照 | 7.584±0.129→7.677±0.074→7.627±0.171 | 576.144→576.144→576.144 |

保留查询级无状态 Function 复用，相对功能正确的原生 map 少 **6.870 / 5.078 B/输入行（约 0.86% / 0.82%）**；相应吞吐区间重叠，**不申领吞吐提升**。这是低复杂度的小分配收益，不冒充高倍率优化。原生 CAST 边界仍有明确成本：相对原不等价标量基线，最终混合 OR 吞吐约 **−4.4%**、多 **139.110 B/输入行**（区间不重叠）；普通混合查询多 **102.822 B/输入行**，吞吐均值 −3.5% 但区间重叠。为保留功能不恢复该标量内联，也不通过例外分支追回差距。未改宽函数在原基线／最终各两个 fork 出现 2744.115 与 2722.515 B/输入行，误差区间重叠，不归因于本次两行复用；三层子查询分配不变、吞吐区间重叠。

最终测量期 JFR `target/jfr-cast-conversion-hoist-after-20261006/` 为 376 个 worker CPU / 1858 个分配样本；CAST 的逐行捕获 lambda 分配由 11 个降为 0，剩余 CAST 栈 Long 44、MonoMapFuseable 24、MonoJust 14 个仍是结果及原生错误边界，不能当作可直接删除的开销。与前录制的计数不能换算精确字节或 CPU 百分比，量化以正式 GC/JMH 为准。没有 live-heap A/B，不申领常驻堆下降；此阶段没有增加历史输入驻留、资源上限或默认并行度。高基数含 CAST 的聚合暂未单独量化，不能拿上述投影对照代表它；其他 Binary/FunctionMap.scalar 组合的错误等价仍有待验证，整体优化目标继续保持，未提交／推送。

### 原始行聚合能力透传（2026-10-06）

目标：删除原始标量输入聚合中不必要的逐行 Record 包装，不增加中间输入驻留。范围仅现有 MapAggFeature／CountAggFeature 工厂和 WindowedAggregateStage 原始输入边界，复用已有 RawScalarValueMapper.acceptsAnyRow 能力，不新增 SPI、缓存、SQL／数值类型特判或执行框架，不改变默认限制、精确 DISTINCT／UNIQUE 状态和分组窗口生命周期。

前置取证：基线 `e851a389…` 的既有 countDistinctRepeated／countUniqueRepeated 短 JFR（`target/jfr-raw-aggregate-capability-before-20261006/`）分别有 103／87 个 worker CPU、612／616 个分配样本，其中 DefaultReactorQLRecord 为 602／610 个。工厂用 isConstant 代替 mapper 的显式能力，使 this 数值流回退到包装；采样不能换算精确 B/行或 CPU 百分比。

步骤：先扩展既有预构造输入聚合基准，加入普通五聚合及带 WHERE 对照，保留 before 制品及正式 GC/JMH；然后仅透传能力，保留既有 ReactorQLRecord 的读取／别名绑定语义。集中差分验证数字、Map、混合及已有 Record 输入、空流、错误恢复、Context、需求、取消和重复订阅，再完整构建与同配置 A/B。已授权直接执行；若不能用小改保持上述协议边界则不实施，若分配／吞吐无可靠收益则撤回。验证结果回填本节；未测 live heap 不声称常驻堆下降。

普通五聚合及带 WHERE 的 before JFR 使用新增夹具、未改生产实现的制品 `deef20c873410f86c904d7d9a9121783e78a9d791138efa07610dedb82175721`（`target/raw-aggregate-capability-fixture-before-20261006-benchmarks.jar`），分别有 608／640 个 worker 分配样本，其中 DefaultReactorQLRecord 为 607／639 个。短录制含启动／前置校验，不将 CPU 叶子混合样本归因于单个 SQL；正式 JMH 的预热后分配和吞吐用于量化。生产候选仅将两个工厂的 isConstant 改为显式 acceptsAnyRow；已有 ReactorQLRecord 始终走原 newRecord 别名绑定及 add 读取，不当成底层数值。工厂注释同步既有能力含义；没有扩大 raw keyed／窗口路径，也没有绕过值级错误处理。

集中完整离线 `mvn -o -q -Pjmh -DtrimStackTrace=false package` 通过：51 份报告、**546 tests、0 failures/errors/skipped**（排除旧 Benchmarks）。日志 `target/raw-aggregate-capability-final-build-20261006.log`；after 制品 SHA-256 `4f64290c34fd7a096ad294c633bd944c79d285dbe5feaa35abd8fa0ff7e05bdf`。WindowedAggregateStageTest 覆盖完整五聚合、WHERE、去重、已有 Record 的原别名／新别名及重复消费、混合输入、空流、Context、零初始需求、源错误与一次取消；ScalarAggregateErrorBoundaryTest 使用真实保留 Publisher oracle，对原始数字及已有 Record 验证非法参数逐值恢复、独立 COUNT 与失败回调只终止／取消一次。现有 Map／自定义属性／分组窗口测试复用本次完整验证。

正式 A/B 使用同 JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler。JSON：`target/jmh-raw-aggregate-capability-before-20261006.json`、`target/jmh-raw-aggregate-controls-before-20261006.json` → `target/jmh-raw-aggregate-capability-after-20261006.json`。

| 场景 | 吞吐，M 输入行/s（before→after） | 分配，B/输入行（before→after） |
| --- | --- | --- |
| 原始数值 COUNT/SUM/AVG/MIN/MAX | 35.577±3.203→41.652±0.394 | 32.026→0.026 |
| 同五聚合带 WHERE | 46.417±0.367→48.411±2.111 | 32.026→0.026 |
| 原始数值 COUNT DISTINCT | 77.089±2.437→93.072±3.630 | 32.397→0.396 |
| 原始数值 COUNT UNIQUE | 75.627±1.901→85.761±1.066 | 32.397→0.397 |
| Map 全局五聚合对照 | 34.946±0.545→34.624±0.161 | 0.00185→0.00186 |
| Map 高基数五聚合对照 | 3.796±0.123→3.613±0.081 | 765.011→765.012 |
| 宽函数查询对照 | 1.630±0.073→1.628±0.017 | 2733.315→2733.315 |

保留低复杂度能力透传：原始数值五聚合、COUNT DISTINCT／UNIQUE 吞吐分别约 **+17.1%／+20.7%／+13.4%**，这些区间不重叠；四个原始数值场景均少约 **32 B/输入行**，普通聚合约少 99.9%、精确去重约少 98.8%。WHERE 吞吐区间重叠，不申领其 +4.3% 均值差为确定收益。以上是引擎内处理预构造输入的测量，不代表来源生成／传输成本或所有 SQL 的整体加速。

Map 高基数首次吞吐均值 −4.8%、区间略重叠，因此仅此项另做同配置 before／after 复测（`target/jmh-raw-aggregate-highcard-before-repeat-20261006.json`／`target/jmh-raw-aggregate-highcard-after-repeat-20261006.json`）：**3.798±0.156→3.661±0.338 M**，区间重叠，after 两 fork 均值为 3.869／3.453 M；分配仍为 765.011→765.012 B/行。未建立确定回退或提升，但吞吐噪声与负均值风险如实保留；不通过热身、类型／键分布、JIT 或状态布局特调追回均值。该路径不消费 acceptsAnyRow，本阶段未改变 keyed 处理或累加器状态。Map 全局和宽函数吞吐区间也重叠，分配无实质变化，不申领其他 SQL／高基数常驻堆收益。

本轮不引入缓存／池、特殊值分支、容量阈值或组合聚合框架；Record 保护是既有输入协议边界而非数值类型快路。没有收集历史行，AVG/SUM/MIN/MAX 继续实时更新固定状态；精确 DISTINCT/UNIQUE 仍按不同键保留必要状态及原资源限制。没有 live-heap A/B，分配减少不等于实测常驻堆下降。未提交／推送，整体优化目标继续保持。

after 短 JFR `target/jfr-raw-aggregate-capability-after-20261006/` 中，COUNT DISTINCT／UNIQUE、普通／带 WHERE 五聚合的 worker 分配样本为 127／115／1／1；这四条录制均没有 DefaultReactorQLRecord 分配样本（before 为 602／610／607／639）。去重剩余样本主要是每查询精确键状态的桶数组，普通聚合只采到偶发订阅／结果容器。采样数量不换算字节或 CPU 百分比；部分 CPU 叶子仍混有前置校验／JIT 符号归属，不能把其中 addUnique 名字直接当成普通 SUM 的热点。正式 GC/JMH 的 32 B/行减少与移除包装吻合；没有据此追加算法／类型快路。最终 `git diff --check` 通过，源码及测试在完整构建后未再修改。

### 高基数存活堆与当前宽函数测量期热点复核（2026-10-06）

复核现有 HighCardinalityLiveHeapProbe 生命周期、WindowedAggregateStage 状态所有权及前述 10k／50k 活跃键斜率与类直方图。当前 keyed 路径仍只保留精确索引和固定累加状态；数组快照共享、单组容器内联已经实施。Record 回退路径的 lastGroupKeys 用于保留上游及本层组键序列，不能无条件删除；普通原始行路径没有该引用。上一轮只改变全局原始行能力透传，不改变 keyed 状态布局，因而复用仍覆盖该布局的存活堆证据，不另搭探针或按固定键分布重写聚合状态。没有新的高基数常驻堆收益。

当前制品 SHA-256 `4f64290c34fd7a096ad294c633bd944c79d285dbe5feaa35abd8fa0ff7e05bdf` 的真实 16 列宽函数查询，在确认 JMH 预热完成、进入 measurement 后向其唯一标记 fork 附加 12 秒 JFR，文件为 `target/jfr-wide-functions-post-capability-measurement-20261006.jfr`。固定 JDK 17.0.18、512 MB/G1；录制完整写出，worker 有 729 个 CPU、3528 个分配样本。主要 CPU 叶子为 Map 读取／属性映射、JSONParser.readObject、日期处理与 Map 写入；主要分配为 JSON 文本解析、必要日期结果及 Map／数组。函数内 JSON 多路径规范化已复用，单路径文本解析不额外复制解析树；日期转换已区分直接 LocalDateTime 与本地时区转换，不能绕过原有 Timestamp／时区语义。没有证据支持跨列文档／日期缓存、专用函数布局或继续微调，故不改生产代码。最初 30 秒诊断 fork 在附加前结束，未取得录制；其缺少显式 JVM 配置，不使用诊断分数比较吞吐。后续 75 秒诊断只提供上述 measurement-only 热点，不作为正式 A/B。

### 二元运算标量组合错误边界核对（计划，2026-10-06）

目标：在继续减少运算符前，核对 BinaryMapFeature／BinaryCalculateMapFeature 的标量计算与原保留 Mono.zip 的错误恢复、终止及独立列语义。复用 ScalarAggregateErrorBoundaryTest 的真实 retained Publisher oracle（关闭聚合／metadata 快路并将同一个属性 mapper 暴露为普通 Function），同时记录输出／终止信号与 continuation 值。先覆盖直接及嵌套数值运算、SELECT 顺序、WHERE、SUM／AVG／COUNT；以非法数值证明到达转换边界，不预判结果或改生产。

若差分证实共享内联错误范围不同，则在 BinaryMapFeature owner 统一恢复原生边界，不新增异常类型分派、私有 Context、输入猜测、状态框架或补偿 Subscriber；扩展相关 Context／请求／取消／异步参数回归，在完整阶段集中验证，并以当前制品正式基线量化真实宽投影、混合操作符与子查询成本。若差分通过则保持生产实现，记录有效证据后寻找其他测量期热点。功能修正不冒充性能提升；默认限制与上一轮原始标量聚合能力透传不变。已授权直接执行，不新增确认点。

有效判别 `target/binary-boundary-discriminator-20261006.log`：17 项定向测试中两个新增测试失败。直接加／乘／除及嵌套 pow 的保留 Publisher 恢复回调数据是 null，优化路径是含 id 的整行；含 value+2 的 SUM/AVG/COUNT 原可得到 total=17、mean=8.5、rows=3 并完成，优化路径却以 TypeCastException 终止。这支持共享 calculator 错误范围不等价，而非异常类型或查询特例。删除 BinaryMapFeature 的标量分支，所有 calculator 统一回到原 Mono.zip 与 wrapper；未改计算／转换规则，也不另设 Boolean／数值／SQL 例外。阶段集中验证与成本测量待完成。

首次完整验证只有两个门禁失败：旧结构测试仍把 score+1 断言为融合标量投影；新取消夹具的 doOnCancel 在原生 oracle 自身即观测到两次调用。结构测试保留原 SQL、一个 handle、无 filter 断言，并同时验证普通属性投影仍融合、二元 calculator 是独立 Publisher 以及两者精确结果／类型。Reactive Streams 允许重复 cancel、要求幂等，故新测试改用官方 doFinally(CANCEL) 核对每订阅只清理一次，不把原生重复取消调用特调成一次；失败回调、原异常终止、Context 和原数值断言保持。生产实现不为这两个测试补兼容分支。修正后集中完整验证待完成。

最终完整离线 `mvn -o -q -Pjmh -DtrimStackTrace=false package` 通过：51 份当前报告（排除旧 Benchmarks）、**549 tests、0 failures/errors/skipped**。日志 `target/binary-boundary-native-build-recheck-20261007.log`，功能正确制品 SHA-256 `674e6132204634b342f7e94d71947497dd700ff3284870239160953bf393afda`；构建后没有生产／测试变更，`git diff --check` 通过。

正式 A/B：同 JDK 17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler；`target/jmh-binary-boundary-before-20261007.json`／`target/jmh-binary-boundary-deep-before-20261007.json` → `target/jmh-binary-boundary-native-after-20261007.json`。

| 场景 | 吞吐，M 输入行/s（before→原生正确版） | 分配，B/输入行（before→原生正确版） |
| --- | --- | --- |
| 16 列混合 OR | 2.758±0.016→2.194±0.033 | 789.339→1162.017 |
| 16 列混合运算符 | 3.470±0.084→2.812±0.072 | 617.294→892.757 |
| 16 列宽函数 | 1.651±0.031→1.359±0.019 | 2733.315→3312.931 |
| 三层子查询 | 7.599±0.099→7.725±0.111 | 576.144→576.145 |

功能修正有确定成本：前三项吞吐分别约 −20.4%／−19.0%／−17.7%，分配增加 372.678／275.463／579.616 B/输入行；不申领此阶段性能提升，也不恢复不等价内联。子查询区间重叠，不申领提升。默认限制、原始聚合能力透传与 keyed 状态布局不变；没有 live-heap A/B，不声称常驻堆变化。

下一阶段先对上述功能正确制品采集 measurement-only JFR，筛选可由现有工厂／原生操作符的小改解决的重复装配；只接受真实 SQL 的明确收益，不增加缓存框架、补偿订阅器、Context 私有键、SQL／类型分支或聚合组合层。未获高收益证据时记录不改，不为追回错误快路的分数放宽语义。

### 原生二元边界修正版的高收益候选筛选（2026-10-07）

使用上述 `674e613…` 制品，分别运行既有 16 列混合运算符和宽函数基准；JDK 17.0.18、512 MB/G1、单线程，确认 3×1s 预热完成、进入 60 秒 measurement 后附加 12 秒 JFR。文件 `target/jfr-binary-native-mixed-measurement-20261007.jfr`／`target/jfr-binary-native-wide-measurement-20261007.jfr`；诊断日志 `target/jfr-binary-native-{mixed,wide}-diagnostic-20261007.log`。沙箱禁止 JMH 的本地回环连接，使用批准的沙箱外本地测量；两次录制完整落盘、fork 正常结束。诊断吞吐不用于 A/B，也不混入正式分数。

混合查询有 **774 个 worker CPU、3516 个分配样本**：属性／Map 读取、LIKE 的前缀／换行语义检查及原生订阅为主要 CPU 来源；结果 Map 节点 332、zip 内部订阅器 257、结果桶数组 176 个分配样本。`ScalarValueMapper.apply` 下 MonoJust 为 93 个样本，该栈同时涵盖属性和常量，不能将其全部归于字面量。宽函数有 **281 个 CPU、1439 个分配样本**：JSONParser.readObject、String／Map 和日期处理仍突出；JSON 文本解析、JSONPath 求值、结果 Map 与 zip 订阅器是主要分配来源，同步 mapper 的 MonoJust 仅 9 个样本。数量只定位来源，不能换算 CPU 百分比或精确 B/行；JFR sampled weight 不作为此轮精确分配量。

结论：本轮**不新增生产优化**。字面量 Publisher 复用未取得足够高收益证据，不增加查询固定字段／分支；不通过删除必要 zip／defer 改变函数冷求值、列订阅顺序和错误范围。LIKE 换行检查保留原 Java regex 语义，JSON 静态路径已预编译、文本解析树已直接读取，剩余来源不能靠跨列缓存、路径／日期类型专门分支或新执行层解决。高基数状态布局也未发现简单可删冗余，不以移走解析成本、修改上限或固定键分布申领引擎收益。保持前述已验证的原始聚合能力透传收益，整体优化目标尚未完成；FunctionMapFeature.scalar／scalar2／scalar3 的真实 Publisher 错误差分仍未全覆盖。

阶段验收复用仍覆盖相同源码与制品的 **549 项完整测试**，未因纯 JFR／文档更新重跑构建。最终 JAR 与保存的 `target/binary-boundary-native-correct-20261007-benchmarks.jar` SHA-256 均为 `674e613…`；`git diff --check` 通过，没有新增默认限制、常驻缓存、历史输入驻留或生产依赖。没有新的吞吐、分配或 live-heap 改善可申领；未提交／推送。

### 多参数函数工厂的原生错误边界差分（计划，2026-10-07）

范围：`FunctionMapFeature.scalar/scalar2/scalar3` 及现有 `ScalarAggregateErrorBoundaryTest`，先以同一属性 mapper 的真实 Publisher oracle，核对直接 round／pow／substring／日期／正则函数、SELECT 顺序、WHERE 和 SUM／AVG／COUNT 的正常／非法／空输入。观察输出、终止信号、恢复次数与回调数据；必须确认夹具到达错误边界。范围不含 JSON 或已修正的一元函数／二元 calculator，不根据函数名、输入类型或异常种类推断快路。

差分通过保留工厂；差分失败则在共同函数工厂恢复原参数／值级边界，不加异常补偿、私有 Context、订阅器、缓存或新 SPI。集中回归公开 mapper 覆写、异步／多值参数、参数缺省、Context／需求／取消，再完整构建与既有真实 SQL 同配置 A/B。基线复用 `674e613…` 保存制品。功能成本如实量化；未取得简单高收益性能证据不追加微调。实施已获授权，不新增确认点。

有效差分 `target/function-factory-discriminator-20261007.log`：20 项定向测试中两项新增测试失败，7 类直接函数的恢复回调由原 null 变为整行；SUM／AVG(round/pow) 原保持独立 COUNT=4 并完成，快路以 TypeCastException 终止。共同根因仍是函数计算从原生 flatMap 移到行／累加器错误范围，不是单个函数转换错误。

实施统一删除 FunctionMapFeature 的 scalar marker 分支、双／三参数快速字段与 FixedArgumentList；scalar 工厂始终保留原参数流及 collectList.flatMap 回调。scalar2 仅为同一原生工厂的二参数回调便捷入口；移除本分支新增、HEAD 未发布的 scalar3／TernaryScalarMapper 双回调入口，六个内置三参数函数和既有回归同步使用原 List 工厂。SQL 函数名、参数数目／默认值、缺参索引语义、公开构造器和 protected apply 调用链不变。专用固定参数容器删除，FunctionMapFeature 从 534 行缩至 241 行；不保留无效快路或无依据兼容壳，不新增状态／Subscriber。阶段验证与成本测量待完成。

首轮完整构建：新增函数差分与生命周期测试均通过；旧 RawWhereBeforeRecordTest 仍断言 str_contains 是 raw 同步能力，现改为验证原 SQL 的 Publisher 边界并保留所有输出／COUNT 断言，普通属性过滤仍是 raw 正对照。另有八项窗口测试因 ReactorDebugAgent 不能沙箱内自附加失败，改用批准的沙箱外原命令，不删除调试代理或跳过测试；原 testGroupByTimeHaving 的墙钟计时使窗口多一组，改为官方虚拟时间，同一 supplier 内构造两个计时器，保留原 HAVING 与四项输出数量要求，不对生产窗口时间／调度做修改。

最终完整离线 `mvn -o -q -Pjmh -DtrimStackTrace=false package` 通过，日志 `target/function-factory-native-build-recheck-20261007.log`。新差分覆盖直接函数／两种 SELECT 顺序、空值、WHERE、global／keyed／window SUM/AVG/COUNT；新失败回调回归覆盖 round/pow/date_add 的零初始需求、Context、原异常与一次清理。公开 mapper 替换／恢复、原二／三参数缺省／缺参／可变 List 和异步参数测试均保留并通过。下一步仅录制此原生修正版的参数适配热点，再判断是否有无需移动函数计算边界的小改；当前修正不计作性能提升。

原生修正版 **553 tests、0 failures/errors/skipped**（51 份当前报告，排除旧 Benchmarks），SHA-256 `4509060820b6c54e544835c773548f6f66b30a1710c7d7950b8f1e47ed886064`，保存为 `target/function-factory-native-correct-20261007-benchmarks.jar`。

### 显式同步参数的原生流适配优化（计划，2026-10-07）

前置 JFR `target/jfr-function-params-native-measurement-20261007.jfr` 对上述语义修正版的既有 profilingCommonFunctions（字符串／日期嵌套与聚合 SQL）在预热后录制 12 秒，固定 JDK17/512 MB/G1，有 559 个 worker CPU、3522 个分配样本。FluxConcatMap.drain 为 92 个 CPU 叶子样本；IterableSubscription 541、参数 ArrayListSpliterator 202、ConcatMapImmediate 190、ScalarSubscription 175 个分配样本，显示同步参数仍按独立 Publisher 逐项订阅。样本不换算精确 B/行或 CPU 占比；函数计算 collectList.flatMap 保留，不以删除 List／错误边界追分。

候选仅在 FunctionMapFeature.createParameterStream 内，对所有参数**显式声明 ScalarValueMapper** 的多参数流，用官方 Flux.handle 逐项取值、跳过 null／填入原 defaultValue，替代参数级 concatMap/Mono 适配；函数回调与 List 收集、普通 Publisher 投影不变。判断使用小循环，不增加每行 Stream／额外缓存／SPI／状态类；任意普通、异步或多值参数仍走原 concatMap。不得只根据 isConstant、函数名或参数类型判断。

先差分真实 retained concatMap 参数流，验证同步参数抛错时 continuation 的**同一个 mapper 输入**、跳过及失败回调、空值／缺省／顺序、冷取值、Context、需求与取消；复用公开 mapper 与 protected apply 覆写及异步／多值测试。集中完整验证后，同配置 JMH+GC 比较常用函数、真实混合／宽函数及异步／多层子查询负对照。若参数错误边界不等价或收益不足则撤回候选，不设计补偿层。阶段内只量化瞬时分配；未测 live heap 不申领常驻堆下降。

判别 `target/function-parameter-handle-discriminator-final-20261007.log`：普通参数错误的恢复 mapper 输入、null／default 与列表结果等价；**零需求／分次请求的求值序列不等价**——原生为 `[first, subscribed, second, first received]`，handle 为 `[subscribed, first, first received, second]`。这是原参数 concatMap 的可观察协议，不按夹具特判更改它。候选已完整撤回，不加额外 factory 模式、提前求值、预取状态、消费者异常补偿或手写迭代器。初版失败回调测试假设原生必须终止为消费者异常，oracle 自身不满足，因此仅此断言属于无效夹具；修正为逐项对照真实原生恢复与终止序列，不把该失败归于候选。新增 FunctionParameterStreamTest 保留同一 marker 对象的参数恢复／需求时序／失败回调差分，作为以后防复发的边界。

正式数据 `target/jmh-function-parameter-native-before-20261007.json` 为功能修正版 `450906…` 的单线程、JDK17.0.18、512 MB/G1、3×1s 预热、5×1s 测量、2 forks、GC profiler，未测被撤回候选、不申领其性能。与上一阶段 `target/jmh-binary-boundary-native-after-20261007.json`（`674e613…`）的同配置结果比较函数语义修正成本：

| 场景 | 吞吐，M 输入行/s（函数修正前→原生函数） | 分配，B/输入行（修正前→原生函数） |
| --- | --- | --- |
| 16 列混合运算符 | 2.812±0.072→2.366±0.044 | 892.757→1022.237 |
| 16 列宽函数 | 1.359±0.019→0.602±0.013 | 3312.931→6862.619 |
| 三层子查询对照 | 7.725±0.111→7.473±0.050 | 576.145→576.146 |

两条目标 SQL 的吞吐分别约 **−15.9%／−55.7%**，分配多 129.480／3549.688 B/输入行，不能称作性能优化；是删除不等价函数内联并保留原生参数／计算范围的成本。子查询均值 −3.3%、初轮区间不重叠，也不能声称无回退或提升；未据此改变无关代码或 JIT／夹具，后续需独立复测才能判断稳定性。当前常用函数基线为 **1.077±0.015 M、3736.049 B/输入行**；逐行双异步参数对照 **4.062±0.160 M、1110.913 B/输入行**，这两项没有本轮修正前正式数据，不虚构收益对比。所有结果仅为预构造输入的引擎测量，不代表来源生成／传输或 live heap。

子查询仅此项独立 3 forks 同配置复测 `target/jmh-function-factory-deep-control-repeat-20261007.json` 为 **7.577±0.187 M、576.145 B/输入行**，三个 fork 均值 7.785／7.519／7.428 M；与修正前 **7.725±0.111 M** 区间重叠。没有建立稳定回退或提升，初轮 −3.3% 及 fork 波动仍如实保留，不通过改预热／来源／执行布局追回均值。

撤回候选后的完整 `mvn -o -q -Pjmh -DtrimStackTrace=false package` 为 **556 tests、0 failures/errors/skipped**，52 份当前报告（排除旧 Benchmarks）；日志 `target/function-factory-native-final-build-20261007.log`，最终制品 SHA-256 `63c5f72760da469d3330f990c16d8a8960cb5083f2fb7639bb8ce290949bd26d`。与正式测量原生修正版 `450906…` 逐项比对 **504 个 ReactorQL/JMH class 字节完全相同**；差异仅测试／打包元数据，不因更新测试或文档重跑性能。当前 FunctionMapFeature 241 行，不保留 handle 候选、标量计算双轨或固定参数容器。最终 `git diff --check` 通过，未提交／推送；没有 live-heap A/B，不申领常驻堆变化。

根因与防复发（trellis-break-loop，回填已有原始文档；该 owning module 无 Trellis spec/template，不安装新框架或自动提交）：**E 隐含假设／D 覆盖缺口／B 跨层契约**——同步函数的标量值等价不能证明从 flatMap 移至行级求值后仍保留错误数据、独立聚合及列生命周期。之前修正一元和二元 calculator 不覆盖多参数工厂，属于共享范围尚未完成，不证明新的 soft fast path。预防由真实 retained Publisher 差分观察继续处理后的行／独立 COUNT、callback data 与 terminal signal，加上直接参数流的零需求时序反例；错误路径无需特例 Subscriber／Context／异常匹配。邻近 map 工厂／异步参数作为反例保留；不凭本节 556 tests 宣称所有 JSON／条件表达式的错误等价已经全覆盖。整体吞吐与堆优化目标仍未完成，下一轮回到独立的实际 SQL CPU／内存取证，不追逐不等价性能上界。

### 关联查询与多行 JOIN 的修正后热点复核（2026-10-07）

范围仅当前功能正确制品 `63c5f727…` 的既有预构造关联 lookup／多行 INNER JOIN 夹具，不新增来源缓存、去关联化或专用 Record 布局。固定 JDK17.0.18、512 MB/G1、单线程，确认 3×1s 预热完成后，在 60 秒 measurement 内附加 12 秒 JFR；日志 `target/jfr-correlated-refresh-diagnostic-20261007.log`／`target/jfr-join-containers-diagnostic-20261007.log`，录制 `target/jfr-correlated-refresh-measurement-20261007.jfr`／`target/jfr-join-containers-measurement-20261007.jfr`。正常结束；诊断分数不是正式 A/B。

关联 lookup 有 553 个 worker CPU／3521 个分配样本：标量需求／map 订阅和绑定 Map 为主要来源；具名参数 HashMap 450、上下文 Record 容器 HashMap 280、Record 198 个样本。多行 JOIN 有 341／1645：HashMap.putVal、属性读取、ensureRecords、比较／discard；别名节点 436、别名桶数组 294、Record 303、容器 HashMap 211 个样本。HashMap.resize 包含首次桶数组建立，不能当成反复扩容。当前默认 newContainer 已是 HashMap(4)，具名参数也已有小容器；两别名加 this 不存在容量过大可直接追回的空间。结果副本和具名绑定保留已有可变隔离／别名冲突契约，剩余原生订阅对应实际逐行执行。本轮不新增关联／JOIN 优化，不重复已撤回的 Map.forEach 复制，不加内联双来源视图、small-map、对象池或额外快路。样本不换算 CPU 百分比、B/行或常驻堆。

代码收敛计划：三参数快路上一阶段撤回后，DefaultReactorQLMetadata 仍残留只用于该快路的五个私有 Object 参数重载（substring／replaceText／splitPart／dateAdd／dateDiff）。精确调用检索确认已无调用；原 List 工厂进入 String／LocalDateTime 实现，不经这些入口。删除这五个失效适配方法，保留实际 List／类型明确的实现、注册、参数／错误／限制与公共 API，不扩展到其他 owner。这是删除失效设计，不申领未测吞吐或堆收益；阶段末集中完整构建与差异检查，结果回填本节。

上述清理完整离线 `mvn -o -q -Pjmh -DtrimStackTrace=false package` **556 tests、0 failures/errors/skipped**，52 份当前报告（排除旧 Benchmarks）；`target/relational-evidence-cleanup-build-20261007.log`，制品 SHA-256 `0bbc4ac6de83d18dcca0537885588f4b6b203352847edabefbd8506f53de0b7c`。只删除 5 个未调用私有适配方法，未新增逻辑；不因代码体积清理申领吞吐／常驻内存收益。`git diff --check` 通过。

### 真实宽函数的原生计算下界校正（2026-10-07）

基准审计发现：WideSqlWorkloadBenchmark.nativeJsonOperatorRow 用 sequence 反推 longitude／level，虽能作为旧固定夹具的值 oracle，**没有解析 JSON，不是等工作量原生性能对照**。此前使用该入口推断距 Java 原生 JSON 性能的结果不成立；SQL 自身正式吞吐／GC 数字不因此失效。原混合运算符／宽字段对照读取真实输入，不机械扩展该问题。当前 16 列函数查询也没有完整同结果的原生入口，已有 nativeWideProjectionControl 仅为简单字段查询，不能代表宽函数下界。

本阶段只改 owning JMH 夹具：nativeJsonOperatorRow 从真实 JSON 文本或已解析 Map 读取两条固定 JSONPath，文本按同 SQL 两次独立求值解析，不跨列共享文档；补完整 16 列函数原生入口，执行同一 WHERE、运算／字符串／两 JSONPath／三个原日期转换及两个日期偏移，使用同容量独立可变结果 Map。setup 比较所有行、所有值／类型、顺序、数量与一次源订阅；增加 sequence 不变而 JSON 更改的反例，防止从生成规律偷算。预解析输入也显式标注，解析成本不挪到测量外后伪称引擎优化。

集中构建与同配置 GC/JMH 后，必要时录 measurement-only 原生 JFR以区分计算／解析下界和引擎装配成本。原生对照仅为当前有界正常业务数据的计算成本，不具 ReactorQL 自定义 Feature、动态参数、资源限制与错误恢复合同，不声称可替代产品链路。该阶段未授权任何生产特化、解析缓存或函数／类型专用执行器；不存在优化就如实报告差距。默认资源／并发限制不变。

原生对照修正的集中 `mvn -o -q -Pjmh -DtrimStackTrace=false package` 通过：**556 tests、0 failures/errors/skipped**，52 份当前报告（排除旧 Benchmarks）；日志 `target/native-wide-reference-build-20261007.log`，JAR SHA-256 `f2c6471e6cb0bb106ba21b5dd5450b69bde4746e7fc999e631a01702afa02561`。四个新／修正的原生入口非 fork smoke 均执行 setup 成功（`target/native-wide-reference-smoke-20261007.log`）；smoke 分数不用作性能证据。生产源码未改，修正的是测量合同。

正式数据 `target/jmh-native-wide-reference-20261007.json`：JDK17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler；单位始终为输入行，宽函数的 WHERE 与输出数量也相同。

| 场景 | ReactorQL 吞吐，M 输入行/s | 原生计算下界，M 输入行/s | ReactorQL／原生分配，B/输入行 |
| --- | --- | --- | --- |
| 16 列宽函数，JSON 文本 | 0.572±0.028 | 2.983±0.051 | 6862.620／2158.865 |
| 16 列宽函数，预解析 JSON | 0.564±0.013 | 3.523±0.246 | 6302.202／1252.838 |
| 16 列 JSON 操作符，JSON 文本 | 1.163±0.048 | 1.822±0.081 | 4805.168／4093.158 |
| 16 列 JSON 操作符，预解析 JSON | 1.428±0.107 | 3.245±0.129 | 3608.015／2080.004 |

解释：文本 JSON 操作符已达该有界正常数据计算下界约 64%，但完整宽函数约 19%；不能把后者的差距只归于 JSON 解析。预解析 JSON 对宽函数节约约 560 B/输入行，吞吐区间重叠；对 JSON 操作符节约约 1197 B/输入行且吞吐提高，但这是**输入表示变化**，没有计入上游解析／传输，不申领引擎生产优化或端到端收益。原生下界不承担完整资源／扩展／错误契约，不能据此删除必要 Publisher 边界。没有新增 live-heap A/B，不声称常驻堆下降。

### 已成功编译正则的重复安全扫描（2026-10-07）

目标与范围：检查 `DefaultReactorQLMetadata.compileRegex` 对已验证且成功编译的同一个不可变模式文本，是否仍逐行运行与 settings 无关的 `NESTED_QUANTIFIER_PATTERN` Matcher。只使用已有单槽 `CompiledRegex` 的已成功编译事实；不增加缓存、状态字段、预编译表、SQL／类型分支或新操作符。新模式、未持有条目的 metadata、非法模式仍执行原扫描；每次的输入／模式长度限制和 flags 转换仍保留，构建后 setting 变化必须立即生效。

先按既有真实字面量／动态／交替 `regexp_like` SQL 录 JFR 与正式 GC/JMH 基线。仅在重复安全扫描明确贡献分配时，将扫描放到模式文本不等于已成功编译条目的分支；新模式仍在 flags 转换前扫描，保持错误优先级。已有条目以一次局部引用快照读取，文本是否已验证不依赖 flags，实际 Pattern 命中仍必须匹配 flags。并发替换不改变局部不可变快照的有效性。

验证：补当前既有回归的有效／危险模式切换、改变 flags、非法模式不污染旧条目、模式／输入上限动态收紧、失败 flag 转换的安全错误优先级及普通 metadata 回退。复用 Context、并发订阅和多函数测试；阶段末集中完整构建，同配置三条正则基准和宽函数／混合运算／多层子查询负对照 A/B。收益不足或任何稳定回退则撤回，不靠合并参数流追回分数。不测 live heap 就不申领常驻内存改善。

前置取证：初次 jcmd attach 握手失败，使用 JVM 官方延迟 JFR 录制；第一轮 CLI jvmArgsAppend 覆盖了注解堆参数，故不使用该轮数值。显式 `-Xms512m -Xmx512m -XX:+UseG1GC` 的录制为 `target/jfr-regex-validation-512m-measurement-20261007.jfr`，日志 `target/jfr-regex-validation-512m-diagnostic-20261007.log`；JVM 启动后延迟 30 秒录制 12 秒，3×1s 预热和 setup 已结束，60 秒 measurement 正常完成。611 个 worker CPU、3537 个分配样本，其中 466 个 int[] 的 ReactorQL owner 为 assertSafeRegexPattern，实际匹配与参数订阅仍有独立来源。计数只定位热点，忽略 sampled weight，不换算精确 B/行或 CPU 占比；诊断分数不用于 A/B。

候选只移动上述安全扫描至新模式分支，以已有不可变编译条目的单次局部引用证明同文本已经通过固定扫描；没有新增字段或跨行驻留。模式长度仍在条目读取前校验，输入长度仍在各函数原入口校验，新模式仍先安全校验再转换 flags。正式基线使用上述 `f2c6471…` JAR，构建／A/B 完成前不申领收益。

阶段集中构建 `mvn -o -q -Pjmh -DtrimStackTrace=false package` 通过，**559 tests、0 failures/errors/skipped**，52 份当前报告（排除旧 Benchmarks）；日志 `target/regex-validation-build-20261007.log`，JAR SHA-256 `7723be3c0eb66a16e8852d4302d8a547edeadb488a2d56e6b468ebb2e0c89c0d`。RegexPatternReuseTest 新增三项回归：没有 metadata owner 的公开回调仍校验危险模式；暖条目之后切换危险模式须在失败 flags 转换之前报告原安全错误，再次使用合法旧文本与不同 flags 仍正确；已有文本的输入长度 setting 收紧／放宽立即生效。原动态模式交替、四函数隔离、inline flags、模式长度 setting、并发／Context 等测试保留通过，未放宽断言。`git diff --check` 通过；正式 A/B 见下。

正式 A/B `target/jmh-regex-validation-before-20261007.json` → `target/jmh-regex-validation-after-20261007.json`；宽函数负对照复用同轮同配置 `target/jmh-native-wide-reference-20261007.json`。均为 JDK17.0.18、512 MB/G1、单线程、3×1s 预热、5×1s 测量、2 forks、GC profiler，两个 before 文件均测 `f2c6471…`，after 测 `7723be3…`。

| 场景 | 吞吐，M 输入行/s（before→after） | 分配，B/输入行（before→after） |
| --- | --- | --- |
| 字面量正则 | 4.215±0.017→4.592±0.102 | 1456.037→1224.037 |
| 重复动态正则 | 4.155±0.016→4.574±0.050 | 1456.037→1224.037 |
| 交替动态正则 | 3.102±0.065→3.145±0.069 | 2120.038→2120.038 |
| 16 列混合运算符 | 2.396±0.015→2.380±0.026 | 1022.237→1022.237 |
| 三层子查询 | 6.554±0.070→6.663±0.330 | 592.146→592.146 |
| 16 列宽函数 | 0.572±0.028→0.585±0.018 | 6862.620±34.423→6841.018±0.0005 |

字面量／重复动态正则吞吐分别 **+8.9%／+10.1%**，区间不重叠，均少 **232 B/输入行（−15.9%）**；因此保留这处低复杂度共同函数改动。交替模式与三个负对照吞吐区间均重叠，未发现稳定回退；不申领它们的吞吐改善。宽函数不使用 regexp_*，其分配 before 两个 fork 分别约 6841.019／6884.220，after 均约 6841.018，分配区间重叠；不是本优化的确定收益。没有修改 SQL／输入分布／原生参数链路，也没有新增缓存条目或常驻状态；没有 live-heap A/B，不申领常驻堆下降。

优化后相同配置／延迟／测量窗口的 JFR 为 `target/jfr-regex-validation-512m-after-measurement-20261007.jfr`，日志 `target/jfr-regex-validation-512m-after-diagnostic-20261007.log`，正常完成。606 个 worker CPU、3515 个分配样本；完整 owner 计数中 assertSafeRegexPattern 分配样本从 **466 降至 0**，实际正则匹配的 Matcher／int[] 仍存在，函数原生参数／flatMap 订阅也是剩余主要来源。只用采样证明目标来源消失，不按 before／after 采样总量申领 B/行；精确分配收益只来自上述 GC/JMH。构建后只做性能取证与文档回填，生产／测试未再修改，最终 JAR 仍为 `7723be3…`；差异检查通过，未提交／推送。完整宽函数的参数订阅热点不能通过先前已撤回的不等价 handle／标量内联来消除；整体接近原生计算及常驻堆目标仍未全部完成。

### 完整宽函数的普通 Publisher 适配边界（2026-10-07）

对当前 `7723be3…` 制品的完整宽函数 SQL 录制 `target/jfr-full-wide-functions-current-measurement-20261007.jfr`（日志 `target/jfr-full-wide-functions-current-diagnostic-20261007.log`）：JDK17.0.18、512 MB/G1、单线程、3×1s 预热，启动延迟 30 秒在 60 秒 measurement 中录 12 秒；正常完成。622 个 worker CPU、3565 个分配样本；55 个 Flux.wrap CPU 叶子样本**全部直接来自 FunctionMapFeature 普通 createMapper 返回的 Flux.from(apply(...))**。参数 ConcatMap、JSON 文本容器、结果 Map 和实际订阅仍是独立分配来源，ScalarValueMapper.apply 的 96 个 MonoJust 样本不能区分常量／属性。只按来源定位，不将这些计数换算 CPU 百分比或 B/行。

未决候选：普通 mapper 的公开返回类型本来是 Publisher，是否可以直接返回 protected apply 的结果、省去 Flux 适配。实施范围只可能是 FunctionMapFeature 普通分支与原兼容测试；不改变 distinct／unique、参数流、计算／恢复位置、公开 mapper 字段和 protected apply 调用链。先用真正的原实现及官方非 Mono/Flux 的 Fuseable.ScalarCallable Publisher 差分求值／订阅／需求时机；同属参数流的普通 Mono、Flux、多值和错误作为反例。若原适配承担实际 assembly-time 求值或恢复职责，不直接删它，也不为某一 Publisher 类型加专门分支、工厂模式或补偿层。

另录已修正的同工作量完整原生函数参考，只作为有界正常数据的计算成本证据，不作为 SQL 资源／错误契约 oracle。阶段末集中执行新增差分与完整验证；只有通过共享边界判别，才实施生产候选并进行同配置 A/B。真实判别失败就保留原实现与回归，转到其他热点；不为了消除 Flux.wrap 样本改变功能。

初轮完整验证仅新增两项夹具失败：把 Reactor 3.4.34 的 Mono.fromCallable 错认为 request 时才计算。实际源码与观测均显示它在 subscribe 中、onSubscribe 之后计算；因此零需求的直接值／空值回调计数已经增加，并非生产故障。修正夹具为对照真正原适配在 mapper.apply 时完成 ScalarCallable.call、但不订阅来源，与直接 protected apply 在此时不计算、随后产生来源订阅的差异。值相同、空结果都可以零需求完成，不能声称这两项信号不同。仍保留所有计数／值／空流断言，不为复制原协议新增生产分支。修正后的集中验证待完成。

最终判别：FunctionMapFeatureCompatibilityTest 新增两项实际原 mapper／protected apply 差分均通过，原适配对官方 ScalarCallable 的普通值／null 都在 mapper.apply 时计算一次、来源订阅为 0；直接结果此时不计算，经过真实 Mono.from 消费会订阅来源并计算。原型**不能无条件替换既有适配器**；未曾修改生产分支，也不加 Mono／Callable 类型豁免、专用工厂、补偿订阅器或例外模式。保留两项回归作为该公开扩展边界的证据，不将合法来源当成“无关特例”。没有候选性能分数可申领。

完整原生对照 JFR `target/jfr-native-full-wide-functions-current-measurement-20261007.jfr`（日志 `target/jfr-native-full-wide-functions-current-diagnostic-20261007.log`）使用相同配置与延迟测量窗，正常完成，有 678 个 worker CPU、3561 个分配样本；主要来源是 HashMap.put/get、JSONParser.readObject、LocalDate.create 及实际字符串／结果 Map。没有 Record 分配栈；两个独立文本 JSONPath 的真实解析／内部求值和三次日期转换仍存在，未从 sequence 偷算或在测量外预解析。原生与 SQL 的采样总数不能相除当作性能比例；已有正式计算下界数据继续有效，诊断吞吐不计入正式分数。

修正夹具后的完整 `mvn -o -q -Pjmh -DtrimStackTrace=false package` **561 tests、0 failures/errors/skipped**，52 份当前报告（排除旧 Benchmarks）；日志 `target/function-result-adapter-audit-build-recheck-20261007.log`，JAR SHA-256 `c061524c051d102ee33451b9b3781964449fef537fe5dded91218c48ee4968c9`。本阶段只新增测试／取证／文档，生产和 JMH 源码未改，未因测试新增重跑已有有效 A/B；`git diff --check` 通过，未提交／推送。上一阶段的通用正则收益保留，本阶段不申领新吞吐、分配或常驻堆收益。

下一取证重点：函数分组键（如 lower／日期函数）在当前功能正确版本不再声明 ScalarValueMapper，因此不能进入 WindowedAggregateStage 的同步键增量计划，仍走 GroupByValueFeature 的原 Publisher 分组路径。先补真实窗口／高基数／函数键夹具，观察必要键／累加器、GroupedFlux／订阅和在途行的实际 CPU／live-heap；同时保留普通属性键和无函数窗口为对照。不得为恢复快路重新内联函数、改变默认并发／上限、加入跨行键缓存或在尚无证据时设计异步聚合执行框架。广义优化目标仍未完成。

### 函数分组键的高基数取证与私有收集器收敛（2026-10-07）

初轮取证目标与 owning module：现有 HighCardinalityNativeBenchmark／HighCardinalityLiveHeapProbe 与本原始文档，先只补测量；JFR定位后的小范围生产候选与验证记录见后文。真实 SQL 使用 50,000 行计数窗口、lower(key) 分组、COUNT 与 COUNT/SUM/AVG/MAX 两类，1／2 个事件每键；来源同时提供原始混合大小写 key 和等价 normalizedKey，属性键控制只用于区分函数计算／Publisher 分组的成本，预计算来源不申领引擎收益。SELECT 统一读取 normalizedKey，避免在函数键结果中多算一遍 lower。原生参考从实际 key 再执行 lower，而非使用序号／预计算字段推断；setup 严格比较字段、值、类型、组数、一次来源订阅及运行计划，使用有限超时识别异常等待。

先集中构建 JMH，执行 setup，再以 JDK17.0.18／512 MB/G1 的 measurement-only JFR 定位 COUNT／多聚合的来源；正常完成后录同配置 GC/JMH。若 GroupedFlux／聚合订阅等是主要驻留来源，扩展既有 live-heap 探针观测开放窗口的 10k／50k 键和重复事件／未使用 payload，取消后再核对释放。保持原默认无界分组并发／资源上限；不降低并发伪称降堆，也不以内存驻留测量替代瞬时 B/输入行。

候选选择门槛：只允许既有边界内、可由串行信号或已验证不可变事实证明的通用小改。函数内联、按函数名恢复同步标记、跨行键／日期缓存、新异步聚合框架或 Subscriber 均不在本阶段范围；发现低复杂度候选后先补共享路径功能／需求／取消／错误／Context 差分，阶段末集中验证及 A/B，再决定保留。没有满足门槛的候选就报告真实成本与后续限制，不能放宽功能追分。

取证已确认：COUNT 与四聚合在 50k 键下均主要耗时于 Reactor FluxFlatMap.drainLoop 的 innerComplete 路径，源码每次 drain 扫描活跃 inner 数组；不能通过更换函数同步标记、降低默认并发或强制顺序输出回避。多聚合另有明确的不必要分配来源：DefaultReactorQL 私有结果 collect 使用 ConcurrentHashMap.compute，产生 ReservationNode、桶数组及逐值捕获回调。merge 已串行化 collect 的 onNext，容器仅在该订阅内累加、完成后进入 setResults；不需要并发 Map，也不需额外锁或状态机。

本阶段初轮候选把这一私有累加容器改为普通 HashMap，并用 get／put 直接维持原单值、首值 List 原地追加、PendingAggregateValues 完成后还原 COW List 的规则。**HashMap 部分已因实际覆盖顺序差分失败撤回**（本节末尾）；修订候选只删除 compute 回调，仍用原 ConcurrentHashMap，不模拟其遍历或补别名特例。不改 publish/refCount、原生算子、空源 Record、公开容器、聚合完成时机、并发／资源默认值。补充独立异步聚合、Context、重复订阅隔离、零需求、空源及展开覆盖顺序回归，连同既有多值／错误／取消用例集中验证。正式 A/B 只覆盖受影响的函数键多聚合及不经过该 collector 的属性键控制，1／2 个事件每键，配置沿用 2 forks／3×1s warmup／5×1s measurement／512 MB G1；主要 CPU 扫描不能由该候选解决，不预先申领吞吐收益。

重开依据：前文“非融合多聚合的订阅私有结果容器试验”是在大量输入共享单一 collector 的全局多值场景，仅替换 Map 的收益约 0.09 B/输入行，因此当时正确撤回。本次是新的高基数证据：每 1／2 条输入建立一个 collector，JFR 明确观察到该 owner 的 ReservationNode 和桶数组；同时删除 compute 的逐值回调，而非再次盲目替换 Map。旧场景未被证明有收益，后续也不申领其收益；新场景若仍只有可忽略收益或稳定回退，同样撤回，不新增任何按查询形态触发的实现。

既有 HighCardinalityLiveHeapProbe 增加 --computed-key=function|property，同一混合大小写输入、相同 SELECT、相同开放窗口需求与未使用 payload 的弱引用观测，仅分组表达式不同。10k／50k 键和取消后释放用于辨别必要的代表行／订阅／键状态；不把预计算属性控制或减少并发算作生产收益。上述新探针待阶段末集中构建与实际运行。

测量夹具的完整构建 `target/function-key-evidence-build-20261007.log` 正常完成，基线 JAR SHA-256 `2163fb9179b86b8cfbc93fb83c6ce70e255597f77eb41cb0db6d51e53d87e26b`；10 个非 fork smoke 入口／参数组合全部执行严格 setup 通过（`target/function-key-evidence-smoke-20261007.log`），smoke 分数不是性能证据。COUNT／四聚合 measurement-only JFR 分别为 `target/jfr-function-key-count-before-measurement-20261007.jfr`／`target/jfr-function-key-aggregates-before-measurement-20261007.jfr`，正常结束。JDK17.0.18、512 MB/G1、3×1s warmup，90s measurement 内启动延迟60秒录12秒，确认不覆盖 setup／预热。worker CPU／分配样本为927／536和918／515；其中911和901个CPU叶子样本来自 FluxFlatMap.drainLoop←innerComplete。多聚合 collector 的 ConcurrentHashMap.compute 预约节点／桶数组是独立明确分配源；样本只作来源证据，不转换为CPU占比、精确字节或常驻内存。

仅上述私有 collector 收敛后，完整 `mvn -o -q -Pjmh -DtrimStackTrace=false package` **563 tests、0 failures/errors/skipped**，52份当前报告（排除旧 Benchmarks）；5项 LegacyMultiValueAggregateTest 均通过，含新增独立异步结果／Context／重复订阅隔离及空源／需求回归。日志 `target/function-key-collector-build-20261007.log`，候选 JAR SHA-256 `75dbd55f6c5815a51caee00ca29513f555657a38c39bd6820539a6daa9ed61ab`；没有跳过 DebugAgent 或测试。两处 source failed／time window source failed 的 onErrorDropped 日志在本阶段基线日志中也存在，来自原错误路径测试，未隐藏日志或改弱断言。阶段差异检查通过。

正式同配置 GC/JMH 为 `target/jmh-function-key-collector-before-20261007.json`→`target/jmh-function-key-collector-after-20261007.json`：单线程、2 forks、3×1s warmup、5×1s measurement、512 MB/G1，单位始终为输入行；SQL／输入／结果及订阅合同不变。

| 场景 | 吞吐，输入行/s（before→after） | 分配，B/输入行（before→after） |
| --- | --- | --- |
| 函数键四聚合，1条/键 | 6445.8±228.8→6608.0±113.6 | 8058.356±50.996→7826.356±12.749 |
| 函数键四聚合，2条/键 | 26840.4±497.6→27601.3±238.6 | 4301.450±6.375→4197.450±6.375 |
| 属性键四聚合控制，1条/键 | 3.487±0.134→3.349±0.154 M | 829.012→829.012 |
| 属性键四聚合控制，2条/键 | 4.988±0.276→4.838±0.313 M | 414.525→414.525 |

初轮 HashMap 候选的函数键分配平均每输入行少**232／104 B（−2.88%／−2.42%）**，各自区间不重叠；before 的1条/键两fork为8090.356／8026.356，after为7834.356／7818.356，保留JIT分配差异，不把某一fork的差额当作固定保证。1条/键吞吐区间重叠；2条/键本轮平均+2.84%、区间刚分离，但两个未走collector的控制均有约3%～4%下行且区间重叠，不将该小幅变化扩展为稳定CPU收益或主热点已解决。**这些数据属于已经撤回的候选，不是最终产品收益**；全局多值旧场景没有新的同版本A/B，不申领其吞吐收益。

基线开放窗口探针（不与JMH并行）`target/liveheap-function-key-before-20261007.log`：10k／50k键、2条/键，COUNT函数键总heap约32.17／145.94 MB，五聚合函数键约66.86／319.36 MB，五聚合属性键约8.07／24.40 MB。函数键弱引用payload仅每键最后一条仍存活（10k／50k），早先的10k／50k历史payload已释放；属性键全部释放。六次取消后payload全部为0、输出保持0。所测函数键SELECT仍需要每组最后一行的deviceId，代表行保留不能当成完整历史积压，也不能为降堆改变该功能。heap包含JVM、探针自身弱引用元数据和必要状态；这些对照说明原Publisher路径成本明显，但预计算属性键并非同一SQL的引擎优化，不从其差值申领产品收益。

初轮优化后的 JFR `target/jfr-function-key-aggregates-after-measurement-20261007.jfr` 正常结束，有923／685个worker CPU／分配样本；private collector 的 ReservationNode 完整owner计数28→0、并发桶数组35→0，但910个CPU叶子样本仍为原drainLoop←innerComplete。初轮live-heap `target/liveheap-function-key-after-20261007.log`（相同探针class与JVM配置）取消后的payload均为0，五聚合10k／50k键总heap约66.70／318.63 MB，只有约0.16／0.73 MB小幅差额，COUNT／属性控制本身也有快照差异，不能申领稳定常驻堆改善。20／100条事件每键的重复输入控制同样只保留每键一个payload，不保留全部历史输入；探针自身保留WeakReference元数据，因此不能直接用总heap增量推断聚合状态增长。上述after JFR／堆数据仍属于已撤回HashMap候选，不覆盖修订候选。

最终功能判别发现初轮门槛遗漏：私有Map虽然没有并发写入，但它的遍历顺序会经setResults参与`$this` Map展开和普通聚合别名的同名覆盖。真实查询`select emit_map(this) "$this",count(1) a0,…,count(1) a10 from test`在12个collector键时，ConcurrentHashMap和HashMap扩容阈值不同，a0原值0变成1；不是仅键顺序不同。正常负载的563 tests不能排除该差异，因此HashMap候选撤回，不调初始容量、不按别名／列数补特例，不复刻Map遍历算法。修订为保留原ConcurrentHashMap与遍历／展开行为，只删除compute逐值回调；新增真实查询回归，以原compute累加和遍历投影作为oracle，随后集中完整构建及同配置正式A/B。修订结果待验证，所有前述初轮after数字不申领为最终收益。

#### Bug Analysis: 私有聚合容器的展开覆盖顺序（trellis-break-loop）

##### 1. Root Cause Category

E 隐含假设／D 测试缺口／B 跨层契约：把“串行、私有、同键值”误认为“遍历顺序无可观察影响”，漏掉setResults的Map展开与同名覆盖。

##### 2. Why Fixes Failed

仅普通COUNT/SUM等和小列数fixture通过，不能证明展开覆盖不变；容器改名／换类型不是必要的compute分配优化。

##### 3. Prevention Mechanisms

P0已加入真实宽聚合／`$this`展开的原compute oracle回归；保留原容器，不用容量／别名特调补偿。代码旁说明遍历顺序的兼容边界。

##### 4. Systematic Expansion

类似容器替换须追到消费者的展开／覆盖／可变发布，而非只审查线程安全；本次不借机重排SELECT优先级或重构其他Map所有者。

##### 5. Knowledge Capture

回填本原始文档与回归即可；owning ReactorQL module没有Trellis spec/template，不安装新工作流、不修改无关模块规范、不自动提交。修订版功能及性能证据见以下记录。

修订版（保留原容器，仅删除compute回调）集中完整 `mvn -o -q -Pjmh -DtrimStackTrace=false package` **564 tests、0 failures/errors/skipped**，52份当前报告（排除旧Benchmarks），6项LegacyMultiValueAggregateTest均通过。日志 `target/function-key-collector-native-map-build-20261007.log`，最终JAR SHA-256 `86146064d71bc320ce5cbd167017ce47344673d9da7cce1946f14e81a3eae74e`。实际判别查询against该制品的a0=0、actualMatchesOriginal=true，未模拟或特判别名／扩容阈值。HashMap失败候选另保存为 `target/function-key-collector-hashmap-rejected-20261007-benchmarks.jar`，不用于交付或最终收益。

修订版正式同配置GC/JMH：仍使用原 `target/jmh-function-key-collector-before-20261007.json` 基线，after为 `target/jmh-function-key-collector-native-map-after-20261007.json`；JMH源码／输入／SQL／运行配置不变，不复用失败候选after。

| 场景 | 吞吐，输入行/s（before→最终after） | 分配，B/输入行（before→最终after） |
| --- | --- | --- |
| 函数键四聚合，1条/键 | 6445.8±228.8→6564.5±169.4 | 8058.356±50.996→7834.356 |
| 函数键四聚合，2条/键 | 26840.4±497.6→27046.4±525.4 | 4301.450±6.375→4205.450±6.375 |
| 属性键四聚合控制，1条/键 | 3.487±0.134→3.418±0.156 M | 829.012→829.012 |
| 属性键四聚合控制，2条/键 | 4.988±0.276→4.925±0.362 M | 414.525→414.525 |

最终平均少分配**224／96 B/输入行（−2.78%／−2.23%）**，各自分配区间不重叠；after1条/键两fork均7834.356，2条/键为4209.450／4201.450。四条吞吐区间均重叠，不申领稳定吞吐收益，也不把主CPU热点视为已解决。候选只删除不必要的逐值compute回调、使用现有串行collector边界直接写入，复杂度低且原展开覆盖回归通过，因此保留；无最终成对live-heap A/B，不申领常驻堆改善。生产／测试／JMH源在上述构建后未再变更。

最终保留原容器版的measurement-only JFR `target/jfr-function-key-collector-native-map-after-measurement-20261007.jfr`（日志 `target/jfr-function-key-collector-native-map-after-diagnostic-20261007.log`）正常完成，配置／延迟测量窗与before一致。920个worker CPU／663个分配样本；collector ReservationNode完整owner计数**28→0**，原ConcurrentHashMap桶数组仍有11个样本，正确保留容器而不是申领桶数组消失。904个CPU叶子样本仍落在drainLoop，其中903个为innerComplete调用链；主扫描成本没有消除。只用样本判断来源，精确B/输入行只来自正式GC/JMH。

最终制品的开放50k键／五聚合取消核对正常结束（`target/liveheap-function-key-collector-native-map-final-20261007.log`）：接收100k条，输出0，代表行payload存活50k→取消后0，heap约319.45→8.04 MB。与原路径约319.36 MB相符，进一步说明未解决主要常驻分组／订阅成本；该单次核对不作为新的成对resident-heap改善证据。最终 `git diff --check` 通过，JAR仍为上述`86146064…`，源代码／测试／JMH未再改动，所有构建／JMH／JFR／探针操作均已结束，未提交或推送。

后续只继续高收益取证，不追加collector微调：在已有HighCardinalityNativeBenchmark补“SQL子查询在运行时计算分组键→外层窗口聚合”的真实形态，与直接函数GROUP BY、现有属性控制比较；要求同字段／值／类型／组数、一次来源订阅与计划检查，计算仍包含在测量内。先确认既有计划是否支持，再JFR定位订阅／合并／必要状态成本。此为查询形态对照，不自动改写用户SQL、不把不同SQL的差距算作引擎优化，也不新增函数同步标记、跨行键缓存、异步聚合框架或默认并发调整。是否有可保真的通用小改仍以证据决定。

### SQL内前置分组键计算的子查询取证（计划，2026-10-07）

作用域只在原HighCardinalityNativeBenchmark／HighCardinalityLiveHeapProbe及本说明，当前不改生产实现。现有SubSelectFromFeature对普通子查询实时执行start(ctx)并将结果转为派生记录，没有在此路径collect全源；DefaultReactorQL的raw源入口只允许内置Table，但WindowedAggregateStage仍支持派生Record上的属性键增量计划。当前`86146064…`制品的实际计划探针已确认：`select normalizedKey,count(1)… from (select lower(key) normalizedKey,score from test) n group by _window(50000),normalizedKey`进入fused-count-window-aggregate、retainSourceRecord=false、rawSource=false；给入错误的预计算normalizedKey仍输出实际key计算结果，不依赖预计算字段。

只在现有JMH新增COUNT／四聚合的这种SQL形态，同一混合大小写输入、同一50k计数窗口、1／2事件每键，完整lower计算处于测量内；原native参考继续读取actual key。setup逐组比较字段／值／类型、组数、键唯一性、一次来源订阅与计划，另以raw key改变且预计算值错误的单行输入防止偷算。既有live探针增加computed-key=subquery，用同源schema／SELECT／开放窗口／需求观察payload释放与取消；保留函数／属性对照，不添加生产专用路径。集中构建／setup后录measurement-only JFR和GC/JMH；查询形态之间的差异只能解释成本，不能当作代码优化收益或普遍自动改写等价证明。原默认限制／并发／函数错误边界与资源合同保持不变。

取证夹具集中完整构建已通过（`target/subquery-function-key-evidence-build-20261007.log`）：564 tests、0 failures/errors/skipped，52份当前报告；制品SHA-256 `a5ae25820a87504daa3cd3e5f4d6e9bd621f8a6367d47219f4d03441eacc6dc9`。与上一已验证制品比较222个生产源码owner及内部class，字节差异为0；本阶段只改测量代码。COUNT／四聚合×1／2事件每键的四个新smoke组合严格setup全部通过，smoke不申领性能收益。

四聚合measurement-only JFR `target/jfr-subquery-function-key-aggregates-measurement-20261007.jfr` 正常结束；同JDK17.0.18、512MB/G1、单线程、3×1s warmup、90s measurement，启动延迟60秒录12秒，确认不覆盖setup或预热。746个worker CPU、3525个分配样本；CPU叶子包括HashMap.putVal／resize（128／107）、实际lower（45）和FluxFlatMap.drainLoop（87）。主要Map分配owner为派生别名容器（337）、派生结果快照节点／数组（177／142）、输出结果节点（247）、输入group-key metadata节点／数组（125／172）。仅定位来源，不将样本比例、weight或诊断吞吐当成精确B/行、CPU比例或常驻堆收益。

候选筛选保持低复杂度：派生Map快照负责源／结果修改隔离，来源别名容器和group-key metadata属于可观察Record契约，不直接共享或删除；也不增加专门group-key字段、Record视图层、raw-subquery SPI或自动SQL改写。先完成同制品正式GC/JMH与开放窗口取消取证，再判断是否存在值得保留的通用小改。当前没有新增生产优化，也不把不同SQL形态的差值计入引擎收益。

正式同制品GC/JMH已正常完成（`target/jmh-subquery-function-key-evidence-20261007.json`及同名log）：JDK17.0.18、512MB/G1、单线程、2 forks、3×1s warmup、5×1s measurement，所有setup严格断言通过。吞吐单位为**百万输入行/s**，分配单位为**B/输入行**；表中的原生参考计算actual key并实时lower，不在setup偷算。四聚合为COUNT／SUM／AVG／MAX。

| 查询形态 | 1事件/键吞吐 | 1事件/键分配 | 2事件/键吞吐 | 2事件/键分配 |
| --- | --- | --- | --- | --- |
| SQL内计算键→外层属性键COUNT | 2.059±0.064 | 1656.222 | 2.388±0.047 | 1356.936 |
| SQL内计算键→外层属性键四聚合 | 1.659±0.030 | 1936.223 | 1.869±0.063 | 1496.936 |
| 已规范化属性键COUNT控制 | 4.609±0.622 | 549.011 | 6.968±1.181 | 274.525 |
| 已规范化属性键四聚合控制 | 3.501±0.120 | 829.012 | 4.782±0.400 | 414.525 |
| 原生actual-key四聚合计算参考 | 8.151±0.540 | 440.145 | 15.949±1.858 | 232.879 |

这不是生产before／after：新增查询利用已有Record增量窗口阶段，包含真实lower及派生Record的成本；属性控制省去了lower，原生参考没有完整SQL／Feature／错误资源合同。此前直接函数GROUP BY的正式分数只保留为前一制品的成本背景，不与本表混成同JMH-class A/B，也不从不同形态推导引擎提升倍数。高基数Publisher组完成时的合并扫描仍是最大待解决CPU热点，不能用降低默认并发或强制顺序输出代替兼容优化。

同一当前制品的开放流探针 `target/liveheap-subquery-function-key-evidence-20261007.log` 正常完成12个组合：三种键形态×COUNT／五聚合×10k／50k键，每键2事件，lazy输入，结果订阅不收集、源保持开放。五聚合额外包含MIN。输入接收精确为20k／100k，输出均0，未关闭窗口；每个组合的取消后unused payload均为0。

| 键形态／聚合 | 10k键开放heap，MB | 50k键开放heap，MB | 开放时输入payload存活 | 50k键取消后heap，MB |
| --- | --- | --- | --- | --- |
| SQL内计算键→外层属性COUNT | 7.592 | 22.667 | 0 | 7.905 |
| SQL内计算键→外层属性五聚合 | 9.048 | 29.958 | 0 | 7.871 |
| 已规范化属性COUNT控制 | 6.538 | 17.134 | 0 | 7.817 |
| 已规范化属性五聚合控制 | 7.999 | 24.384 | 0 | 7.837 |
| 直接函数GROUP BY COUNT | 32.424 | 145.815 | 每键最后1条 | 7.864 |
| 直接函数GROUP BY五聚合 | 66.783 | 319.504 | 每键最后1条 | 7.900 |

heap包含JVM、探针WeakReference元数据和必要键状态，不是精确owner-size，也不是严格resident-heap优化A/B。直接函数形态的SELECT还要从每组代表行读取deviceId，保留最后一条输入符合现有结果契约；更早的历史payload已释放，不是AVG／MAX在收集全组原始行。派生形态外层直接使用已计算分组键，现有compact阶段无需保留输入Record，不能因此自动删掉原SQL的代表行。精确无界分组仍需O(活跃键数)的键／累加状态；AVG保存sum/count、MIN/MAX保存当前值，不保存历史输入集合。

本阶段结论：已完成高收益成本取证，但**未新增生产实现**。实际可用的低复杂度建议是在业务语义允许、错误／时间／排序边界经调用方验证时，显式将键计算放到SQL前层、外层只作窗口键聚合；这使用现有通用能力，不自动重写SQL、不引入缓存／同步函数特判／异步聚合框架。剩余大块成本是普通Publisher函数求值、派生快照／别名和公共group-key metadata；本轮不为了消除样本引入新的Record表示或改变它们的契约。最终制品仍为`a5ae258…`，生产class与上一验证制品一致，564 tests通过；阶段差异检查通过，未提交／推送。

### 窗口输出分组键的重复防御复制（计划，2026-10-07）

既有子查询measurement-only JFR在CastUtils.castArray下采到80个Object[]分配样本；源码WindowedAggregateStage.GroupState.writeOutputGroupKeys对Collection先经castArray创建独立可变ArrayList，再复制成第二个ArrayList。只删除第二次不必要复制：Collection沿用castArray的独立可变副本，数组／单值仍保留外层ArrayList以隔离数组并保持可变性。不更改castArray公开返回语义、不引入新的类型标记／框架／缓存，也不省掉group-key metadata或源／结果别名快照。

影响范围只在该输出复制方法及窗口聚合回归；新增真实SQL下Collection／数组／单值元数据的输出可变性与双向隔离覆盖。复用当前`a5ae258…`及上一阶段相同JMH源码的正式before，集中完整构建后同配置after覆盖派生键COUNT／四聚合、原始属性控制与原生计算参考，再复录同measurement-only JFR。只有功能保真、目标分配可重复下降且吞吐无稳定回退才保留；不预先申领常驻堆改善，不改变任何默认限制、操作符或信号时序。

修订验收：初轮构建仅因缺Collection导入未通过；补齐后完整`mvn -o -q -Pjmh -DtrimStackTrace=false package`通过，565 tests、0 failures/errors/skipped，52份当前报告。日志`target/subquery-group-key-copy-build-fixed-20261007.log`，制品SHA-256 `44dca65b30642ffc7d15569c2df911715ea616ea5518a70fb54b096384a746fd`；新增三种元数据形态在真实SQL下验证零需求后request(1)、值／类型、输出可变及双向隔离。生产class差异均限于WindowedAggregateStage这个source owner及内部class，53个JMH owner/inner class与before逐字节相同，测量代码／SQL／输入无变更。

正式before为`target/jmh-subquery-function-key-evidence-20261007.json`，after为`target/jmh-subquery-group-key-copy-after-20261007.json`；保留before制品`target/subquery-group-key-copy-before-20261007-benchmarks.jar`。配置仍为JDK17.0.18、512MB/G1、单线程、2 forks、3×1s warmup、5×1s measurement，单位为输入行。after覆盖两个派生SQL及属性／原生四聚合控制；不存在基准预计算或放宽setup断言。

| 场景 | 1事件/键B/输入行，before→after | 2事件/键B/输入行，before→after |
| --- | --- | --- |
| 派生键COUNT | 1656.222→1632.222（−1.45%） | 1356.936→1344.935（−0.88%） |
| 派生键四聚合 | 1936.223→1912.223（−1.24%） | 1496.936→1484.936（−0.80%） |
| 属性键四聚合控制 | 829.012→829.012 | 414.525→414.525 |
| 原生actual-key四聚合控制 | 440.145→440.145 | 232.879→232.879 |

四个正例各少24／12 B/输入行，即此负载下每输出组少24 B；两个控制的分配不变。COUNT的after吞吐分别2.057±0.034／2.368±0.057 M输入行/s，四聚合1.668±0.057／1.891±0.050 M；与before及所有控制的吞吐区间均重叠，**不申领稳定CPU提升**。这是小幅通用分配优化，不是主热点或常驻堆问题已解决。

after measurement-only JFR `target/jfr-window-output-key-copy-after-measurement-20261007.jfr`（同名diagnostic log）正常结束；同SQL／1事件每键／JVM配置／延迟60秒录12秒，确认在3×1s预热后的90秒measurement内。750个worker CPU、3485个分配样本；输出复制方法直接owner的ArrayList样本75→0，必要castArray的独立副本仍有Object[]样本80→96以及ArrayList样本2，不能申领所有分组键复制消失或用采样变化计算字节。Map／Record与普通函数订阅仍为主要来源。精确分配下降只来自上表GC/JMH，未测量新的成对resident-heap A/B，不声称常驻堆下降。

保留这处极小的通用去重复制，但停止该方向微调；所有默认值、操作符、错误／Context／需求／取消和代表行所有权不变。阶段diff检查通过，制品未再变更，未提交／推送。下一步转向尚未覆盖的真实输入形态／SQL热点取证，而不是增加同步能力标记、专用Record字段或新的执行框架。

### JavaBean输入的嵌套宽投影取证（计划，2026-10-07）

现有NestedPropertyBenchmark仅覆盖Map输入，src/jmh尚无JavaBean／POJO属性宽投影；默认Feature的非Map属性最终由PropertyUtils.getProperty读取。先补这一真实输入边界的测量，不立即更改属性实现。复用原14列、65,536条输入和嵌套属性过滤，增加map／bean输入参数，统一deviceId→device_id输出别名；JavaBean使用正常公开getter，包含原始int／boolean属性与嵌套payload/meta，反射取值及原生getter／必要装箱仍在测量内，不预转Map、不准备访问器或输入类型专用路径。

setup严格比较完整字段／值／类型／顺序、过滤后条数及SQL／native各一次来源订阅；同源primitive getter参考保留真实计算和结果容器成本，不当作完整SQL错误／资源契约。集中构建与setup后，先录bean SQL测量期JFR，再正式同配置SQL／native、map／bean GC/JMH；新字段命名和输入容器使旧NestedPropertyBenchmark数据不再是该JMH-class的严格before。生产实现保持上一`44dca65b…`不变，只有JFR指出可通用、低复杂度且保真的高收益损耗，才另行实施；不引入反射／MethodHandle缓存、新框架、编译期源类型标记或吞掉异常／日志来跑分。

取证完成：`target/jfr-nested-bean-before-measurement-20261007.jfr`仅覆盖预热后的测量区间，823个worker CPU／3486个分配样本；主要byte[]／String分配栈经过DefaultPropertyFeature.doGetProperty，CPU叶包含DefaultResolver的isMapped／isIndexed／next和反射访问校验。样本计数只定位来源，不代表精确分配字节或常驻堆归属。同JDK17.0.18、512MB/G1、1线程、2forks、3×1s预热、5×1s测量、GC的正式`target/jmh-nested-bean-evidence-20261007.json`：Map SQL／native为7.193±0.144／27.158±0.615 M输入行/s，191.974／159.968 B/输入行；bean为1.620±0.021／51.471±1.940 M，1487.774／171.957 B/输入行。仅为输入形态与正常计算参考，不能申领引擎收益。

已用真实编译SQL及公开BeanUtilsBean.setInstance复现：查询构建后安装的PropertyUtilsBean.getProperty覆写仍影响简单／完整嵌套路径；getSimpleProperty和人工分段读取产生不同结果。回归位于DefaultPropertyFeatureTest.shouldPreserveConfiguredBeanUtilsLookupAfterQueryCompilation，finally恢复原委托。不安装直接getter、分段替换、默认类保护或访问器缓存。当前依赖已是commons-beanutils1.11.0，旧版本缓存源码不是升级依据。

### 原始行读取的Record命名空间边界修正（计划，2026-10-07）

实际公开API差分已复现：Context.newContainer预置virtual=7时，全局count(virtual)原始行路径输出0、原Record路径为1；窗口按type分组时count(_group_by_key)原始行路径输出0、原Record路径为1。根因是原始行字段读取跳过了Record的结果容器回退和分组生成元数据，并非聚合算法错误。先修兼容边界，再考虑任意普通行类型的原始行能力扩展。

只在现有原始行入口收紧执行资格：第三方／派生Context可能覆盖结果容器语义，沿用完整Record路径；内置默认Context保持原先原始行路径。内置属性映射器对公共分组元数据名称不声明source-only资格，保留源字段优先与Record回退。新增真实SQL覆盖全局／键聚合／WHERE的Context预置字段，以及分组元数据聚合与同名源字段优先。无新SPI／标记／缓存／状态机，不改变默认限制；阶段末完整测试、原反例复核及默认Map／bean宽投影负对照，不用保守回退证明新的吞吐收益。

阶段验证完成：完整`mvn -o -q -Pjmh -DtrimStackTrace=false package`通过，568 tests、0失败／错误／跳过，52份报告（排除陈旧Benchmarks报告），日志`target/raw-property-namespace-build-fixed-20261007.log`。原公开Context及分组元数据反例均与legacy一致。制品SHA256为`83ff7f14c3935e50c710778d14105f91d8d54fa18c941b896e6dce104b819268`，保存在`target/raw-property-namespace-validated-20261007-benchmarks.jar`。同配置宽投影负对照`target/jmh-raw-property-namespace-negative-20261007.json`：Map／bean SQL为7.082±0.615／1.590±0.027 M输入行/s，分配仍191.974／1487.774 B/输入行；native分配亦不变，所有吞吐误差区间重叠。默认属性键四聚合`target/jmh-raw-property-namespace-group-control-20261007.json`的1／2行每键为3.504±0.178／4.728±0.371 M，829.012／414.525 B/输入行；相同JMH-class的group-key-copy-after控制为3.444±0.107／4.841±0.363 M且分配相同。未观察稳定回退，也不把兼容性修正算作性能提升。

### 普通对象复合键分组的有界热点取证（计划，2026-10-07）

普通对象能力扩展候选已撤回；以下实施与测量为历史取证，不代表当前生产快路径。

复用CompositeKeyAggregateBenchmark的50,000行、唯一／重复复合键及count／sum／avg／max，增加map／bean输入参数；正常公开getter在SQL测量内执行，不预转换Map。setup核对来源单次订阅、完整字段／值／类型及每键聚合，不仅总条数。先集中构建，再录bean测量期JFR与正式map／bean GC/JMH，生产代码不变。只有临时Record／分组元数据确为高收益可删除所有者且source alias、生成元数据、空值／虚拟Map属性、公开BeanUtils委托与错误边界等价，才复用现有原始行能力；不增加来源类判断、getter缓存、补偿容器、特殊字段清单或新的执行框架。若需复杂补偿才能正确则关闭候选，保持现有Record路径。

before取证完成，制品`e16333f273abcc0115d7422638efd632165a2ff81b1d960f504dca8a35fe7a88`（`target/bean-composite-before-20261007-benchmarks.jar`），222个生产类与命名空间修正制品完全一致，568 tests通过。测量期`target/jfr-bean-composite-before-measurement-20261007.jfr`有804 CPU／3596个分配样本；完整栈明确区分输入GroupFeature.writeGroupKey的LinkedList224／节点231、HashMap86／节点315／桶49与必要输出Record的Map62／桶209。CPU叶除BeanUtils解析外包含HashMap.putVal86／resize41、DefaultReactorQLRecord.ensureRecords29。只定位来源，不把样本数当作字节或百分比。正式`target/jmh-bean-composite-before-20261007.json`采用同JDK17.0.18、512MB/G1、2forks、3×1s预热、5×1s测量、GC：Map唯一／重复为3.545±0.136／20.619±0.223 M输入行/s，965.020／5.082 B/输入行；bean为1.980±0.027／3.782±0.078 M，1348.857／373.083 B/输入行。不是引擎收益，作为相同benchmark源码的正式before。

实施边界：内置简单属性只在其现有读取函数对空结果Map没有合成值时声明acceptsAnyRow；这是默认Record结果回退为空的等价证明，虚拟属性直接复用原属性实现判断，不新增字段名单。普通对象仍调用相同准备读取函数及公开BeanUtils.getProperty；已有Map读取／第三方Feature／Context边界不变。键聚合仅传播现有所有维度、累加器及WHERE的acceptsAnyRow能力，已有ReactorQLRecord保持Record路径。复用全局聚合已存在的能力传播，不新增WHERE投影快路、API、缓存、类型保护或状态机。集中验证值／类型、缺失与虚拟属性、元数据、BeanUtils后注册覆写、求值次数、错误恢复、Context、背压和取消，再正式同类A/B及after JFR；不预先申领常驻堆收益。

扩展边界审计额外发现：公开Hooks.onOperatorError下，比较失败时native reduce报告参与比较的输入值，原融合Record路径报告Record，新原始行路径报告普通对象。根因是终止错误在外层组状态而非真正reduce语义边界处理。只在MapAggFeature比较累加器现有带Context的add／addRaw边界用同一addValue捕获比较错误，交给Operators.onOperatorError(value)，仍然终止，不进入onErrorContinue；不按异常／输入类型分支，不创建补偿Record。ScalarAggregateErrorBoundaryTest用真实可抛错Comparable、min／max、全局／窗口、bean／Map／上游Record、legacy／融合验证Hook输入值与终止范围。初步性能对照对应修复前中间制品，不作为最终交付数据；完整测试与正式after须覆盖这个根本修正。

尚未关闭的既有风险：真实`max(score)`＋`group by _window(2),type`＋onErrorContinue下，原兼容flatMap的inner-error scope会以value=null恢复并丢弃失败组；当前融合窗口原先即直接终止（before制品可复现），不是本次普通对象能力传播引入。全局reduce仍直接终止，两条路径一致。新增compatibilityWindowComparisonFailureUsesEnclosingInnerErrorScope精确保留原兼容路径契约；Hook回归分别覆盖全局带continue及窗口不带continue，不用错误的“所有reduce都全链路不可恢复”前提改写原生语义。原始行／Record融合间的正常值和当前错误信号应保持一致，但总体功能等价及整个优化目标不能据此宣告完成。按systematic-solving收敛到实际聚合→内层merge边界取证，禁止通过异常类型名单、默认并发修改、私有Context键或新的恢复状态机掩盖差异；后续优先处理这一风险，未正式验收前不提交／发布候选。

本阶段证据状态：普通对象原始行能力扩展候选已撤回，不作为已完成收益。以下为撤回前历史测量：原始行传播中间制品`e3a6a6e9…`的同类A/B为bean唯一键1.980±0.027→2.316±0.025 M输入行/s（+17.0%）、1348.857→980.863 B/输入行（−27.3%）；重复键3.782±0.078→4.988±0.053 M（+31.9%）、373.083→5.090 B/输入行（−98.6%），约368 B/输入行的逐行容器被删除。Map两种形态吞吐误差区间重叠，分配变化仅约0.007 B/输入行的订阅级能力检查开销。after JSON为`target/jmh-ordinary-source-capability-after-20261007.json`；59个JMH-owner类与before完全相同。`target/jfr-ordinary-source-capability-after-measurement-20261007.jfr`有3562个分配样本，原输入GroupFeature.writeGroupKey所有者均不再出现，必要输出Record Map／桶仍在；此JFR亦是比较Hook修正前中间制品，不据此申领最终吞吐结果。

比较Hook根本修正后完整构建`target/ordinary-source-comparison-scope-build-final-20261007.log`通过，574 tests、0失败／错误／跳过，52份报告。当前候选制品SHA256为`881beab1bd8103ccb29032b23fed854efd9087daa5812481b7c4f6d339dc6c4b`，另存`target/ordinary-source-capability-candidate-20261007-benchmarks.jar`。早先失败包括新用例的ABSENT保留字／遗漏原生自动分组字段，以及误把窗口外层可恢复scope也断言为全链路终止；均依据实际legacy oracle修正，未改变原兼容语义或放宽值／类型断言。

撤回前开放流驻留取证使用同一个独立探针字节码（`/private/tmp/ReactorQlOrdinarySourceHeapProbe.java`编译到target/manual-ordinary-source-heap），before／候选各10k／50k复合键、每键2事件、窗口多一条才关闭，懒源后接Flux.never，无输出收集。四例输入精确20k／100k、输出0，源payload存活均0，取消后仍0：AVG等本来已是增量状态，不保存输入历史。50k键总堆在5次显式GC后由28,627,672→23,920,576 bytes（27.30→22.81 MiB），取消后8,071,176／8,056,432 bytes；10k键由9,025,664→8,147,600 bytes。日志为`target/liveheap-{bean-composite-before,ordinary-source-capability-candidate}-20261007-benchmarks.jar-{10000,50000}-20261007.log`。只作相同探针下的单JVM驻留观察，包含弱引用元数据和JVM常量；候选已撤回，不申领这些数值为交付收益。

撤回依据与保留边界：实际生命周期差分表明风险不只在窗口。全局单个max原生／融合均终止；全局max＋count的原生merge只放弃失败max，count处理全部8行并输出total=8，融合则在第2行终止。原生按键分组可在取消后重建同键组；首段窗口、逐键窗口和多聚合又分别保留不同取消／恢复范围。公开Hooks.onNextError可由合法源回调在订阅后、失败前改变，原生merge会读取新策略；订阅时将策略与onErrorStop比较再选择快路，仍会错误终止，不能作为完整资格证明。ScalarAggregateErrorBoundaryTest新增原生多聚合独立归约及动态全局Hook回归，保存真实边界，不用正常值测试替代错误scope证据。

生产只撤回PropertyMapFeature对普通属性的acceptsAnyRow声明／非Map原始读取，以及WindowedAggregateStage.applyRawKeyed的普通对象能力传播；普通对象回到原Record路径，Map／已有Record、默认限制与公开扩展行为不变。比较累加器的原生onOperatorError输入值修正保留。既有融合聚合的merge恢复差异并未因此消失，整个聚合融合仍不能宣称全面功能等价。没有安装订阅时策略探测、私有Context键、每异常恢复分支或新恢复状态机；不为这条已拒绝候选继续追加压测。阶段完整离线构建通过，576 tests、0失败／错误／跳过、52份报告（排除陈旧Benchmarks）；日志`target/ordinary-source-candidate-withdrawal-build-20261007.log`，制品`target/ordinary-source-candidate-withdrawn-validated-20261007-benchmarks.jar`的SHA256为`45f49eff927a9b0f76df47d511778a3bb0c2b1cf1ab1f9444651a7e238dd0956`。测试绿不代表融合恢复差异已经修复；本阶段没有新的可申领吞吐／堆收益，没有提交或发布。

### 派生表别名转换函数的查询期复用（计划，2026-10-07）

复用关联子查询测量期JFR的确定分配所有者：SubSelectFromFeature每次执行子查询时重新创建只捕获固定别名的转换闭包，当前源码仍为同一原生map装配。范围仅该查询期转换函数；拟在构建派生表计划时创建一次，仍由原生map对每个结果调用resultToRecord，保留别名为空时动态读取record.getName以及结果快照隔离。不减少map操作符，不缓存结果／来源，不提前求值、不改变Context、需求、取消或错误传播，不绕过Feature或Record公开扩展。

先以当前576-test制品建立同一JMH源码的关联子查询、关闭缓存的多层子查询及宽函数负对照GC/JMH基线；复用各setup的来源订阅／条数断言与NestedSqlWorkloadBenchmark的完整结果oracle，不把只计数的关联基准当作完整值／类型证明。实现后补派生表多订阅／并发别名隔离和Context／错误／取消覆盖，集中完整构建，再按同配置复测并确认分配所有者消失。若只改变少量分配则如实量化，不宣称高收益CPU或常驻堆改善；若需要新状态或扩展假设则撤回。

保留实现只把SubSelectFromFeature的别名转换Function提升到查询构建期局部变量，仍在原生map中逐值调用相同的resultToRecord表达式。DerivedTablePlanReuseTest验证两层派生表的两个同时存活订阅、完整值／类型／输出修改隔离、重复订阅、零需求、Context、原源错误及一次取消。完整离线`mvn -o -q -Pjmh -DtrimStackTrace=false package`通过，578 tests、0失败／错误／跳过、53份报告（排除旧Benchmarks）；日志`target/subselect-alias-mapper-build-20261007.log`。制品SHA256为`ee7e440647058cc6db54b7354394b87b14ae85f65e016eb801ff41ccca9f4e07`，151个目标JMH及其生成类与before完全相同，没有修改基准源码。

相同JDK17.0.18、512MB/G1、单线程、2 forks、3×1s预热、5×1s测量、GC profiler：`target/jmh-subselect-alias-mapper-before-20261007.json`→`target/jmh-subselect-alias-mapper-after-20261007.json`。关联子查询2.626±0.043→2.565±0.022 M输入行/s，1344.457→1328.457 B/输入行（少16 B，约1.2%）；关闭缓存的嵌套子查询8320.6±190.5→8336.2±557.3外层输入行/s，732313.381→732281.414 B/外层行（少约32 B，不将其换算成底层lookup每行收益）；宽函数负对照0.596±0.017→0.594±0.016 M输入行/s、6841.018 B/输入行不变。各首次吞吐区间重叠，关联目标点估计约−2.4%，因此进行了单目标、after→before顺序独立复测，不忽略这个信号。

反向顺序同配置`target/jmh-subselect-alias-mapper-isolated-{after,before}-20261007.json`分别为2.653±0.034／2.564±0.007 M输入行/s，分配仍1328.457／1344.457 B/输入行。吞吐方向反转，未确认稳定CPU提升或回退，只保留明确的小额逐子查询分配下降，不外推总体提速。after测量期JFR `target/jfr-subselect-alias-mapper-after-measurement-20261007.jfr`正常结束，3×1s预热后60s测量中delay30s／duration12s；3531个worker分配样本中SubSelectFromFeature闭包为0，已有before取证为3521个样本中的230个，其他必要Map／Record／原生操作符仍在。样本只定位来源，没有常驻堆A/B，不能声称常驻内存降低。保留此无额外状态／缓存的通用函数复用，不继续围绕它微调；既有聚合融合的错误隔离风险仍未关闭，整体目标保持未完成。

### LIKE 已匹配字面量区间的重复换行检查（计划，2026-10-07）

结论：候选已撤回，保留扩展的Pattern差分回归。移除重复字符检查在本次真实混合SQL上没有确认吞吐或分配收益，不继续引入扫描变体或调参。

当前578-test制品ee7e4406…的真实16列operatorMixWideProjection测量期JFR已完成：`target/jfr-operator-mix-native-boundaries-current-20261007.jfr`，758个worker CPU／3596个分配样本；LikeFilter.hasLineTerminator有50个CPU叶样本，是确定的同步计算所有者，不将样本数换算成CPU百分比。literal matcher构建时已确认固定片段没有Java regex行终止符，startsWith／endsWith／indexOf又证明输入对应区间与该片段相同；重复检查这些区间没有额外语义价值。

范围只LikeFilter字面量编译结果中的换行检查：只扫描通配符覆盖区间，保留动态／regex语法／字面量自身有换行的原Pattern路径，以及空值、NOT LIKE、Unicode五种行终止符和原求值／响应式错误边界。不生成substring，不新增缓存／配置／API／来源类型分支或自定义操作符。先对当前相同JMH类记录混合AND／OR查询、原生计算与完整宽函数负对照基线；再实施并补与原Pattern逐值差分的边界回归，集中完整测试后同配置GC/JMH和after JFR。验收为真实混合SQL吞吐提升或至少无稳定回退且算法消除明确重复计算；不预申领堆收益，不为数据分布调参。

候选阶段完整构建579 tests、0失败／错误／跳过、53份报告通过；新增LikeFilterTest.matchedLiteralRegionsKeepUnicodeAndLineTerminatorSemantics与原Pattern逐值比较LIKE／NOT LIKE，覆盖长Unicode固定片段、五种换行、CRLF、重复片段、emoji和固定片段本身含换行的fallback。候选制品`3d518bab…`另存`target/like-matched-region-rejected-20261007-benchmarks.jar`，28个WideSqlWorkloadBenchmark及生成类与before完全相同。

同JDK17.0.18、512MB/G1、单线程、2 forks、3×1s预热、5×1s测量、GC：`target/jmh-like-matched-region-{before,after}-20261007.json`。混合AND 2.383±0.026→2.383±0.025 M输入行/s，1022.237 B/输入行不变；混合OR 1.870±0.015→1.898±0.020 M，1337.193 B/输入行不变，置信区间仍重叠。未修改的native负对照16.772±0.383→17.790±0.543 M、140.705 B/输入行不变，完整宽函数负对照0.581±0.011→0.591±0.012 M、6841.019 B/输入行不变；不能把环境／运行波动算成引擎增益。after测量期JFR `target/jfr-like-matched-region-after-measurement-20261007.jfr`正常结束，727 CPU／3593分配样本；hasLineTerminator仍有33个CPU叶样本，Map／Record／原生MonoZip等所有者仍在。采样不作为实际吞吐或堆收益证明。

生产LikeFilter恢复before实现，保留新增差分测试。最终完整离线构建`target/like-matched-region-withdrawal-build-20261007.log`再次通过：579 tests、0失败／错误／跳过、53份报告；当前制品SHA256为`db19755d9772171ce1dbbe8aab742769234d50836bf3ff7de6cbc65441c0c001`。520个ReactorQL及基准类字节码与此前已验证ee7e4406…完全一致（JAR封装hash不同），本轮没有新的生产性能改动或收益。原生算子／属性读取仍是主要成本，既有聚合融合错误隔离风险未关闭；整体目标未完成，不以不断增加微型快路径代替高收益优化。

### 原生 Reactor 运行时对照（诊断计划，2026-10-07）

已有真实SQL的JFR显示原生FluxFlatMap内层完成扫描及宽投影原生组合边界仍有显著成本。先使用本地标准Reactor 3.4.34／3.7.19进行相同引擎、SQL、输入和JMH字节码对照，不复制第三方算子、不修改并发／排序／默认限制、不增加快路或恢复框架。范围只target诊断制品及临时探针；不修改POM、生产源码或将classfile兼容当作升级验收。

从当前已验证db19755d…制品生成诊断副本，移除全部reactor/core与reactor/util及对应多版本路径；逐条验证其余ZIP条目字节相同。分别显式加载目标core，检查版本／CodeSource及实际类加载日志，避免新旧运行时混用。集中验证正常SQL／函数／聚合、多层派生表值与隔离、Context、零需求、取消、源错误和原生独立归约／动态Hook错误边界。通过后先作混合SQL与直接函数高基数分组的有界GC/JMH对照；没有稳定高收益则关闭路线，不继续升级或调参。有收益也需完整依赖及功能兼容验收才可更改生产依赖；既有融合错误scope差异不因运行时smoke通过而视为关闭。

结论：本轮运行时升级路线关闭，**未修改POM、生产源码或默认参数**。诊断副本`target/runtime-core-isolated-diagnostic-20261007.jar`的SHA256为`b55c58382ecfee579841b7f8e43c839cea6db604875b886c93540bffea6a209b`；移除905个core／util条目，保留10434个条目与原制品逐条字节一致，并确认不存在对应多版本回退类。临时`ReactorQlRuntimeCompatibilityProbe`对两个版本均验证正常运算／函数／增量聚合、两层派生表多订阅快照隔离、Context、零需求、一次取消、源错误，以及原生多归约独立恢复和订阅后动态公开Hook；宽列与嵌套基准setup完整值／类型／来源订阅oracle均通过。日志`target/runtime-smoke-{3.4.34,3.7.19}-20261007.log`；420／433个实际加载的具名core类均来自指定版本，没有混用。此有界探针不是新版全套依赖、公开扩展或完整升级兼容验收。

正式对照`target/runtime-jmh-{3.4.34,3.7.19}-20261007.json`：相同过滤后引擎／JMH字节码与SQL／输入，JDK17.0.18、512MB/G1、单线程、2 forks、3×1s预热、5×1s测量、GC；高基数固定50k行窗口、每键1事件。两侧各8个fork类加载日志共2768／2872个具名core类加载均来自目标JAR，wrong-origin=0。单位均为输入行。

| 场景 | 吞吐，3.4.34→3.7.19（输入行/s） | B/输入行，3.4.34→3.7.19 |
| --- | --- | --- |
| 直接函数键四聚合 | 5825±166→5949±112（+2.1%，区间重叠） | 7866.355→7866.355 |
| 混合运算宽列 | 2.311±0.118→2.533±0.031 M（单轮+9.6%） | 1022.237→1022.611 |
| 宽列函数 | 0.614±0.013→0.588±0.014 M（点估计−4.2%，区间略重叠） | 6862.619→6608.604（−3.7%） |
| 未改原生运算负对照 | 20.385±1.978→20.170±1.470 M（区间重叠） | 140.705→140.705 |

已有当前真实负载JFR定位的函数分组`FluxFlatMap.drainLoop←innerComplete`主热点，在官方3.4.34／3.7.19源码中仍使用相同完成扫描逻辑；该文件差异只在Publisher标准化入口等处，没有替换扫描算法。新版`concatMap(mapper)`默认由XS_BUFFER_SIZE预取改为无预取，不能把版本切换当作无默认行为影响的算子替换。混合宽列正向结果尚未作反向独立复测；本轮不推广新版，不申领生产提速／常驻堆下降，也不为了某个SQL挑运行时、改变预取／并发或复制第三方实现。没有新生产候选需要after JFR或全量升级构建；当前db19755d…制品及579-test完整验证仍有效，不为诊断／文档重复构建。

后续首先收敛既有聚合融合的原生错误隔离差异，保留标准reduce／merge生命周期，而非扩展融合范围或自建错误补偿层；这仍是功能验收风险，不能被正常值测试或本轮运行时探针覆盖。高基数精确分组的必要键／累加状态与AVG／MAX的历史输入驻留是两回事；后者本来就未收集全组历史，不再围绕这一错误前提优化。

### 聚合融合边界撤回与原生增量路径保留（计划，2026-10-07）

契约仍为全SQL场景的通用吞吐／堆优化，同时保持公开错误策略、Context、背压、取消和默认限制。既有融合状态把多个独立归约器、层级分组／窗口生命周期及结果快照压成一个终止范围，原生max＋count、同键组重建和动态Hook均有确定反例。新增`target/aggregate-output-scope-before-20261007.log`还确认单个全局COUNT／SUM／MAX也跳过源Map的公开forEach快照读取：合法读取失败时原生报snapshot failed，融合却正常输出；首个仅覆盖entrySet的探针不足以触发实际forEach边界，补齐真实方法后才成为有效证据。不能据此缩成某个SQL形态、异常／输入类型名单或订阅时Hook资格判断。

本阶段首先在DefaultReactorQL入口撤回整个不等价WindowedAggregateStage选路，使用现有groupBy／columnMapper和原生reduce／merge；原生AVG／MIN／MAX继续实时增量处理，不改成收集历史输入。保留标量行表达式、标准Publisher函数路径、regex证明复用、子查询与已验证容器优化；不改变限额／并发／依赖，不重建错误恢复状态机，也不以原生回退作为整个性能目标已达成。新增真实SQL的默认／原生生命周期差分和快照错误回归，复用既有动态Hook／独立归约合同，先集中验证受影响功能边界。然后清理已不可达的融合实现／计划专属断言，刷新基准的执行计划资格而保留输入／SQL／完整值／类型oracle，完整构建并量化性能代价。撤回前源文件与当前已验证制品保留为可恢复证据；整个优化目标保持ACTIVE，后续仍需沿真实JFR优化原生路径。

初轮集中验证222 tests中新增3项原生生命周期回归通过，另有2个失效融合计划断言和1个真实取消差异。原默认Scalar属性参数在MapAggFeature的handle映射下，Number.doubleValue归约失败触发两次源doOnCancel，原兼容Publisher映射只触发一次；ScalarAggregateErrorBoundaryTest.reductionFailureIsNotRecoveredAsAMappingFailure的once-only断言保留，不能把它当作失效测试。MapAggFeature／CountAggFeature统一恢复metadata.flatMap参数映射订阅边界，包括数值／比较归约及精确集合资源失败，不添加取消去重代理或异常类型分支。失效计划断言改为现有STATEFUL[group]，完整值、类型和限制错误oracle不改；待同一边界阶段复验。

入口撤回阶段验证完成：DefaultReactorQL移除整个融合／raw聚合选路及其不可达字段／计划分支；仍存在的独立融合class／Accumulator API及融合专属测试、基准计划断言将在下一阶段清理，不宣告已完成全量交付。新增AggregateNativeLifecycleTest的3项回归覆盖全局／按键／窗口及复合分组的单／多归约真实SQL差分、订阅后动态Hook、源Map快照失败的终止及原生恢复；保留完整结果／类型、输入消费、取消与错误身份校验。集中`mvn -o -q -Pjmh -DtrimStackTrace=false -Dtest=AggregateNativeLifecycleTest,ScalarAggregateErrorBoundaryTest,LegacyMultiValueAggregateTest,DerivedTablePlanReuseTest,ReactorQLTest,GroupByWindowTest,AggregationResourceLimitTest,SubqueryCacheTest package`通过：222 tests、0失败／错误／跳过，8份指定报告，无遗漏。日志`target/aggregate-native-entry-stage-package-20261007.log`，当前阶段制品SHA256为`aa6b8cdc72c6754c5e447943b9f5a400db27e140d902686a17b3a0b136a46829`，另存`target/aggregate-native-entry-boundary-stage-validated-20261007-benchmarks.jar`。这不是旧579-test全套结果，陈旧报告不得计入。

同一临时快照探针对当前制品复验，默认／显式原生的COUNT、SUM、MAX六例均产生相同snapshot failed终止信号，默认计划不再显示融合；日志`target/aggregate-output-scope-native-entry-after-20261007.log`。原db19755d…制品保留为`target/aggregate-fusion-entry-withdrawal-before-validated-20261007-benchmarks.jar`，撤回前相关源代码保留在`target/aggregate-fusion-entry-withdrawal-before-source-20261007.tar.gz`，可恢复。初次验证因新增文件缺许可证头停在license门禁，补齐后才进入真实测试；不是功能失败，也未跳过DebugAgent或门禁。阶段`git diff --check`通过，无commit／push。本阶段不申领吞吐／堆改善，旧融合收益不能继续算作当前收益。

#### Bug Analysis: 聚合同步能力与原生生命周期混淆

##### 1. Root Cause Category

B（跨边界契约）、E（隐含假设）和D（覆盖缺口）：单值／同步／增量能力只说明计算方式，不证明多个Publisher共享同一个终止、取消或结果构建范围。合法Map读取与公开Hook同样可观察，不是只需排除某类比较异常。

##### 2. Why Fixes Failed

之前的订阅时错误策略快照无法覆盖订阅后Hook改变；单看全局单归约不覆盖外层merge和独立健康归约器；entrySet探针未命中实际forEach快照边界。分别依据动态Hook、多归约／层级组差分和真实快照方法修正观察，不把这些不足当作引入新guard的理由。

##### 3. Prevention Mechanisms

| 优先级 | 机制 | 当前状态 |
| --- | --- | --- |
| P0 | 保留标准reduce／merge和参数映射的独立订阅生命周期 | 已恢复入口 |
| P0 | 真实SQL、动态Hook、快照读取和once-only取消回归 | 222项阶段测试通过 |
| P1 | 清理不再可达的融合框架与专属计划资格，保持值／类型oracle | 下一阶段 |

##### 4. Systematic Expansion

后续原始行前置、行级融合或表达式简化同样需证明所跳过的公开读取和错误数据／Context／取消范围等价；不能以正常值一致、标量声明或某个输入类推导全部Publisher语义。不会扩展为新的恢复执行框架。

##### 5. Knowledge Capture

根因、代码边界和防复发覆盖回填本文及相邻代码注释。owning模块没有`.trellis/spec`或模板，不为本修复安装工作流、生成平行规范或提交；不扩展到未经确认的`.ai`／共享技能知识沉淀。

`collect_row`参数key／value任一侧读取失败时，合法ScalarValueMapper的原捷径直接终止，而同一mapper暴露为普通Function时，原生zip＋flatMap按onErrorContinue跳过失败行、保留后续good=2。因此删除该捷径，统一保留原生参数映射；AggregateNativeLifecycleTest同时覆盖参数读取恢复和已经活动的多聚合来源只取消一次。删除WindowedAggregateStage、IncrementalValueAggMapFeature和四个内置聚合中的Accumulator工厂／帮助方法；sum／avg／min／max仍用原生MathFlux函数及已有订阅缓存安全标记，不改变默认配置。

原生兼容边界：没有ORDER BY的多层分组不承诺融合实现的全局首次出现顺序；输出保持完整行多重集及值／类型。原生时间窗口在零结果需求时可继续准备闭窗结果，而不是已删除concatMap(prefetch=1)的溢出时点。扩展Record绑定的分组键保持其List／数组／单值类型及原身份，不新增统一归一化／深复制契约；写入新键时仍复制调用方原始上游键列表。键预算只计原生按键层，不计时间窗口为额外键。`target/aggregate-native-cleanup-oracle-20261007.log`使用前一已验证原生制品独立核对这些边界；对应测试不改实际结果／错误／一次活动源取消合同。

完整离线`mvn -o -q -Pjmh -DtrimStackTrace=false package`通过：584 tests、0 failures／errors／skips、54份当前测试报告，排除陈旧Benchmarks和重命名前报告；保留DebugAgent及许可证门禁。日志`target/aggregate-native-cleanup-full-package-accepted-20261007.log`，制品`target/aggregate-native-cleanup-validated-20261007-benchmarks.jar`的SHA256为`b58a72e3e284eecd42100549e0f9b4d4135e750fc316b89f41fc343b4ad93fcd`。删除的类和Accumulator内类均不在新JAR中，47份旧编译class recoverably移至`target/aggregate-cleanup-stale-classes-20261007/`；没有删除整个target证据目录。相关源码归档`target/aggregate-native-plan-cleanup-before-source-20261007.tar.gz`可恢复。阶段git diff --check通过，无commit／push。本阶段只完成安全和复杂度收敛，不申领新的吞吐或常驻堆收益。
