# 常用 JSON 与数据函数支持开发计划

## 背景

通用智能体在生成 `dataset_query` / ReactorQL SQL 时，会自然使用 MySQL、PostgreSQL 以及通用 SQL/JSON 风格的函数，例如 `json_extract`、`json_value`、`json_contains`、`jsonb_extract_path_text`、`regexp_replace`、`round`、`floor` 等。

当前目标是补齐这些常用数据函数，让智能体生成的查询更接近可直接执行的 SQL，而不是依赖提示词反复规避。

## 目标

- 增加常用数值函数：`round`、`floor`、`ceil`、`abs`、`sqrt`、`pow`、`power`。
- 增加常用字符串和正则函数：`lower`、`upper`、`length`、`char_length`、`trim`、`ltrim`、`rtrim`、`replace`、`substring`、`regexp_replace`、`regexp_like`、`regexp_extract`、`regexp_substr`。
- 增加 JSONPath 提取函数：`json_get`、`json_path`、`json_extract`、`json_value`、`json_query`、`json_exists`。
- 覆盖 MySQL 常见 JSON 函数名：`json_unquote`、`json_type`、`json_valid`、`json_length`、`json_keys`、`json_contains`、`json_contains_path`、`json_overlaps`、`json_array`、`json_object`、`json_merge`、`json_merge_preserve`、`json_merge_patch`。
- 覆盖 PostgreSQL 常见 JSON 函数名：`json_extract_path`、`json_extract_path_text`、`jsonb_extract_path`、`jsonb_extract_path_text`、`json_array_length`、`jsonb_array_length`、`json_object_keys`、`jsonb_object_keys`、`json_typeof`、`to_json`。
- 增加通用 JSON 比较与集合函数：`json_equal` / `json_equals`、`json_intersect` / `json_intersection`、`json_union`、`json_diff` / `json_except`；这些非数据库同名函数只作为 ReactorQL 扩展函数，必须避免覆盖数据库同名语义。
- JSON 参数不局限于字符串，必须支持 `Map`、`Collection`、Java 数组和标量；字符串仅在内容是合法 JSON 时解析成结构化 JSON。
- 同步更新 dataset 工具提示词，引导智能体优先使用稳定函数。

## 非目标

- 不实现完整数据库方言；但已覆盖的 MySQL / PostgreSQL 同名 JSON 函数必须尽量保持数据库行为一致，不能因为 Java 实现方便而改成自定义语义。
- 不支持 JSON 写操作，例如 `json_set`、`json_insert`、`json_remove`，除非后续出现明确场景。
- 不改变 ReactorQL 的 `NULL` / 空发布语义；缺失值仍按现有 `Mono.empty()` 风格处理。
- 本 PR 不升级 JSqlParser；JSqlParser 5.3 相关适配和 `trim(leading ... from ...)` 等表达式访问增强后续单独合并到 `1.1` 分支。
- 本 PR 不引入新的 Feature Registry / Feature Policy 公共 API；安全函数与非安全函数的注册隔离纳入下一版本设计。

## 依赖选择

- 使用 Jayway JsonPath 作为 JSONPath 引擎。
- `json-path:3.0.0` 当前 classfile 为 Java 17，不符合本项目当前 `java.version=1.8`。
- 推荐引入 `com.jayway.jsonpath:json-path:2.10.0`，该版本面向 Java 8，兼容当前 Java 8 构建。

## JSONPath 编译策略

- 静态 JSONPath 必须在 `ValueMapFeature#createMapper(...)` 阶段使用 `JsonPath.compile(...)` 预编译，例如：
  - `json_get(value, '$.point.lon')` / `json_path(value, '$.point.lon')`
  - `json_extract(value, "$.point.lat")`
- 兼容 JSqlParser 将双引号 `"$..path"` 解析为带引号列名的情况；当参数是带引号列名且内容以 `$` 开头时，按 JSONPath 字面量处理。
- 动态 JSONPath，例如 `json_get(value, pathColumn)`，运行时再编译。
- 不引入无界全局缓存；如后续需要缓存动态路径，只能使用有容量上限的缓存，并补充测试和说明。
- JSONPath 函数自身必须做安全边界控制，包括路径长度限制、拒绝递归扫描 / filter / 函数调用 / 通配符等高风险表达式；这些工具类函数应在自身实现中保证资源使用可控，而不是依赖外部注册策略兜底。

## 工具函数安全边界

工具函数应尽量保持可用，不因为存在恶意输入风险就整体禁用；但必须避免方向错误或危险参数造成执行线程长时间占用、内存放大或 JVM 压力。

这些默认限制可通过 `ReactorQLMetadata` settings 调整，例如 `ReactorQL.builder().setting(...)` 或 SQL hint 写入的 metadata setting；但实现必须同时校验硬上限，避免不可信 SQL 通过 hint 将限制调成无界或危险值。

- JSON 函数：
  - JSONPath 长度默认限制为 1024；setting key：`function.json.maxPathLength`。
  - JSON 文本输入默认限制为 2MB；setting key：`function.json.maxTextLength`。
  - JSON 字符串化输出默认限制为 2MB；setting key：`function.json.maxOutputLength`。
  - JSON 结构深度默认限制为 128；setting key：`function.json.maxDepth`。
  - 单个 object / array 容器大小默认限制为 100000；setting key：`function.json.maxContainerSize`。
- 正则函数：
  - pattern 默认长度限制为 1024；setting key：`function.regex.maxPatternLength`。
  - input 默认长度限制为 256KB；setting key：`function.regex.maxInputLength`。
  - replacement 默认长度限制为 64KB；setting key：`function.regex.maxReplacementLength`。
  - 拒绝典型嵌套量词，例如 `(a+)+`、`(.*){...}` 这类容易导致灾难性回溯的表达式。
  - 保留 Java 正则的常用能力，包括捕获组、大小写标志和 `$1` 替换。
- 字符串生成函数：
  - `repeat`、`replace`、`regexp_replace` 的结果默认长度限制为 1MB；setting key：`function.maxGeneratedStringLength`。
  - `split_part` 不再通过 `String#split` 构造完整数组，按目标下标从前或从后扫描，避免大字符串被分隔成大量中间对象。
- 错误语义：
  - 明显危险或资源超限的参数抛出 `UnsupportedOperationException`，让调用方能定位 SQL 参数问题。
  - 常规业务边界，例如 `regexp_extract` 捕获组不存在，返回空值，避免因下标误用中断整条查询。

## JSON 结构规范化

- `String`：
  - 合法 JSON 文本解析成对象、数组或标量。
  - 非法 JSON 文本保留为普通字符串。
- `Map`：作为 JSON object 直接处理。
- `Collection` / Java 数组：作为 JSON array 直接处理。
- `Number`、`Boolean`、其他标量：作为 JSON scalar 处理。
- JSON 输出需要字符串化时，通过 JsonPath 所使用的 JSON provider 输出，避免手写拼接。

## 函数语义

- `json_get(json, path[, default])` / `json_path(json, path[, default])`：读取 JSONPath；缺失时返回空值，有 `default` 时返回默认值。
- `json_extract(json, path[, path...])`：MySQL 风格；单 path 返回单值，多 path 返回数组。
- `json_value(json, path[, default])`：读取标量；对象或数组返回 JSON 字符串。
- `json_query(json, path)`：读取对象或数组；标量按实际值返回。
- `json_exists(json[, path])`：路径存在返回 `true`，缺失返回 `false`，默认 path 为 `$`。
- `json_extract_path(json, key[, key...])`：PostgreSQL 风格路径段访问，内部转换为 JSONPath。
- `json_extract_path_text(json, key[, key...])`：同上，但结果转为文本。
- `json_unquote(value)`：结构化值转 JSON 字符串，普通标量转字符串。
- `json_valid(value)`：非字符串对象视为合法 JSON；字符串必须能解析为 JSON 才返回 `true`。
- `json_type` / `json_typeof(value)`：返回 `object`、`array`、`string`、`integer`、`double`、`boolean`、`null`。
- `json_length(json[, path])`：对象返回 key 数，数组返回元素数，标量返回 1，空值或路径缺失返回空值。
- `json_keys(json[, path])` / `json_object_keys(json)`：对象返回 key 数组，非对象返回空数组。
- `json_array(...)`：返回参数数组。
- `json_object(k1, v1, k2, v2, ...)`：返回对象；奇数参数时忽略最后一个孤立 key。

## 数据库同名函数语义对齐

- `json_merge` / `json_merge_preserve` 按 MySQL / MariaDB 的 preserve 语义实现，不等同于 `Map.putAll`：
  - 相邻数组合并为一个数组。
  - 对象同名 key 保留双方值；重复 key 的 value 合并或包装为数组，而不是右值覆盖。
  - 对象与数组、标量混合时，按数据库规则将非数组包装后再合并，具体边界以测试固定。
- `json_merge_patch` 按 MySQL `JSON_MERGE_PATCH` / RFC 7396 语义实现：
  - object 同名 key 由右侧 patch 覆盖；右侧值为 JSON `null` 时删除该 key。
  - 非 object patch 可整体替换目标值。
  - 这是最接近“右值覆盖 / patch”的数据库同名函数，但它不是简单的浅层 `Map.putAll`，需要递归处理 object 与 JSON null 删除语义。
- PostgreSQL `jsonb || jsonb` 属于操作符而非本计划已列函数名；如后续增加对应函数或操作符，应按 PostgreSQL 行为固定：
  - object 仅顶层合并，重复 key 取右侧值。
  - array 拼接。
  - 其他组合将非数组包装成单元素数组后再拼接。
  - 不做递归深合并。
- `json_union`、`json_intersect`、`json_diff` / `json_except` 是 ReactorQL 扩展集合函数，不能作为 `json_merge` 的别名。

## JSON 比较与集合语义

- 深度相等：
  - object 按 key/value 深度比较，key 顺序不影响结果。
  - array 按顺序比较。
  - number 按数值比较，`1` 与 `1.0` 视为相等。
  - string、boolean、null 按值比较。
- `json_contains(target, candidate[, path])`：
  - object：candidate 是 target 的深度子集。
  - array：candidate 数组中的每个元素都能在 target 数组中找到深度相等项。
  - scalar：按深度相等判断。
- `json_contains_path(json, 'one'|'all', path[, path...])`：兼容 MySQL one/all 语义。
- `json_overlaps(left, right)`：
  - object：存在相同 key 且 value 有重叠。
  - array：存在深度相等元素。
  - scalar：深度相等。
- `json_intersect(left, right[, ...])`：
  - array：返回交集，按左侧出现顺序去重。
  - object：只保留双方都有且 value 有交集的 key。
- `json_union(left, right[, ...])`：
  - array：返回去重并集，先左后右。
  - object：按 key 做扩展集合并集；同 key 值都为 object 时可递归合并，否则保留左侧优先或右侧优先必须在测试中固定，但不得映射为数据库同名 `json_merge`。
- `json_diff(left, right[, ...])` / `json_except(left, right[, ...])`：
  - array：返回左侧有、右侧没有的元素。
  - object：返回左侧有、右侧没有或 value 不同的字段。

## 实现任务

1. 新增 JsonPath 依赖，确认 Java 8 编译兼容。
2. 新增 `JsonPathFunctionMapFeature` 抽象基类，只负责参数校验、路径预编译和参数流装配；各 JSON 函数行为单独继承实现，避免每行数据执行时按函数名 `switch` 分派。
   - `JsonFunctionSupport`：承载路径读取、settings 限制解析和数据库函数通用流程。
   - `JsonValueSupport`：承载 JSON 文本解析、Java Map/List/数组规范化、类型判断和输出长度限制。
   - `JsonCollectionOperations`：承载 deep equals、contains、overlaps、intersect/union/diff、merge 语义。
3. 在 `DefaultReactorQLMetadata` 注册所有新增函数，并把可用于 `group by` 的行级函数加入 `GroupByValueFeature`。
4. 不在本 PR 升级 JSqlParser；如实现过程中遇到当前 4.6 不支持的 SQL 语法，先记录为 `1.1` 分支后续任务，不混入本 PR。
5. 补充 `ReactorQLTest`：
   - 数值、字符串、正则函数基本用例。
   - `REGEXP_REPLACE(value, '.*"lon":([^,]+).*', '$1')` 回归用例。
   - JSON 字符串、`Map`、`List`、数组、标量输入用例。
   - 静态 JSONPath 与动态 JSONPath 用例。
   - MySQL / PostgreSQL JSON 函数别名用例；数据库同名函数必须按数据库语义断言，特别是 `json_merge` / `json_merge_preserve`、`json_merge_patch` 和未来 PostgreSQL `jsonb ||` 对应函数的差异。
   - JSON contains / overlaps / equal / intersect / union / diff 用例。
   - 缺失路径、非法 JSON、默认值、空数组 / 空对象边界。
6. 更新 dataset 工具帮助文档，优先推荐 `json_get(value, '$.path')`，仅在源数据不是合法 JSON 时推荐正则提取。
7. 运行验证：
   - `./mvnw -Dtest=ReactorQLTest#testCommonSqlFunctions,ReactorQLTest#testRegexpFunctions,ReactorQLTest#testJsonPathFunctions,ReactorQLTest#testJsonDatabaseCompatibilityFunctions test`
   - `./mvnw -Djacoco.skip=true test`
   - 如 Java 21 + JaCoCo 0.8.7 仍有插桩噪声，在 PR 中明确记录并给出跳过 JaCoCo 的测试证据。

## 下一版本：Feature Registry / Policy 设计

当前 `DefaultReactorQLMetadata` 通过静态 `globalFeatures` 注册内置函数。下一版本应把“安全函数 / 非安全函数”的边界前移到 ReactorQL 构造阶段，而不是在每次查找 `Feature` 时由调用方额外指定。

### 目标

- ReactorQL 构造时显式指定 `FeatureRegistry` 和 `FeaturePolicy`；`ReactorQLMetadata#getFeature(...)` 只按当前 metadata 已绑定的 registry 查询，不增加 `safeOnly` 一类调用参数。
- 内置工具类函数默认属于安全函数，但函数自身仍必须做资源和输入边界控制，例如 JSONPath、正则、字符串重复、集合展开等函数都需要限制高成本输入或明确失败语义。
- 非安全函数主要指平台额外提供的内部能力函数，例如访问租户内部数据、资产权限上下文、系统配置、远程服务或运维数据的函数；这些函数不得进入默认安全 registry，必须由平台侧显式注册并通过 policy 授权。
- 子查询 metadata 必须继承父查询的 registry 与 policy，避免主查询安全而子查询重新落回全局默认。
- 保留现有 `Builder#feature(...)` 的兼容入口，但内部应落到当前 ReactorQL 实例的 registry，不再写入静态全局 Map。

### 建议 API 形态

```java
ReactorQL ql = ReactorQL
        .builder()
        .sql(sql)
        .featureRegistry(FeatureRegistry.defaults()
                                        .with(platformFeatures))
        .featurePolicy(FeaturePolicy.safeOnly()
                                    .allow("value-map:platform_allowed"))
        .build();
```

建议新增概念：

- `FeatureRegistry`：只负责按标准化后的 feature id 注册和查询 `Feature`；默认 registry 包含当前内置安全函数。
- `FeatureDescriptor`：描述 `Feature`、安全级别、来源和说明；安全级别可从 `SAFE`、`INTERNAL`、`UNSAFE` 起步，后续按平台场景细分。
- `FeaturePolicy`：在 registry 进入 metadata 或构建 mapper 前做一次过滤 / 校验，支持 safe-only、allow-list、deny-list 和平台自定义规则。
- `DefaultFeatureRegistry`：替代 `DefaultReactorQLMetadata` 内的静态 `globalFeatures`，集中承载内置函数注册。

### 策略边界

- 不建议把安全策略做成 `getFeature(featureId, safeOnly)` 或类似查找时参数，因为上层表达式解析、`ValueMapFeature`、`FilterFeature`、`GroupFeature` 都会间接查找函数，调用链很容易漏传。
- 不建议仅靠函数命名区分安全性；同名函数可能在不同平台 registry 中有不同实现，最终应以 registry 中的 descriptor 和构造时 policy 为准。
- 不建议把普通工具函数列为非安全函数来规避输入风险；工具函数应自行处理恶意输入和资源上限，policy 只负责“是否允许访问某类能力”。
- 默认内置函数应保持大小写不敏感的查找行为，registry 内统一使用小写 id。

### 迁移步骤

1. 抽取 `DefaultFeatureRegistry`，把当前静态注册逻辑迁移为默认 registry 构建逻辑。
2. 在 `DefaultReactorQLMetadata` 中持有 registry / policy 快照，移除对静态 `globalFeatures` 的直接依赖。
3. 扩展 `ReactorQL.Builder`，新增 registry / policy 配置入口；现有 `feature(...)` 继续可用并写入实例级 registry。
4. 为平台内部函数补充 descriptor，默认不进入 safe-only registry。
5. 补充测试：
   - 默认构造可使用全部内置安全函数。
   - safe-only policy 下平台内部函数不可用。
   - allow-list 后指定内部函数可用。
   - 子查询继承父查询 policy。
   - 自定义 `feature(...)` 不污染其他 ReactorQL 实例。

## 风险与待确认

- JSONPath 动态路径是否需要 bounded cache，取决于 dataset 查询中动态路径的使用频率。
- dataset 帮助文档位于 `cloud.jetlinks` 独立仓库，建议后续实现 PR 合并后再创建对应文档同步 PR，避免跨仓库混入一个提交。
- JSqlParser 5.3 升级和相关 SQL 表达式兼容适配后续单独合并到 `1.1` 分支。
- Feature Registry / Policy 需要作为下一版本公共 API 设计，落地前需确认默认 policy 是否保持完全兼容，以及平台内部函数的安全级别和命名边界。
