# ReactorQL 结构化异常诊断开发计划

## 背景

当前 ReactorQL 大量错误直接抛出 `UnsupportedOperationException`，错误信息通常只有“Unsupported expression”或“参数数量错误”。
这类自由文本不利于用户定位 SQL 问题，也不利于大模型根据错误自动修正查询。

## 目标

- 保留现有 `UnsupportedOperationException` 兼容性，不破坏外部调用方和既有测试。
- 为 SQL 解析、编译、函数参数、资源限制、安全限制等错误提供结构化诊断信息。
- 尽可能提供 SQL 表达式、行号、列号、原因、建议和示例。
- 提供国际化扩展点：错误码、默认文案参数，以及默认中英文资源。
- `suggestion` 和 `example` 只面向 SQL 使用者提供修复建议或支持用法，不暴露类名、注册表、解析树、执行器、缓存等实现细节。

## 异常模型

新增 `ReactorQLException extends UnsupportedOperationException`，核心字段：

- `i18nCode`：稳定错误码，例如 `error.reactorql.function_argument_count`。
- `i18nArgs`：用于平台或调用方国际化渲染的参数。
- `line` / `column`：SQL 起始位置；能从 JSqlParser AST 或 ParseException 取得时填充。
- `expression`：出错 SQL 表达式或原始 SQL。
- `reason`：具体失败原因。
- `suggestion`：推荐修复方向，只描述支持用法或安全边界。
- `example`：可复制的正确写法。

`getMessage()` 输出默认英文诊断文本，`getLocalizedMessage()` 通过 `ResourceBundle` 读取 `i18n/reactorql/messages_*.properties`。
JetLinks 平台侧也可以直接读取 `getI18nCode()` / `getI18nArgs()`，再走平台 `LocaleUtils` 或统一错误响应体系。

## 首批错误码

- `error.reactorql.syntax`：SQL 解析失败。
- `error.reactorql.unsupported_expression`：不支持的 select/value 表达式。
- `error.reactorql.unsupported_condition`：不支持的 where/having 条件。
- `error.reactorql.unsupported_group_expression`：不支持的 group by 表达式。
- `error.reactorql.unsupported_from`：不支持的 from 表达式。
- `error.reactorql.unsupported_flat_map`：不支持的列转行表达式。
- `error.reactorql.function_argument_count`：函数参数数量错误。
- `error.reactorql.invalid_argument`：函数参数值、setting 或安全限制错误。
- `error.reactorql.resource_limit`：输入行数、JSON 文本、输出长度、窗口大小等资源限制错误。

## 位置信息策略

- 编译期表达式：利用 JSqlParser 4.6 的 `ASTNodeAccess#getASTNode()` 和 `SimpleNode#jjtGetFirstToken()` 读取行列。
- 解析期错误：从 `ParseException.currentToken.next` 读取行列。
- 当前 `SqlParserUtils.quoteNonAsciiAliases(...)` 可能改写 SQL。第一阶段只保证未改写或位置未受影响的 SQL 能返回准确行列；后续如需精准映射，可为 SQL 改写过程补 offset mapping。

## 落地阶段

### 阶段 1：核心模型和高频入口

已覆盖：

- `DefaultReactorQLMetadata(String sql)`：SQL 解析失败包装成 `ReactorQLException`。
- `ValueMapFeature#createMapperNow`：不支持的 value/select 表达式。
- `FilterFeature#createPredicateNow`：不支持的 where/having 条件。
- `FunctionMapFeature`：函数参数缺失和数量错误。
- `DefaultReactorQL`：不支持的 select 表达式和 group by 表达式。
- 常见资源/安全参数错误：正则风险、字符串长度、setting、日期单位、`time_bucket` interval。

### 阶段 2：继续替换重点函数

已覆盖：

- JSONPath 相关安全错误：`JsonFunctionSupport`、`JsonValueSupport`。
- `DateFormatFeature`、`SingleParameterFunctionMapFeature`、`CoalesceMapFeature`。
- `OrderBySupport` 的排序窗口、topN、setting 类型和值域限制。
- `MergeByKeyFeature` 的参数、setting、排序、重复键和单键行数限制。
- 聚合和列转行的参数错误：`MapAggFeature`、`CollectListAggFeature`、`CollectRowAggMapFeature`、`ArrayValueFlatMapFeature`。
- FROM 组合函数和子查询集合操作错误：`zip`、`combine`、子查询 set operation。
- `if`、`_window` 等常用函数的参数数量和值域错误。

文案边界：

- `reason` 可以描述失败原因，但不要求用户理解内部扩展点。
- `suggestion` 只给出可执行建议，例如“使用 FROM 子句”“增加 LIMIT”“使用简单 JSONPath”“使用 _window('1m')”。
- `example` 给出 SQL 或 setting 写法，不展示内部类名、方法名、缓存、解析树或执行器。

### 阶段 3：运行时上下文增强

- 对运行期函数求值失败增加 row/parameter 上下文，避免只看到底层 `TypeCastException` 或 `ArithmeticException`。
- 对响应式链路中传播的异常保留原始 cause 和 SQL 表达式。
- 评估是否为 `TypeCastException` 增加结构化诊断字段，或在 ValueMap/Filter 边界统一包装。

## 测试要求

- 断言异常仍然是 `UnsupportedOperationException` 的子类。
- 断言错误码、表达式、原因、建议、示例字段存在。
- 断言语法错误和表达式错误尽量带行列信息。
- 断言 `getLocalizedMessage()` 可以读取英文和中文资源。
- 断言新增错误码在中英文资源中存在。
- 断言 JSON 非安全路径、ORDER BY 资源限制、日期格式、窗口参数、merge_by_key 参数和运行期排序校验等场景使用结构化异常。
- 断言对外建议不包含实现细节关键词。
- 现有错误场景测试保持通过。

当前验证结果：

- `mvn -q test`：260 tests, 0 failures, 0 errors, 0 skipped。
- JaCoCo：instruction 93.57%，branch 81.43%，line 94.52%，不低于基准分支覆盖率。
- `git diff --check`：通过。
