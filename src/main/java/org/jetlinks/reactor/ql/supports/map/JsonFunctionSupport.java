/*
 * Copyright 2025 JetLinks https://www.jetlinks.cn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jetlinks.reactor.ql.supports.map;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.exception.ReactorQLException;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.jetlinks.reactor.ql.utils.ExpressionUtils;

import java.util.*;
import java.util.regex.Pattern;

/**
 * JSON 函数公共支撑逻辑。
 *
 * 负责 metadata settings 安全上限、JSONPath 静态/动态路径校验和数据库函数通用流程；
 * 具体值规范化和集合语义拆到独立 helper，避免单个函数类继续膨胀。
 */
final class JsonFunctionSupport {

    static final Object EMPTY = new Object();
    static final JsonPath ROOT_PATH = JsonPath.compile("$");

    private static final Pattern INTEGER = Pattern.compile("-?\\d+");
    private static final Configuration JSON_PATH_CONF = Configuration.defaultConfiguration();

    private static final int DEFAULT_MAX_JSON_PATH_LENGTH = 1024;
    private static final int DEFAULT_MAX_JSON_TEXT_LENGTH = 2 * 1024 * 1024;
    private static final int DEFAULT_MAX_JSON_OUTPUT_LENGTH = 2 * 1024 * 1024;
    private static final int DEFAULT_MAX_JSON_DEPTH = 128;
    private static final int DEFAULT_MAX_JSON_CONTAINER_SIZE = 100_000;
    private static final int HARD_MAX_JSON_PATH_LENGTH = 8192;
    private static final int HARD_MAX_JSON_TEXT_LENGTH = 16 * 1024 * 1024;
    private static final int HARD_MAX_JSON_OUTPUT_LENGTH = 16 * 1024 * 1024;
    private static final int HARD_MAX_JSON_DEPTH = 512;
    private static final int HARD_MAX_JSON_CONTAINER_SIZE = 1_000_000;

    private JsonFunctionSupport() {
    }

    static JsonLimits jsonLimits(ReactorQLMetadata metadata) {
        return new JsonLimits(
                intSetting(metadata, JsonPathFunctionMapFeature.SETTING_MAX_JSON_PATH_LENGTH, DEFAULT_MAX_JSON_PATH_LENGTH, HARD_MAX_JSON_PATH_LENGTH),
                intSetting(metadata, JsonPathFunctionMapFeature.SETTING_MAX_JSON_TEXT_LENGTH, DEFAULT_MAX_JSON_TEXT_LENGTH, HARD_MAX_JSON_TEXT_LENGTH),
                intSetting(metadata, JsonPathFunctionMapFeature.SETTING_MAX_JSON_OUTPUT_LENGTH, DEFAULT_MAX_JSON_OUTPUT_LENGTH, HARD_MAX_JSON_OUTPUT_LENGTH),
                intSetting(metadata, JsonPathFunctionMapFeature.SETTING_MAX_JSON_DEPTH, DEFAULT_MAX_JSON_DEPTH, HARD_MAX_JSON_DEPTH),
                intSetting(metadata, JsonPathFunctionMapFeature.SETTING_MAX_JSON_CONTAINER_SIZE, DEFAULT_MAX_JSON_CONTAINER_SIZE, HARD_MAX_JSON_CONTAINER_SIZE)
        );
    }

    private static int intSetting(ReactorQLMetadata metadata, String key, int defaultValue, int hardMax) {
        if (metadata == null) {
            return defaultValue;
        }
        long value;
        try {
            value = metadata
                    .getSetting(key)
                    .map(CastUtils::castNumber)
                    .map(Number::longValue)
                    .orElse((long) defaultValue);
        } catch (RuntimeException e) {
            throw ReactorQLException.builder(ReactorQLException.INVALID_ARGUMENT)
                    .reason("JSON setting[" + key + "] 必须是数字")
                    .suggestion("将 setting 设置为允许范围内的正整数，或删除 hint 使用默认值。")
                    .example("select /*+ " + key + "(1024) */ json_get(payload, '$.id') id from test")
                    .cause(e)
                    .build();
        }
        // SQL hint 可写入 metadata settings，settings 只能在硬上限内调节，不能绕过函数自保护。
        if (value <= 0 || value > hardMax) {
            throw ReactorQLException.invalidArgument(
                    "非法 JSON setting[" + key + "]: " + value + ", hardMax=" + hardMax,
                    "将 setting 设置为允许范围内的正整数，或删除 hint 使用默认值。",
                    "select /*+ " + key + "(1024) */ json_get(payload, '$.id') id from test"
            );
        }
        return (int) value;
    }

    static JsonPath compileStaticPath(JsonLimits limits, Expression expression) {
        Optional<Object> simple = ExpressionUtils.getSimpleValue(expression);
        if (simple.isPresent()) {
            String path = String.valueOf(simple.get());
            assertSafeJsonPath(limits, path);
            return path.startsWith("$") ? compilePath(limits, path) : null;
        }
        String text = expression.toString();
        if (text.length() >= 2) {
            char first = text.charAt(0);
            char last = text.charAt(text.length() - 1);
            if ((first == '\'' && last == '\'') || (first == '"' && last == '"')) {
                String path = text.substring(1, text.length() - 1);
                if (path.startsWith("$")) {
                    assertSafeJsonPath(limits, path);
                    return compilePath(limits, path);
                }
            }
        }
        return null;
    }

    static JsonPath compilePath(JsonLimits limits, String path) {
        assertJsonPathLength(limits, path);
        try {
            return JsonPath.compile(path);
        } catch (RuntimeException e) {
            throw ReactorQLException.builder(ReactorQLException.INVALID_ARGUMENT)
                    .reason("JSONPath 编译失败: " + e.getMessage())
                    .suggestion("使用简单、确定的 JSONPath，例如 $.a.b、$['a']、$.items[0]；避免递归扫描、filter、函数和通配符。")
                    .example("select json_get(payload, '$.deviceId') deviceId from test")
                    .cause(e)
                    .build();
        }
    }

    static Object readPath(JsonLimits limits, Object document, Object pathValue, JsonPath staticPath) {
        try {
            Object normalized = normalize(limits, document);
            if (normalized == null || pathValue == null) {
                return EMPTY;
            }
            if (staticPath != null) {
                return staticPath.read(normalized, JSON_PATH_CONF);
            }
            String dynamicPath = String.valueOf(pathValue);
            assertSafeJsonPath(limits, dynamicPath);
            return JsonPath.using(JSON_PATH_CONF).parse(normalized).read(dynamicPath);
        } catch (PathNotFoundException | IllegalArgumentException e) {
            return EMPTY;
        }
    }

    static void assertSafeJsonPath(JsonLimits limits, String path) {
        assertJsonPathLength(limits, path);
        if (path.contains("..") || path.indexOf('?') >= 0 || path.indexOf('(') >= 0 || path.indexOf('*') >= 0) {
            throw ReactorQLException.invalidArgument(
                    "JSONPath 包含高风险表达式: " + path,
                    "仅使用固定字段和数组下标路径，避免 ..、?、函数调用和 * 通配符导致大量扫描或不可控资源消耗。",
                    "select json_get(payload, '$.point.lon') lon from test"
            );
        }
    }

    private static void assertJsonPathLength(JsonLimits limits, String path) {
        if (path.length() > limits.maxJsonPathLength) {
            throw ReactorQLException.resourceLimit(
                    "JSONPath 长度超过最大限制: " + path.length() + ", max=" + limits.maxJsonPathLength,
                    "缩短 JSONPath，或在可信场景下通过 function.json.maxPathLength 调整允许上限。",
                    "select json_get(payload, '$.deviceId') deviceId from test"
            );
        }
    }

    static Object value(List<Object> args, int index) {
        if (index >= args.size()) {
            return null;
        }
        Object value = args.get(index);
        return value == EMPTY ? null : value;
    }

    static Object jsonGet(JsonFunctionContext context, boolean scalar) {
        JsonLimits limits = context.limits();
        Object result = context.readPath(0, 1);
        if (result == EMPTY) {
            return context.size() > 2 ? normalize(limits, context.value(2)) : null;
        }
        result = normalize(limits, result);
        if (scalar && (result instanceof Map || result instanceof Collection)) {
            return toJson(limits, result);
        }
        return result;
    }

    static Object jsonExtract(JsonFunctionContext context) {
        JsonLimits limits = context.limits();
        Object doc = context.value(0);
        if (context.size() == 2) {
            Object result = context.readPath(0, 1);
            return result == EMPTY ? null : normalize(limits, result);
        }
        List<Object> values = new ArrayList<>();
        for (int i = 1; i < context.size(); i++) {
            Object result = context.readPath(doc, context.value(i), context.staticPath(i));
            values.add(result == EMPTY ? null : normalize(limits, result));
        }
        return values;
    }

    static Object jsonExists(JsonFunctionContext context) {
        if (context.size() > 1) {
            return context.readPath(0, 1) != EMPTY;
        }
        return context.readPath(context.value(0), "$", ROOT_PATH) != EMPTY;
    }

    static Object jsonExtractPath(JsonFunctionContext context, boolean text) {
        JsonLimits limits = context.limits();
        StringBuilder path = new StringBuilder("$");
        for (int i = 1; i < context.size(); i++) {
            path.append(toPathSegment(String.valueOf(context.value(i))));
        }
        String pathText = path.toString();
        assertJsonPathLength(limits, pathText);
        Object result = context.readPath(context.value(0), pathText, JsonPath.compile(pathText));
        if (result == EMPTY) {
            return null;
        }
        result = normalize(limits, result);
        return text ? stringifyScalar(limits, result) : result;
    }

    static Object jsonLength(JsonFunctionContext context) {
        JsonLimits limits = context.limits();
        Object target = context.value(0);
        if (context.size() > 1) {
            Object result = context.readPath(0, 1);
            target = result == EMPTY ? null : result;
        }
        target = normalize(limits, target);
        if (target == null) {
            return null;
        }
        if (target instanceof Map) {
            return ((Map<?, ?>) target).size();
        }
        if (target instanceof Collection) {
            return ((Collection<?>) target).size();
        }
        return 1;
    }

    static Object jsonKeys(JsonFunctionContext context) {
        Object target = context.value(0);
        if (context.size() > 1) {
            Object result = context.readPath(0, 1);
            target = result == EMPTY ? null : result;
        }
        return objectKeys(context.limits(), target);
    }

    static Object jsonContains(JsonFunctionContext context) {
        JsonLimits limits = context.limits();
        Object target = context.value(0);
        if (context.size() > 2) {
            Object result = context.readPath(0, 2);
            target = result == EMPTY ? null : result;
        }
        return contains(limits, normalize(limits, target), normalize(limits, context.value(1)));
    }

    static Object jsonContainsPath(JsonFunctionContext context) {
        boolean all = "all".equalsIgnoreCase(String.valueOf(context.value(1)));
        boolean any = false;
        for (int i = 2; i < context.size(); i++) {
            boolean exists = context.readPath(context.value(0), context.value(i), context.staticPath(i)) != EMPTY;
            if (exists && !all) {
                return true;
            }
            any |= exists;
            if (!exists && all) {
                return false;
            }
        }
        return all ? any : false;
    }

    static Object jsonObject(JsonLimits limits, List<Object> args) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i + 1 < args.size(); i += 2) {
            map.put(String.valueOf(value(args, i)), normalize(limits, value(args, i + 1)));
        }
        return map;
    }

    static Object reduce(JsonFunctionContext context, JsonCollectionOperations.Combiner combiner) {
        if (context.size() == 0) {
            return null;
        }
        JsonLimits limits = context.limits();
        Object result = normalize(limits, context.value(0));
        for (int i = 1; i < context.size(); i++) {
            result = combiner.combine(limits, result, normalize(limits, context.value(i)));
        }
        return result;
    }

    static Object normalize(JsonLimits limits, Object value) {
        return JsonValueSupport.normalize(limits, value);
    }

    static int jsonDepth(JsonLimits limits, Object value) {
        return JsonValueSupport.jsonDepth(limits, value);
    }

    static Object arrayLength(JsonLimits limits, Object value) {
        return JsonValueSupport.arrayLength(limits, value);
    }

    static Object objectKeys(JsonLimits limits, Object value) {
        return JsonValueSupport.objectKeys(limits, value);
    }

    static String stringifyScalar(JsonLimits limits, Object value) {
        return JsonValueSupport.stringifyScalar(limits, value);
    }

    static String quoteJsonString(JsonLimits limits, String value) {
        return JsonValueSupport.quoteJsonString(limits, value);
    }

    static String toJson(JsonLimits limits, Object value) {
        return JsonValueSupport.toJson(limits, value);
    }

    static boolean jsonValid(JsonLimits limits, Object value) {
        return JsonValueSupport.jsonValid(limits, value);
    }

    static String mysqlJsonType(JsonLimits limits, Object value) {
        return JsonValueSupport.mysqlJsonType(limits, value);
    }

    static String postgresJsonType(JsonLimits limits, Object value) {
        return JsonValueSupport.postgresJsonType(limits, value);
    }

    static boolean deepEquals(JsonLimits limits, Object left, Object right) {
        return JsonCollectionOperations.deepEquals(limits, left, right);
    }

    static boolean contains(JsonLimits limits, Object target, Object candidate) {
        return JsonCollectionOperations.contains(limits, target, candidate);
    }

    static boolean overlaps(JsonLimits limits, Object left, Object right) {
        return JsonCollectionOperations.overlaps(limits, left, right);
    }

    static Object intersect(JsonLimits limits, Object left, Object right) {
        return JsonCollectionOperations.intersect(limits, left, right);
    }

    static Object union(JsonLimits limits, Object left, Object right) {
        return JsonCollectionOperations.union(limits, left, right);
    }

    static Object diff(JsonLimits limits, Object left, Object right) {
        return JsonCollectionOperations.diff(limits, left, right);
    }

    static Object mergePreserve(JsonLimits limits, Object left, Object right) {
        return JsonCollectionOperations.mergePreserve(limits, left, right);
    }

    static Object mergePatch(JsonLimits limits, Object target, Object patch) {
        return JsonCollectionOperations.mergePatch(limits, target, patch);
    }

    private static String toPathSegment(String key) {
        if (INTEGER.matcher(key).matches()) {
            return "[" + key + "]";
        }
        return "['" + key.replace("\\", "\\\\").replace("'", "\\'") + "']";
    }

    static final class JsonLimits {
        final int maxJsonPathLength;
        final int maxJsonTextLength;
        final int maxJsonOutputLength;
        final int maxJsonDepth;
        final int maxJsonContainerSize;

        private JsonLimits(int maxJsonPathLength,
                           int maxJsonTextLength,
                           int maxJsonOutputLength,
                           int maxJsonDepth,
                           int maxJsonContainerSize) {
            this.maxJsonPathLength = maxJsonPathLength;
            this.maxJsonTextLength = maxJsonTextLength;
            this.maxJsonOutputLength = maxJsonOutputLength;
            this.maxJsonDepth = maxJsonDepth;
            this.maxJsonContainerSize = maxJsonContainerSize;
        }
    }
}
