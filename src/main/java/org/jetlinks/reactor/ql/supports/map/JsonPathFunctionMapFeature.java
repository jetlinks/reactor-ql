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
import com.jayway.jsonpath.spi.json.JsonProvider;
import lombok.Getter;
import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.jetlinks.reactor.ql.utils.ExpressionUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * JSONPath 与数据库同名 JSON 函数支持。
 *
 * 负责在 ReactorQL 行级表达式中提供 MySQL / PostgreSQL 常见 JSON 函数名，并将 Java
 * Map、Collection、数组和合法 JSON 字符串统一规范化后再执行路径读取、比较和合并。
 * 数据库同名函数优先保持数据库行为；ReactorQL 自定义集合函数仅作为扩展能力存在。
 *
 * @since 1.0.21
 */
public class JsonPathFunctionMapFeature implements ValueMapFeature {

    private static final Object EMPTY = new Object();
    private static final Pattern INTEGER = Pattern.compile("-?\\d+");
    private static final Configuration JSON_PATH_CONF = Configuration.defaultConfiguration();
    private static final JsonProvider JSON_PROVIDER = JSON_PATH_CONF.jsonProvider();
    public static final String SETTING_MAX_JSON_PATH_LENGTH = "function.json.maxPathLength";
    public static final String SETTING_MAX_JSON_TEXT_LENGTH = "function.json.maxTextLength";
    public static final String SETTING_MAX_JSON_OUTPUT_LENGTH = "function.json.maxOutputLength";
    public static final String SETTING_MAX_JSON_DEPTH = "function.json.maxDepth";
    public static final String SETTING_MAX_JSON_CONTAINER_SIZE = "function.json.maxContainerSize";

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

    @Getter
    private final String id;

    private final String name;
    private final int minParamSize;
    private final int maxParamSize;

    public JsonPathFunctionMapFeature(String name, int minParamSize, int maxParamSize) {
        this.name = name.toLowerCase(Locale.ENGLISH);
        this.minParamSize = minParamSize;
        this.maxParamSize = maxParamSize;
        this.id = FeatureId.ValueMap.of(name).getId();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression, ReactorQLMetadata metadata) {
        net.sf.jsqlparser.expression.Function function = (net.sf.jsqlparser.expression.Function) expression;
        List<Expression> expressions = function.getParameters() == null
                ? Collections.emptyList()
                : function.getParameters().getExpressions();
        if (expressions.size() < minParamSize || expressions.size() > maxParamSize) {
            throw new UnsupportedOperationException("函数[" + expression + "]参数数量错误");
        }
        JsonLimits limits = jsonLimits(metadata);
        validateJsonPathArgumentExpressions(limits, function.getName(), expressions);
        String functionName = function.getName().toLowerCase(Locale.ENGLISH);

        List<Function<ReactorQLRecord, Publisher<?>>> mappers = expressions
                .stream()
                .map(expr -> ValueMapFeature.createMapperNow(expr, metadata))
                .collect(Collectors.toList());
        JsonPath path1 = compilePathAt(limits, functionName, expressions, 1);
        JsonPath path2 = compilePathAt(limits, functionName, expressions, 2);
        JsonPath path3 = compilePathAt(limits, functionName, expressions, 3);
        List<JsonPath> restPaths = compileRestPaths(limits, functionName, expressions);
        Function<Publisher<?>, Publisher<?>> wrapper = metadata.createWrapper(expression);

        return record -> Flux
                .fromIterable(mappers)
                .concatMap(mapper -> Mono.fromDirect(mapper.apply(record)).cast(Object.class).defaultIfEmpty(EMPTY), 0)
                .collectList()
                .flatMap(args -> Mono.justOrEmpty(evaluate(limits, args, path1, path2, path3, restPaths)))
                .as(wrapper);
    }

    private Object evaluate(JsonLimits limits, List<Object> args, JsonPath path1, JsonPath path2, JsonPath path3, List<JsonPath> restPaths) {
        switch (name) {
            case "json_get":
                return jsonGet(limits, args, path1, path2, path3, restPaths, false);
            case "json_value":
                return jsonGet(limits, args, path1, path2, path3, restPaths, true);
            case "json_query":
                return jsonGet(limits, args, path1, path2, path3, restPaths, false);
            case "json_extract":
                return jsonExtract(limits, args, path1, path2, path3, restPaths);
            case "json_exists":
                return jsonExists(limits, args, path1, path2, path3, restPaths);
            case "json_extract_path":
            case "jsonb_extract_path":
                return jsonExtractPath(limits, args, false);
            case "json_extract_path_text":
            case "jsonb_extract_path_text":
                return jsonExtractPath(limits, args, true);
            case "json_unquote":
                return stringifyScalar(limits, value(args, 0));
            case "json_quote":
                return quoteJsonString(limits, String.valueOf(value(args, 0)));
            case "json_depth":
                return jsonDepth(limits, value(args, 0));
            case "json_type":
                return mysqlJsonType(limits, value(args, 0));
            case "json_typeof":
            case "jsonb_typeof":
                return postgresJsonType(limits, value(args, 0));
            case "json_valid":
                return jsonValid(limits, value(args, 0));
            case "json_length":
                return jsonLength(limits, args, path1, path2, path3, restPaths);
            case "json_array_length":
            case "jsonb_array_length":
                return arrayLength(limits, value(args, 0));
            case "json_keys":
                return jsonKeys(limits, args, path1, path2, path3, restPaths);
            case "json_object_keys":
            case "jsonb_object_keys":
                return objectKeys(limits, value(args, 0));
            case "json_contains":
                return jsonContains(limits, args, path1, path2, path3, restPaths);
            case "json_contains_path":
                return jsonContainsPath(limits, args, path1, path2, path3, restPaths);
            case "json_overlaps":
                return overlaps(limits, normalize(limits, value(args, 0)), normalize(limits, value(args, 1)));
            case "json_array":
            case "json_build_array":
            case "jsonb_build_array":
                return args.stream().map(arg -> normalize(limits, arg)).collect(Collectors.toList());
            case "json_object":
            case "json_build_object":
            case "jsonb_build_object":
                return jsonObject(limits, args);
            case "to_json":
                return normalize(limits, value(args, 0));
            case "json_equal":
            case "json_equals":
                return deepEquals(limits, normalize(limits, value(args, 0)), normalize(limits, value(args, 1)));
            case "json_intersect":
            case "json_intersection":
                return reduce(limits, args, (left, right) -> intersect(limits, left, right));
            case "json_union":
                return reduce(limits, args, (left, right) -> union(limits, left, right));
            case "json_diff":
            case "json_except":
                return reduce(limits, args, (left, right) -> diff(limits, left, right));
            case "json_merge":
            case "json_merge_preserve":
                return reduce(limits, args, JsonPathFunctionMapFeature::mergePreserve);
            case "json_merge_patch":
                return reduce(limits, args, JsonPathFunctionMapFeature::mergePatch);
            default:
                throw new UnsupportedOperationException("Unsupported json function:" + name);
        }
    }


    private static void validateJsonPathArgumentExpressions(JsonLimits limits, String functionName, List<Expression> expressions) {
        String name = functionName.toLowerCase(Locale.ENGLISH);
        if ("json_extract".equals(name)) {
            for (int i = 1; i < expressions.size(); i++) {
                assertSafeStaticJsonPath(limits, expressions.get(i));
            }
        } else if (isSingleJsonPathFunction(name)) {
            if (expressions.size() > 1) {
                assertSafeStaticJsonPath(limits, expressions.get(1));
            }
        } else if ("json_contains".equals(name)) {
            if (expressions.size() > 2) {
                assertSafeStaticJsonPath(limits, expressions.get(2));
            }
        } else if ("json_contains_path".equals(name)) {
            for (int i = 2; i < expressions.size(); i++) {
                assertSafeStaticJsonPath(limits, expressions.get(i));
            }
        }
    }

    private static boolean isSingleJsonPathFunction(String name) {
        return "json_get".equals(name)
                || "json_value".equals(name)
                || "json_query".equals(name)
                || "json_exists".equals(name)
                || "json_length".equals(name)
                || "json_keys".equals(name);
    }

    private static JsonPath compilePathAt(JsonLimits limits, String functionName, List<Expression> expressions, int index) {
        if (index >= expressions.size() || !isJsonPathArgument(functionName, index)) {
            return null;
        }
        return compileStaticPath(limits, expressions.get(index));
    }

    private static List<JsonPath> compileRestPaths(JsonLimits limits, String functionName, List<Expression> expressions) {
        if (expressions.size() <= 4) {
            return Collections.emptyList();
        }
        List<JsonPath> paths = new ArrayList<>(expressions.size() - 4);
        for (int i = 4; i < expressions.size(); i++) {
            paths.add(isJsonPathArgument(functionName, i) ? compileStaticPath(limits, expressions.get(i)) : null);
        }
        return paths;
    }

    private static boolean isJsonPathArgument(String name, int index) {
        if ("json_extract".equals(name)) {
            return index >= 1;
        }
        if (isSingleJsonPathFunction(name)) {
            return index == 1;
        }
        if ("json_contains".equals(name)) {
            return index == 2;
        }
        if ("json_contains_path".equals(name)) {
            return index >= 2;
        }
        return false;
    }

    private static void assertSafeStaticJsonPath(JsonLimits limits, Expression expression) {
        Optional<Object> simple = ExpressionUtils.getSimpleValue(expression);
        if (simple.isPresent()) {
            assertSafeJsonPath(limits, String.valueOf(simple.get()));
        }
    }

    private static void assertSafeJsonPath(JsonLimits limits, String path) {
        assertJsonPathLength(limits, path);
        if (path.contains("..") || path.indexOf('?') >= 0 || path.indexOf('(') >= 0 || path.indexOf('*') >= 0) {
            throw new UnsupportedOperationException("unsupported unsafe json path:" + path);
        }
    }

    private static void assertJsonPathLength(JsonLimits limits, String path) {
        if (path.length() > limits.maxJsonPathLength) {
            throw new UnsupportedOperationException("json path too long");
        }
    }

    private static JsonPath compileStaticPath(JsonLimits limits, Expression expression) {
        Optional<Object> simple = ExpressionUtils.getSimpleValue(expression);
        if (simple.isPresent() && String.valueOf(simple.get()).startsWith("$")) {
            String path = String.valueOf(simple.get());
            assertSafeJsonPath(limits, path);
            return JsonPath.compile(path);
        }
        String text = expression.toString();
        if (text.length() >= 2) {
            char first = text.charAt(0);
            char last = text.charAt(text.length() - 1);
            if ((first == '\'' && last == '\'') || (first == '"' && last == '"')) {
                String path = text.substring(1, text.length() - 1);
                if (path.startsWith("$")) {
                    assertSafeJsonPath(limits, path);
                    return JsonPath.compile(path);
                }
            }
        }
        return null;
    }

    private static Object value(List<Object> args, int index) {
        if (index >= args.size()) {
            return null;
        }
        Object value = args.get(index);
        return value == EMPTY ? null : value;
    }

    private static Object jsonGet(JsonLimits limits, List<Object> args, JsonPath path1, JsonPath path2, JsonPath path3, List<JsonPath> restPaths, boolean scalar) {
        Object result = readPath(limits, value(args, 0), value(args, 1), pathAt(path1, path2, path3, restPaths, 1));
        if (result == EMPTY) {
            return args.size() > 2 ? normalize(limits, value(args, 2)) : null;
        }
        result = normalize(limits, result);
        if (scalar && (result instanceof Map || result instanceof Collection)) {
            return toJson(limits, result);
        }
        return result;
    }

    private static Object jsonExtract(JsonLimits limits, List<Object> args, JsonPath path1, JsonPath path2, JsonPath path3, List<JsonPath> restPaths) {
        Object doc = value(args, 0);
        if (args.size() == 2) {
            Object result = readPath(limits, doc, value(args, 1), pathAt(path1, path2, path3, restPaths, 1));
            return result == EMPTY ? null : normalize(limits, result);
        }
        List<Object> values = new ArrayList<>();
        for (int i = 1; i < args.size(); i++) {
            Object result = readPath(limits, doc, value(args, i), pathAt(path1, path2, path3, restPaths, i));
            values.add(result == EMPTY ? null : normalize(limits, result));
        }
        return values;
    }

    private static Object jsonExists(JsonLimits limits, List<Object> args, JsonPath path1, JsonPath path2, JsonPath path3, List<JsonPath> restPaths) {
        Object path = args.size() > 1 ? value(args, 1) : "$";
        return readPath(limits, value(args, 0), path, args.size() > 1 ? pathAt(path1, path2, path3, restPaths, 1) : JsonPath.compile("$")) != EMPTY;
    }

    private static Object jsonExtractPath(JsonLimits limits, List<Object> args, boolean text) {
        StringBuilder path = new StringBuilder("$");
        for (int i = 1; i < args.size(); i++) {
            path.append(toPathSegment(String.valueOf(value(args, i))));
        }
        assertJsonPathLength(limits, path.toString());
        Object result = readPath(limits, value(args, 0), path.toString(), JsonPath.compile(path.toString()));
        if (result == EMPTY) {
            return null;
        }
        result = normalize(limits, result);
        return text ? stringifyScalar(limits, result) : result;
    }

    private static Object jsonLength(JsonLimits limits, List<Object> args, JsonPath path1, JsonPath path2, JsonPath path3, List<JsonPath> restPaths) {
        Object target = value(args, 0);
        if (args.size() > 1) {
            Object result = readPath(limits, target, value(args, 1), pathAt(path1, path2, path3, restPaths, 1));
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

    private static Object arrayLength(JsonLimits limits, Object value) {
        Object normalized = normalize(limits, value);
        return normalized instanceof Collection ? ((Collection<?>) normalized).size() : null;
    }

    private static Object jsonKeys(JsonLimits limits, List<Object> args, JsonPath path1, JsonPath path2, JsonPath path3, List<JsonPath> restPaths) {
        Object target = value(args, 0);
        if (args.size() > 1) {
            Object result = readPath(limits, target, value(args, 1), pathAt(path1, path2, path3, restPaths, 1));
            target = result == EMPTY ? null : result;
        }
        return objectKeys(limits, target);
    }

    private static Object objectKeys(JsonLimits limits, Object value) {
        Object normalized = normalize(limits, value);
        if (!(normalized instanceof Map)) {
            return null;
        }
        return new ArrayList<>(((Map<?, ?>) normalized).keySet());
    }

    private static Object jsonContains(JsonLimits limits, List<Object> args, JsonPath path1, JsonPath path2, JsonPath path3, List<JsonPath> restPaths) {
        Object target = value(args, 0);
        if (args.size() > 2) {
            Object result = readPath(limits, target, value(args, 2), pathAt(path1, path2, path3, restPaths, 2));
            target = result == EMPTY ? null : result;
        }
        return contains(limits, normalize(limits, target), normalize(limits, value(args, 1)));
    }

    private static Object jsonContainsPath(JsonLimits limits, List<Object> args, JsonPath path1, JsonPath path2, JsonPath path3, List<JsonPath> restPaths) {
        boolean all = "all".equalsIgnoreCase(String.valueOf(value(args, 1)));
        boolean any = false;
        for (int i = 2; i < args.size(); i++) {
            boolean exists = readPath(limits, value(args, 0), value(args, i), pathAt(path1, path2, path3, restPaths, i)) != EMPTY;
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


    private static int jsonDepth(JsonLimits limits, Object value) {
        return jsonDepth(limits, normalize(limits, value), 0);
    }

    private static int jsonDepth(JsonLimits limits, Object normalized, int depth) {
        assertJsonDepth(limits, depth);
        if (normalized instanceof Map) {
            int max = 0;
            for (Object child : ((Map<?, ?>) normalized).values()) {
                max = Math.max(max, jsonDepth(limits, child, depth + 1));
            }
            return max + 1;
        }
        if (normalized instanceof Collection) {
            int max = 0;
            for (Object child : ((Collection<?>) normalized)) {
                max = Math.max(max, jsonDepth(limits, child, depth + 1));
            }
            return max + 1;
        }
        return 1;
    }

    private static Object jsonObject(JsonLimits limits, List<Object> args) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i + 1 < args.size(); i += 2) {
            map.put(String.valueOf(value(args, i)), normalize(limits, value(args, i + 1)));
        }
        return map;
    }

    private static Object reduce(JsonLimits limits, List<Object> args, Combiner combiner) {
        if (args.isEmpty()) {
            return null;
        }
        Object result = normalize(limits, value(args, 0));
        for (int i = 1; i < args.size(); i++) {
            result = combiner.combine(result, normalize(limits, value(args, i)));
        }
        return result;
    }

    private static JsonPath pathAt(JsonPath path1, JsonPath path2, JsonPath path3, List<JsonPath> restPaths, int index) {
        switch (index) {
            case 1:
                return path1;
            case 2:
                return path2;
            case 3:
                return path3;
            default:
                int restIndex = index - 4;
                return restIndex >= 0 && restIndex < restPaths.size() ? restPaths.get(restIndex) : null;
        }
    }

    private static Object readPath(JsonLimits limits, Object document, Object pathValue, JsonPath staticPath) {
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

    private static Object normalize(JsonLimits limits, Object value) {
        return normalize(limits, value, 0);
    }

    private static Object normalize(JsonLimits limits, Object value, int depth) {
        if (value == EMPTY) {
            return null;
        }
        assertJsonDepth(limits, depth);
        if (value instanceof CharSequence) {
            return normalizeText(limits, String.valueOf(value), depth);
        }
        if (value instanceof Map) {
            return normalizeMap(limits, (Map<?, ?>) value, depth);
        }
        if (value instanceof Collection) {
            return normalizeCollection(limits, (Collection<?>) value, depth);
        }
        if (value != null && value.getClass().isArray()) {
            return normalizeArray(limits, value, depth);
        }
        return value;
    }

    private static Object normalizeText(JsonLimits limits, String text, int depth) {
        if (!mayBeJson(text)) {
            return text;
        }
        assertJsonTextLength(limits, text);
        Object parsed;
        try {
            parsed = JSON_PROVIDER.parse(text);
        } catch (RuntimeException ignore) {
            return text;
        }
        assertJsonStructure(limits, parsed, depth + 1);
        return parsed;
    }

    private static Map<String, Object> normalizeMap(JsonLimits limits, Map<?, ?> value, int depth) {
        assertJsonContainerSize(limits, value.size());
        Map<String, Object> map = new LinkedHashMap<>();
        value.forEach((k, v) -> map.put(String.valueOf(k), normalize(limits, v, depth + 1)));
        return map;
    }

    private static List<Object> normalizeCollection(JsonLimits limits, Collection<?> value, int depth) {
        assertJsonContainerSize(limits, value.size());
        List<Object> list = new ArrayList<>(value.size());
        for (Object item : value) {
            list.add(normalize(limits, item, depth + 1));
        }
        return list;
    }

    private static List<Object> normalizeArray(JsonLimits limits, Object value, int depth) {
        int len = Array.getLength(value);
        assertJsonContainerSize(limits, len);
        List<Object> list = new ArrayList<>(len);
        for (int i = 0; i < len; i++) {
            list.add(normalize(limits, Array.get(value, i), depth + 1));
        }
        return list;
    }

    private static void assertJsonStructure(JsonLimits limits, Object value, int depth) {
        assertJsonDepth(limits, depth);
        if (value instanceof Map) {
            assertJsonContainerSize(limits, ((Map<?, ?>) value).size());
            for (Object child : ((Map<?, ?>) value).values()) {
                assertJsonStructure(limits, child, depth + 1);
            }
            return;
        }
        if (value instanceof Collection) {
            Collection<?> collection = (Collection<?>) value;
            assertJsonContainerSize(limits, collection.size());
            for (Object child : collection) {
                assertJsonStructure(limits, child, depth + 1);
            }
            return;
        }
        Class<?> type = value == null ? null : value.getClass();
        if (type != null && type.isArray()) {
            int len = Array.getLength(value);
            assertJsonContainerSize(limits, len);
            for (int i = 0; i < len; i++) {
                assertJsonStructure(limits, Array.get(value, i), depth + 1);
            }
        }
    }

    private static void assertJsonDepth(JsonLimits limits, int depth) {
        if (depth > limits.maxJsonDepth) {
            throw new UnsupportedOperationException("json depth too deep:" + depth);
        }
    }

    private static void assertJsonContainerSize(JsonLimits limits, int size) {
        if (size > limits.maxJsonContainerSize) {
            throw new UnsupportedOperationException("json container too large:" + size);
        }
    }

    private static void assertJsonTextLength(JsonLimits limits, String text) {
        if (text.length() > limits.maxJsonTextLength) {
            throw new UnsupportedOperationException("json text too long:" + text.length());
        }
    }

    private static void assertJsonOutputLength(JsonLimits limits, int length) {
        if (length > limits.maxJsonOutputLength) {
            throw new UnsupportedOperationException("json output too long:" + length);
        }
    }

    private static JsonLimits jsonLimits(ReactorQLMetadata metadata) {
        return new JsonLimits(
                intSetting(metadata, SETTING_MAX_JSON_PATH_LENGTH, DEFAULT_MAX_JSON_PATH_LENGTH, HARD_MAX_JSON_PATH_LENGTH),
                intSetting(metadata, SETTING_MAX_JSON_TEXT_LENGTH, DEFAULT_MAX_JSON_TEXT_LENGTH, HARD_MAX_JSON_TEXT_LENGTH),
                intSetting(metadata, SETTING_MAX_JSON_OUTPUT_LENGTH, DEFAULT_MAX_JSON_OUTPUT_LENGTH, HARD_MAX_JSON_OUTPUT_LENGTH),
                intSetting(metadata, SETTING_MAX_JSON_DEPTH, DEFAULT_MAX_JSON_DEPTH, HARD_MAX_JSON_DEPTH),
                intSetting(metadata, SETTING_MAX_JSON_CONTAINER_SIZE, DEFAULT_MAX_JSON_CONTAINER_SIZE, HARD_MAX_JSON_CONTAINER_SIZE)
        );
    }

    private static int intSetting(ReactorQLMetadata metadata, String key, int defaultValue, int hardMax) {
        if (metadata == null) {
            return defaultValue;
        }
        long value = metadata
                .getSetting(key)
                .map(CastUtils::castNumber)
                .map(Number::longValue)
                .orElse((long) defaultValue);
        // SQL hint 可写入 metadata settings，settings 只能在硬上限内调节，不能绕过函数自保护。
        if (value <= 0 || value > hardMax) {
            throw new UnsupportedOperationException("invalid setting[" + key + "]:" + value);
        }
        return (int) value;
    }

    private static final class JsonLimits {
        private final int maxJsonPathLength;
        private final int maxJsonTextLength;
        private final int maxJsonOutputLength;
        private final int maxJsonDepth;
        private final int maxJsonContainerSize;

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

    private static String toPathSegment(String key) {
        if (INTEGER.matcher(key).matches()) {
            return "[" + key + "]";
        }
        return "['" + key.replace("'", "\\'") + "']";
    }

    private static String stringifyScalar(JsonLimits limits, Object value) {
        Object normalized = normalize(limits, value);
        if (normalized == null) {
            return null;
        }
        if (normalized instanceof Map || normalized instanceof Collection) {
            return toJson(limits, normalized);
        }
        return String.valueOf(normalized);
    }


    private static String quoteJsonString(JsonLimits limits, String value) {
        assertJsonTextLength(limits, value);
        StringBuilder builder = new StringBuilder(value.length() + 2);
        builder.append('"');
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            switch (ch) {
                case '"':
                    builder.append("\\\"");
                    break;
                case '\\':
                    builder.append("\\\\");
                    break;
                case '\b':
                    builder.append("\\b");
                    break;
                case '\f':
                    builder.append("\\f");
                    break;
                case '\n':
                    builder.append("\\n");
                    break;
                case '\r':
                    builder.append("\\r");
                    break;
                case '\t':
                    builder.append("\\t");
                    break;
                default:
                    if (ch < 0x20) {
                        builder.append(String.format("\\u%04x", (int) ch));
                    } else {
                        builder.append(ch);
                    }
                    break;
            }
            assertJsonOutputLength(limits, builder.length());
        }
        builder.append('"');
        assertJsonOutputLength(limits, builder.length());
        return builder.toString();
    }

    private static String toJson(JsonLimits limits, Object value) {
        String json = JSON_PROVIDER.toJson(normalize(limits, value));
        assertJsonOutputLength(limits, json.length());
        return json;
    }

    private static boolean mayBeJson(String text) {
        if (text.isEmpty()) {
            return false;
        }
        char first = text.charAt(0);
        return first == '{' || first == '[' || first == '"' || first == '-'
                || text.startsWith("true") || text.startsWith("false") || text.startsWith("null")
                || (first >= '0' && first <= '9');
    }

    private static boolean jsonValid(JsonLimits limits, Object value) {
        if (value instanceof CharSequence) {
            String text = String.valueOf(value);
            if (!mayBeJson(text)) {
                return false;
            }
            if (text.length() > limits.maxJsonTextLength) {
                return false;
            }
            try {
                assertJsonStructure(limits, JSON_PROVIDER.parse(text), 0);
                return true;
            } catch (RuntimeException e) {
                return false;
            }
        }
        return value != null;
    }

    private static String mysqlJsonType(JsonLimits limits, Object value) {
        String type = jsonType(limits, value, true);
        return type == null ? null : type.toUpperCase(Locale.ENGLISH);
    }

    private static String postgresJsonType(JsonLimits limits, Object value) {
        return jsonType(limits, value, false);
    }

    private static String jsonType(JsonLimits limits, Object value, boolean mysqlNumberTypes) {
        Object normalized = normalize(limits, value);
        if (normalized == null) {
            return "null";
        }
        if (normalized instanceof Map) {
            return "object";
        }
        if (normalized instanceof Collection) {
            return "array";
        }
        if (normalized instanceof Boolean) {
            return "boolean";
        }
        if (normalized instanceof Number) {
            if (!mysqlNumberTypes) {
                return "number";
            }
            Number number = (Number) normalized;
            if (number instanceof Float || number instanceof Double || number instanceof BigDecimal) {
                return "double";
            }
            return "integer";
        }
        return "string";
    }

    private static boolean deepEquals(JsonLimits limits, Object left, Object right) {
        Object normalizedLeft = normalize(limits, left);
        Object normalizedRight = normalize(limits, right);
        if (normalizedLeft instanceof Number && normalizedRight instanceof Number) {
            return new BigDecimal(String.valueOf(normalizedLeft)).compareTo(new BigDecimal(String.valueOf(normalizedRight))) == 0;
        }
        if (normalizedLeft instanceof Map && normalizedRight instanceof Map) {
            Map<?, ?> l = (Map<?, ?>) normalizedLeft;
            Map<?, ?> r = (Map<?, ?>) normalizedRight;
            if (l.size() != r.size()) {
                return false;
            }
            for (Map.Entry<?, ?> entry : l.entrySet()) {
                if (!r.containsKey(entry.getKey()) || !deepEquals(limits, entry.getValue(), r.get(entry.getKey()))) {
                    return false;
                }
            }
            return true;
        }
        if (normalizedLeft instanceof Collection && normalizedRight instanceof Collection) {
            Iterator<?> li = ((Collection<?>) normalizedLeft).iterator();
            Iterator<?> ri = ((Collection<?>) normalizedRight).iterator();
            while (li.hasNext() && ri.hasNext()) {
                if (!deepEquals(limits, li.next(), ri.next())) {
                    return false;
                }
            }
            return !li.hasNext() && !ri.hasNext();
        }
        return Objects.equals(normalizedLeft, normalizedRight);
    }

    private static boolean contains(JsonLimits limits, Object target, Object candidate) {
        if (target instanceof Map && candidate instanceof Map) {
            Map<?, ?> targetMap = (Map<?, ?>) target;
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) candidate).entrySet()) {
                if (!targetMap.containsKey(entry.getKey()) || !contains(limits, targetMap.get(entry.getKey()), entry.getValue())) {
                    return false;
                }
            }
            return true;
        }
        if (target instanceof Collection) {
            Collection<?> targetList = (Collection<?>) target;
            if (candidate instanceof Collection) {
                for (Object value : ((Collection<?>) candidate)) {
                    if (!containsAny(limits, targetList, value)) {
                        return false;
                    }
                }
                return true;
            }
            return containsAny(limits, targetList, candidate);
        }
        return deepEquals(limits, target, candidate);
    }

    private static boolean containsAny(JsonLimits limits, Collection<?> collection, Object value) {
        for (Object item : collection) {
            if (deepEquals(limits, item, value)) {
                return true;
            }
        }
        return false;
    }

    private static boolean overlaps(JsonLimits limits, Object left, Object right) {
        if (left instanceof Map && right instanceof Map) {
            Map<?, ?> l = (Map<?, ?>) left;
            Map<?, ?> r = (Map<?, ?>) right;
            for (Map.Entry<?, ?> entry : l.entrySet()) {
                if (r.containsKey(entry.getKey()) && overlaps(limits, entry.getValue(), r.get(entry.getKey()))) {
                    return true;
                }
            }
            return false;
        }
        if (left instanceof Collection && right instanceof Collection) {
            for (Object item : ((Collection<?>) left)) {
                if (containsAny(limits, (Collection<?>) right, item)) {
                    return true;
                }
            }
            return false;
        }
        return deepEquals(limits, left, right);
    }

    @SuppressWarnings("unchecked")
    private static Object intersect(JsonLimits limits, Object left, Object right) {
        if (left instanceof Collection && right instanceof Collection) {
            List<Object> result = new ArrayList<>();
            for (Object item : ((Collection<?>) left)) {
                if (containsAny(limits, (Collection<?>) right, item) && !containsAny(limits, result, item)) {
                    result.add(item);
                }
            }
            return result;
        }
        if (left instanceof Map && right instanceof Map) {
            Map<String, Object> result = new LinkedHashMap<>();
            Map<String, Object> l = (Map<String, Object>) left;
            Map<String, Object> r = (Map<String, Object>) right;
            for (Map.Entry<String, Object> entry : l.entrySet()) {
                if (r.containsKey(entry.getKey()) && overlaps(limits, entry.getValue(), r.get(entry.getKey()))) {
                    result.put(entry.getKey(), intersectValue(limits, entry.getValue(), r.get(entry.getKey())));
                }
            }
            return result;
        }
        return deepEquals(limits, left, right) ? left : Collections.emptyList();
    }

    @SuppressWarnings("unchecked")
    private static Object union(JsonLimits limits, Object left, Object right) {
        if (left instanceof Collection || right instanceof Collection) {
            List<Object> result = new ArrayList<>();
            appendDistinct(limits, result, left instanceof Collection ? (Collection<?>) left : Collections.singletonList(left));
            appendDistinct(limits, result, right instanceof Collection ? (Collection<?>) right : Collections.singletonList(right));
            return result;
        }
        if (left instanceof Map && right instanceof Map) {
            Map<String, Object> result = new LinkedHashMap<>((Map<String, Object>) left);
            ((Map<String, Object>) right).forEach((key, value) -> result.merge(key, value, (l, r) -> union(limits, l, r)));
            return result;
        }
        return deepEquals(limits, left, right) ? left : Arrays.asList(left, right);
    }

    @SuppressWarnings("unchecked")
    private static Object diff(JsonLimits limits, Object left, Object right) {
        if (left instanceof Collection) {
            List<Object> result = new ArrayList<>();
            Collection<?> rightList = right instanceof Collection ? (Collection<?>) right : Collections.singletonList(right);
            for (Object item : ((Collection<?>) left)) {
                if (!containsAny(limits, rightList, item)) {
                    result.add(item);
                }
            }
            return result;
        }
        if (left instanceof Map && right instanceof Map) {
            Map<String, Object> result = new LinkedHashMap<>();
            Map<String, Object> r = (Map<String, Object>) right;
            ((Map<String, Object>) left).forEach((key, value) -> {
                if (!r.containsKey(key) || !deepEquals(limits, value, r.get(key))) {
                    result.put(key, value);
                }
            });
            return result;
        }
        return deepEquals(limits, left, right) ? null : left;
    }

    private static Object intersectValue(JsonLimits limits, Object left, Object right) {
        if (left instanceof Collection || left instanceof Map) {
            return intersect(limits, left, right);
        }
        return left;
    }

    private static void appendDistinct(JsonLimits limits, List<Object> result, Collection<?> values) {
        for (Object value : values) {
            if (!containsAny(limits, result, value)) {
                result.add(value);
            }
        }
    }

    /**
     * MySQL JSON_MERGE / JSON_MERGE_PRESERVE 会保留重复 key，不能简化成 Map.putAll。
     */
    @SuppressWarnings("unchecked")
    private static Object mergePreserve(Object left, Object right) {
        if (left instanceof Map && right instanceof Map) {
            Map<String, Object> result = new LinkedHashMap<>((Map<String, Object>) left);
            ((Map<String, Object>) right).forEach((key, value) -> result.merge(key, value, JsonPathFunctionMapFeature::mergeDuplicateValue));
            return result;
        }
        List<Object> result = new ArrayList<>();
        appendMergeArray(result, left);
        appendMergeArray(result, right);
        return result;
    }

    private static Object mergeDuplicateValue(Object left, Object right) {
        if (left instanceof Map && right instanceof Map) {
            return mergePreserve(left, right);
        }
        List<Object> result = new ArrayList<>();
        appendMergeArray(result, left);
        appendMergeArray(result, right);
        return result;
    }

    private static void appendMergeArray(List<Object> result, Object value) {
        if (value instanceof Collection) {
            result.addAll((Collection<?>) value);
        } else {
            result.add(value);
        }
    }

    /**
     * MySQL JSON_MERGE_PATCH follows RFC 7396: object patch is recursive and JSON null deletes keys.
     */
    @SuppressWarnings("unchecked")
    private static Object mergePatch(Object target, Object patch) {
        if (!(patch instanceof Map)) {
            return patch;
        }
        Map<String, Object> result = target instanceof Map
                ? new LinkedHashMap<>((Map<String, Object>) target)
                : new LinkedHashMap<>();
        ((Map<String, Object>) patch).forEach((key, value) -> {
            if (value == null) {
                result.remove(key);
            } else {
                result.put(key, mergePatch(result.get(key), value));
            }
        });
        return result;
    }

    private interface Combiner {
        Object combine(Object left, Object right);
    }
}
