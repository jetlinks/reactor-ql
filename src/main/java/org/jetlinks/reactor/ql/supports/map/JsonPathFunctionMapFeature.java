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
        validateJsonPathArgumentExpressions(function.getName(), expressions);

        List<Function<ReactorQLRecord, Publisher<?>>> mappers = expressions
                .stream()
                .map(expr -> ValueMapFeature.createMapperNow(expr, metadata))
                .collect(Collectors.toList());
        JsonPath path1 = expressions.size() > 1 ? compileStaticPath(expressions.get(1)) : null;
        JsonPath path2 = expressions.size() > 2 ? compileStaticPath(expressions.get(2)) : null;
        JsonPath path3 = expressions.size() > 3 ? compileStaticPath(expressions.get(3)) : null;
        List<JsonPath> restPaths = expressions.size() > 4
                ? expressions.subList(4, expressions.size())
                             .stream()
                             .map(JsonPathFunctionMapFeature::compileStaticPath)
                             .collect(Collectors.toList())
                : Collections.emptyList();
        Function<Publisher<?>, Publisher<?>> wrapper = metadata.createWrapper(expression);

        return record -> Flux
                .fromIterable(mappers)
                .concatMap(mapper -> Mono.fromDirect(mapper.apply(record)).cast(Object.class).defaultIfEmpty(EMPTY), 0)
                .collectList()
                .flatMap(args -> Mono.justOrEmpty(evaluate(args, path1, path2, path3, restPaths)))
                .as(wrapper);
    }

    private Object evaluate(List<Object> args, JsonPath path1, JsonPath path2, JsonPath path3, List<JsonPath> restPaths) {
        switch (name) {
            case "json_get":
                return jsonGet(args, path1, path2, path3, restPaths, false, false);
            case "json_value":
                return jsonGet(args, path1, path2, path3, restPaths, true, false);
            case "json_query":
                return jsonGet(args, path1, path2, path3, restPaths, false, true);
            case "json_extract":
                return jsonExtract(args, path1, path2, path3, restPaths);
            case "json_exists":
                return jsonExists(args, path1, path2, path3, restPaths);
            case "json_extract_path":
            case "jsonb_extract_path":
                return jsonExtractPath(args, false);
            case "json_extract_path_text":
            case "jsonb_extract_path_text":
                return jsonExtractPath(args, true);
            case "json_unquote":
                return stringifyScalar(value(args, 0));
            case "json_quote":
                return quoteJsonString(String.valueOf(value(args, 0)));
            case "json_depth":
                return jsonDepth(value(args, 0));
            case "json_type":
                return mysqlJsonType(value(args, 0));
            case "json_typeof":
            case "jsonb_typeof":
                return postgresJsonType(value(args, 0));
            case "json_valid":
                return jsonValid(value(args, 0));
            case "json_length":
                return jsonLength(args, path1, path2, path3, restPaths);
            case "json_array_length":
            case "jsonb_array_length":
                return arrayLength(value(args, 0));
            case "json_keys":
                return jsonKeys(args, path1, path2, path3, restPaths);
            case "json_object_keys":
            case "jsonb_object_keys":
                return objectKeys(value(args, 0));
            case "json_contains":
                return jsonContains(args, path1, path2, path3, restPaths);
            case "json_contains_path":
                return jsonContainsPath(args, path1, path2, path3, restPaths);
            case "json_overlaps":
                return overlaps(normalize(value(args, 0)), normalize(value(args, 1)));
            case "json_array":
            case "json_build_array":
            case "jsonb_build_array":
                return args.stream().map(JsonPathFunctionMapFeature::normalize).collect(Collectors.toList());
            case "json_object":
            case "json_build_object":
            case "jsonb_build_object":
                return jsonObject(args);
            case "to_json":
                return normalize(value(args, 0));
            case "json_equal":
            case "json_equals":
                return deepEquals(normalize(value(args, 0)), normalize(value(args, 1)));
            case "json_intersect":
            case "json_intersection":
                return reduce(args, JsonPathFunctionMapFeature::intersect);
            case "json_union":
                return reduce(args, JsonPathFunctionMapFeature::union);
            case "json_diff":
            case "json_except":
                return reduce(args, JsonPathFunctionMapFeature::diff);
            case "json_merge":
            case "json_merge_preserve":
                return reduce(args, JsonPathFunctionMapFeature::mergePreserve);
            case "json_merge_patch":
                return reduce(args, JsonPathFunctionMapFeature::mergePatch);
            default:
                throw new UnsupportedOperationException("Unsupported json function:" + name);
        }
    }


    private static void validateJsonPathArgumentExpressions(String functionName, List<Expression> expressions) {
        String name = functionName.toLowerCase(Locale.ENGLISH);
        if ("json_get".equals(name) || "json_extract".equals(name) || "json_value".equals(name) || "json_query".equals(name)) {
            for (int i = 1; i < expressions.size(); i++) {
                assertSafeStaticJsonPath(expressions.get(i));
            }
        } else if ("json_exists".equals(name) || "json_length".equals(name) || "json_keys".equals(name)) {
            if (expressions.size() > 1) {
                assertSafeStaticJsonPath(expressions.get(1));
            }
        } else if ("json_contains".equals(name)) {
            if (expressions.size() > 2) {
                assertSafeStaticJsonPath(expressions.get(2));
            }
        } else if ("json_contains_path".equals(name)) {
            for (int i = 2; i < expressions.size(); i++) {
                assertSafeStaticJsonPath(expressions.get(i));
            }
        }
    }

    private static void assertSafeStaticJsonPath(Expression expression) {
        Optional<Object> simple = ExpressionUtils.getSimpleValue(expression);
        if (simple.isPresent()) {
            assertSafeJsonPath(String.valueOf(simple.get()));
        }
    }

    private static void assertSafeJsonPath(String path) {
        if (path.length() > 1024) {
            throw new UnsupportedOperationException("json path too long");
        }
        if (path.contains("..") || path.indexOf('?') >= 0 || path.indexOf('(') >= 0 || path.indexOf('*') >= 0) {
            throw new UnsupportedOperationException("unsupported unsafe json path:" + path);
        }
    }

    private static JsonPath compileStaticPath(Expression expression) {
        Optional<Object> simple = ExpressionUtils.getSimpleValue(expression);
        if (simple.isPresent() && String.valueOf(simple.get()).startsWith("$")) {
            String path = String.valueOf(simple.get());
            assertSafeJsonPath(path);
            return JsonPath.compile(path);
        }
        String text = expression.toString();
        if (text.length() >= 2) {
            char first = text.charAt(0);
            char last = text.charAt(text.length() - 1);
            if ((first == '\'' && last == '\'') || (first == '"' && last == '"')) {
                String path = text.substring(1, text.length() - 1);
                if (path.startsWith("$")) {
                    assertSafeJsonPath(path);
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

    private static Object jsonGet(List<Object> args, JsonPath path1, JsonPath path2, JsonPath path3, List<JsonPath> restPaths, boolean scalar, boolean query) {
        Object result = readPath(value(args, 0), value(args, 1), pathAt(path1, path2, path3, restPaths, 1));
        if (result == EMPTY) {
            return args.size() > 2 ? normalize(value(args, 2)) : null;
        }
        result = normalize(result);
        if (scalar && (result instanceof Map || result instanceof Collection)) {
            return toJson(result);
        }
        return result;
    }

    private static Object jsonExtract(List<Object> args, JsonPath path1, JsonPath path2, JsonPath path3, List<JsonPath> restPaths) {
        Object doc = value(args, 0);
        if (args.size() == 2) {
            Object result = readPath(doc, value(args, 1), pathAt(path1, path2, path3, restPaths, 1));
            return result == EMPTY ? null : normalize(result);
        }
        List<Object> values = new ArrayList<>();
        for (int i = 1; i < args.size(); i++) {
            Object result = readPath(doc, value(args, i), pathAt(path1, path2, path3, restPaths, i));
            values.add(result == EMPTY ? null : normalize(result));
        }
        return values;
    }

    private static Object jsonExists(List<Object> args, JsonPath path1, JsonPath path2, JsonPath path3, List<JsonPath> restPaths) {
        Object path = args.size() > 1 ? value(args, 1) : "$";
        return readPath(value(args, 0), path, args.size() > 1 ? pathAt(path1, path2, path3, restPaths, 1) : JsonPath.compile("$")) != EMPTY;
    }

    private static Object jsonExtractPath(List<Object> args, boolean text) {
        StringBuilder path = new StringBuilder("$");
        for (int i = 1; i < args.size(); i++) {
            path.append(toPathSegment(String.valueOf(value(args, i))));
        }
        Object result = readPath(value(args, 0), path.toString(), JsonPath.compile(path.toString()));
        if (result == EMPTY) {
            return null;
        }
        result = normalize(result);
        return text ? stringifyScalar(result) : result;
    }

    private static Object jsonLength(List<Object> args, JsonPath path1, JsonPath path2, JsonPath path3, List<JsonPath> restPaths) {
        Object target = value(args, 0);
        if (args.size() > 1) {
            Object result = readPath(target, value(args, 1), pathAt(path1, path2, path3, restPaths, 1));
            target = result == EMPTY ? null : result;
        }
        target = normalize(target);
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

    private static Object arrayLength(Object value) {
        Object normalized = normalize(value);
        return normalized instanceof Collection ? ((Collection<?>) normalized).size() : null;
    }

    private static Object jsonKeys(List<Object> args, JsonPath path1, JsonPath path2, JsonPath path3, List<JsonPath> restPaths) {
        Object target = value(args, 0);
        if (args.size() > 1) {
            Object result = readPath(target, value(args, 1), pathAt(path1, path2, path3, restPaths, 1));
            target = result == EMPTY ? null : result;
        }
        return objectKeys(target);
    }

    private static Object objectKeys(Object value) {
        Object normalized = normalize(value);
        if (!(normalized instanceof Map)) {
            return null;
        }
        return new ArrayList<>(((Map<?, ?>) normalized).keySet());
    }

    private static Object jsonContains(List<Object> args, JsonPath path1, JsonPath path2, JsonPath path3, List<JsonPath> restPaths) {
        Object target = value(args, 0);
        if (args.size() > 2) {
            Object result = readPath(target, value(args, 2), pathAt(path1, path2, path3, restPaths, 2));
            target = result == EMPTY ? null : result;
        }
        return contains(normalize(target), normalize(value(args, 1)));
    }

    private static Object jsonContainsPath(List<Object> args, JsonPath path1, JsonPath path2, JsonPath path3, List<JsonPath> restPaths) {
        boolean all = "all".equalsIgnoreCase(String.valueOf(value(args, 1)));
        boolean any = false;
        for (int i = 2; i < args.size(); i++) {
            boolean exists = readPath(value(args, 0), value(args, i), pathAt(path1, path2, path3, restPaths, i)) != EMPTY;
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


    private static int jsonDepth(Object value) {
        Object normalized = normalize(value);
        if (normalized instanceof Map) {
            int max = 0;
            for (Object child : ((Map<?, ?>) normalized).values()) {
                max = Math.max(max, jsonDepth(child));
            }
            return max + 1;
        }
        if (normalized instanceof Collection) {
            int max = 0;
            for (Object child : ((Collection<?>) normalized)) {
                max = Math.max(max, jsonDepth(child));
            }
            return max + 1;
        }
        return 1;
    }

    private static Object jsonObject(List<Object> args) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i + 1 < args.size(); i += 2) {
            map.put(String.valueOf(value(args, i)), normalize(value(args, i + 1)));
        }
        return map;
    }

    private static Object reduce(List<Object> args, Combiner combiner) {
        if (args.isEmpty()) {
            return null;
        }
        Object result = normalize(value(args, 0));
        for (int i = 1; i < args.size(); i++) {
            result = combiner.combine(result, normalize(value(args, i)));
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

    private static Object readPath(Object document, Object pathValue, JsonPath staticPath) {
        try {
            Object normalized = normalize(document);
            if (normalized == null || pathValue == null) {
                return EMPTY;
            }
            if (staticPath != null) {
                return staticPath.read(normalized, JSON_PATH_CONF);
            }
            String dynamicPath = String.valueOf(pathValue);
            assertSafeJsonPath(dynamicPath);
            return JsonPath.using(JSON_PATH_CONF).parse(normalized).read(dynamicPath);
        } catch (PathNotFoundException | IllegalArgumentException e) {
            return EMPTY;
        }
    }

    private static Object normalize(Object value) {
        if (value == EMPTY) {
            return null;
        }
        if (value instanceof CharSequence) {
            String text = String.valueOf(value);
            if (!mayBeJson(text)) {
                return text;
            }
            try {
                return JSON_PROVIDER.parse(text);
            } catch (RuntimeException ignore) {
                return text;
            }
        }
        if (value instanceof Map) {
            Map<String, Object> map = new LinkedHashMap<>();
            ((Map<?, ?>) value).forEach((k, v) -> map.put(String.valueOf(k), normalize(v)));
            return map;
        }
        if (value instanceof Collection) {
            return ((Collection<?>) value).stream().map(JsonPathFunctionMapFeature::normalize).collect(Collectors.toList());
        }
        Class<?> type = value == null ? null : value.getClass();
        if (type != null && type.isArray()) {
            int len = Array.getLength(value);
            List<Object> list = new ArrayList<>(len);
            for (int i = 0; i < len; i++) {
                list.add(normalize(Array.get(value, i)));
            }
            return list;
        }
        return value;
    }

    private static String toPathSegment(String key) {
        if (INTEGER.matcher(key).matches()) {
            return "[" + key + "]";
        }
        return "['" + key.replace("'", "\\'") + "']";
    }

    private static String stringifyScalar(Object value) {
        Object normalized = normalize(value);
        if (normalized == null) {
            return null;
        }
        if (normalized instanceof Map || normalized instanceof Collection) {
            return toJson(normalized);
        }
        return String.valueOf(normalized);
    }


    private static String quoteJsonString(String value) {
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
        }
        return builder.append('"').toString();
    }

    private static String toJson(Object value) {
        return JSON_PROVIDER.toJson(normalize(value));
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

    private static boolean jsonValid(Object value) {
        if (value instanceof CharSequence) {
            String text = String.valueOf(value);
            if (!mayBeJson(text)) {
                return false;
            }
            try {
                JSON_PROVIDER.parse(text);
                return true;
            } catch (RuntimeException e) {
                return false;
            }
        }
        return value != null;
    }

    private static String mysqlJsonType(Object value) {
        String type = jsonType(value, true);
        return type == null ? null : type.toUpperCase(Locale.ENGLISH);
    }

    private static String postgresJsonType(Object value) {
        return jsonType(value, false);
    }

    private static String jsonType(Object value, boolean mysqlNumberTypes) {
        Object normalized = normalize(value);
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

    private static boolean deepEquals(Object left, Object right) {
        left = normalize(left);
        right = normalize(right);
        if (left instanceof Number && right instanceof Number) {
            return new BigDecimal(String.valueOf(left)).compareTo(new BigDecimal(String.valueOf(right))) == 0;
        }
        if (left instanceof Map && right instanceof Map) {
            Map<?, ?> l = (Map<?, ?>) left;
            Map<?, ?> r = (Map<?, ?>) right;
            if (l.size() != r.size()) {
                return false;
            }
            for (Map.Entry<?, ?> entry : l.entrySet()) {
                if (!r.containsKey(entry.getKey()) || !deepEquals(entry.getValue(), r.get(entry.getKey()))) {
                    return false;
                }
            }
            return true;
        }
        if (left instanceof Collection && right instanceof Collection) {
            Iterator<?> li = ((Collection<?>) left).iterator();
            Iterator<?> ri = ((Collection<?>) right).iterator();
            while (li.hasNext() && ri.hasNext()) {
                if (!deepEquals(li.next(), ri.next())) {
                    return false;
                }
            }
            return !li.hasNext() && !ri.hasNext();
        }
        return Objects.equals(left, right);
    }

    private static boolean contains(Object target, Object candidate) {
        if (target instanceof Map && candidate instanceof Map) {
            Map<?, ?> targetMap = (Map<?, ?>) target;
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) candidate).entrySet()) {
                if (!targetMap.containsKey(entry.getKey()) || !contains(targetMap.get(entry.getKey()), entry.getValue())) {
                    return false;
                }
            }
            return true;
        }
        if (target instanceof Collection) {
            Collection<?> targetList = (Collection<?>) target;
            if (candidate instanceof Collection) {
                for (Object value : ((Collection<?>) candidate)) {
                    if (!containsAny(targetList, value)) {
                        return false;
                    }
                }
                return true;
            }
            return containsAny(targetList, candidate);
        }
        return deepEquals(target, candidate);
    }

    private static boolean containsAny(Collection<?> collection, Object value) {
        for (Object item : collection) {
            if (deepEquals(item, value)) {
                return true;
            }
        }
        return false;
    }

    private static boolean overlaps(Object left, Object right) {
        if (left instanceof Map && right instanceof Map) {
            Map<?, ?> l = (Map<?, ?>) left;
            Map<?, ?> r = (Map<?, ?>) right;
            for (Map.Entry<?, ?> entry : l.entrySet()) {
                if (r.containsKey(entry.getKey()) && overlaps(entry.getValue(), r.get(entry.getKey()))) {
                    return true;
                }
            }
            return false;
        }
        if (left instanceof Collection && right instanceof Collection) {
            for (Object item : ((Collection<?>) left)) {
                if (containsAny((Collection<?>) right, item)) {
                    return true;
                }
            }
            return false;
        }
        return deepEquals(left, right);
    }

    @SuppressWarnings("unchecked")
    private static Object intersect(Object left, Object right) {
        if (left instanceof Collection && right instanceof Collection) {
            List<Object> result = new ArrayList<>();
            for (Object item : ((Collection<?>) left)) {
                if (containsAny((Collection<?>) right, item) && !containsAny(result, item)) {
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
                if (r.containsKey(entry.getKey()) && overlaps(entry.getValue(), r.get(entry.getKey()))) {
                    result.put(entry.getKey(), intersectValue(entry.getValue(), r.get(entry.getKey())));
                }
            }
            return result;
        }
        return deepEquals(left, right) ? left : Collections.emptyList();
    }

    @SuppressWarnings("unchecked")
    private static Object union(Object left, Object right) {
        if (left instanceof Collection || right instanceof Collection) {
            List<Object> result = new ArrayList<>();
            appendDistinct(result, left instanceof Collection ? (Collection<?>) left : Collections.singletonList(left));
            appendDistinct(result, right instanceof Collection ? (Collection<?>) right : Collections.singletonList(right));
            return result;
        }
        if (left instanceof Map && right instanceof Map) {
            Map<String, Object> result = new LinkedHashMap<>((Map<String, Object>) left);
            ((Map<String, Object>) right).forEach((key, value) -> result.merge(key, value, JsonPathFunctionMapFeature::union));
            return result;
        }
        return deepEquals(left, right) ? left : Arrays.asList(left, right);
    }

    @SuppressWarnings("unchecked")
    private static Object diff(Object left, Object right) {
        if (left instanceof Collection) {
            List<Object> result = new ArrayList<>();
            Collection<?> rightList = right instanceof Collection ? (Collection<?>) right : Collections.singletonList(right);
            for (Object item : ((Collection<?>) left)) {
                if (!containsAny(rightList, item)) {
                    result.add(item);
                }
            }
            return result;
        }
        if (left instanceof Map && right instanceof Map) {
            Map<String, Object> result = new LinkedHashMap<>();
            Map<String, Object> r = (Map<String, Object>) right;
            ((Map<String, Object>) left).forEach((key, value) -> {
                if (!r.containsKey(key) || !deepEquals(value, r.get(key))) {
                    result.put(key, value);
                }
            });
            return result;
        }
        return deepEquals(left, right) ? null : left;
    }

    private static Object intersectValue(Object left, Object right) {
        if (left instanceof Collection || left instanceof Map) {
            return intersect(left, right);
        }
        return left;
    }

    private static void appendDistinct(List<Object> result, Collection<?> values) {
        for (Object value : values) {
            if (!containsAny(result, value)) {
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
