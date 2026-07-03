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
import com.jayway.jsonpath.spi.json.JsonProvider;
import org.jetlinks.reactor.ql.exception.ReactorQLException;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.util.*;

/**
 * JSON 值规范化与标量函数支持。
 *
 * 将 Java Map、Collection、数组和合法 JSON 字符串统一成可比较结构，并集中执行深度、
 * 容器大小、输入文本和输出 JSON 文本的资源限制。
 */
final class JsonValueSupport {

    private static final JsonProvider JSON_PROVIDER = Configuration.defaultConfiguration().jsonProvider();

    private JsonValueSupport() {
    }

    static Object normalize(JsonFunctionSupport.JsonLimits limits, Object value) {
        return normalize(limits, value, 0);
    }

    private static Object normalize(JsonFunctionSupport.JsonLimits limits, Object value, int depth) {
        if (value == JsonFunctionSupport.EMPTY) {
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

    private static Object normalizeText(JsonFunctionSupport.JsonLimits limits, String text, int depth) {
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

    private static Map<String, Object> normalizeMap(JsonFunctionSupport.JsonLimits limits, Map<?, ?> value, int depth) {
        assertJsonContainerSize(limits, value.size());
        Map<String, Object> map = new LinkedHashMap<>();
        value.forEach((k, v) -> map.put(String.valueOf(k), normalize(limits, v, depth + 1)));
        return map;
    }

    private static List<Object> normalizeCollection(JsonFunctionSupport.JsonLimits limits, Collection<?> value, int depth) {
        assertJsonContainerSize(limits, value.size());
        List<Object> list = new ArrayList<>(value.size());
        for (Object item : value) {
            list.add(normalize(limits, item, depth + 1));
        }
        return list;
    }

    private static List<Object> normalizeArray(JsonFunctionSupport.JsonLimits limits, Object value, int depth) {
        int len = Array.getLength(value);
        assertJsonContainerSize(limits, len);
        List<Object> list = new ArrayList<>(len);
        for (int i = 0; i < len; i++) {
            list.add(normalize(limits, Array.get(value, i), depth + 1));
        }
        return list;
    }

    private static void assertJsonStructure(JsonFunctionSupport.JsonLimits limits, Object value, int depth) {
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

    static int jsonDepth(JsonFunctionSupport.JsonLimits limits, Object value) {
        return jsonDepth(limits, normalize(limits, value), 0);
    }

    private static int jsonDepth(JsonFunctionSupport.JsonLimits limits, Object normalized, int depth) {
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

    static Object arrayLength(JsonFunctionSupport.JsonLimits limits, Object value) {
        Object normalized = normalize(limits, value);
        return normalized instanceof Collection ? ((Collection<?>) normalized).size() : null;
    }

    static Object objectKeys(JsonFunctionSupport.JsonLimits limits, Object value) {
        Object normalized = normalize(limits, value);
        if (!(normalized instanceof Map)) {
            return null;
        }
        return new ArrayList<>(((Map<?, ?>) normalized).keySet());
    }

    static String stringifyScalar(JsonFunctionSupport.JsonLimits limits, Object value) {
        Object normalized = normalize(limits, value);
        if (normalized == null) {
            return null;
        }
        if (normalized instanceof Map || normalized instanceof Collection) {
            return toJson(limits, normalized);
        }
        return String.valueOf(normalized);
    }

    static String quoteJsonString(JsonFunctionSupport.JsonLimits limits, String value) {
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

    static String toJson(JsonFunctionSupport.JsonLimits limits, Object value) {
        String json = JSON_PROVIDER.toJson(normalize(limits, value));
        assertJsonOutputLength(limits, json.length());
        return json;
    }

    static boolean jsonValid(JsonFunctionSupport.JsonLimits limits, Object value) {
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

    static String mysqlJsonType(JsonFunctionSupport.JsonLimits limits, Object value) {
        String type = jsonType(limits, value, true);
        return type == null ? null : type.toUpperCase(Locale.ENGLISH);
    }

    static String postgresJsonType(JsonFunctionSupport.JsonLimits limits, Object value) {
        return jsonType(limits, value, false);
    }

    private static String jsonType(JsonFunctionSupport.JsonLimits limits, Object value, boolean mysqlNumberTypes) {
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

    private static boolean mayBeJson(String text) {
        if (text.isEmpty()) {
            return false;
        }
        char first = text.charAt(0);
        return first == '{' || first == '[' || first == '"' || first == '-'
                || text.startsWith("true") || text.startsWith("false") || text.startsWith("null")
                || (first >= '0' && first <= '9');
    }

    private static void assertJsonDepth(JsonFunctionSupport.JsonLimits limits, int depth) {
        if (depth > limits.maxJsonDepth) {
            throw ReactorQLException.resourceLimit(
                    "JSON 结构深度超过最大限制: " + depth + ", max=" + limits.maxJsonDepth,
                    "减少嵌套层级，或在可信场景下通过 function.json.maxDepth 调整上限；实现仍会保留硬上限。",
                    "select json_depth(payload) depth from test"
            );
        }
    }

    private static void assertJsonContainerSize(JsonFunctionSupport.JsonLimits limits, int size) {
        if (size > limits.maxJsonContainerSize) {
            throw ReactorQLException.resourceLimit(
                    "JSON object/array 容器大小超过最大限制: " + size + ", max=" + limits.maxJsonContainerSize,
                    "减少单个 object/array 的元素数量，或在可信场景下通过 function.json.maxContainerSize 调整上限。",
                    "select json_length(payload, '$.items') itemSize from test"
            );
        }
    }

    private static void assertJsonTextLength(JsonFunctionSupport.JsonLimits limits, String text) {
        if (text.length() > limits.maxJsonTextLength) {
            throw ReactorQLException.resourceLimit(
                    "JSON 文本长度超过最大限制: " + text.length() + ", max=" + limits.maxJsonTextLength,
                    "缩短 JSON 文本，或在可信场景下通过 function.json.maxTextLength 调整上限；实现仍会保留硬上限。",
                    "select json_valid(payload) valid from test"
            );
        }
    }

    private static void assertJsonOutputLength(JsonFunctionSupport.JsonLimits limits, int length) {
        if (length > limits.maxJsonOutputLength) {
            throw ReactorQLException.resourceLimit(
                    "JSON 输出长度超过最大限制: " + length + ", max=" + limits.maxJsonOutputLength,
                    "减少需要字符串化的 JSON 内容，或在可信场景下通过 function.json.maxOutputLength 调整上限。",
                    "select json_unquote(json_get(payload, '$.name')) name from test"
            );
        }
    }
}
