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

import com.jayway.jsonpath.JsonPath;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.JsonExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.exception.ReactorQLException;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * PostgreSQL/MySQL 风格 JSON 访问操作符支持。
 *
 * 负责将 JSQLParser 解析出的 {@code value->'key'}、{@code value->>'key'}、
 * {@code value#>'{path}'} 和 {@code value#>>'{path}'} 操作符链编译为静态 JSONPath；
 * 集合、对象和字符串 JSON 的读取行为复用 JSON 函数实现，避免和
 * {@code json_get/json_extract} 产生两套语义。
 */
public final class JsonOperatorMapFeature {

    private static final String JSON_OPERATOR = "->";
    private static final String JSON_TEXT_OPERATOR = "->>";
    private static final String POSTGRES_PATH_PREFIX = "#";
    private static final Pattern INTEGER = Pattern.compile("-?\\d+");

    private JsonOperatorMapFeature() {
    }

    public static Function<ReactorQLRecord, Publisher<?>> createMapper(JsonExpression expression,
                                                                       ReactorQLMetadata metadata) {
        List<String> operators = expression.getOperators();
        List<String> idents = expression.getIdents();
        if (operators == null || idents == null || operators.isEmpty() || operators.size() != idents.size()) {
            throw ReactorQLException.invalidArgument(
                    expression,
                    "JSON 操作符表达式结构不完整",
                    "使用 value->'key'、value->>'key'、value#>'{a,b}' 或 value#>>'{a,b}' 这类明确路径访问。",
                    "select payload->>'deviceId' deviceId from test"
            );
        }

        boolean scalar = JSON_TEXT_OPERATOR.equals(operators.get(operators.size() - 1));
        JsonFunctionSupport.JsonLimits limits = JsonFunctionSupport.jsonLimits(metadata);
        String path = compilePathText(expression, operators, idents, limits);
        return createMapper(expression.getExpression(), expression, scalar, path, metadata, limits);
    }

    public static Optional<Function<ReactorQLRecord, Publisher<?>>> createPostgresPathMapper(BinaryExpression expression,
                                                                                              ReactorQLMetadata metadata) {
        String operator = expression.getStringExpression();
        boolean scalar = ">>".equals(operator);
        if (!scalar && !">".equals(operator)) {
            return Optional.empty();
        }
        if (!(expression.getLeftExpression() instanceof Column) || !(expression.getRightExpression() instanceof StringValue)) {
            return Optional.empty();
        }
        Column left = ((Column) expression.getLeftExpression());
        String document = left.getFullyQualifiedName();
        if (!document.endsWith(POSTGRES_PATH_PREFIX)) {
            return Optional.empty();
        }
        document = document.substring(0, document.length() - POSTGRES_PATH_PREFIX.length());
        if (document.isEmpty()) {
            return Optional.empty();
        }

        JsonFunctionSupport.JsonLimits limits = JsonFunctionSupport.jsonLimits(metadata);
        String path = compilePostgresPathArray(((StringValue) expression.getRightExpression()).getValue(), limits);
        return Optional.of(createMapper(new Column(document), expression, scalar, path, metadata, limits));
    }

    private static Function<ReactorQLRecord, Publisher<?>> createMapper(Expression documentExpression,
                                                                        Expression sourceExpression,
                                                                        boolean scalar,
                                                                        String path,
                                                                        ReactorQLMetadata metadata,
                                                                        JsonFunctionSupport.JsonLimits limits) {
        JsonPath staticPath = JsonFunctionSupport.compilePath(limits, path);
        Function<ReactorQLRecord, Publisher<?>> documentMapper = ValueMapFeature.createMapperNow(documentExpression, metadata);
        Function<Publisher<?>, Publisher<?>> wrapper = metadata.createWrapper(sourceExpression);

        return record -> Mono
                .fromDirect(documentMapper.apply(record))
                .flatMap(document -> {
                    Object result = JsonFunctionSupport.readPath(limits, document, path, staticPath);
                    if (result == JsonFunctionSupport.EMPTY) {
                        return Mono.empty();
                    }
                    Object normalized = JsonFunctionSupport.normalize(limits, result);
                    return Mono.justOrEmpty(scalar ? JsonFunctionSupport.stringifyScalar(limits, normalized) : normalized);
                })
                .as(wrapper);
    }

    private static String compilePathText(JsonExpression expression,
                                          List<String> operators,
                                          List<String> idents,
                                          JsonFunctionSupport.JsonLimits limits) {
        StringBuilder path = new StringBuilder("$");
        for (int i = 0; i < operators.size(); i++) {
            String operator = operators.get(i);
            if (!JSON_OPERATOR.equals(operator) && !JSON_TEXT_OPERATOR.equals(operator)) {
                throw ReactorQLException.invalidArgument(
                        expression,
                        "不支持的 JSON 操作符: " + operator,
                        "仅支持 ->、->>、#>、#>> 兼容访问语义；复杂 JSONPath 请使用 json_get/json_path。",
                        "select payload->>'deviceId' deviceId from test"
                );
            }
            if (JSON_TEXT_OPERATOR.equals(operator) && i + 1 < operators.size()) {
                throw ReactorQLException.invalidArgument(
                        expression,
                        "JSON 文本操作符 ->> 只能作为最后一个操作符",
                        "先用 -> 继续访问对象或数组，最后一步再使用 ->> 转文本。",
                        "select payload->'device'->>'id' deviceId from test"
                );
            }
            appendPath(path, parseSegment(idents.get(i)), limits);
        }
        return path.toString();
    }

    private static String compilePostgresPathArray(String pathArray, JsonFunctionSupport.JsonLimits limits) {
        String text = pathArray.trim();
        if (text.startsWith("{") && text.endsWith("}")) {
            text = text.substring(1, text.length() - 1);
        }
        StringBuilder path = new StringBuilder("$");
        if (!text.isEmpty()) {
            for (String segment : text.split(",")) {
                String value = unquotePathArraySegment(segment.trim());
                appendPath(path, new Segment(value, INTEGER.matcher(value).matches(), false), limits);
            }
        }
        return path.toString();
    }

    private static void appendPath(StringBuilder path,
                                   Segment segment,
                                   JsonFunctionSupport.JsonLimits limits) {
        if (segment.jsonPath) {
            JsonFunctionSupport.assertSafeJsonPath(limits, segment.value);
            if (path.length() == 1 && path.charAt(0) == '$') {
                path.setLength(0);
                path.append(segment.value);
            } else if (segment.value.length() > 1) {
                path.append(segment.value.substring(1));
            }
            return;
        }
        if (segment.arrayIndex) {
            path.append('[').append(segment.value).append(']');
            return;
        }
        path.append(toKeySegment(segment.value));
    }

    private static Segment parseSegment(String ident) {
        String text = ident == null ? "" : ident.trim();
        if (text.length() >= 2 && text.charAt(0) == '\'' && text.charAt(text.length() - 1) == '\'') {
            String value = unquoteSqlString(text.substring(1, text.length() - 1));
            return new Segment(value, false, isJsonPathLiteral(value));
        }
        if (INTEGER.matcher(text).matches()) {
            return new Segment(text, true, false);
        }
        return new Segment(text, false, isJsonPathLiteral(text));
    }

    private static boolean isJsonPathLiteral(String value) {
        return "$".equals(value) || value.startsWith("$.") || value.startsWith("$[");
    }

    private static String unquoteSqlString(String value) {
        return value.replace("''", "'").replace("\\'", "'");
    }

    private static String unquotePathArraySegment(String value) {
        if (value.length() >= 2 && value.charAt(0) == '"' && value.charAt(value.length() - 1) == '"') {
            return value.substring(1, value.length() - 1).replace("\\\"", "\"").replace("\\\\", "\\");
        }
        return value;
    }

    private static String toKeySegment(String key) {
        return "['" + key.replace("\\", "\\\\").replace("'", "\\'") + "']";
    }

    private static final class Segment {
        private final String value;
        private final boolean arrayIndex;
        private final boolean jsonPath;

        private Segment(String value, boolean arrayIndex, boolean jsonPath) {
            this.value = value;
            this.arrayIndex = arrayIndex;
            this.jsonPath = jsonPath;
        }
    }
}
