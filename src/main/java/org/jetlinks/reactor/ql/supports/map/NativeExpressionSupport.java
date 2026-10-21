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

import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.JsonFunction;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TrimFunction;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.exception.ReactorQLException;
import org.jetlinks.reactor.ql.feature.ScalarValueMapper;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.internal.FirstValueMapper;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.function.Function;

/** Compile dedicated parser nodes through the same function SPI and value semantics as ordinary calls. */
public final class NativeExpressionSupport {

    private NativeExpressionSupport() {
    }

    public static Function<ReactorQLRecord, Publisher<?>> extract(ExtractExpression expression, ReactorQLMetadata metadata) {
        return ValueMapFeature.createMapperNow(function("extract", new StringValue(expression.getName()),
                expression.getExpression()), metadata);
    }

    public static Function<ReactorQLRecord, Publisher<?>> json(JsonFunction expression, ReactorQLMetadata metadata) {
        String name = "JSON_" + expression.getType().name();
        if (!("JSON_VALUE".equals(name) || "JSON_QUERY".equals(name) || "JSON_EXISTS".equals(name))
                || expression.getInputExpression() == null || expression.getJsonPathExpression() == null
                || expression.getInputExpression().isUsingFormatJson()
                || expression.getInputExpression().getEncoding() != null
                || !expression.getPassingExpressions().isEmpty()
                || !expression.getAdditionalQueryPathArguments().isEmpty()
                || expression.getOnEmptyBehavior() != null || expression.getOnErrorBehavior() != null
                || expression.getWrapperType() != null || expression.getQuotesType() != null
                || expression.getScalarsType() != null || expression.isReturningFormatJson()
                || expression.getReturningEncoding() != null) {
            throw ReactorQLException.unsupportedExpression(expression,
                    "JSON 查询支持文档、路径及 JSON_VALUE RETURNING 类型；其他方言选项尚未实现。",
                    "select json_value(payload, '$.temperature' returning double) temperature from test");
        }
        Expression function = function(name, expression.getInputExpression().getExpression(),
                expression.getJsonPathExpression());
        if (expression.getReturningType() != null) {
            if (!"JSON_VALUE".equals(name)) {
                throw ReactorQLException.unsupportedExpression(expression,
                        "RETURNING 类型转换目前只支持 JSON_VALUE。", "select json_value(payload, '$.id' returning bigint) id from test");
            }
            CastExpression cast = new CastExpression();
            cast.setLeftExpression(function);
            cast.setColDataType(expression.getReturningType());
            function = cast;
        }
        return ValueMapFeature.createMapperNow(function, metadata);
    }

    public static Function<ReactorQLRecord, Publisher<?>> trim(TrimFunction expression, ReactorQLMetadata metadata) {
        if (!expression.isUsingFromKeyword()) {
            return ValueMapFeature.createMapperNow(function("trim", expression.getExpression()), metadata);
        }
        Expression characters = expression.getExpression() == null ? new StringValue(" ") : expression.getExpression();
        Function<ReactorQLRecord, Publisher<?>> source = ValueMapFeature.createMapperNow(expression.getFromExpression(), metadata);
        Function<ReactorQLRecord, Publisher<?>> chars = ValueMapFeature.createMapperNow(characters, metadata);
        TrimFunction.TrimSpecification specification = expression.getTrimSpecification();
        if (source instanceof ScalarValueMapper && chars instanceof ScalarValueMapper) {
            return (ScalarValueMapper) row -> trimValue(((ScalarValueMapper) source).applyScalar(row),
                    ((ScalarValueMapper) chars).applyScalar(row), specification);
        }
        Function<Object[], Object> trim = values -> trimValue(values[0], values[1], specification);
        return row -> Mono.zip(trim, FirstValueMapper.first(source, row), FirstValueMapper.first(chars, row));
    }

    private static Object trimValue(Object source, Object characters, TrimFunction.TrimSpecification specification) {
        if (source == null || characters == null) {
            return null;
        }
        String text = String.valueOf(source);
        String chars = String.valueOf(characters);
        int start = 0;
        int end = text.length();
        if (specification != TrimFunction.TrimSpecification.TRAILING) {
            while (start < end && chars.indexOf(text.charAt(start)) >= 0) {
                start++;
            }
        }
        if (specification != TrimFunction.TrimSpecification.LEADING) {
            while (end > start && chars.indexOf(text.charAt(end - 1)) >= 0) {
                end--;
            }
        }
        return text.substring(start, end);
    }

    private static net.sf.jsqlparser.expression.Function function(String name, Expression... arguments) {
        net.sf.jsqlparser.expression.Function function = new net.sf.jsqlparser.expression.Function();
        function.setName(name);
        function.setParameters(new ExpressionList<>(arguments));
        return function;
    }
}
