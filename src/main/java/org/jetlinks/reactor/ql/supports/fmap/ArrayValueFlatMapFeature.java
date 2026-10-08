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
package org.jetlinks.reactor.ql.supports.fmap;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import org.apache.commons.collections.CollectionUtils;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.exception.ReactorQLException;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ValueFlatMapFeature;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.function.BiFunction;
import java.util.List;

/**
 * Expands an array, iterable or Publisher parameter into result rows using the native
 * parameter subscription and flatMap lifecycle. Each output owns a shallow Record copy;
 * source values and Context are not deep-copied or retained by the feature itself.
 *
 * <p>Example: {@code select flat_array(arr) arrValue}.</p>
 */
public class ArrayValueFlatMapFeature implements ValueFlatMapFeature {

    static String ID = FeatureId.ValueFlatMap.of("flat_array").getId();

    private final String id;

    public ArrayValueFlatMapFeature() {
        this("flat_array");
    }

    public ArrayValueFlatMapFeature(String name) {
        this.id = FeatureId.ValueFlatMap.of(name).getId();
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public BiFunction<String, Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> createMapper(Expression expression, ReactorQLMetadata metadata) {
        Function function = ((Function) expression);

        List<Expression> expressions = function.getParameters() == null
                ? null
                : function.getParameters().getExpressions();
        if (CollectionUtils.isEmpty(expressions) || expressions.size() != 1) {
            int actual = expressions == null ? 0 : expressions.size();
            throw ReactorQLException.functionArgumentCount(expression, 1, 1, actual);
        }

        Expression expr = expressions.get(0);

        java.util.function.Function<ReactorQLRecord, Publisher<?>> valueMap = ValueMapFeature.createMapperNow(expr, metadata);

        return (alias, flux) -> flux
                .flatMap(record -> Flux
                        .from(valueMap.apply(record))
                        .as(CastUtils::flatStream)
                        // A fan-out value needs its own result container, including when another
                        // flat_array stage expands this row again or the downstream retains it.
                        .map(v -> record.copy().setResult(alias, v)));
    }
}
