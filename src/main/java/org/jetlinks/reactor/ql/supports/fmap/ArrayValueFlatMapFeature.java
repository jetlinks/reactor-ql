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
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ValueFlatMapFeature;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.function.BiFunction;

/**
 * select flat_array(arr) arrValue
 */
public class ArrayValueFlatMapFeature implements ValueFlatMapFeature {

    static String ID = FeatureId.ValueFlatMap.of("flat_array").getId();

    @Override
    public String getId() {
        return ID;
    }

    @Override
    public BiFunction<String, Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> createMapper(Expression expression, ReactorQLMetadata metadata) {
        Function function = ((Function) expression);

        if (function.getParameters() == null || CollectionUtils.isEmpty(function.getParameters().getExpressions())) {
            throw new IllegalArgumentException("函数[" + expression + "]参数不能为空");
        }

        Expression expr = function.getParameters().getExpressions().get(0);

        java.util.function.Function<ReactorQLRecord, Publisher<?>> valueMap = ValueMapFeature.createMapperNow(expr, metadata);

        return (alias, flux) -> flux
                .flatMap(record -> Flux
                        .from(valueMap.apply(record))
                        .as(CastUtils::flatStream)
                        .map(v -> record.setResult(alias, v)));
    }
}
