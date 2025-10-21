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
package org.jetlinks.reactor.ql.supports.agg;

import net.sf.jsqlparser.expression.Expression;
import org.apache.commons.collections.CollectionUtils;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ValueAggMapFeature;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.util.List;
import java.util.function.Function;

public class CollectRowAggMapFeature implements ValueAggMapFeature {

    private static final String ID = FeatureId.ValueAggMap.of("collect_row").getId();

    @Override
    public Function<Flux<ReactorQLRecord>, Flux<Object>> createMapper(Expression expression, ReactorQLMetadata metadata) {

        net.sf.jsqlparser.expression.Function function = ((net.sf.jsqlparser.expression.Function) expression);

        List<Expression> expressions;
        if (function.getParameters() == null || CollectionUtils.isEmpty(expressions = function
                .getParameters()
                .getExpressions())) {
            throw new IllegalArgumentException("函数参数不能为空:" + expression);
        }
        if (expressions.size() != 2) {
            throw new IllegalArgumentException("函数参数数量必须为2:" + expression);
        }

        Function<ReactorQLRecord, Publisher<?>> key = ValueMapFeature.createMapperNow(expressions.get(0), metadata);
        Function<ReactorQLRecord, Publisher<?>> value = ValueMapFeature.createMapperNow(expressions.get(1), metadata);

        return flux -> metadata
                .flatMap(flux,
                         record -> Mono.zip(
                                 Mono.from(key.apply(record)),
                                 Mono.from(value.apply(record))
                         ))
                .collectMap(Tuple2::getT1, Tuple2::getT2)
                .cast(Object.class)
                .flux();
    }

    @Override
    public String getId() {
        return ID;
    }
}
