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
import org.jetlinks.reactor.ql.DefaultReactorQL;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.exception.ReactorQLException;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ValueAggMapFeature;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.internal.BoundedStateSupport;
import org.jetlinks.reactor.ql.internal.StatefulAggregationSupport;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class CollectRowAggMapFeature implements ValueAggMapFeature {

    private static final String ID = FeatureId.ValueAggMap.of("collect_row").getId();
    private static final String LIMIT_SUGGESTION =
            "增加窗口、缩小输入范围或在可信场景下调大受硬上限保护的配置。";
    private static final String LIMIT_EXAMPLE =
            "select collect_row(name, value) rows from test group by _window(1000)";

    @Override
    public Function<Flux<ReactorQLRecord>, Flux<Object>> createMapper(Expression expression, ReactorQLMetadata metadata) {
        Mappers mappers = createMappers(expression, metadata);
        int maxCollectionSize = StatefulAggregationSupport.readLimit(metadata);
        // Keep parameter failures in the native mapping scope, separate from collection failures.
        return flux -> metadata
                .flatMap(flux,
                         record -> Mono.zip(
                                 Mono.from(mappers.key.apply(record)),
                                 Mono.from(mappers.value.apply(record))
                         ))
                .collect(
                        HashMap::new,
                        (Map<Object, Object> rows, Tuple2<?, ?> tuple) -> put(
                                rows,
                                tuple.getT1(),
                                tuple.getT2(),
                                maxCollectionSize
                        )
                )
                .cast(Object.class)
                .flux();
    }

    private Mappers createMappers(Expression expression, ReactorQLMetadata metadata) {
        net.sf.jsqlparser.expression.Function function =
                (net.sf.jsqlparser.expression.Function) expression;
        List<Expression> expressions;
        if (function.getParameters() == null || CollectionUtils.isEmpty(expressions = function
                .getParameters()
                .getExpressions())) {
            throw ReactorQLException.functionArgumentCount(expression, 2, 2, 0);
        }
        if (expressions.size() != 2) {
            throw ReactorQLException.functionArgumentCount(expression, 2, 2, expressions.size());
        }
        return new Mappers(
                ValueMapFeature.createMapperNow(expressions.get(0), metadata),
                ValueMapFeature.createMapperNow(expressions.get(1), metadata)
        );
    }

    private void put(Map<Object, Object> rows,
                     Object key,
                     Object value,
                     int maxCollectionSize) {
        BoundedStateSupport.put(
                rows,
                key,
                value,
                maxCollectionSize,
                DefaultReactorQL.SETTING_AGGREGATE_MAX_COLLECTION_SIZE,
                LIMIT_SUGGESTION,
                LIMIT_EXAMPLE
        );
    }

    @Override
    public String getId() {
        return ID;
    }

    private static final class Mappers {

        private final Function<ReactorQLRecord, Publisher<?>> key;
        private final Function<ReactorQLRecord, Publisher<?>> value;

        private Mappers(Function<ReactorQLRecord, Publisher<?>> key,
                        Function<ReactorQLRecord, Publisher<?>> value) {
            this.key = key;
            this.value = value;
        }
    }
}
