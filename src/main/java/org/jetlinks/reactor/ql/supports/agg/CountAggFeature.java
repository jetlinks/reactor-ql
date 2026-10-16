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
import net.sf.jsqlparser.statement.select.AllColumns;
import org.apache.commons.collections.CollectionUtils;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ValueAggMapFeature;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.internal.StatefulAggregationSupport;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.function.Function;

/**
 * COUNT 聚合的原生 Publisher 实现。
 *
 * <p>参数映射保留独立订阅及其错误／取消边界。
 * 精确 DISTINCT/UNIQUE 保留每组键状态，按原集合上限终止而不淘汰。</p>
 */
public class CountAggFeature implements ValueAggMapFeature {

    public static final String ID = FeatureId.ValueAggMap.of("count").getId();

    private final boolean subscriptionCacheSafe;

    public CountAggFeature() {
        this(false);
    }

    public CountAggFeature(boolean subscriptionCacheSafe) {
        this.subscriptionCacheSafe = subscriptionCacheSafe;
    }


    @Override
    public Function<Flux<ReactorQLRecord>, Flux<Object>> createMapper(Expression expression, ReactorQLMetadata metadata) {

        net.sf.jsqlparser.expression.Function function = ((net.sf.jsqlparser.expression.Function) expression);

        if (function.isAllColumns()
                || function.getParameters() == null
                || CollectionUtils.isEmpty(function.getParameters().getExpressions())) {
            return flux -> flux.count().cast(Object.class).flux();
        }

        Expression expr = function.getParameters().getExpressions().get(0);
        if (expr instanceof AllColumns) {
            return flux -> flux.count().cast(Object.class).flux();
        }

        Function<ReactorQLRecord, Publisher<?>> mapper = ValueMapFeature.createMapperNow(expr, metadata);
        // Preserve the native mapping subscription's error and cancellation lifecycle,
        // including failures in downstream exact-state collectors.
        Function<Flux<ReactorQLRecord>, Flux<Object>> valueMapper =
                flux -> metadata.flatMap(flux, mapper);

        //去重记数
        if (function.isDistinct()) {
            int max = StatefulAggregationSupport.readLimit(metadata);
            return flux -> valueMapper
                    .apply(flux)
                    .transform(values -> StatefulAggregationSupport.countDistinct(values, max).flux())
                    .cast(Object.class);
        }
        //统计唯一值的个数
        if (function.isUnique()) {
            int max = StatefulAggregationSupport.readLimit(metadata);
            return flux -> valueMapper
                    .apply(flux)
                    .transform(values -> StatefulAggregationSupport.countUnique(values, max).flux())
                    .cast(Object.class);
        }

        return flux -> valueMapper
                .apply(flux)
                .count()
                .cast(Object.class).flux();


    }

    @Override
    public String getId() {
        return ID;
    }

    @Override
    public boolean isSubscriptionCacheSafe() {
        return subscriptionCacheSafe;
    }

}
