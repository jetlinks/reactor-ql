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

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.SubSelect;
import org.jetlinks.reactor.ql.DefaultReactorQL;
import org.jetlinks.reactor.ql.ReactorQLContext;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FromFeature;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.internal.BoundedStateSupport;
import org.jetlinks.reactor.ql.internal.ExistsValueMapper;
import org.jetlinks.reactor.ql.internal.SubscriptionContext;
import org.jetlinks.reactor.ql.supports.SubqueryCorrelationAnalyzer;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Function;


public class SelectFeature implements ValueMapFeature {

    private final static String ID = FeatureId.ValueMap.select.getId();

    @Override
    public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression, ReactorQLMetadata metadata) {
        SubSelect select = ((SubSelect) expression);

        String alias = select.getAlias() != null ? select.getAlias().getName() : null;

        Function<ReactorQLContext, Flux<ReactorQLRecord>> mapper = FromFeature.createFromMapperByFrom(select, metadata);

        Function<ReactorQLRecord, Flux<Object>> execute = record -> mapper
                .apply(record.bindNamedRecords(record.getContext()
                        .transfer((table, source) -> source
                                .map(val -> ReactorQLRecord
                                        .newRecord(alias, val, record.getContext())
                                        .addNamedRecords(record)))))
                .map(ReactorQLRecord::getRecord);

        boolean cacheable = metadata
                .getSetting(DefaultReactorQL.SETTING_SUBQUERY_CACHE)
                .map(CastUtils::castBoolean)
                .orElse(true)
                && SubqueryCorrelationAnalyzer.isSubscriptionCacheable(select, metadata);
        if (!cacheable) {
            return new SubqueryMapper(execute, false, 0);
        }
        int maxRows = readMaxRows(metadata);
        metadata.setting(DefaultReactorQL.SETTING_SUBQUERY_CACHE_ACTIVE, true);
        return new SubqueryMapper(execute, true, maxRows);

    }

    private int readMaxRows(ReactorQLMetadata metadata) {
        return BoundedStateSupport.readLimit(
                metadata,
                DefaultReactorQL.SETTING_SUBQUERY_MAX_ROWS,
                DefaultReactorQL.DEFAULT_SUBQUERY_MAX_ROWS,
                DefaultReactorQL.HARD_MAX_SUBQUERY_ROWS,
                "使用 1 到 " + DefaultReactorQL.HARD_MAX_SUBQUERY_ROWS + " 之间的结果行上限。",
                DefaultReactorQL.SETTING_SUBQUERY_MAX_ROWS + "=65536"
        );
    }

    @Override
    public String getId() {
        return ID;
    }

    private static final class SubqueryMapper implements ExistsValueMapper {

        private final Function<ReactorQLRecord, Flux<Object>> execute;
        private final boolean cacheable;
        private final int maxRows;
        private final Object rowsCacheKey = new Object();
        private final Object existsCacheKey = new Object();

        private SubqueryMapper(Function<ReactorQLRecord, Flux<Object>> execute,
                               boolean cacheable,
                               int maxRows) {
            this.execute = execute;
            this.cacheable = cacheable;
            this.maxRows = maxRows;
        }

        @Override
        public Publisher<?> apply(ReactorQLRecord record) {
            if (!cacheable) {
                return execute.apply(record);
            }
            return Flux.deferContextual(context -> {
                SubscriptionContext subscription = context.getOrDefault(SubscriptionContext.class, null);
                return subscription == null
                        ? execute.apply(record)
                        : subscription.cacheMany(rowsCacheKey, () -> execute.apply(record), maxRows);
            });
        }

        @Override
        public Mono<Boolean> exists(ReactorQLRecord record) {
            if (!cacheable) {
                return execute.apply(record).hasElements();
            }
            return Mono.deferContextual(context -> {
                SubscriptionContext subscription = context.getOrDefault(SubscriptionContext.class, null);
                return subscription == null
                        ? execute.apply(record).hasElements()
                        : subscription.cacheMono(
                                existsCacheKey,
                                () -> execute.apply(record).hasElements()
                        );
            });
        }
    }
}
