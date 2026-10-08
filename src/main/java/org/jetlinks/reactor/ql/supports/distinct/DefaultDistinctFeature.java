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
package org.jetlinks.reactor.ql.supports.distinct;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.*;
import org.jetlinks.reactor.ql.DefaultReactorQL;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.DistinctFeature;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ScalarValueMapper;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.internal.BoundedStateSupport;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public class DefaultDistinctFeature implements DistinctFeature {

    private static final Object EMPTY_KEY = new Object();
    private static final String LIMIT_SUGGESTION =
            "缩小输入范围、增加窗口，或在可信场景下调大受硬上限保护的配置。";
    private static final String LIMIT_EXAMPLE =
            "select distinct deviceId from test where timestamp > now() - 1h";

    @Override
    public Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> createDistinctMapper(Distinct distinct, ReactorQLMetadata metadata) {

        int maxRows = readMaxRows(metadata);
        List<SelectItem> items = distinct.getOnSelectItems();
        if (items == null) {
            return flux -> boundedDistinct(flux, ReactorQLRecord::getRecord, maxRows);
        }
        List<Function<ReactorQLRecord, Mono<Object>>> keySelector = new ArrayList<>();
        List<ScalarValueMapper> scalarKeySelector = new ArrayList<>();
        boolean[] allScalar = {true};
        for (SelectItem item : items) {
            item.accept(new SelectItemVisitor() {
                @Override
                public void visit(AllColumns allColumns) {
                    keySelector.add(record -> Mono.justOrEmpty(record.getRecord()));
                    scalarKeySelector.add(ReactorQLRecord::getRecord);
                }

                @Override
                public void visit(AllTableColumns allTableColumns) {
                    String tname = allTableColumns.getTable().getAlias() != null ? allTableColumns
                            .getTable()
                            .getAlias()
                            .getName() : allTableColumns.getTable().getName();
                    keySelector.add(record -> Mono.justOrEmpty(record.getRecord(tname)));
                    scalarKeySelector.add(record -> record.getRecordValue(tname));
                }

                @Override
                public void visit(SelectExpressionItem selectExpressionItem) {
                    Expression expr = selectExpressionItem.getExpression();
                    Function<ReactorQLRecord, Publisher<?>> mapper = ValueMapFeature.createMapperNow(expr, metadata);
                    keySelector.add(record -> Mono.from(mapper.apply(record)));
                    if (mapper instanceof ScalarValueMapper) {
                        scalarKeySelector.add((ScalarValueMapper) mapper);
                    } else {
                        allScalar[0] = false;
                    }
                }
            });
        }
        if (keySelector.isEmpty()) {
            return flux -> boundedDistinct(flux, ReactorQLRecord::getRecord, maxRows);
        }
        if (allScalar[0] && !metadata.isCheckpoint()) {
            return flux -> boundedDistinct(
                    flux,
                    record -> createScalarKey(record, scalarKeySelector),
                    maxRows
            );
        }
        return createDistinct(keySelector, metadata);
    }

    private Object createScalarKey(ReactorQLRecord record, List<ScalarValueMapper> selectors) {
        if (selectors.size() == 1) {
            Object value = selectors.get(0).applyScalar(record);
            return value == null ? EMPTY_KEY : value;
        }
        Object[] values = new Object[selectors.size()];
        int size = 0;
        for (ScalarValueMapper selector : selectors) {
            Object value = selector.applyScalar(record);
            if (value != null) {
                values[size++] = value;
            }
        }
        return new DistinctKey(values, size);
    }

    protected Function<Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> createDistinct(List<Function<ReactorQLRecord,
            Mono<Object>>> keySelector, ReactorQLMetadata metadata) {
        int maxRows = readMaxRows(metadata);
        return flux -> BoundedStateSupport
                .distinct(
                        metadata.flatMap(flux, record -> Flux
                                .fromIterable(keySelector)
                                .flatMap(mapper -> mapper.apply(record), keySelector.size(), keySelector.size())
                                .collectList()
                                .map(list -> Tuples.of(list, record))),
                        Tuple2::getT1,
                        maxRows,
                        DefaultReactorQL.SETTING_DISTINCT_MAX_ROWS,
                        LIMIT_SUGGESTION,
                        LIMIT_EXAMPLE
                )
                .map(Tuple2::getT2);
    }

    private <K> Flux<ReactorQLRecord> boundedDistinct(
            Flux<ReactorQLRecord> source,
            Function<? super ReactorQLRecord, ? extends K> keySelector,
            int maxRows) {
        return BoundedStateSupport.distinct(
                source,
                keySelector,
                maxRows,
                DefaultReactorQL.SETTING_DISTINCT_MAX_ROWS,
                LIMIT_SUGGESTION,
                LIMIT_EXAMPLE
        );
    }

    private int readMaxRows(ReactorQLMetadata metadata) {
        return BoundedStateSupport.readLimit(
                metadata,
                DefaultReactorQL.SETTING_DISTINCT_MAX_ROWS,
                DefaultReactorQL.DEFAULT_DISTINCT_MAX_ROWS,
                DefaultReactorQL.HARD_MAX_DISTINCT_ROWS,
                "使用 1 到 " + DefaultReactorQL.HARD_MAX_DISTINCT_ROWS + " 之间的去重行数上限。",
                DefaultReactorQL.SETTING_DISTINCT_MAX_ROWS + "=65536"
        );
    }

    @Override
    public String getId() {
        return FeatureId.Distinct.defaultId.getId();
    }

    private static final class DistinctKey {

        private final Object[] values;
        private final int size;
        private final int hash;

        private DistinctKey(Object[] values, int size) {
            this.values = values;
            this.size = size;
            int hash = 1;
            for (int i = 0; i < size; i++) {
                hash = 31 * hash + Objects.hashCode(values[i]);
            }
            this.hash = hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof DistinctKey)) {
                return false;
            }
            DistinctKey that = (DistinctKey) obj;
            if (size != that.size) {
                return false;
            }
            for (int i = 0; i < size; i++) {
                if (!Objects.equals(values[i], that.values[i])) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }
}
