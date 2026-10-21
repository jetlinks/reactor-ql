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
package org.jetlinks.reactor.ql.supports.from;

import lombok.Getter;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.NamedExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.ParenthesedFromItem;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.Values;
import org.jetlinks.reactor.ql.ReactorQLContext;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FromFeature;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class FromValuesFeature implements FromFeature {

    @Override
    public Function<ReactorQLContext, Flux<ReactorQLRecord>> createFromMapper(FromItem fromItem, ReactorQLMetadata metadata) {
        Values values;
        Alias aliasInfo;
        if (fromItem instanceof ParenthesedFromItem) {
            ParenthesedFromItem parenthesed = ((ParenthesedFromItem) fromItem);
            values = ((Values) parenthesed.getFromItem());
            aliasInfo = parenthesed.getAlias();
        } else {
            values = ((Values) fromItem);
            aliasInfo = values.getAlias();
        }

        List<Function<ReactorQLContext, Flux<ReactorQLRecord>>> mappers = new ArrayList<>();
        for (Object row : values.getExpressions()) {
            MapperBuilder.acceptRow(row, metadata, mappers::add);
        }
        String alias = aliasInfo == null ? null : aliasInfo.getName();
        List<String> columns = null;
        if (aliasInfo != null && aliasInfo.getAliasColumns() != null) {
            columns = aliasInfo.getAliasColumns().stream()
                            .map(c -> c.name)
                            .collect(Collectors.toList());
        }

        List<String> fColumns = columns == null ? Collections.emptyList() : columns;

        int size = fColumns.size();

        BiFunction<Integer, Integer, String> nameMapper = (valueIndex, recordIndex) -> recordIndex >= size ? "$" + recordIndex : fColumns.get(recordIndex);

        return ctx -> Flux.merge(Flux.fromIterable(mappers)
                                     .index((idx, mapper) -> mapper
                                             .apply(ctx)
                                             .index()
                                             .collectMap(tp2 -> nameMapper.apply(idx.intValue(), tp2
                                                     .getT1()
                                                     .intValue()), tp2 -> tp2.getT2().getRecord())
                                             .map(map -> ReactorQLRecord.newRecord(alias, map, ctx))),
                                 mappers.size());
    }

    @Override
    public String getId() {
        return FeatureId.From.values.getId();
    }

    private static class MapperBuilder {
        ReactorQLMetadata metadata;
        Consumer<Function<ReactorQLContext, Flux<ReactorQLRecord>>> consumer;

        public MapperBuilder(ReactorQLMetadata metadata, Consumer<Function<ReactorQLContext, Flux<ReactorQLRecord>>> consumer) {
            this.metadata = metadata;
            this.consumer = consumer;
        }

        @Getter
        List<Function<ReactorQLContext, Flux<ReactorQLRecord>>> mappers = new ArrayList<>();

        static void acceptRow(Object row,
                              ReactorQLMetadata metadata,
                              Consumer<Function<ReactorQLContext, Flux<ReactorQLRecord>>> consumer) {
            MapperBuilder builder = new MapperBuilder(metadata, consumer);
            if (row instanceof ParenthesedExpressionList) {
                builder.visit(((ParenthesedExpressionList<?>) row));
                return;
            }
            if (row instanceof ExpressionList) {
                builder.visit(((ExpressionList<?>) row));
                return;
            }
            if (row instanceof Select) {
                builder.visit(((Select) row));
                return;
            }
            if (row instanceof Expression) {
                builder.visit(new ExpressionList<>((Expression) row));
                return;
            }
            throw new UnsupportedOperationException("不支持的 values 表达式:" + row);
        }

        public void visit(Select subSelect) {
            consumer.accept(FromFeature.createFromMapperByFrom(subSelect, metadata));
        }

        public void visit(ExpressionList<?> expressionList) {
            Flux<Function<ReactorQLRecord, Publisher<?>>> mappers =
                    Flux.fromIterable(expressionList.getExpressions())
                        .map(expr -> ValueMapFeature.createMapperNow(expr, metadata));
            consumer.accept(ctx -> mappers
                    .flatMap(mapper -> mapper.apply(ReactorQLRecord
                                                            .newRecord(null, null, ctx)
                                                            .addRecords(ctx.getParameters())))
                    .map(val -> ReactorQLRecord.newRecord(null, val, ctx)));
        }
    }

}
