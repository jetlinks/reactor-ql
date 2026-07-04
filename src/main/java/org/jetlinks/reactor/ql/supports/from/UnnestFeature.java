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

import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.TableFunction;
import org.apache.commons.collections.CollectionUtils;
import org.jetlinks.reactor.ql.ReactorQLContext;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.exception.ReactorQLException;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FromFeature;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * SQL dialect compatible array/list flattening table function.
 *
 * <pre>{@code
 * select * from unnest(new_array(1, 2, 3)) as t(value)
 * select t.id, u.value from test t cross join unnest(t.values) as u(value)
 * }</pre>
 */
public class UnnestFeature implements FromFeature {

    private final String name;
    private final String id;

    public UnnestFeature(String name) {
        this.name = name;
        this.id = FeatureId.From.of(name).getId();
    }

    @Override
    public java.util.function.Function<ReactorQLContext, Flux<ReactorQLRecord>> createFromMapper(FromItem fromItem,
                                                                                                 ReactorQLMetadata metadata) {
        TableFunction table = (TableFunction) fromItem;
        Function function = table.getFunction();
        List<Expression> expressions = function.getParameters() == null
                ? Collections.emptyList()
                : function.getParameters().getExpressions();
        if (CollectionUtils.isEmpty(expressions)) {
            throw ReactorQLException.functionArgumentCount(function, 1, 9999, 0);
        }

        List<java.util.function.Function<ReactorQLRecord, Publisher<?>>> valueMappers = expressions
                .stream()
                .map(expression -> ValueMapFeature.createMapperNow(expression, metadata))
                .collect(Collectors.toList());

        String alias = table.getAlias() == null ? null : table.getAlias().getName();
        List<String> columns = getAliasColumns(table.getAlias(), valueMappers.size());

        return ctx -> {
            ReactorQLRecord baseRecord = ReactorQLRecord
                    .newRecord(null, null, ctx)
                    .addRecords(ctx.getParameters());
            if (valueMappers.size() == 1) {
                return Flux
                        .from(valueMappers.get(0).apply(baseRecord))
                        .as(CastUtils::flatStream)
                        .map(value -> ReactorQLRecord.newRecord(alias, toSingleRow(columns, value), ctx));
            }
            return Flux
                    .fromIterable(valueMappers)
                    .concatMap(mapper -> Flux
                            .from(mapper.apply(baseRecord))
                            .as(CastUtils::flatStream)
                            .collectList())
                    .collectList()
                    .flatMapMany(values -> toRows(ctx, alias, columns, values));
        };
    }

    private Map<String, Object> toSingleRow(List<String> columns, Object value) {
        Map<String, Object> row = new LinkedHashMap<>();
        if (value instanceof Map && columns.size() > 1) {
            putMapColumns(row, columns, (Map<?, ?>) value);
            return row;
        }
        if (value != null) {
            row.put(columnName(columns, 0), value);
        }
        return row;
    }

    private List<String> getAliasColumns(Alias alias, int parameterSize) {
        if (alias != null && alias.getAliasColumns() != null) {
            return alias
                    .getAliasColumns()
                    .stream()
                    .map(column -> column.name)
                    .collect(Collectors.toList());
        }
        if (parameterSize == 1) {
            return Collections.singletonList(name);
        }
        List<String> columns = new ArrayList<>(parameterSize);
        for (int i = 0; i < parameterSize; i++) {
            columns.add(name + "_" + i);
        }
        return columns;
    }

    private Flux<ReactorQLRecord> toRows(ReactorQLContext ctx,
                                         String alias,
                                         List<String> columns,
                                         List<List<Object>> values) {
        int rowSize = values
                .stream()
                .mapToInt(List::size)
                .max()
                .orElse(0);
        if (rowSize == 0) {
            return Flux.empty();
        }
        return Flux
                .range(0, rowSize)
                .map(index -> ReactorQLRecord.newRecord(alias, toRow(columns, values, index), ctx));
    }

    private Map<String, Object> toRow(List<String> columns, List<List<Object>> values, int index) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 0; i < values.size(); i++) {
            Object value = valueAt(values.get(i), index);
            if (value != null) {
                row.put(columnName(columns, i), value);
            }
        }
        return row;
    }

    private void putMapColumns(Map<String, Object> row, List<String> columns, Map<?, ?> value) {
        if (columns.size() == 2 && value.containsKey("key") && value.containsKey("value")) {
            Object key = value.get("key");
            Object val = value.get("value");
            if (key != null) {
                row.put(columns.get(0), key);
            }
            if (val != null) {
                row.put(columns.get(1), val);
            }
            return;
        }
        for (String column : columns) {
            Object val = value.get(column);
            if (val != null) {
                row.put(column, val);
            }
        }
    }

    private String columnName(List<String> columns, int index) {
        return index < columns.size() ? columns.get(index) : name + "_" + index;
    }

    private Object valueAt(List<Object> values, int index) {
        return index < values.size() ? values.get(index) : null;
    }

    @Override
    public String getId() {
        return id;
    }
}
