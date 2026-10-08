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

import com.google.common.collect.Maps;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;
import org.apache.commons.collections.CollectionUtils;
import org.jetlinks.reactor.ql.ReactorQLContext;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.exception.ReactorQLException;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FromFeature;
import org.jetlinks.reactor.ql.feature.ValueAggMapFeature;
import org.jetlinks.reactor.ql.feature.PropertyFeature;
import org.jetlinks.reactor.ql.internal.StatefulAggregationSupport;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CollectListAggFeature implements ValueAggMapFeature {

    public static final String ID = FeatureId.ValueAggMap.of("collect_list").getId();
    private static final String LIMIT_SUGGESTION =
            "增加窗口、缩小输入范围或在可信场景下调大受硬上限保护的配置。";
    private static final String LIMIT_EXAMPLE =
            "select collect_list(value) values from test group by _window(1000)";


    @Override
    public Function<Flux<ReactorQLRecord>, Flux<Object>> createMapper(Expression expression, ReactorQLMetadata metadata) {

        net.sf.jsqlparser.expression.Function function = ((net.sf.jsqlparser.expression.Function) expression);

        Function<Flux<ReactorQLRecord>, Flux<Object>> mapper;

        if (function.getParameters() == null || CollectionUtils.isEmpty(function.getParameters().getExpressions())) {
            mapper = flux -> flux.map(ReactorQLRecord::getRecord);
        } else {
            Expression expr = function.getParameters().getExpressions().get(0);
            if (expr instanceof SubSelect) {
                Function<ReactorQLContext, Flux<ReactorQLRecord>> _mapper =
                        FromFeature.createFromMapperByFrom(((SubSelect) expr), metadata);
                mapper = flux -> _mapper
                        .apply(ReactorQLContext.ofDatasource((r) -> flux))
                        .map(ReactorQLRecord::getRecord);
            } else {
                List<String> columns = resolveColumns(function);
                PropertyFeature feature = metadata.getFeatureNow(PropertyFeature.ID);
                mapper = flux -> flux.map(record -> mapColumns(record, feature, columns));
            }
        }

        int maxCollectionSize = StatefulAggregationSupport.readLimit(metadata);
        if (function.isDistinct()) {
            return mapper.andThen(flux -> StatefulAggregationSupport
                    .collectSet(flux, maxCollectionSize)
                    .cast(Object.class)
                    .flux());
        }

        if (function.isUnique()) {
            return mapper.andThen(flux -> StatefulAggregationSupport
                    .collectUnique(flux, maxCollectionSize)
                    .cast(Object.class)
                    .flux());
        }

        return mapper.andThen(flux -> StatefulAggregationSupport
                .collectList(flux, maxCollectionSize)
                .cast(Object.class)
                .flux());

    }

    private List<String> resolveColumns(net.sf.jsqlparser.expression.Function function) {
        return function
                .getParameters()
                .getExpressions()
                .stream()
                .map(c -> {
                    if (c instanceof StringValue) {
                        return ((StringValue) c).getValue();
                    }
                    if (c instanceof Column) {
                        return ((Column) c).getColumnName();
                    }
                    throw ReactorQLException.invalidArgument(
                            c,
                            "collect_list 的列参数必须是列名或字符串常量",
                            "只列出需要收集的字段名；如果要收集子查询结果，请把第一个参数写成子查询。",
                            "select collect_list('deviceId', 'value') rows from test"
                    );
                })
                .collect(Collectors.toList());
    }

    private Map<String, Object> mapColumns(ReactorQLRecord record,
                                           PropertyFeature feature,
                                           List<String> columns) {
        Map<String, Object> values = Maps.newLinkedHashMapWithExpectedSize(columns.size());
        Map<String, Object> records = record.getRecords(true);
        Object row = record.getRecord();
        for (String column : columns) {
            Object value = feature
                    .getProperty(column, records)
                    .orElseGet(() -> feature.getProperty(column, row).orElse(null));
            if (value != null) {
                values.put(column, value);
            }
        }
        return values;
    }

    @Override
    public String getId() {
        return ID;
    }
}
