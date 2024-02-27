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
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FromFeature;
import org.jetlinks.reactor.ql.feature.PropertyFeature;
import org.jetlinks.reactor.ql.feature.ValueAggMapFeature;
import org.jetlinks.reactor.ql.utils.CastUtils;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CollectListAggFeature implements ValueAggMapFeature {

    public static final String ID = FeatureId.ValueAggMap.of("collect_list").getId();


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
                List<String> columns = function
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
                            throw new UnsupportedOperationException("不支持的表达式:" + expression);
                        })
                        .collect(Collectors.toList());

                PropertyFeature feature = metadata.getFeatureNow(PropertyFeature.ID);

                mapper = flux -> flux
                        .map(record -> {
                            Map<String, Object> values = Maps.newLinkedHashMapWithExpectedSize(columns.size());
                            Map<String, Object> records = record.getRecords(true);
                            Object row = record.getRecord();
                            for (String column : columns) {
                                Object val = feature
                                        .getProperty(column, records)
                                        .orElseGet(() -> feature.getProperty(column, row).orElse(null));
                                if (null != val) {
                                    values.put(column, val);
                                }
                            }
                            return values;
                        });
            }
        }

        if (function.isDistinct()) {
            return mapper.andThen(flux -> flux.collect(Collectors.toSet()).cast(Object.class).flux());
        }

        if (function.isUnique()) {
            return mapper.andThen(flux -> flux.as(CastUtils::uniqueFlux).collectList().cast(Object.class).flux());
        }

        return mapper.andThen(flux -> flux.collectList().cast(Object.class).flux());

    }

    @Override
    public String getId() {
        return ID;
    }
}
