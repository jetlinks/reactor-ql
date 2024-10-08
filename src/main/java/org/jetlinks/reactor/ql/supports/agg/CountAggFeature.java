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

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CountAggFeature implements ValueAggMapFeature {

    public static final String ID = FeatureId.ValueAggMap.of("count").getId();


    @Override
    public Function<Flux<ReactorQLRecord>, Flux<Object>> createMapper(Expression expression, ReactorQLMetadata metadata) {

        net.sf.jsqlparser.expression.Function function = ((net.sf.jsqlparser.expression.Function) expression);

        if (function.getParameters() == null || CollectionUtils.isEmpty(function.getParameters().getExpressions())) {
            return flux -> flux.count().cast(Object.class).flux();
        }

        Expression expr = function.getParameters().getExpressions().get(0);

        Function<ReactorQLRecord, Publisher<?>> mapper = ValueMapFeature.createMapperNow(expr, metadata);

        //去重记数
        if (function.isDistinct()) {
            return flux -> metadata
                    .flatMap(flux, mapper)
                    .distinct()
                    .count()
                    .cast(Object.class)
                    .flux();
        }
        //统计唯一值的个数
        if (function.isUnique()) {
            return flux -> metadata
                    .flatMap(flux, mapper)
                    .collect(Collectors.groupingBy(
                            Function.identity(),
                            ConcurrentHashMap::new,
                            Collectors.counting()))
                    .map(map -> {
                        long count = 0;
                        for (Long value : map.values()) {
                            if (value == 1) {
                                count++;
                            }
                        }
                        return count;
                    })
                    .flux()
                    .cast(Object.class);
        }

        return flux -> metadata
                .flatMap(flux, mapper)
                .count()
                .cast(Object.class).flux();


    }

    @Override
    public String getId() {
        return ID;
    }
}
