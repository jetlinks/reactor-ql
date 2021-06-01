package org.jetlinks.reactor.ql.supports.agg;

import net.sf.jsqlparser.expression.Expression;
import org.apache.commons.collections.CollectionUtils;
import org.jetlinks.reactor.ql.ReactorQLContext;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.GroupFeature;
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
        if (function.getParameters() == null || CollectionUtils.isEmpty(expressions = function.getParameters().getExpressions())) {
            throw new IllegalArgumentException("函数参数不能为空:" + expression);
        }
        if (expressions.size() != 2) {
            throw new IllegalArgumentException("函数参数数量必须为2:" + expression);
        }

        Function<ReactorQLRecord, Publisher<?>> key = ValueMapFeature.createMapperNow(expressions.get(0), metadata);
        Function<ReactorQLRecord, Publisher<?>> value = ValueMapFeature.createMapperNow(expressions.get(1), metadata);

        return flux -> flux
                .flatMap(record -> Mono.zip(
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
