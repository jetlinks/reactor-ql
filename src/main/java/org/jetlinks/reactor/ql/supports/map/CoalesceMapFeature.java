package org.jetlinks.reactor.ql.supports.map;

import lombok.Getter;
import net.sf.jsqlparser.expression.Expression;
import org.apache.commons.collections.CollectionUtils;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.utils.ExpressionUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CoalesceMapFeature implements ValueMapFeature {

    @Getter
    private final String id;

    public CoalesceMapFeature() {
        this.id = FeatureId.ValueMap.of("coalesce").getId();
    }

    @Override
    public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression, ReactorQLMetadata metadata) {

        net.sf.jsqlparser.expression.Function function = ((net.sf.jsqlparser.expression.Function) expression);

        List<Expression> parameters = ExpressionUtils.getFunctionParameter(function);
        if (CollectionUtils.isEmpty(parameters)) {
            throw new UnsupportedOperationException("函数[" + expression + "]必须传入参数");
        }
        List<Function<ReactorQLRecord, Publisher<?>>> mappers = parameters
                .stream()
                .map(expr -> ValueMapFeature.createMapperNow(expr, metadata))
                .collect(Collectors.toList());

        return v -> {
            Flux<Object> flux = null;
            for (Function<ReactorQLRecord, Publisher<?>> mapper : mappers) {
                Flux<Object> that = Flux.from(mapper.apply(v));
                if (flux == null) {
                    flux = that;
                } else {
                    flux = flux.switchIfEmpty(that);
                }
            }
            return flux == null ? Flux.empty() : flux;
        };
    }
}
