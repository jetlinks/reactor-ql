package org.jetlinks.reactor.ql.supports.map;

import lombok.Getter;
import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class FunctionMapFeature implements ValueMapFeature {

    int maxParamSize;
    int minParamSize;

    public Function<Flux<Object>, Publisher<Object>> mapper;

    @Getter
    private final String id;

    private Object defaultValue;

    @SuppressWarnings("all")
    public FunctionMapFeature(String function, int max, int min, Function<Flux<Object>, Publisher<?>> mapper) {
        this.maxParamSize = max;
        this.minParamSize = min;
        this.mapper = (Function) mapper;
        this.id = FeatureId.ValueMap.of(function).getId();
    }

    @Override
    public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression, ReactorQLMetadata metadata) {

        net.sf.jsqlparser.expression.Function function = ((net.sf.jsqlparser.expression.Function) expression);

        List<Expression> parameters;
        if (function.getParameters() == null && minParamSize != 0) {
            throw new UnsupportedOperationException("函数[" + expression + "]必须传入参数");
        }
        if (function.getParameters() == null) {
            return v -> mapper.apply(Flux.empty());
        }
        parameters = function.getParameters().getExpressions();
        if (parameters.size() > maxParamSize || parameters.size() < minParamSize) {
            throw new UnsupportedOperationException("函数[" + expression + "]参数数量错误");
        }
        List<Function<ReactorQLRecord, Publisher<Object>>> mappers = createParamMappers(metadata, parameters);
        // fun(distinct col)
        if (function.isDistinct()) {
            return v -> Flux
                    .from(apply(v, mappers))
                    .distinct();
        }
        // fun(unique col)
        if (function.isUnique()) {
            return v -> CastUtils.uniqueFlux(Flux.from(apply(v, mappers)));
        }

        return v -> apply(v, mappers);
    }

    @SuppressWarnings("all")
    protected List<Function<ReactorQLRecord, Publisher<Object>>> createParamMappers(ReactorQLMetadata metadata,
                                                                                    List<Expression> parameters) {
        return (List) parameters
                .stream()
                .map(expr -> ValueMapFeature.createMapperNow(expr, metadata))
                .collect(Collectors.toList());
    }

    public FunctionMapFeature defaultValue(Object defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }

    protected Publisher<Object> apply(ReactorQLRecord record,
                                      List<Function<ReactorQLRecord, Publisher<Object>>> mappers) {
        return mapper.apply(
                Flux.fromIterable(mappers)
                    .flatMap(mp -> {
                        if (defaultValue != null) {
                            return Mono
                                    .fromDirect(mp.apply(record))
                                    .defaultIfEmpty(defaultValue);
                        }
                        return mp.apply(record);
                    }));
    }
}
