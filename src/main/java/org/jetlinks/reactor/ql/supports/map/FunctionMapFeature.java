package org.jetlinks.reactor.ql.supports.map;

import lombok.Getter;
import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FunctionMapFeature implements ValueMapFeature {

    int maxParamSize;
    int minParamSize;

    public Function<Stream<Object>, Object> mapper;

    @Getter
    private String id;

    public FunctionMapFeature(String function, int max, int min, Function<Stream<Object>, Object> mapper) {
        this.maxParamSize = max;
        this.minParamSize = min;
        this.mapper = mapper;
        this.id = FeatureId.ValueMap.of(function).getId();
    }

    @Override
    public Function<Object, Object> createMapper(Expression expression, ReactorQLMetadata metadata) {

        net.sf.jsqlparser.expression.Function function = ((net.sf.jsqlparser.expression.Function) expression);

        List<Expression> parameters;
        if (function.getParameters() == null && minParamSize != 0) {
            throw new UnsupportedOperationException("函数[" + expression + "]必须传入参数");
        }
        if (function.getParameters() == null) {
            return v -> mapper.apply(Stream.empty());
        }
        parameters = function.getParameters().getExpressions();
        if (parameters.size() > maxParamSize || parameters.size() < minParamSize) {
            throw new UnsupportedOperationException("函数[" + expression + "]参数数量错误");
        }
        List<Function<Object, Object>> mappers = parameters.stream()
                .map(expr -> ValueMapFeature.createMapperNow(expr, metadata))
                .collect(Collectors.toList());

        return v -> mapper.apply(mappers.stream().map(mp -> mp.apply(v)));
    }
}
