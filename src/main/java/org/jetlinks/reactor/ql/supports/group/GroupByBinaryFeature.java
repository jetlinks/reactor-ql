package org.jetlinks.reactor.ql.supports.group;

import lombok.Getter;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import org.apache.commons.collections.CollectionUtils;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.GroupByFeature;
import org.jetlinks.reactor.ql.utils.CastUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.util.function.Tuple2;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.jetlinks.reactor.ql.feature.FeatureId.ValueMap.createValeMapperNow;

public class GroupByBinaryFeature implements GroupByFeature {

    @Getter
    private String id;

    private BiFunction<Object, Object, Object> mapper;

    public GroupByBinaryFeature(String type, BiFunction<Object, Object, Object> mapper) {
        this.id = FeatureId.GroupBy.of(type).getId();
        this.mapper = mapper;
    }

    @Override
    public <T> Function<Flux<T>, Flux<? extends Flux<T>>> createMapper(Expression expression, ReactorQLMetadata metadata) {

        Tuple2<Function<Object, Object>, Function<Object, Object>> tuple2 = FeatureId.ValueMap.createBinaryMapper(expression, metadata);

        Function<Object, Object> leftMapper = tuple2.getT1();
        Function<Object, Object> rightMapper = tuple2.getT2();

        return flux -> flux.groupBy(v -> mapper.apply(leftMapper.apply(v), rightMapper.apply(v)));
    }

}
