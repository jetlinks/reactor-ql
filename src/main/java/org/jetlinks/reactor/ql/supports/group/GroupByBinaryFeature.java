package org.jetlinks.reactor.ql.supports.group;

import lombok.Getter;
import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.GroupFeature;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * 根据运算表达式来进行分组
 * <pre>
 *     group by val/10
 * </pre>
 *
 * @author zhouhao
 * @since 1.0
 */
public class GroupByBinaryFeature implements GroupFeature {

    @Getter
    private String id;

    private BiFunction<Object, Object, Object> mapper;

    public GroupByBinaryFeature(String type, BiFunction<Object, Object, Object> mapper) {
        this.id = FeatureId.GroupBy.of(type).getId();
        this.mapper = mapper;
    }

    @Override
    public <T> Function<Flux<T>, Flux<? extends Flux<T>>> createGroupMapper(Expression expression, ReactorQLMetadata metadata) {

        Tuple2<Function<Object, Object>, Function<Object, Object>> tuple2 = ValueMapFeature.createBinaryMapper(expression, metadata);

        Function<Object, Object> leftMapper = tuple2.getT1();
        Function<Object, Object> rightMapper = tuple2.getT2();

        return flux -> flux.groupBy(v -> mapper.apply(leftMapper.apply(v), rightMapper.apply(v)));
    }

}
