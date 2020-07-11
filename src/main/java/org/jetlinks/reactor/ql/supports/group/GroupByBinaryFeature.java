package org.jetlinks.reactor.ql.supports.group;

import lombok.Getter;
import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.GroupFeature;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
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
    private final String id;

    private final BiFunction<Object, Object, Object> mapper;

    public GroupByBinaryFeature(String type, BiFunction<Object, Object, Object> mapper) {
        this.id = FeatureId.GroupBy.of(type).getId();
        this.mapper = mapper;
    }

    @Override
    public Function<Flux<ReactorQLRecord>, Flux<? extends Flux<ReactorQLRecord>>> createGroupMapper(Expression expression, ReactorQLMetadata metadata) {

        Tuple2<Function<ReactorQLRecord, ? extends Publisher<?>>,
                Function<ReactorQLRecord, ? extends Publisher<?>>> tuple2 = ValueMapFeature.createBinaryMapper(expression, metadata);

        Function<ReactorQLRecord, ? extends Publisher<?>> leftMapper = tuple2.getT1();
        Function<ReactorQLRecord, ? extends Publisher<?>> rightMapper = tuple2.getT2();

        return flux -> flux
                .flatMap(ctx -> Mono.zip(
                        Mono.from(leftMapper.apply(ctx)),
                        Mono.from(rightMapper.apply(ctx)), mapper)
                        .zipWith(Mono.just(ctx)))
                .groupBy(Tuple2::getT1, Tuple2::getT2,Integer.MAX_VALUE);
    }

}
