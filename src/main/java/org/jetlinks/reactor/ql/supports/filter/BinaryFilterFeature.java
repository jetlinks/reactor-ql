package org.jetlinks.reactor.ql.supports.filter;

import lombok.Getter;
import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FilterFeature;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

public abstract class BinaryFilterFeature implements FilterFeature {

    @Getter
    private String id;

    public BinaryFilterFeature(String type) {
        this.id = FeatureId.Filter.of(type).getId();
    }

    @Override
    public BiFunction<ReactorQLRecord, Object, Mono<Boolean>> createPredicate(Expression expression, ReactorQLMetadata metadata) {
        Tuple2<Function<ReactorQLRecord, ? extends Publisher<?>>,
                Function<ReactorQLRecord, ? extends Publisher<?>>> tuple2 = ValueMapFeature.createBinaryMapper(expression, metadata);

        Function<ReactorQLRecord, ? extends Publisher<?>> leftMapper = tuple2.getT1();
        Function<ReactorQLRecord, ? extends Publisher<?>> rightMapper = tuple2.getT2();

        return (row, column) -> Mono.zip(Mono.from(leftMapper.apply(row)), Mono.from(rightMapper.apply(row)), this::test);
    }

    protected boolean test(Object left, Object right) {
        if (left instanceof Map && ((Map<?, ?>) left).size() == 1) {
            left = ((Map<?, ?>) left).values().iterator().next();
        }
        if (right instanceof Map && ((Map<?, ?>) right).size() == 1) {
            right = ((Map<?, ?>) right).values().iterator().next();
        }
        if (left instanceof Date || right instanceof Date || left instanceof LocalDateTime || right instanceof LocalDateTime || left instanceof Instant || right instanceof Instant) {
            return doTest(CastUtils.castDate(left), CastUtils.castDate(right));
        }
        if (left instanceof Number || right instanceof Number) {
            return doTest(CastUtils.castNumber(left), CastUtils.castNumber(right));
        }
        if (left instanceof String || right instanceof String) {
            return doTest(String.valueOf(left), String.valueOf(right));
        }
        return doTest(left, right);
    }

    protected abstract boolean doTest(Number left, Number right);

    protected abstract boolean doTest(Date left, Date right);

    protected abstract boolean doTest(String left, String right);

    protected abstract boolean doTest(Object left, Object right);

}
