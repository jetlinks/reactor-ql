package org.jetlinks.reactor.ql.supports.filter;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FilterFeature;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.util.function.BiFunction;
import java.util.function.Function;

public class LikeFilter implements FilterFeature {

    private static final  String ID = FeatureId.Filter.of("like").getId();

    @Override
    public BiFunction<ReactorQLRecord, Object, Mono<Boolean>> createPredicate(Expression expression, ReactorQLMetadata metadata) {
        Tuple2<Function<ReactorQLRecord, ? extends Publisher<?>>,
                Function<ReactorQLRecord, ? extends Publisher<?>>> tuple2 = ValueMapFeature.createBinaryMapper(expression, metadata);

        Function<ReactorQLRecord, ? extends Publisher<?>> leftMapper = tuple2.getT1();
        Function<ReactorQLRecord, ? extends Publisher<?>> rightMapper = tuple2.getT2();

        LikeExpression like = ((LikeExpression) expression);
        boolean not = like.isNot();

        return (row, column) -> Mono.zip(Mono.from(leftMapper.apply(row)), Mono.from(rightMapper.apply(row)), (left, right) -> doTest(not, left, right));
    }

    protected boolean doTest(boolean not, Object left, Object right) {
        return not != (String.valueOf(left).matches(String.valueOf(right).replace("%", ".*")));
    }

    @Override
    public String getId() {
        return ID;
    }
}
