package org.jetlinks.reactor.ql.supports.filter;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FilterFeature;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.Date;
import java.util.function.BiFunction;
import java.util.function.Function;

public class BetweenFilter implements FilterFeature {

    private static final String ID = FeatureId.Filter.between.getId();

    @Override
    public BiFunction<ReactorQLRecord, Object, Mono<Boolean>> createPredicate(Expression expression, ReactorQLMetadata metadata) {

        Between betweenExpr = ((Between) expression);
        Expression left = betweenExpr.getLeftExpression();
        Expression between = betweenExpr.getBetweenExpressionStart();
        Expression and = betweenExpr.getBetweenExpressionEnd();

        return doCreate(left, between, and, metadata, betweenExpr.isNot());
    }

    static BiFunction<ReactorQLRecord, Object, Mono<Boolean>> doCreate(Expression left,
                                                                       Expression between,
                                                                       Expression and,
                                                                       ReactorQLMetadata metadata,
                                                                       boolean not) {
        Function<ReactorQLRecord, Publisher<?>> leftMapper = ValueMapFeature.createMapperNow(left, metadata);
        Function<ReactorQLRecord, Publisher<?>> betweenMapper = ValueMapFeature.createMapperNow(between, metadata);
        Function<ReactorQLRecord, Publisher<?>> andMapper = ValueMapFeature.createMapperNow(and, metadata);
        return (row, column) -> Mono
                .zip(Mono.from(leftMapper.apply(row)), Mono.from(betweenMapper.apply(row)), Mono.from(andMapper.apply(row)))
                .map(tp3 -> not != predicate(tp3.getT1(), tp3.getT2(), tp3.getT3()));
    }

    static boolean predicate(Object val, Object between, Object and) {
        if (val == null || between == null || and == null) {
            return false;
        }
        if (val.equals(between) || val.equals(and)) {
            return true;
        }
        if (val instanceof Date || between instanceof Date || and instanceof Date) {
            val = CastUtils.castDate(val);
            between = CastUtils.castDate(between);
            and = CastUtils.castDate(and);
        }
        if (val instanceof Number || between instanceof Number || and instanceof Number) {
            double doubleVal = CastUtils.castNumber(val).doubleValue();
            return doubleVal >= CastUtils.castNumber(between).doubleValue() && doubleVal <= CastUtils.castNumber(and).doubleValue();
        }

        Object[] arr = new Object[]{val, between, and};
        Arrays.sort(arr);
        return arr[1] == val;
    }

    @Override
    public String getId() {
        return ID;
    }
}
