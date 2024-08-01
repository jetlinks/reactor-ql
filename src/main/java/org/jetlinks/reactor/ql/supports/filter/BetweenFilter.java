package org.jetlinks.reactor.ql.supports.filter;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FilterFeature;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.jetlinks.reactor.ql.utils.CompareUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

public class BetweenFilter implements FilterFeature {

    private static final String ID = FeatureId.Filter.between.getId();

    @SuppressWarnings("all")
    private static final ThreadLocal<List<Comparable>> SHARE = ThreadLocal.withInitial(() -> new ArrayList<>(3));

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

    public static boolean predicate(Object val, Object between, Object and) {
        try {
            if (val == null || between == null || and == null) {
                return false;
            }
            if (val.equals(between) || val.equals(and)) {
                return true;
            }
            if (val instanceof Date || between instanceof Date || and instanceof Date) {
                val = CastUtils.castDate(val, ignore -> null);
                between = CastUtils.castDate(between, ignore -> null);
                and = CastUtils.castDate(and, ignore -> null);
            }
            if (val instanceof Number || between instanceof Number || and instanceof Number) {
                Number numberValue = CastUtils.castNumber(val, (ignore) -> null),
                        numberBetween = CastUtils.castNumber(between, (ignore) -> null),
                        numberAnd = CastUtils.castNumber(and, (ignore) -> null);

                if (numberValue == null ||
                        numberBetween == null ||
                        numberAnd == null) {
                    return false;
                }

                return CompareUtils.compare(numberValue, numberBetween) >= 0
                        && CompareUtils.compare(numberValue, numberAnd) <= 0;
            }

            if (val == null || between == null || and == null) {
                return false;
            }
            return compare0(val, between, and);
        } catch (Throwable error) {
            return false;
        }
    }

    private static Comparable<?> castComparable(Object va) {
        if (va instanceof Comparable) {
            return (Comparable<?>) va;
        }
        return String.valueOf(va);
    }

    @SuppressWarnings("all")
    protected static boolean compare0(Object val, Object between, Object and) {
        List<Comparable> arr = SHARE.get();
        try {
            arr.add(castComparable(val));
            arr.add(castComparable(between));
            arr.add(castComparable(and));
            Collections.sort(arr);
            return val == arr.get(1);
        } finally {
            arr.clear();
        }
    }

    @Override
    public String getId() {
        return ID;
    }
}
