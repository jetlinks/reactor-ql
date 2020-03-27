package org.jetlinks.reactor.ql.supports.filter;

import lombok.Getter;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import org.apache.commons.collections.CollectionUtils;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FilterFeature;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.utils.CastUtils;
import reactor.util.function.Tuple2;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.jetlinks.reactor.ql.feature.FeatureId.ValueMap.createValeMapperNow;

public abstract class BinaryFilterFeature implements FilterFeature {

    @Getter
    private String id;

    public BinaryFilterFeature(String type) {
        this.id = FeatureId.Filter.of(type).getId();
    }

    @Override
    public BiPredicate<Object, Object> createMapper(Expression expression, ReactorQLMetadata metadata) {
        Tuple2<Function<Object, Object>, Function<Object, Object>> tuple2 = FeatureId.ValueMap.createBinaryMapper(expression, metadata);

        Function<Object, Object> leftMapper = tuple2.getT1();
        Function<Object, Object> rightMapper = tuple2.getT2();

        return (row, column) -> predicate(leftMapper.apply(row), rightMapper.apply(row));
    }

    private boolean predicate(Object left, Object right) {
        if (left instanceof Number || right instanceof Number) {
            return doPredicate(CastUtils.castNumber(left), CastUtils.castNumber(right));
        }
        if (left instanceof Date || right instanceof Date) {
            return doPredicate(CastUtils.castDate(left), CastUtils.castDate(right));
        }
        if (left instanceof String || right instanceof String) {
            return doPredicate(String.valueOf(left), String.valueOf(right));
        }
        return doPredicate(left, right);
    }

    protected abstract boolean doPredicate(Number left, Number right);

    protected abstract boolean doPredicate(Date left, Date right);

    protected abstract boolean doPredicate(String left, String right);

    protected abstract boolean doPredicate(Object left, Object right);

}
