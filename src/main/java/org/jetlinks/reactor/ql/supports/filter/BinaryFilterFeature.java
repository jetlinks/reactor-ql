package org.jetlinks.reactor.ql.supports.filter;

import lombok.Getter;
import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FilterFeature;
import org.jetlinks.reactor.ql.utils.CastUtils;
import reactor.util.function.Tuple2;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.function.BiPredicate;
import java.util.function.Function;

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

        return (row, column) -> test(leftMapper.apply(row), rightMapper.apply(row));
    }

    protected boolean test(Object left, Object right) {
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
