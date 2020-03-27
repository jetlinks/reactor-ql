package org.jetlinks.reactor.ql.supports.group;

import lombok.Getter;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.GroupByFeature;
import org.jetlinks.reactor.ql.utils.CastUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;

import java.math.BigDecimal;
import java.util.Date;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.jetlinks.reactor.ql.feature.FeatureId.ValueMap.createValeMapperNow;

public class GroupByBinaryFeature implements GroupByFeature {

    @Getter
    private String id;
    private String type;

    private BiFunction<Number, Number, Object> mapper;

    public GroupByBinaryFeature(String type, BiFunction<Number, Number, Object> mapper) {
        this.id = FeatureId.GroupBy.of(type).getId();
        this.type = type;
        this.mapper = mapper;
    }

    @Override
    public <T> Function<Flux<T>, Flux<GroupedFlux<Object, T>>> createMapper(Expression expression, ReactorQLMetadata metadata) {
        BinaryExpression bie = ((BinaryExpression) expression);

        Expression left = bie.getLeftExpression();
        Expression right = bie.getRightExpression();

        Function<Object, Object> leftMapper = createValeMapperNow(left, metadata);
        Function<Object, Object> rightMapper = createValeMapperNow(right, metadata);

        return flux -> flux.groupBy(v -> getGroupKey(leftMapper.apply(v), rightMapper.apply(v)));
    }

    private Object getGroupKey(Object left, Object right) {
        if (left instanceof Number || right instanceof Number) {
            return doGetGroupKey(CastUtils.castNumber(left), CastUtils.castNumber(right));
        }
        if (left instanceof Date || right instanceof Date) {
            return doGetGroupKey(CastUtils.castDate(left), CastUtils.castDate(right));
        }
        if (left instanceof String || right instanceof String) {
            return doGetGroupKey(String.valueOf(left), String.valueOf(right));
        }
        return doGetGroupKey(left, right);
    }

    protected Object doGetGroupKey(Number left, Number right) {
        return mapper.apply(left, right);
    }

    protected Object doGetGroupKey(String left, String right) {
        return doGetGroupKey(new BigDecimal(left), new BigDecimal(right));
    }

    protected Object doGetGroupKey(Date left, Date right) {
        return doGetGroupKey(left.getTime(), right.getTime());
    }

    protected Object doGetGroupKey(Object left, Object right) {
        throw new UnsupportedOperationException("不支持的操作: " + left + " " + type + " " + right);
    }

}
