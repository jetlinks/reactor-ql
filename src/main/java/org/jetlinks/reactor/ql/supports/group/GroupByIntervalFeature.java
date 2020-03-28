package org.jetlinks.reactor.ql.supports.group;

import net.sf.jsqlparser.expression.*;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.GroupByFeature;
import org.jetlinks.reactor.ql.supports.ExpressionVisitorAdapter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.jetlinks.reactor.ql.utils.CastUtils.parseDuration;

public class GroupByIntervalFeature implements GroupByFeature {

    public final static String ID = FeatureId.GroupBy.interval.getId();

    @Override
    public String getId() {
        return ID;
    }

    @Override
    public <T> java.util.function.Function<Flux<T>, Flux<? extends Flux<T>>> createMapper(Expression expression, ReactorQLMetadata metadata) {

        Function function = ((Function) expression);
        if (function.getParameters() == null || function.getParameters().getExpressions().isEmpty()) {
            throw new UnsupportedOperationException("interval函数参数错误");
        }
        Expression expr = function.getParameters().getExpressions().get(0);
        Duration interval;
        if (expr instanceof StringValue) {
            interval = parseDuration(((StringValue) expr).getValue());
        } else if (expr instanceof LongValue) {
            interval = Duration.ofMillis(((LongValue) expr).getValue());
        } else {
            throw new UnsupportedOperationException("不支持的时间参数:" + expr);
        }
        Duration duration = interval;
        return flux -> flux.window(duration);
    }


}
