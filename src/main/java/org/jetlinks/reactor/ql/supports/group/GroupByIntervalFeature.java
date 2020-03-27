package org.jetlinks.reactor.ql.supports.group;

import net.sf.jsqlparser.expression.*;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.GroupByFeature;
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
        AtomicReference<Duration> interval = new AtomicReference<>();
        function.getParameters()
                .accept(new ExpressionVisitorAdapter() {

                    @Override
                    public void visit(StringValue stringValue) {
                        interval.set(parseDuration(stringValue.getValue()));
                    }

                    @Override
                    public void visit(LongValue longValue) {
                        interval.set(Duration.ofMillis(longValue.getValue()));
                    }
                });

        return flux -> flux.window(interval.get());
    }


}
