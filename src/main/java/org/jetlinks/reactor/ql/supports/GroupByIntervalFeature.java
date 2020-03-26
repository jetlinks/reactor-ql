package org.jetlinks.reactor.ql.supports;

import net.sf.jsqlparser.expression.*;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.GroupByFeature;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

public class GroupByIntervalFeature implements GroupByFeature {

    public final static String ID = FeatureId.GroupBy.of("interval").getId();

    @Override
    public String getId() {
        return ID;
    }

    @Override
    public <T> Flux<GroupedFlux<Object, T>> apply(Flux<T> flux, Expression expression, ReactorQLMetadata metadata) {

        Function function = ((Function) expression);
        AtomicReference<Duration> interval = new AtomicReference<>();
        function.getAttribute()
                .accept(new ExpressionVisitorAdapter() {

                    @Override
                    public void visit(StringValue stringValue) {
                        interval.set(Duration.parse(stringValue.getValue()));
                    }

                    @Override
                    public void visit(LongValue longValue) {
                        interval.set(Duration.ofMillis(longValue.getValue()));
                    }
                });

        return flux
                .window(interval.get())
                .flatMap(window -> {
                    long time = System.currentTimeMillis();
                    return window.groupBy((d) -> time);
                });
    }
}
