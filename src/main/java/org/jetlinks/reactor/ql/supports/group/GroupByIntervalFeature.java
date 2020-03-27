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

public class GroupByIntervalFeature implements GroupByFeature {

    public final static String ID = FeatureId.GroupBy.interval.getId();

    @Override
    public String getId() {
        return ID;
    }

    @Override
    public <T> java.util.function.Function<Flux<T>,Flux<GroupedFlux<Object,T>>> createMapper( Expression expression, ReactorQLMetadata metadata) {

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

        return flux->flux
                .window(interval.get())
                .flatMap(window -> {
                    long time = System.currentTimeMillis();
                    return window.groupBy((d) -> time);
                });
    }

    static Duration parseDuration(String timeString) {

        char[] all = timeString.toCharArray();
        if ((all[0] == 'P') || (all[0] == '-' && all[1] == 'P')) {
            return Duration.parse(timeString);
        }
        Duration duration = Duration.ofSeconds(0);
        char[] tmp = new char[32];
        int numIndex = 0;
        for (char c : all) {
            if (c == '-' || (c >= '0' && c <= '9')) {
                tmp[numIndex++] = c;
                continue;
            }
            long val = new BigDecimal(tmp, 0, numIndex).longValue();
            numIndex = 0;
            Duration plus = null;
            if (c == 'D' || c == 'd') {
                plus = Duration.ofDays(val);
            } else if (c == 'H' || c == 'h') {
                plus = Duration.ofHours(val);
            } else if (c == 'M' || c == 'm') {
                plus = Duration.ofMinutes(val);
            } else if (c == 's') {
                plus = Duration.ofSeconds(val);
            } else if (c == 'S') {
                plus = Duration.ofMillis(val);
            } else if (c == 'W' || c == 'w') {
                plus = Duration.ofDays(val * 7);
            }
            if (plus != null) {
                duration = duration.plus(plus);
            }
        }
        return duration;
    }
}
