package org.jetlinks.reactor.ql.supports.map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.StringValue;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.function.Function;


public class DateFormatFeature implements ValueMapFeature {

    private final static String ID = FeatureId.ValueMap.of("date_format").getId();

    @Override
    public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression, ReactorQLMetadata metadata) {
        net.sf.jsqlparser.expression.Function now = ((net.sf.jsqlparser.expression.Function) expression);
        try {
            List<Expression> expres = now.getParameters().getExpressions();

            if (expres.size() < 2) {
                throw new UnsupportedOperationException("错误的参数,正确例子: date_format(date,'yyyy-MM-dd')");
            }

            Expression val = expres.get(0);
            Expression formatExpr = expres.get(1);
            ZoneId tz = expres.size() > 2 ? ZoneId.of(((StringValue) expres.get(2)).getValue()) : ZoneId.systemDefault();

            if (formatExpr instanceof StringValue) {
                Function<ReactorQLRecord, Publisher<?>> mapper = ValueMapFeature.createMapperNow(val, metadata);
                StringValue format = ((StringValue) formatExpr);
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format.getValue());
                return ctx -> Mono.from(mapper.apply(ctx)).map(value -> formatter.format(CastUtils.castDate(value).toInstant().atZone(tz)));
            }
        } catch (Exception e) {
            throw new UnsupportedOperationException("错误的参数,正确例子: date_format(date,'yyyy-MM-dd','Asia/Shanghai')", e);
        }
        throw new UnsupportedOperationException("错误的参数,正确例子: date_format(date,'yyyy-MM-dd')");
    }

    @Override
    public String getId() {
        return ID;
    }
}
