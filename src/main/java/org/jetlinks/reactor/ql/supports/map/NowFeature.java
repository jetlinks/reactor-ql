package org.jetlinks.reactor.ql.supports.map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.StringValue;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.Function;


public class NowFeature implements ValueMapFeature {

    private static String ID = FeatureId.ValueMap.of("now").getId();

    @Override
    public Function<Object, Object> createMapper(Expression expression, ReactorQLMetadata metadata) {
        net.sf.jsqlparser.expression.Function now = ((net.sf.jsqlparser.expression.Function) expression);

        if (now.getParameters() != null) {
            for (Expression expr : now.getParameters().getExpressions()) {
                if (expr instanceof StringValue) {
                    StringValue format = ((StringValue) expr);
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format.getValue());
                    return v -> formatter.format(LocalDateTime.now());

                }
            }
        }
        return v -> System.currentTimeMillis();
    }

    @Override
    public String getId() {
        return ID;
    }
}
