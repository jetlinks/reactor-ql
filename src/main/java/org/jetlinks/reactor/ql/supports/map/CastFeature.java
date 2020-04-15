package org.jetlinks.reactor.ql.supports.map;

import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.Date;
import java.util.function.Function;


public class CastFeature implements ValueMapFeature {

    private static String ID = FeatureId.ValueMap.of("cast").getId();

    @Override
    public Function<ReactorQLRecord, ? extends Publisher<?>> createMapper(Expression expression, ReactorQLMetadata metadata) {
        CastExpression cast = ((net.sf.jsqlparser.expression.CastExpression) expression);

        Expression left = cast.getLeftExpression();

        String type = cast.getType().getDataType().toLowerCase();

        Function<ReactorQLRecord, ? extends Publisher<?>> mapper = ValueMapFeature.createMapperNow(left, metadata);

        return ctx -> Mono.from(mapper.apply(ctx)).map(value -> doCast(value, type));
    }

    protected Object doCast(Object val, String type) {

        switch (type) {
            case "string":
            case "varchar":
                return String.valueOf(val);
            case "number":
            case "decimal":
                return new BigDecimal(String.valueOf(val));
            case "int":
            case "integer":
                return castNumber(val).intValue();
            case "long":
                return castNumber(val).longValue();
            case "double":
                return castNumber(val).doubleValue();
            case "bool":
            case "boolean":
                return castBoolean(val);
            case "byte":
                return castNumber(val).byteValue();
            case "float":
                return castNumber(val).floatValue();
            case "date":
                return castDate(val);
            default:
                return val;
        }
    }

    protected Date castDate(Object number) {
        return CastUtils.castDate(number);
    }

    protected Number castNumber(Object number) {
        return CastUtils.castNumber(number);
    }

    protected boolean castBoolean(Object val) {
        return CastUtils.castBoolean(val);
    }

    @Override
    public String getId() {
        return ID;
    }
}
