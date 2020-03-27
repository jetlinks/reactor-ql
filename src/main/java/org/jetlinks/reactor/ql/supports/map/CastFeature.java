package org.jetlinks.reactor.ql.supports.map;

import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.utils.CastUtils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.function.Function;


public class CastFeature implements ValueMapFeature {

    private static String ID = FeatureId.ValueMap.of("cast").getId();

    @Override
    public Function<Object, Object> createMapper(Expression expression, ReactorQLMetadata metadata) {
        CastExpression cast = ((net.sf.jsqlparser.expression.CastExpression) expression);

        Expression left = cast.getLeftExpression();

        ColDataType type = cast.getType();

        Function<Object, Object> mapper = FeatureId.ValueMap.createValeMapperNow(left, metadata);


        return v -> doCast(mapper.apply(v), type);
    }

    protected Object doCast(Object val, ColDataType type) {

        switch (type.getDataType().toLowerCase()) {
            case "string":
            case "varchar":
                return String.valueOf(val);
            case "int":
                return castNumber(val).intValue();
            case "long":
                return castNumber(val).longValue();
            case "double":
                return castNumber(val).doubleValue();
            case "float":
                return castNumber(val).floatValue();
            case "date":
                return castDate(val);
        }

        return val;
    }

    protected Date castDate(Object number) {
        return CastUtils.castDate(number);
    }

    protected Number castNumber(Object number) {
        return CastUtils.castNumber(number);
    }

    @Override
    public String getId() {
        return ID;
    }
}
