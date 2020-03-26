package org.jetlinks.reactor.ql.supports;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import org.apache.commons.beanutils.BeanUtilsBean2;
import org.apache.commons.beanutils.PropertyUtils;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.FeatureId;

import java.lang.reflect.InvocationTargetException;
import java.util.function.Function;

public class PropertyMapFeature implements ValueMapFeature {

    static String ID = FeatureId.ValueMap.of("property").getId();

    @Override
    public Function<Object, Object> createMapper(Expression expression, ReactorQLMetadata metadata) {
        Column column = ((Column) expression);
        String name = column.getColumnName();
        if (name.equalsIgnoreCase("this")) {
            return Function.identity();
        }
        return obj -> doGetProperty(obj, name);
    }

    protected Object doGetProperty(Object obj, String column) {
        try {
            return PropertyUtils.getProperty(obj, column);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public String getId() {
        return ID;
    }
}
