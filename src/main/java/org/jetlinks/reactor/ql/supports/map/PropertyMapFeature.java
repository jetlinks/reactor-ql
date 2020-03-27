package org.jetlinks.reactor.ql.supports.map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import org.apache.commons.beanutils.PropertyUtils;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;

import java.util.Map;
import java.util.function.Function;

public class PropertyMapFeature implements ValueMapFeature {

    static String ID = FeatureId.ValueMap.property.getId();

    @Override
    public Function<Object, Object> createMapper(Expression expression, ReactorQLMetadata metadata) {
        Column column = ((Column) expression);
        String name = column.getColumnName();
        if (name.startsWith("\"")) {
            name = name.substring(1);
        }
        if (name.endsWith("\"")) {
            name = name.substring(0, name.length() - 1);
        }
        if (name.equalsIgnoreCase("this")) {
            return Function.identity();
        }
        String fName = name;
        return obj -> doGetProperty(obj, fName);
    }

    protected Object doGetProperty(Map<String, Object> obj, String column) {


        return obj.get(column);
    }

    protected Object doGetProperty(Object obj, String column) {
        if (obj instanceof Map) {
            return doGetProperty(((Map) obj), column);
        }
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
