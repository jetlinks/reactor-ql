package org.jetlinks.reactor.ql.supports;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.PropertyUtils;
import org.jetlinks.reactor.ql.feature.PropertyFeature;
import org.jetlinks.reactor.ql.supports.map.CastFeature;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.jetlinks.reactor.ql.utils.SqlUtils;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

@Slf4j
public class DefaultPropertyFeature implements PropertyFeature {

    @Override
    public Optional<Object> getProperty(Object property, Object value) {
        if (value == null) {
            return Optional.empty();
        }
        if (property instanceof String) {
            property = SqlUtils.getCleanStr((String) property);
        }
        if ("this".equals(property) || "$".equals(property) || "*".equals(property)) {
            return Optional.of(value);
        }

        if (property instanceof Number) {
            int index = ((Number) property).intValue();
            return Optional.ofNullable(CastUtils.castArray(value).get(index));
        }
        Function<Object, Object> mapper = Function.identity();
        String strProperty = String.valueOf(property);
        if (strProperty.contains("::")) {
            String[] cast = strProperty.split("::");

            strProperty = cast[0];
            mapper = v -> CastFeature.castValue(v, cast[1]);
        }

        Object direct = doGetProperty(strProperty, value);
        if (direct != null) {
            return Optional.of(direct).map(mapper);
        }
        Object tmp = value;
        String[] arr = strProperty.split("[.]");
        if (arr.length == 1) {
            return Optional.empty();
        }
        for (String prop : arr) {
            tmp = doGetProperty(prop, tmp);
            if (tmp == null) {
                return Optional.empty();
            }
        }
        return Optional.of(tmp).map(mapper);
    }

    protected Object doGetProperty(String property, Object value) {
        if ("this".equals(property) || "$".equals(property)) {
            return value;
        }
        if (value instanceof Map) {
            return ((Map<?, ?>) value).get(property);
        }
        try {
            return PropertyUtils.getProperty(value, property);
        } catch (Exception e) {
            log.warn("get property error", e);
        }
        return null;

    }


}
