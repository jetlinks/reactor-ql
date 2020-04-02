package org.jetlinks.reactor.ql.supports;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.PropertyUtils;
import org.jetlinks.reactor.ql.feature.PropertyFeature;
import org.jetlinks.reactor.ql.utils.CastUtils;

import java.util.Map;
import java.util.Optional;

@Slf4j
public class DefaultPropertyFeature implements PropertyFeature {

    @Override
    public Optional<Object> getProperty(Object property, Object value) {
        if (value == null) {
            return Optional.empty();
        }
        if ("this".equals(property) || "$".equals(property)) {
            return Optional.of(value);
        }

        if (property instanceof Number) {
            int index = ((Number) property).intValue();
            return Optional.ofNullable(CastUtils.castArray(value).get(index));
        }

        String strProperty = String.valueOf(property);
        Object tmp = value;
        String[] arr = strProperty.split("[.]");

        for (String prop : arr) {
            tmp = doGetProperty(prop, tmp);
            if (tmp == null) {
                return Optional.empty();
            }
        }
        return Optional.of(tmp);
    }

    protected Object doGetProperty(String property, Object value) {

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
