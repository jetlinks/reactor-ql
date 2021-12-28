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

    public static final DefaultPropertyFeature GLOBAL = new DefaultPropertyFeature();

    @Override
    public Optional<Object> getProperty(Object property, Object source) {
        if (source == null) {
            return Optional.empty();
        }
        if (property instanceof String) {
            property = SqlUtils.getCleanStr((String) property);
        }
        //当前值
        if ("this".equals(property) || "$".equals(property) || "*".equals(property)) {
            return Optional.of(source);
        }
        //数字,可能是获取数组中的值
        if (property instanceof Number) {
            int index = ((Number) property).intValue();
            return Optional.ofNullable(CastUtils.castArray(source).get(index));
        }

        Function<Object, Object> mapper = Function.identity();
        String strProperty = String.valueOf(property);

        //类型转换,类似PostgreSQL的写法,name::string
        if (strProperty.contains("::")) {
            String[] cast = strProperty.split("::");
            strProperty = cast[0];
            mapper = v -> CastFeature.castValue(v, cast[1]);
        }
        //尝试先获取一次值，大部分是这种情况,避免不必要的判断.
        Object direct = doGetProperty(strProperty, source);
        if (direct != null) {
            return Optional.of(direct).map(mapper);
        }
        //值为null ,可能是其他获取方式.

        Object tmp = source;
        // a.b.c 的情况
        String[] props = strProperty.split("[.]", 2);
        if (props.length <= 1) {
            return Optional.empty();
        }
        while (props.length > 1) {
            tmp = doGetProperty(props[0], tmp);
            if (tmp == null) {
                return Optional.empty();
            }
            Object fast = doGetProperty(props[1], tmp);
            if (fast != null) {
                return Optional.of(fast).map(mapper);
            }
            if (props[1].contains(".")) {
                props = props[1].split("[.]", 2);
            } else {
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
