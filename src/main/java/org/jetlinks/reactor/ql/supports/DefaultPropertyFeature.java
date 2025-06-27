package org.jetlinks.reactor.ql.supports;

import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.PropertyUtils;
import org.jetlinks.reactor.ql.feature.PropertyFeature;
import org.jetlinks.reactor.ql.supports.map.CastFeature;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.jetlinks.reactor.ql.utils.SqlUtils;

import java.util.*;
import java.util.function.Function;
import java.util.regex.Pattern;

@Slf4j
public class DefaultPropertyFeature implements PropertyFeature {

    public static final DefaultPropertyFeature GLOBAL = new DefaultPropertyFeature();

    private static final Pattern splitPattern = Pattern.compile("[.]");
    private static final Pattern castPattern = Pattern.compile("::");

    protected String[] splitDot(String str, int limit) {
        return splitPattern.split(str, limit);
    }

    protected String[] splitCast(String str) {
        return castPattern.split(str);
    }

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
            String[] cast = splitCast(strProperty);
            strProperty = cast[0];
            mapper = v -> CastFeature.castValue(v, cast[1]);
        }
        //尝试先获取一次值，大部分是这种情况,避免不必要的判断.
        Object direct = doGetProperty0(strProperty, source);
        if (direct != null) {
            return Optional.of(direct).map(mapper);
        }
        //值为null ,可能是其他获取方式.

        Object tmp = source;
        // a.b.c 的情况
        String[] props = splitDot(strProperty, 2);
        if (props.length <= 1) {
            return Optional.empty();
        }
        while (props.length > 1) {
            tmp = doGetProperty0(props[0], tmp);
            if (tmp == null) {
                return Optional.empty();
            }
            Object fast = doGetProperty0(props[1], tmp);
            if (fast != null) {
                return Optional.of(fast).map(mapper);
            }
            if (props[1].contains(".")) {
                props = splitDot(props[1], 2);
            } else {
                return Optional.empty();
            }
        }
        return Optional.of(tmp).map(mapper);
    }

    private Object doGetProperty0(String property, Object value) {
        if ("this".equals(property) || "$".equals(property)) {
            return value;
        }
        // map类型
        if (value instanceof Map) {
            Object val = ((Map<?, ?>) value).get(property);
            if (val == null) {
                switch (property) {
                    case "$size":
                    case "size":
                        return ((Map<?, ?>) value).size();
                    case "$empty":
                    case "empty":
                        return ((Map<?, ?>) value).isEmpty();
                    case "$keys":
                    case "keys":
                        return ((Map<?, ?>) value).keySet();
                    case "$values":
                    case "values":
                        return ((Map<?, ?>) value).values();
                    case "$entries":
                    case "entries":
                        return Collections2
                                .transform(
                                        ((Map<?, ?>) value).entrySet(),
                                        (e -> {
                                            Map<Object, Object> map = Maps.newHashMapWithExpectedSize(2);
                                            map.put("key", e.getKey());
                                            map.put("value", e.getValue());
                                            return map;
                                        })
                                );
                }
            }
            return val;
        }
        // 集合类型
        if (value instanceof Collection) {
            if (property.startsWith("[") && property.endsWith("]")) {
                property = property.substring(1, property.length() - 1);
            }
            switch (property) {
                case "$size":
                case "size":
                    return ((Collection<?>) value).size();
                case "$empty":
                case "empty":
                    return ((Collection<?>) value).isEmpty();
            }

            // aaa.1
            Number number = CastUtils.castNumber(property, v -> null);
            if (number != null) {
                int idx = number.intValue();
                return getValueFromCollection(idx, (Collection<?>) value);
            }
            return null;
        }
        return doGetProperty(property, value);
    }

    protected Object getValueFromCollection(int index, Collection<?> conn) {
        if (index < 0) {
            index = conn.size() + index;
        }
        if (index < 0 || index >= conn.size()) {
            return null;
        }
        if (conn instanceof List) {
            return ((List<?>) conn).get(index);
        }
        return conn
                .stream()
                .skip(index)
                .findFirst()
                .orElse(null);
    }

    protected Object doGetProperty(String property, Object value) {
        try {
            return PropertyUtils.getProperty(value, property);
        } catch (Exception e) {
            log.warn("get property [{}] from {} error {}", property, value, e.toString());
        }
        return null;
    }


}
