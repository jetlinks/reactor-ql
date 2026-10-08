/*
 * Copyright 2025 JetLinks https://www.jetlinks.cn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jetlinks.reactor.ql.supports;

import com.google.common.base.Preconditions;
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
        if (limit == 2) {
            // 嵌套属性每次只拆首段；固定分隔符无需创建 Matcher 和中间 List。
            int dot = str.indexOf('.');
            return dot < 0
                    ? new String[]{str}
                    : new String[]{str.substring(0, dot), str.substring(dot + 1)};
        }
        return splitPattern.split(str, limit);
    }

    protected String[] splitCast(String str) {
        return castPattern.split(str);
    }

    @Override
    public Optional<Object> getProperty(Object property, Object source) {
        return Optional.ofNullable(getPropertyValue(property, source));
    }

    public Object getPropertyValue(Object property, Object source) {
        if (source == null) {
            return null;
        }
        if (property instanceof String) {
            property = SqlUtils.getCleanStr((String) property);
        }
        //当前值
        if ("this".equals(property) || "$".equals(property) || "*".equals(property)) {
            return source;
        }
        //数字,可能是获取数组中的值
        if (property instanceof Number) {
            return getIndexedPropertyValue(((Number) property).intValue(), source);
        }

        Function<Object, Object> mapper = Function.identity();
        String strProperty = String.valueOf(property);

        //类型转换,类似PostgreSQL的写法,name::string
        if (strProperty.contains("::")) {
            String[] cast = splitCast(strProperty);
            strProperty = cast[0];
            mapper = v -> CastFeature.castValue(v, cast[1]);
        }
        return getNamedPropertyValue(strProperty, source, mapper);
    }

    private Object getIndexedPropertyValue(int index, Object source) {
        if (source instanceof Collection) {
            // 保留完整快照，不能跳过惰性集合中未选元素的转换或错误。
            Object[] values = ((Collection<?>) source).toArray();
            return values[Preconditions.checkElementIndex(index, values.length)];
        }
        return CastUtils.castArray(source).get(index);
    }

    private Object getNamedPropertyValue(String strProperty,
                                         Object source,
                                         Function<Object, Object> mapper) {
        //尝试先获取一次值，大部分是这种情况,避免不必要的判断.
        Object direct = doGetProperty0(strProperty, source);
        if (direct != null) {
            return mapper.apply(direct);
        }
        //值为null ,可能是其他获取方式.

        // 无路径分隔符时已穷尽直接查找；无需为普通缺失属性创建正则 Matcher 和分段数组。
        if (strProperty.indexOf('.') < 0) {
            return null;
        }

        Object tmp = source;
        // a.b.c 的情况
        String[] props = splitDot(strProperty, 2);
        if (props.length <= 1) {
            return null;
        }
        while (props.length > 1) {
            tmp = doGetProperty0(props[0], tmp);
            if (tmp == null) {
                return null;
            }
            Object fast = doGetProperty0(props[1], tmp);
            if (fast != null) {
                return mapper.apply(fast);
            }
            if (props[1].contains(".")) {
                props = splitDot(props[1], 2);
            } else {
                return null;
            }
        }
        return mapper.apply(tmp);
    }

    /**
     * 为已知的属性名准备读取函数；动态属性仍使用 {@link #getPropertyValue(Object, Object)}。
     * 每层先查完整含点键，再按首段向下查，保持与动态路径相同的优先级。
     */
    public Function<Object, Object> preparePropertyValue(String property) {
        if (property == null || property.isEmpty()) {
            return source -> getPropertyValue(property, source);
        }
        String cleaned = SqlUtils.getCleanStr(property);
        if (cleaned.indexOf('.') < 0 || cleaned.contains("::")) {
            // The dynamic entry point cleans the name itself; quoting is not idempotent.
            return source -> getPropertyValue(property, source);
        }
        List<String> heads = new ArrayList<>();
        List<String> tails = new ArrayList<>();
        String remaining = cleaned;
        int dot;
        while ((dot = remaining.indexOf('.')) >= 0) {
            heads.add(remaining.substring(0, dot));
            remaining = remaining.substring(dot + 1);
            tails.add(remaining);
        }
        String[] prefixes = heads.toArray(new String[0]);
        String[] suffixes = tails.toArray(new String[0]);
        return source -> getPreparedPropertyValue(cleaned, prefixes, suffixes, source);
    }

    private Object getPreparedPropertyValue(String property,
                                            String[] prefixes,
                                            String[] suffixes,
                                            Object source) {
        if (source == null) {
            return null;
        }
        Object direct = doGetProperty0(property, source);
        if (direct != null) {
            return direct;
        }
        Object current = source;
        for (int i = 0; i < prefixes.length; i++) {
            current = doGetProperty0(prefixes[i], current);
            if (current == null) {
                return null;
            }
            Object nested = doGetProperty0(suffixes[i], current);
            if (nested != null) {
                return nested;
            }
        }
        return null;
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
