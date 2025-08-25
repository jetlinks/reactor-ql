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
package org.jetlinks.reactor.ql.utils;

import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.*;
import java.util.*;

public class CompareUtils {


    public static int compare(Object source, Object target) {
        if (Objects.equals(source, target)) {
            return 0;
        }

        if (source == null || target == null) {
            return -1;
        }

        if (source.equals(target)) {
            return 0;
        }

        if (source.getClass() == target.getClass() && source instanceof Comparable) {
            return ((Comparable) source).compareTo(target);
        }

        //时间
        {
            if (source instanceof Instant) {
                source = Date.from(((Instant) source));
            }
            if (target instanceof Instant) {
                target = Date.from(((Instant) target));
            }

            if (source instanceof LocalDateTime) {
                source = Date.from(((LocalDateTime) source).atZone(ZoneId.systemDefault()).toInstant());
            }
            if (target instanceof LocalDateTime) {
                target = Date.from(((LocalDateTime) target).atZone(ZoneId.systemDefault()).toInstant());
            }
            if (source instanceof LocalDate) {
                source = Date.from(((LocalDate) source).atStartOfDay(ZoneId.systemDefault()).toInstant());
            }
            if (target instanceof LocalDate) {
                target = Date.from(((LocalDate) target).atStartOfDay(ZoneId.systemDefault()).toInstant());
            }
            if (source instanceof LocalTime) {
                source = Date.from((LocalDateTime.of(LocalDate.now(), ((LocalTime) source)))
                                           .atZone(ZoneId.systemDefault())
                                           .toInstant());
            }
            if (target instanceof LocalTime) {
                target = Date.from((LocalDateTime.of(LocalDate.now(), ((LocalTime) target)))
                                           .atZone(ZoneId.systemDefault())
                                           .toInstant());
            }

            if (source instanceof Date) {
                return compare(((Date) source), target);
            }

            if (target instanceof Date) {
                return -compare(((Date) target), source);
            }
        }

        //枚举
        if (source.getClass().isEnum()) {
            return compare(((Enum<?>) source), target);
        }

        if (target.getClass().isEnum()) {
            return -compare(((Enum<?>) target), source);
        }

        //数字
        {
            if (source instanceof Number) {
                return compare(((Number) source), target);
            }
            if (target instanceof Number) {
                return -compare(((Number) target), source);
            }
        }
        if (source instanceof CharSequence) {
            return compare(String.valueOf(source), target);
        }
        //字符
        if (target instanceof CharSequence) {
            return -compare(String.valueOf(target), source);
        }

        //boolean
        if (source instanceof Boolean
                || target instanceof Boolean) {
            return CastUtils.castBoolean(target) == CastUtils.castBoolean(source)
                    ? 0
                    : -1;
        }

        return -1;
    }

    public static boolean equals(Object source, Object target) {
        try {
            return compare(source, target) == 0;
        } catch (Throwable e) {
            return false;
        }
    }

    private static int compare(Number number, Object target) {
        return compare(number, CastUtils.castNumber(target, (ignore) -> null));
    }

    private static int compare(BigDecimal number, Number target) {
        if (target instanceof BigDecimal) {
            return number.compareTo(((BigDecimal) target));
        } else if (target instanceof BigInteger) {
            return number.compareTo(new BigDecimal(((BigInteger) target)));
        } else {
            return number.compareTo(BigDecimal.valueOf(target.doubleValue()));
        }
    }

    private static int compare(BigInteger number, Number target) {
        if (target instanceof BigDecimal) {
            return number.compareTo(((BigDecimal) target).toBigInteger());
        } else if (target instanceof BigInteger) {
            return number.compareTo(((BigInteger) target));
        } else {
            return number.compareTo(BigInteger.valueOf(target.longValue()));
        }
    }

    public static int compare(Number number, Number target) {
        if (number == null && target == null) {
            return 0;
        }
        if (number != null && target == null) {
            return 1;
        }
        if (number == null) {
            return -1;
        }
        if (number instanceof BigDecimal) {
            return compare((BigDecimal) number, target);
        }
        if (target instanceof BigDecimal) {
            return -compare((BigDecimal) target, number);
        }

        if (number instanceof BigInteger) {
            return compare((BigInteger) number, target);
        }
        if (target instanceof BigInteger) {
            return -compare((BigInteger) target, number);
        }

        return Double.compare(number.doubleValue(), target.doubleValue());
    }

    private static int compare(Enum<?> e, Object target) {
        if (target instanceof Number) {
            return Integer.compare(e.ordinal(), ((Number) target).intValue());
        }
        return e.name().compareToIgnoreCase(String.valueOf(target));
    }

    private static int compare(String string, Object target) {
        return string.compareTo(String.valueOf(target));
    }

    private static int compare(Date date, Object target) {
        try {
            return date.compareTo(CastUtils.castDate(target));
        } catch (Exception ignore) {
            return -1;
        }
    }

    public static boolean contains(Collection<Object> left, Object val) {
        if (val instanceof Collection) {
            for (Object leftVal : left) {
                if (leftVal instanceof Collection) {
                    // 存在任意子集合匹配，则返回true
                    if (CollectionUtils.isEqualCollection((Collection<?>) leftVal, (Collection<?>) val)) {
                        return true;
                    }
                }
            }
        }
        if (val instanceof HashMap) {
            for (Object leftVal : left) {
                // 存在任意Map键值对相同，则返回true
                if (Maps.difference((Map<?, ?>) leftVal, (Map<?,?>)val).areEqual()) {
                    return true;
                }
            }
        }
        return left.contains(val);
    }

}
