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
package org.jetlinks.reactor.ql.supports.map;

import java.math.BigDecimal;
import java.util.*;

/**
 * JSON 集合比较与合并语义。
 *
 * 数据库同名函数优先保持 MySQL / PostgreSQL 的行为差异；ReactorQL 扩展集合函数在这里固定
 * 去重、交集、并集和差集的结构化语义。
 */
final class JsonCollectionOperations {

    private JsonCollectionOperations() {
    }

    static boolean deepEquals(JsonFunctionSupport.JsonLimits limits, Object left, Object right) {
        Object normalizedLeft = JsonValueSupport.normalize(limits, left);
        Object normalizedRight = JsonValueSupport.normalize(limits, right);
        if (normalizedLeft instanceof Number && normalizedRight instanceof Number) {
            return new BigDecimal(String.valueOf(normalizedLeft)).compareTo(new BigDecimal(String.valueOf(normalizedRight))) == 0;
        }
        if (normalizedLeft instanceof Map && normalizedRight instanceof Map) {
            Map<?, ?> l = (Map<?, ?>) normalizedLeft;
            Map<?, ?> r = (Map<?, ?>) normalizedRight;
            if (l.size() != r.size()) {
                return false;
            }
            for (Map.Entry<?, ?> entry : l.entrySet()) {
                if (!r.containsKey(entry.getKey()) || !deepEquals(limits, entry.getValue(), r.get(entry.getKey()))) {
                    return false;
                }
            }
            return true;
        }
        if (normalizedLeft instanceof Collection && normalizedRight instanceof Collection) {
            Iterator<?> li = ((Collection<?>) normalizedLeft).iterator();
            Iterator<?> ri = ((Collection<?>) normalizedRight).iterator();
            while (li.hasNext() && ri.hasNext()) {
                if (!deepEquals(limits, li.next(), ri.next())) {
                    return false;
                }
            }
            return !li.hasNext() && !ri.hasNext();
        }
        return Objects.equals(normalizedLeft, normalizedRight);
    }

    static boolean contains(JsonFunctionSupport.JsonLimits limits, Object target, Object candidate) {
        if (target instanceof Map && candidate instanceof Map) {
            Map<?, ?> targetMap = (Map<?, ?>) target;
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) candidate).entrySet()) {
                if (!targetMap.containsKey(entry.getKey()) || !contains(limits, targetMap.get(entry.getKey()), entry.getValue())) {
                    return false;
                }
            }
            return true;
        }
        if (target instanceof Collection) {
            Collection<?> targetList = (Collection<?>) target;
            if (candidate instanceof Collection) {
                for (Object value : ((Collection<?>) candidate)) {
                    if (!containsAny(limits, targetList, value)) {
                        return false;
                    }
                }
                return true;
            }
            return containsAny(limits, targetList, candidate);
        }
        return deepEquals(limits, target, candidate);
    }

    static boolean overlaps(JsonFunctionSupport.JsonLimits limits, Object left, Object right) {
        if (left instanceof Map && right instanceof Map) {
            Map<?, ?> l = (Map<?, ?>) left;
            Map<?, ?> r = (Map<?, ?>) right;
            for (Map.Entry<?, ?> entry : l.entrySet()) {
                if (r.containsKey(entry.getKey()) && overlaps(limits, entry.getValue(), r.get(entry.getKey()))) {
                    return true;
                }
            }
            return false;
        }
        if (left instanceof Collection && right instanceof Collection) {
            for (Object item : ((Collection<?>) left)) {
                if (containsAny(limits, (Collection<?>) right, item)) {
                    return true;
                }
            }
            return false;
        }
        return deepEquals(limits, left, right);
    }

    @SuppressWarnings("unchecked")
    static Object intersect(JsonFunctionSupport.JsonLimits limits, Object left, Object right) {
        if (left instanceof Collection && right instanceof Collection) {
            List<Object> result = new ArrayList<>();
            for (Object item : ((Collection<?>) left)) {
                if (containsAny(limits, (Collection<?>) right, item) && !containsAny(limits, result, item)) {
                    result.add(item);
                }
            }
            return result;
        }
        if (left instanceof Map && right instanceof Map) {
            Map<String, Object> result = new LinkedHashMap<>();
            Map<String, Object> l = (Map<String, Object>) left;
            Map<String, Object> r = (Map<String, Object>) right;
            for (Map.Entry<String, Object> entry : l.entrySet()) {
                if (r.containsKey(entry.getKey()) && overlaps(limits, entry.getValue(), r.get(entry.getKey()))) {
                    result.put(entry.getKey(), intersectValue(limits, entry.getValue(), r.get(entry.getKey())));
                }
            }
            return result;
        }
        return deepEquals(limits, left, right) ? left : Collections.emptyList();
    }

    @SuppressWarnings("unchecked")
    static Object union(JsonFunctionSupport.JsonLimits limits, Object left, Object right) {
        if (left instanceof Collection || right instanceof Collection) {
            List<Object> result = new ArrayList<>();
            appendDistinct(limits, result, left instanceof Collection ? (Collection<?>) left : Collections.singletonList(left));
            appendDistinct(limits, result, right instanceof Collection ? (Collection<?>) right : Collections.singletonList(right));
            return result;
        }
        if (left instanceof Map && right instanceof Map) {
            Map<String, Object> result = new LinkedHashMap<>((Map<String, Object>) left);
            ((Map<String, Object>) right).forEach((key, value) -> result.merge(key, value, (l, r) -> union(limits, l, r)));
            return result;
        }
        return deepEquals(limits, left, right) ? left : Arrays.asList(left, right);
    }

    @SuppressWarnings("unchecked")
    static Object diff(JsonFunctionSupport.JsonLimits limits, Object left, Object right) {
        if (left instanceof Collection) {
            List<Object> result = new ArrayList<>();
            Collection<?> rightList = right instanceof Collection ? (Collection<?>) right : Collections.singletonList(right);
            for (Object item : ((Collection<?>) left)) {
                if (!containsAny(limits, rightList, item)) {
                    result.add(item);
                }
            }
            return result;
        }
        if (left instanceof Map && right instanceof Map) {
            Map<String, Object> result = new LinkedHashMap<>();
            Map<String, Object> r = (Map<String, Object>) right;
            ((Map<String, Object>) left).forEach((key, value) -> {
                if (!r.containsKey(key) || !deepEquals(limits, value, r.get(key))) {
                    result.put(key, value);
                }
            });
            return result;
        }
        return deepEquals(limits, left, right) ? null : left;
    }

    /**
     * MySQL JSON_MERGE / JSON_MERGE_PRESERVE 会保留重复 key，不能简化成 Map.putAll。
     */
    @SuppressWarnings("unchecked")
    static Object mergePreserve(JsonFunctionSupport.JsonLimits limits, Object left, Object right) {
        if (left instanceof Map && right instanceof Map) {
            Map<String, Object> result = new LinkedHashMap<>((Map<String, Object>) left);
            ((Map<String, Object>) right).forEach((key, value) -> result.merge(key, value, (l, r) -> mergeDuplicateValue(limits, l, r)));
            return result;
        }
        List<Object> result = new ArrayList<>();
        appendMergeArray(result, left);
        appendMergeArray(result, right);
        return result;
    }

    /**
     * MySQL JSON_MERGE_PATCH follows RFC 7396: object patch is recursive and JSON null deletes keys.
     */
    @SuppressWarnings("unchecked")
    static Object mergePatch(JsonFunctionSupport.JsonLimits limits, Object target, Object patch) {
        if (!(patch instanceof Map)) {
            return patch;
        }
        Map<String, Object> result = target instanceof Map
                ? new LinkedHashMap<>((Map<String, Object>) target)
                : new LinkedHashMap<>();
        ((Map<String, Object>) patch).forEach((key, value) -> {
            if (value == null) {
                result.remove(key);
            } else {
                result.put(key, mergePatch(limits, result.get(key), value));
            }
        });
        return result;
    }

    private static Object intersectValue(JsonFunctionSupport.JsonLimits limits, Object left, Object right) {
        if (left instanceof Collection || left instanceof Map) {
            return intersect(limits, left, right);
        }
        return left;
    }

    private static boolean containsAny(JsonFunctionSupport.JsonLimits limits, Collection<?> collection, Object value) {
        for (Object item : collection) {
            if (deepEquals(limits, item, value)) {
                return true;
            }
        }
        return false;
    }

    private static void appendDistinct(JsonFunctionSupport.JsonLimits limits, List<Object> result, Collection<?> values) {
        for (Object value : values) {
            if (!containsAny(limits, result, value)) {
                result.add(value);
            }
        }
    }

    private static Object mergeDuplicateValue(JsonFunctionSupport.JsonLimits limits, Object left, Object right) {
        if (left instanceof Map && right instanceof Map) {
            return mergePreserve(limits, left, right);
        }
        List<Object> result = new ArrayList<>();
        appendMergeArray(result, left);
        appendMergeArray(result, right);
        return result;
    }

    private static void appendMergeArray(List<Object> result, Object value) {
        if (value instanceof Collection) {
            result.addAll((Collection<?>) value);
        } else {
            result.add(value);
        }
    }

    interface Combiner {
        Object combine(JsonFunctionSupport.JsonLimits limits, Object left, Object right);
    }
}
