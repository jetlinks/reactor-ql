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
package org.jetlinks.reactor.ql;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import org.apache.commons.collections.CollectionUtils;
import org.jetlinks.reactor.ql.exception.ReactorQLException;
import org.jetlinks.reactor.ql.feature.ScalarValueMapper;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.jetlinks.reactor.ql.utils.CompareUtils;
import org.jetlinks.reactor.ql.utils.ExpressionUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * ReactorQL 排序执行支持。
 * <p>
 * 全局排序必须等上游完成后才能输出精确 SQL 结果，因此这里统一收敛 ORDER BY 的内存边界：
 * 有 LIMIT 时使用 Top-N 小堆，只保留 {@code offset + limit} 条候选；无 LIMIT 时按 setting 限制可物化行数；
 * 显式设置窗口大小时只做局部窗口排序，不承诺全局有序。
 */
final class OrderBySupport {

    private static final long DEFAULT_ORDER_BY_MAX_ROWS = 10_000L;
    private static final long HARD_MAX_ORDER_BY_MAX_ROWS = 1_000_000L;
    private static final long DEFAULT_ORDER_BY_WINDOW_SIZE = 0L;
    private static final long HARD_MAX_ORDER_BY_WINDOW_SIZE = 100_000L;

    private static final Object NULL_ORDER_VALUE = new Object();

    private OrderBySupport() {
    }

    static BiFunction<ReactorQLContext, Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> create(ReactorQLMetadata metadata) {
        List<OrderByElement> orders = metadata.getSql().getOrderByElements();
        if (CollectionUtils.isEmpty(orders)) {
            return (ctx, flux) -> flux;
        }
        List<Function<ReactorQLRecord, Publisher<?>>> mappers = new ArrayList<>(orders.size());
        List<Boolean> asc = new ArrayList<>(orders.size());
        List<OrderByElement.NullOrdering> nullOrderings = new ArrayList<>(orders.size());
        for (OrderByElement order : orders) {
            Expression expr = order.getExpression();
            mappers.add(ValueMapFeature.createMapperNow(expr, metadata));
            asc.add(order.isAsc());
            nullOrderings.add(order.getNullOrdering());
        }

        Comparator<OrderedRecord> comparator = mappers.size() == 1
                ? createSingleOrderRecordComparator(asc.get(0), nullOrderings.get(0))
                : createMultiOrderRecordComparator(asc, nullOrderings);
        long maxRows = longSetting(metadata,
                                   DefaultReactorQL.SETTING_ORDER_BY_MAX_ROWS,
                                   DEFAULT_ORDER_BY_MAX_ROWS,
                                   HARD_MAX_ORDER_BY_MAX_ROWS,
                                   false);
        long windowSize = longSetting(metadata,
                                      DefaultReactorQL.SETTING_ORDER_BY_WINDOW_SIZE,
                                      DEFAULT_ORDER_BY_WINDOW_SIZE,
                                      HARD_MAX_ORDER_BY_WINDOW_SIZE,
                                      true);
        OrderValueMapper orderValueMapper = createOrderValueMapper(mappers, metadata);
        ScalarValueMapper scalarTopNMapper = mappers.size() == 1
                && !metadata.isCheckpoint()
                && mappers.get(0) instanceof ScalarValueMapper
                ? (ScalarValueMapper) mappers.get(0)
                : null;
        Limit limit = metadata.getSql().getLimit();

        return (ctx, flux) -> Flux.defer(() -> {
            Long rowCount = limitValue(limit, ctx, true);
            Long offset = limitValue(limit, ctx, false);
            Long topN = topNSize(rowCount, offset);
            if (topN != null) {
                assertSortRows(topN, maxRows);
                if (scalarTopNMapper != null) {
                    return topNScalarSingle(flux, scalarTopNMapper, comparator,
                                            asc.get(0), nullOrderings.get(0), topN.intValue());
                }
                return topN(metadata, flux, orderValueMapper, comparator, topN.intValue());
            }
            if (windowSize > 0) {
                return windowSort(flux, orderValueMapper, comparator, windowSize);
            }
            // 精确全局排序仍需物化当前批次，超出上限时 fail-fast，避免长流/无限流触发 OOM。
            return limitSortRows(flux, maxRows)
                    .transform(records -> orderedRecords(metadata, records, orderValueMapper))
                    .sort(comparator)
                    .map(OrderedRecord::getRecord);
        });
    }

    private static OrderValueMapper createOrderValueMapper(List<Function<ReactorQLRecord, Publisher<?>>> mappers,
                                                           ReactorQLMetadata metadata) {
        if (!metadata.isCheckpoint() && mappers.stream().allMatch(ScalarValueMapper.class::isInstance)) {
            ScalarValueMapper[] scalarMappers = mappers
                    .stream()
                    .map(ScalarValueMapper.class::cast)
                    .toArray(ScalarValueMapper[]::new);
            if (scalarMappers.length == 1) {
                ScalarValueMapper mapper = scalarMappers[0];
                return (flux, sequential) -> flux.map(record -> {
                    Object value = mapper.applyScalar(record);
                    return new OrderedRecord(record, value == null ? NULL_ORDER_VALUE : value);
                });
            }
            return (flux, sequential) -> flux.map(record -> {
                Object[] values = new Object[scalarMappers.length];
                for (int i = 0; i < scalarMappers.length; i++) {
                    Object value = scalarMappers[i].applyScalar(record);
                    values[i] = value == null ? NULL_ORDER_VALUE : value;
                }
                return new OrderedRecord(record, values);
            });
        }
        if (mappers.size() == 1) {
            Function<ReactorQLRecord, Publisher<?>> mapper = mappers.get(0);
            Function<ReactorQLRecord, Mono<OrderedRecord>> recordMapper = record -> orderValue(mapper, record)
                    .map(value -> new OrderedRecord(record, value));
            return (flux, sequential) -> sequential
                    ? flux.concatMap(recordMapper)
                    : metadata.flatMap(flux, recordMapper);
        }
        Function<ReactorQLRecord, Mono<OrderedRecord>> recordMapper = record -> Flux
                .fromIterable(mappers)
                .concatMap(mapper -> orderValue(mapper, record))
                .collectList()
                .map(values -> new OrderedRecord(record, values.toArray()));
        return (flux, sequential) -> sequential
                ? flux.concatMap(recordMapper)
                : metadata.flatMap(flux, recordMapper);
    }

    private static Mono<Object> orderValue(Function<ReactorQLRecord, Publisher<?>> mapper, ReactorQLRecord record) {
        return Mono.<Object>from(mapper.apply(record))
                   .defaultIfEmpty(NULL_ORDER_VALUE);
    }

    private static Flux<OrderedRecord> orderedRecords(ReactorQLMetadata metadata,
                                                      Flux<ReactorQLRecord> flux,
                                                      OrderValueMapper mapper) {
        return mapper.apply(flux, false);
    }

    private static Comparator<OrderedRecord> createSingleOrderRecordComparator(boolean asc,
                                                                                 OrderByElement.NullOrdering nullOrdering) {
        return (left, right) -> compareOrderValue(unwrapOrderValue(left.value),
                                                  unwrapOrderValue(right.value),
                                                  asc,
                                                  nullOrdering);
    }

    private static Comparator<OrderedRecord> createMultiOrderRecordComparator(List<Boolean> asc,
                                                                                List<OrderByElement.NullOrdering> nullOrderings) {
        return (left, right) -> {
            Object[] leftValues = left.values();
            Object[] rightValues = right.values();
            int size = asc.size();
            for (int i = 0; i < size; i++) {
                Object leftValue = unwrapOrderValue(leftValues[i]);
                Object rightValue = unwrapOrderValue(rightValues[i]);
                int compare = compareOrderValue(leftValue, rightValue, asc.get(i), nullOrderings.get(i));
                if (compare != 0) {
                    return compare;
                }
            }
            return 0;
        };
    }

    private static Object unwrapOrderValue(Object value) {
        return value == NULL_ORDER_VALUE ? null : value;
    }

    private static int compareOrderValue(Object left,
                                         Object right,
                                         boolean asc,
                                         OrderByElement.NullOrdering nullOrdering) {
        if (left == right) {
            return 0;
        }
        if (left == null || right == null) {
            return compareNull(left == null, asc, nullOrdering);
        }
        int compare = CompareUtils.compare(left, right);
        return asc ? compare : -compare;
    }

    private static int compareNull(boolean leftNull,
                                   boolean asc,
                                   OrderByElement.NullOrdering nullOrdering) {
        boolean nullsFirst = nullOrdering == OrderByElement.NullOrdering.NULLS_FIRST
                || (nullOrdering == null && asc);
        return leftNull
                ? (nullsFirst ? -1 : 1)
                : (nullsFirst ? 1 : -1);
    }

    private static Flux<ReactorQLRecord> topN(ReactorQLMetadata metadata,
                                              Flux<ReactorQLRecord> flux,
                                              OrderValueMapper mapper,
                                              Comparator<OrderedRecord> comparator,
                                              int size) {
        if (size == 0) {
            return Flux.empty();
        }
        Comparator<OrderedRecord> worstFirst = comparator.reversed();
        return emitTopN(orderedRecords(metadata, flux, mapper)
                                .collect(() -> new PriorityQueue<>(size, worstFirst),
                                         (queue, record) -> addTopNRecord(queue, record, size, comparator)),
                        comparator);
    }

    private static Flux<ReactorQLRecord> topNScalarSingle(Flux<ReactorQLRecord> flux,
                                                          ScalarValueMapper mapper,
                                                          Comparator<OrderedRecord> comparator,
                                                          boolean asc,
                                                          OrderByElement.NullOrdering nullOrdering,
                                                          int size) {
        if (size == 0) {
            return Flux.empty();
        }
        Comparator<OrderedRecord> worstFirst = comparator.reversed();
        // 每条输入仍求一次键；只有真正进入有界小堆的记录才需要持有排序包装。
        return emitTopN(flux.collect(() -> new PriorityQueue<>(size, worstFirst),
                                     (queue, record) -> {
                                         Object value = mapper.applyScalar(record);
                                         Object key = value == null ? NULL_ORDER_VALUE : value;
                                         if (queue.size() < size) {
                                             queue.offer(new OrderedRecord(record, key));
                                             return;
                                         }
                                         OrderedRecord worst = queue.peek();
                                         if (worst != null && compareOrderValue(unwrapOrderValue(key),
                                                                                unwrapOrderValue(worst.value),
                                                                                asc,
                                                                                nullOrdering) < 0) {
                                             queue.poll();
                                             queue.offer(new OrderedRecord(record, key));
                                         }
                                     }), comparator);
    }

    private static Flux<ReactorQLRecord> emitTopN(Mono<PriorityQueue<OrderedRecord>> queued,
                                                  Comparator<OrderedRecord> comparator) {
        return queued
                .flatMapMany(retained -> {
                    List<OrderedRecord> sorted = new ArrayList<>(retained);
                    sorted.sort(comparator);
                    return Flux.fromIterable(sorted);
                })
                .map(OrderedRecord::getRecord);
    }

    private static void addTopNRecord(PriorityQueue<OrderedRecord> queue,
                                      OrderedRecord record,
                                      int size,
                                      Comparator<OrderedRecord> comparator) {
        if (queue.size() < size) {
            queue.offer(record);
            return;
        }
        OrderedRecord worst = queue.peek();
        if (worst != null && comparator.compare(record, worst) < 0) {
            queue.poll();
            queue.offer(record);
        }
    }

    private static Flux<ReactorQLRecord> windowSort(Flux<ReactorQLRecord> flux,
                                                    OrderValueMapper mapper,
                                                    Comparator<OrderedRecord> comparator,
                                                    long windowSize) {
        int size = Math.toIntExact(windowSize);
        // 显式启用的局部排序模式：只保证每个固定大小窗口内有序，不承诺 SQL 全局 ORDER BY 语义。
        return flux
                .transform(records -> mapper.apply(records, true))
                .window(size)
                .concatMap(window -> window
                        .sort(comparator)
                        .map(OrderedRecord::getRecord));
    }

    private static Flux<ReactorQLRecord> limitSortRows(Flux<ReactorQLRecord> flux, long maxRows) {
        // 调用方的 Flux.defer 为每次订阅创建此计数；Reactive Streams 保证单个订阅的信号串行。
        long[] rows = {0};
        return flux.handle((record, sink) -> {
            if (rows[0] >= maxRows) {
                sink.error(ReactorQLException.resourceLimit(
                        "ORDER BY 输入行数超过 setting[" + DefaultReactorQL.SETTING_ORDER_BY_MAX_ROWS + "]: " + maxRows,
                        "为 ORDER BY 增加 LIMIT 以启用 Top-N 排序，或在可信场景下调大 orderBy.maxRows；无限流建议使用窗口排序。",
                        "select * from test order by timestamp desc limit 100"
                ));
            } else {
                rows[0]++;
                sink.next(record);
            }
        });
    }

    private static Long topNSize(Long rowCount, Long offset) {
        if (rowCount == null) {
            return null;
        }
        if (rowCount < 0) {
            throw ReactorQLException.invalidArgument(
                    "LIMIT 不能为负数: " + rowCount,
                    "使用非负整数 LIMIT；如果不需要返回结果，可使用 limit 0。",
                    "select * from test order by timestamp desc limit 100"
            );
        }
        long skip = offset == null ? 0 : offset;
        if (skip < 0) {
            throw ReactorQLException.invalidArgument(
                    "LIMIT offset 不能为负数: " + skip,
                    "使用非负整数 offset，或省略 offset。",
                    "select * from test order by timestamp desc limit 10, 100"
            );
        }
        if (Long.MAX_VALUE - skip < rowCount) {
            return Long.MAX_VALUE;
        }
        return skip + rowCount;
    }

    private static Long limitValue(Limit limit, ReactorQLContext context, boolean rowCount) {
        if (limit == null) {
            return null;
        }
        Expression expr = rowCount ? limit.getRowCount() : limit.getOffset();
        if (expr == null) {
            return null;
        }
        return ExpressionUtils
                .getSimpleValue(expr, context)
                .map(val -> CastUtils.castNumber(val).longValue())
                .orElse(null);
    }

    private static void assertSortRows(long rows, long maxRows) {
        if (rows > maxRows || rows > Integer.MAX_VALUE) {
            throw ReactorQLException.resourceLimit(
                    "ORDER BY 缓冲行数超过 setting[" + DefaultReactorQL.SETTING_ORDER_BY_MAX_ROWS + "]: " + maxRows + ", actual=" + rows,
                    "为 ORDER BY 增加 LIMIT，或在可信场景下调大 orderBy.maxRows；如果只需要局部有序，可设置 orderBy.windowSize。",
                    "select * from test order by timestamp desc limit 100"
            );
        }
    }

    private static long longSetting(ReactorQLMetadata metadata,
                                    String key,
                                    long defaultValue,
                                    long hardMax,
                                    boolean allowZero) {
        long value;
        try {
            value = metadata
                    .getSetting(key)
                    .map(CastUtils::castNumber)
                    .map(Number::longValue)
                    .orElse(defaultValue);
        } catch (RuntimeException e) {
            throw ReactorQLException.builder(ReactorQLException.INVALID_ARGUMENT)
                    .reason("ORDER BY setting[" + key + "] 必须是数字")
                    .suggestion("确认 setting 在允许范围内；orderBy.maxRows 必须为正数，orderBy.windowSize 可为 0 或正数。")
                    .example("orderBy.maxRows=10000 或 orderBy.windowSize=1000")
                    .cause(e)
                    .build();
        }
        if ((allowZero ? value < 0 : value <= 0) || value > hardMax) {
            throw ReactorQLException.invalidArgument(
                    "非法 ORDER BY setting[" + key + "]: " + value + ", hardMax=" + hardMax,
                    "确认 setting 在允许范围内；orderBy.maxRows 必须为正数，orderBy.windowSize 可为 0 或正数。",
                    "orderBy.maxRows=10000 或 orderBy.windowSize=1000"
            );
        }
        return value;
    }

    private interface OrderValueMapper {

        Flux<OrderedRecord> apply(Flux<ReactorQLRecord> flux, boolean sequential);
    }

    private static class OrderedRecord {
        private final ReactorQLRecord record;
        // 单键计划直接保存值；多键保持数组，避免多键比较热循环增加表示分支。
        private final Object value;

        private OrderedRecord(ReactorQLRecord record, Object value) {
            this.record = record;
            this.value = value;
        }

        private OrderedRecord(ReactorQLRecord record, Object[] values) {
            this.record = record;
            this.value = values;
        }

        private Object[] values() {
            return (Object[]) value;
        }

        private ReactorQLRecord getRecord() {
            return record;
        }
    }
}
