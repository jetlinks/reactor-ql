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
package org.jetlinks.reactor.ql.internal;

import org.jetlinks.reactor.ql.DefaultReactorQL;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.exception.ReactorQLException;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Operators;
import reactor.core.publisher.SignalType;
import reactor.util.context.Context;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Function;

/**
 * 兼容 {@link GroupedFlux} 分组链路的订阅级资源预算。
 *
 * <p>同一查询订阅中的同一分组层共享总预算，每个窗口或父分组使用独立作用域记录自己的键。
 * 行进入 {@code groupBy} 前预留排队单位，实际分组消费者收到该行时释放；完成、错误或取消时
 * 作用域释放剩余单位。它只限制资源，不淘汰分组或改变精确 SQL 结果。</p>
 *
 * <p>该类型不实现排队或需求状态机。分组包装器仅委派信号并挂接预算释放，背压、排队和
 * 取消仍由 Reactor {@code groupBy} 负责；不暴露队列融合，保证每行消费都经过预算回调。</p>
 */
public final class GroupStateBudget {

    private static final int CLOSED = Integer.MIN_VALUE;

    private final int maxActiveKeys;
    private final int maxBufferedRows;
    private final boolean boundActiveKeys;
    private final boolean boundBufferedRows;
    private final AtomicInteger activeKeys = new AtomicInteger();
    private final AtomicInteger bufferedRows = new AtomicInteger();

    private GroupStateBudget(int maxActiveKeys, int maxBufferedRows) {
        this.maxActiveKeys = maxActiveKeys;
        this.maxBufferedRows = maxBufferedRows;
        this.boundActiveKeys = BoundedStateSupport.isBounded(maxActiveKeys);
        this.boundBufferedRows = BoundedStateSupport.isBounded(maxBufferedRows);
    }

    public static int readMaxActiveKeys(ReactorQLMetadata metadata) {
        return BoundedStateSupport.readLimit(
                metadata,
                DefaultReactorQL.SETTING_GROUP_MAX_ACTIVE_KEYS,
                DefaultReactorQL.DEFAULT_GROUP_MAX_ACTIVE_KEYS,
                DefaultReactorQL.HARD_MAX_GROUP_ACTIVE_KEYS,
                "使用 1 到 " + DefaultReactorQL.HARD_MAX_GROUP_ACTIVE_KEYS
                        + " 之间的活跃分组上限。",
                DefaultReactorQL.SETTING_GROUP_MAX_ACTIVE_KEYS + "=65536"
        );
    }

    public static int readMaxBufferedRows(ReactorQLMetadata metadata) {
        return BoundedStateSupport.readLimit(
                metadata,
                DefaultReactorQL.SETTING_GROUP_MAX_BUFFERED_ROWS,
                DefaultReactorQL.DEFAULT_GROUP_MAX_BUFFERED_ROWS,
                DefaultReactorQL.HARD_MAX_GROUP_BUFFERED_ROWS,
                "使用 1 到 " + DefaultReactorQL.HARD_MAX_GROUP_BUFFERED_ROWS
                        + " 之间的兼容分组排队行上限。",
                DefaultReactorQL.SETTING_GROUP_MAX_BUFFERED_ROWS + "=65536"
        );
    }

    /**
     * 创建带订阅级资源预算的 Reactor {@code groupBy} 转换器。
     *
     * @param metadata      查询元数据，配置会在查询构建期读取并校验
     * @param keySelector   同步分组键选择器；不能返回 {@code null}
     * @param valueSelector 分组元素转换器
     * @param <T>           上游元素类型
     * @param <K>           分组键类型
     * @param <V>           分组元素类型
     * @return 每次订阅隔离、同层窗口间共享总预算的分组转换器
     */
    public static <T, K, V> Function<Flux<T>, Flux<Flux<V>>> createGroupMapper(
            ReactorQLMetadata metadata,
            Function<T, K> keySelector,
            Function<T, V> valueSelector) {
        int maxActiveKeys = readMaxActiveKeys(metadata);
        int maxBufferedRows = readMaxBufferedRows(metadata);
        if (!BoundedStateSupport.isBounded(maxActiveKeys)
                && !BoundedStateSupport.isBounded(maxBufferedRows)) {
            // 缺省保持原有 groupBy 行为，也避免为未启用的保护增加逐行键集合和原子计数。
            return source -> source
                    .groupBy(keySelector, valueSelector, Integer.MAX_VALUE)
                    .map(Function.identity());
        }
        Object stateKey = new Object();
        return source -> Flux.deferContextual(context -> {
            SubscriptionContext subscription = context.getOrDefault(SubscriptionContext.class, null);
            GroupStateBudget budget = subscription == null
                    ? new GroupStateBudget(maxActiveKeys, maxBufferedRows)
                    : subscription.getOrCreateState(
                            stateKey,
                            () -> new GroupStateBudget(maxActiveKeys, maxBufferedRows)
                    );
            return budget.groupBy(source, keySelector, valueSelector);
        });
    }

    private <T, K, V> Flux<Flux<V>> groupBy(
            Flux<T> source,
            Function<T, K> keySelector,
            Function<T, V> valueSelector) {
        Scope scope = new Scope();
        // Integer.MAX_VALUE 会被 Reactor 映射为小块链式队列；使用逻辑上限会让每个高基数分组
        // 预分配同等大小的数组块。前置预算门禁仍能读取并拒绝第一个超限行，不依赖较小 prefetch。
        Flux<GroupedFlux<K, V>> groups = source
                .<T>handle((value, sink) -> {
                    K key = keySelector.apply(value);
                    if (scope.reserve(key)) {
                        sink.next(value);
                    }
                })
                .groupBy(keySelector, valueSelector, Integer.MAX_VALUE)
                .map(group -> (GroupedFlux<K, V>) new BudgetedGroupedFlux<>(group, scope))
                .doFinally(scope::outerTerminated);
        // 保持内层对象的 GroupedFlux 返回类型，避免 ReactorDebugAgent 把分组身份包装成普通 Flux。
        return groups.map(Function.identity());
    }

    private void reserveActiveKey() {
        int current = activeKeys.incrementAndGet();
        if (current > maxActiveKeys) {
            activeKeys.decrementAndGet();
            throw ReactorQLException.resourceLimit(
                    "活跃分组状态超过 setting[" + DefaultReactorQL.SETTING_GROUP_MAX_ACTIVE_KEYS
                            + "]: " + maxActiveKeys,
                    "缩小窗口、降低分组键基数或调大受硬上限保护的活跃分组配置。",
                    "select count(1) total from test group by _window(100), deviceId"
            );
        }
    }

    private void reserveBufferedRow() {
        int current = bufferedRows.incrementAndGet();
        if (current > maxBufferedRows) {
            bufferedRows.decrementAndGet();
            throw ReactorQLException.resourceLimit(
                    "兼容分组排队行超过 setting[" + DefaultReactorQL.SETTING_GROUP_MAX_BUFFERED_ROWS
                            + "]: " + maxBufferedRows,
                    "缩小窗口、降低分组键基数、提高受控分组并发或调大受硬上限保护的排队行配置。",
                    DefaultReactorQL.SETTING_GROUP_MAX_BUFFERED_ROWS + "=65536"
            );
        }
    }

    private void releaseActiveKeys(int count) {
        if (count != 0) {
            activeKeys.addAndGet(-count);
        }
    }

    private void releaseBufferedRows(int count) {
        if (count != 0) {
            bufferedRows.addAndGet(-count);
        }
    }

    private final class Scope {

        // 上游 onNext 串行修改；取消不清空集合，避免为热路径引入并发 Map 或逐行锁。
        private final Set<Object> keys = boundActiveKeys ? new HashSet<>() : null;
        private final AtomicInteger localActiveKeys = new AtomicInteger();
        private final AtomicInteger localBufferedRows = new AtomicInteger();
        private final AtomicInteger references = new AtomicInteger(1);
        private volatile Throwable terminalError;

        private boolean reserve(Object key) {
            try {
                if (boundActiveKeys && keys.add(key)) {
                    reserveActiveKey();
                    if (!incrementWhileOpen(localActiveKeys)) {
                        releaseActiveKeys(1);
                        return false;
                    }
                }
                if (boundBufferedRows) {
                    reserveBufferedRow();
                    if (!incrementWhileOpen(localBufferedRows)) {
                        releaseBufferedRows(1);
                        return false;
                    }
                }
                return true;
            } catch (RuntimeException error) {
                terminalError = error;
                throw error;
            }
        }

        private void retainGroup() {
            references.incrementAndGet();
        }

        private void releaseBufferedRow() {
            if (boundBufferedRows && decrementWhileOpen(localBufferedRows)) {
                releaseBufferedRows(1);
            }
        }

        private void groupTerminated() {
            if (references.decrementAndGet() == 0) {
                close();
            }
        }

        private boolean isTerminalError(Throwable error) {
            return terminalError == error; // NOPMD - Only the exact budget exception instance belongs to this scope.
        }

        // Referenced by doFinally(scope::outerTerminated); PMD does not recognize this method reference.
        @SuppressWarnings("PMD.UnusedPrivateMethod")
        private void outerTerminated(SignalType signal) {
            if (signal == SignalType.ON_COMPLETE) {
                groupTerminated();
            } else {
                // 取消和错误不会保证所有已创建的分组都被订阅，必须在外层统一兜底释放。
                close();
            }
        }

        private void close() {
            releaseActiveKeys(closeCounter(localActiveKeys));
            releaseBufferedRows(closeCounter(localBufferedRows));
        }
    }

    private static boolean incrementWhileOpen(AtomicInteger counter) {
        for (; ; ) {
            int current = counter.get();
            if (current < 0) {
                return false;
            }
            if (counter.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    private static boolean decrementWhileOpen(AtomicInteger counter) {
        for (; ; ) {
            int current = counter.get();
            if (current <= 0) {
                return false;
            }
            if (counter.compareAndSet(current, current - 1)) {
                return true;
            }
        }
    }

    private static int closeCounter(AtomicInteger counter) {
        for (; ; ) {
            int current = counter.get();
            if (current < 0) {
                return 0;
            }
            if (counter.compareAndSet(current, CLOSED)) {
                return current;
            }
        }
    }

    private static final class BudgetedGroupedFlux<K, V> extends GroupedFlux<K, V> {

        private final GroupedFlux<K, V> source;
        private final Scope scope;

        private BudgetedGroupedFlux(GroupedFlux<K, V> source, Scope scope) {
            this.source = source;
            this.scope = scope;
            scope.retainGroup();
        }

        @Override
        public K key() {
            return source.key();
        }

        @Override
        public void subscribe(CoreSubscriber<? super V> actual) {
            source.subscribe(new BudgetedGroupSubscriber<>(actual, scope));
        }
    }

    /**
     * 每次分组订阅只创建一个预算信号适配器，不维护需求量或队列。
     * 不实现 QueueSubscription，保留原 onErrorResume 的非融合边界；终止和重入取消只释放一次。
     */
    private static final class BudgetedGroupSubscriber<V> implements CoreSubscriber<V>, Subscription, Scannable {

        private final CoreSubscriber<? super V> actual;
        private final Scope scope;
        private volatile Subscription upstream;
        private volatile int once;

        @SuppressWarnings("rawtypes")
        private static final AtomicIntegerFieldUpdater<BudgetedGroupSubscriber> ONCE =
                AtomicIntegerFieldUpdater.newUpdater(BudgetedGroupSubscriber.class, "once");

        private BudgetedGroupSubscriber(CoreSubscriber<? super V> actual, Scope scope) {
            this.actual = actual;
            this.scope = scope;
        }

        @Override
        public Context currentContext() {
            return actual.currentContext();
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            if (Operators.validate(upstream, subscription)) {
                upstream = subscription;
                actual.onSubscribe(this);
            }
        }

        @Override
        public void onNext(V value) {
            scope.releaseBufferedRow();
            actual.onNext(value);
        }

        @Override
        public void onError(Throwable error) {
            // groupBy 同时通知外层和内层。仅当前作用域的精确预算异常由外层传播；内层保持
            // 原 Flux.never 的等待取消语义，后续需求不再传给已终止的源。
            upstream = Operators.emptySubscription();
            if (!scope.isTerminalError(error)) {
                try {
                    actual.onError(error);
                } finally {
                    releaseGroup();
                }
            }
        }

        @Override
        public void onComplete() {
            try {
                actual.onComplete();
            } finally {
                releaseGroup();
            }
        }

        @Override
        public void request(long demand) {
            if (Operators.validate(demand)) {
                upstream.request(demand);
            }
        }

        @Override
        public void cancel() {
            try {
                upstream.cancel();
            } finally {
                releaseGroup();
            }
        }

        private void releaseGroup() {
            if (ONCE.compareAndSet(this, 0, 1)) {
                scope.groupTerminated();
            }
        }

        @Override
        public Object scanUnsafe(Attr key) {
            if (key == Attr.PARENT) {
                return upstream;
            }
            if (key == Attr.ACTUAL) {
                return actual;
            }
            if (key == Attr.TERMINATED || key == Attr.CANCELLED) {
                return once != 0;
            }
            if (key == Attr.RUN_STYLE) {
                return Attr.RunStyle.SYNC;
            }
            return null;
        }
    }
}
