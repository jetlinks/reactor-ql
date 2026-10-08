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
import org.jetlinks.reactor.ql.exception.ReactorQLException;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * 单次 ReactorQL 订阅的共享执行状态。
 *
 * <p>当前缓存构建期证明为不关联且可复用的子查询结果，以及需要在同一查询订阅内共享的有界
 * 执行状态。实例由查询根链路在订阅时创建，不跨订阅共享；查询完成、错误或取消后随 Reactor
 * Context 一同释放。</p>
 */
public final class SubscriptionContext {

    private volatile Map<Object, Publisher<?>> cachedValues;
    private volatile Map<Object, Object> sharedStates;

    /**
     * 获取当前查询订阅内按计划身份共享的执行状态。
     *
     * <p>创建函数可能在并发分组窗口中被调用，因此只执行一次。状态不得跨查询订阅逃逸，
     * 持有资源的实现仍需在各自响应式链的终止信号中主动释放。</p>
     *
     * @param key      查询计划内的稳定身份，按对象 identity 使用
     * @param supplier 状态创建函数
     * @param <T>      状态类型
     * @return 当前订阅共享的状态
     */
    @SuppressWarnings("unchecked")
    public <T> T getOrCreateState(Object key, Supplier<? extends T> supplier) {
        return (T) sharedStates().computeIfAbsent(key, ignore -> supplier.get());
    }

    /**
     * 在当前订阅内共享一个单值结果。
     *
     * <p>终止信号会为后续访问重放；若所有正在等待的消费者取消，
     * {@code refCount} 会取消上游并允许下次访问重新执行，避免未终止子查询脱离根链路生命周期。</p>
     */
    @SuppressWarnings("unchecked")
    public <T> Mono<T> cacheMono(Object key,
                                 Supplier<? extends Mono<? extends T>> source) {
        Map<Object, Publisher<?>> cache = cachedValues();
        Publisher<?> cached = cache.get(key);
        if (cached != null) {
            return (Mono<T>) cached;
        }
        // 首次并发访问仍由 computeIfAbsent 原子选定同一个冷 Publisher。
        return (Mono<T>) cache.computeIfAbsent(
                key,
                ignore -> {
                    CachedSource<Mono<? extends T>> cachedSource = new CachedSource<>(source);
                    return Mono
                        .defer(() -> Mono.from(cachedSource.get().get()))
                        // doFinally runs after replay receives the terminal signal. Cancellation and error keep
                        // the first-row closure so refCount reconnect and error replay retain their semantics.
                        .doFinally(signal -> releaseCompletedSource(signal, cachedSource))
                        .flux()
                        .replay(1)
                        .refCount(1)
                        .singleOrEmpty();
                }
        );
    }

    /**
     * 在当前订阅内共享一个有界多值结果。
     *
     * @param key      查询计划内的稳定身份，按对象 identity 使用
     * @param source   首次访问时订阅的数据源
     * @param maxRows  允许缓存的最大结果数
     * @param <T>      结果类型
     * @return 每个调用者独立迭代、底层只执行一次的结果流
     */
    public <T> Flux<T> cacheMany(Object key,
                                 Supplier<? extends Flux<? extends T>> source,
                                 int maxRows) {
        Map<Object, Publisher<?>> cache = cachedValues();
        Publisher<?> cached = cache.get(key);
        if (cached != null) {
            return (Flux<T>) cached;
        }
        return (Flux<T>) cache.computeIfAbsent(
                key,
                ignore -> new CompletedManyCache<>(source, maxRows).read()
        );
    }

    private static final class CompletedManyCache<T> {

        private volatile Flux<T> completedRead;
        private final Flux<T> inFlight;
        private final Flux<T> read;

        private CompletedManyCache(Supplier<? extends Flux<? extends T>> source, int maxRows) {
            CachedSource<Flux<? extends T>> cachedSource = new CachedSource<>(source);
            inFlight = Mono
                    .defer(() -> collectMany(cachedSource.get(), maxRows))
                    // 源完整结束后才发布冷读取器；单值无需为每次读取创建 List 迭代器。
                    .doOnSuccess(values -> {
                        completedRead = values.size() == 1
                                ? Flux.just(values.get(0))
                                : Flux.fromIterable(values);
                    })
                    // completedRead is published to replay before this terminal callback releases the supplier.
                    .doFinally(signal -> releaseCompletedSource(signal, cachedSource))
                    .flux()
                    .replay(1)
                    .refCount(1)
                    .singleOrEmpty()
                    .flatMapIterable(values -> values);
            read = Flux.defer(() -> {
                Flux<T> snapshot = completedRead;
                return snapshot == null ? inFlight : snapshot;
            });
        }

        private Flux<T> read() {
            return read;
        }
    }

    private static final class CachedSource<T> {

        private final AtomicReference<Supplier<? extends T>> source;

        private CachedSource(Supplier<? extends T> source) {
            this.source = new AtomicReference<>(source);
        }

        private Supplier<? extends T> get() {
            Supplier<? extends T> active = source.get();
            if (active == null) {
                throw new IllegalStateException("已完成的子查询缓存不应重新订阅源");
            }
            return active;
        }

        private void release() {
            source.set(null);
        }
    }

    private static void releaseCompletedSource(SignalType signal, CachedSource<?> source) {
        if (signal == SignalType.ON_COMPLETE) {
            source.release();
        }
    }

    private static <T> Mono<List<T>> collectMany(Supplier<? extends Flux<? extends T>> source,
                                                  int maxRows) {
        Flux<T> values = Flux.from(source.get());
        if (!BoundedStateSupport.isBounded(maxRows)) {
            return values.collectList();
        }
        return values
                .take(maxRows + 1L)
                .collectList()
                .flatMap(list -> list.size() > maxRows
                        ? Mono.error(ReactorQLException.resourceLimit(
                                "子查询结果超过 setting["
                                        + DefaultReactorQL.SETTING_SUBQUERY_MAX_ROWS + "]: " + maxRows,
                                "为子查询增加过滤或 LIMIT，或在可信场景下调大受硬上限保护的配置。",
                                "select * from test where id in (select id from lookup limit 1000)"
                        ))
                        : Mono.just(list));
    }

    private Map<Object, Publisher<?>> cachedValues() {
        Map<Object, Publisher<?>> cache = cachedValues;
        if (cache == null) {
            synchronized (this) {
                cache = cachedValues;
                if (cache == null) {
                    cache = new ConcurrentHashMap<>();
                    cachedValues = cache;
                }
            }
        }
        return cache;
    }

    private Map<Object, Object> sharedStates() {
        Map<Object, Object> states = sharedStates;
        if (states == null) {
            synchronized (this) {
                states = sharedStates;
                if (states == null) {
                    states = new ConcurrentHashMap<>();
                    sharedStates = states;
                }
            }
        }
        return states;
    }
}
