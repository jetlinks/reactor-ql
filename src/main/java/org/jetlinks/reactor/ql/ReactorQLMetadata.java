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

import net.sf.jsqlparser.Model;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.PlainSelect;
import org.jetlinks.reactor.ql.feature.Feature;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.concurrent.Queues;

import java.util.Collection;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * 元数据,用于管理特性,进行配置等操作
 *
 * @author zhouhao
 * @see org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata
 * @since 1.0.0
 */
public interface ReactorQLMetadata {

    /**
     * 获取特性
     *
     * @param featureId 特性ID
     * @param <T>       特性类型
     * @return 特性
     */
    <T extends Feature> Optional<T> getFeature(FeatureId<T> featureId);

    /**
     * 获取设置
     *
     * @param key key
     * @return 设置内容
     */
    Optional<Object> getSetting(String key);

    /**
     * 设置并行度,影响filter,groupBy等操作的并行度
     *
     * @param concurrency 并行度
     */
    default void setConcurrency(int concurrency) {
        setting("concurrency", concurrency);
    }

    default int getConcurrency() {
        return getSetting("concurrency")
                .map(CastUtils::castNumber)
                .orElse(Queues.SMALL_BUFFER_SIZE)
                .intValue();
    }

    default boolean isCheckpoint() {
        return getSetting("checkpoint")
                .map(CastUtils::castBoolean)
                .orElse(false);
    }

    default <S, T> Flux<T> flatMap(Flux<S> source, Function<S, ? extends Publisher<? extends T>> mapper) {
        if (getConcurrency() <= 1) {
            return source.concatMap(mapper, 0);
        }
        return source.flatMap(mapper, getConcurrency());
    }

    /**
     * 自定义设置
     *
     * @param key   key
     * @param value value
     * @return this
     */
    ReactorQLMetadata setting(String key, Object value);

    /**
     * 获取原始SQL
     *
     * @return SQL
     */
    PlainSelect getSql();

    /**
     * 释放资源,执行后{@link #getSql()}等才做将抛出{@link IllegalStateException}异常
     */
    void release();

    /**
     * 获取特性,如果不存在则抛出异常
     *
     * @param featureId 特性ID
     * @param <T>       特性类型
     * @return 特性
     */
    default <T extends Feature> T getFeatureNow(FeatureId<T> featureId) {
        return getFeatureNow(featureId, featureId::getId);
    }

    /**
     * 获取特性,如果特性不存在则使用指定等错误消息抛出异常
     *
     * @param featureId    特性ID
     * @param errorMessage 错误消息
     * @param <T>          特性类型
     * @return 特性
     */
    default <T extends Feature> T getFeatureNow(FeatureId<T> featureId, Supplier<String> errorMessage) {
        return getFeature(featureId)
                .orElseThrow(() -> new UnsupportedOperationException("unsupported feature: " + errorMessage.get()));
    }

    Collection<Feature> getFeatures();

    @SuppressWarnings("all")
    default <T extends Publisher<? extends R>, R> Function<T, T> createWrapper(Object expr) {
        if (!isCheckpoint()) {
            return Function.identity();
        }
        String checkpoint = String.valueOf(expr);
        return v -> {
            return (T) ((v instanceof Mono)
                    ? Mono.from(v).checkpoint(checkpoint)
                    : Flux.from(v).checkpoint(checkpoint));
        };
    }

}
