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

import lombok.Getter;
import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.exception.ReactorQLException;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ScalarValueMapper;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 查询级函数参数求值与 Publisher 适配器。
 *
 * <p>List 回调沿用参数流完成后的原生 flatMap 错误边界，不暴露为行级标量映射器。
 * 参数按 SQL 顺序订阅，每次计算使用独立可变列表；不跨行复用参数状态。
 * 单参数逐值 map 工厂只简化同步参数适配，计算仍在原生 map 中。</p>
 */
public class FunctionMapFeature implements ValueMapFeature {

    int maxParamSize;
    int minParamSize;

    public Function<Flux<Object>, Publisher<Object>> mapper;

    private BiFunction<ReactorQLMetadata, Flux<Object>, Publisher<Object>> metadataMapper;

    @Getter
    private final String id;

    private Object defaultValue;

    private Function<Object, Object> valueMapper;

    private Function<Flux<Object>, Publisher<Object>> valueStreamMapper;

    @SuppressWarnings("all")
    public FunctionMapFeature(String function, int max, int min, Function<Flux<Object>, Publisher<?>> mapper) {
        this.maxParamSize = max;
        this.minParamSize = min;
        this.mapper = (Function) mapper;
        this.id = FeatureId.ValueMap.of(function).getId();
    }

    @SuppressWarnings("all")
    public FunctionMapFeature(String function, int max, int min, BiFunction<ReactorQLMetadata, Flux<Object>, Publisher<?>> mapper) {
        this.maxParamSize = max;
        this.minParamSize = min;
        this.mapper = stream -> (Publisher<Object>) mapper.apply(null, stream);
        this.metadataMapper = (BiFunction) mapper;
        this.id = FeatureId.ValueMap.of(function).getId();
    }

    public static FunctionMapFeature scalar(String function,
                                            int max,
                                            int min,
                                            Function<List<Object>, Object> mapper) {
        return new FunctionMapFeature(
                function,
                max,
                min,
                stream -> stream.collectList().flatMap(values -> Mono.justOrEmpty(mapper.apply(values)))
        );
    }

    /**
     * 单参数逐值转换：同步参数按下游需求冷取值，异步/多值参数继续使用原生 {@link Flux#map(Function)}。
     * 返回的行 mapper 仍是冷 Publisher，不改变投影列的订阅、错误组合或取消边界。
     *
     * @param function 函数名，参数个数固定为一
     * @param mapper 同步非阻塞转换，不得保留参数；非空参数必须返回非空值，异常进入响应式错误信号
     * @return 可注册的函数特性；修改公开 mapper 字段后自动恢复普通 Publisher 参数流
     * @since 1.0.21
     */
    public static FunctionMapFeature map(String function, Function<Object, Object> mapper) {
        FunctionMapFeature feature = new FunctionMapFeature(function, 1, 1, stream -> stream.map(mapper));
        feature.valueMapper = Objects.requireNonNull(mapper, "mapper");
        feature.valueStreamMapper = feature.mapper;
        return feature;
    }

    public static FunctionMapFeature scalar(String function,
                                            int max,
                                            int min,
                                            BiFunction<ReactorQLMetadata, List<Object>, Object> mapper) {
        return new FunctionMapFeature(
                function,
                max,
                min,
                (metadata, stream) -> stream
                        .collectList()
                        .flatMap(values -> Mono.justOrEmpty(mapper.apply(metadata, values)))
        );
    }

    /**
     * 创建固定两个参数的单值函数。沿用原生参数收集和 flatMap 计算错误边界，
     * 不把函数计算提升到行级 ScalarValueMapper；缺失参数仍按原 List 索引语义处理。
     *
     * @param function 函数名
     * @param mapper 同步函数；可返回 null 表示无值，不得修改或保留输入参数值，
     *               计算在参数流完成后执行
     * @return 可注册的函数特性
     */
    public static FunctionMapFeature scalar2(String function,
                                             BiFunction<Object, Object, Object> mapper) {
        return scalar(function, 2, 2, values -> mapper.apply(values.get(0), values.get(1)));
    }

    @Override
    public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression, ReactorQLMetadata metadata) {

        net.sf.jsqlparser.expression.Function function = ((net.sf.jsqlparser.expression.Function) expression);

        List<Expression> parameters;
        if (function.getParameters() == null && minParamSize != 0) {
            throw ReactorQLException.functionArgumentCount(expression, minParamSize, maxParamSize, 0);
        }
        if (function.getParameters() == null) {
            return v -> metadataMapper == null ? mapper.apply(Flux.empty()) : applyMapper(metadata, Flux.empty());
        }
        parameters = function.getParameters().getExpressions();
        if (parameters.size() > maxParamSize || parameters.size() < minParamSize) {
            throw ReactorQLException.functionArgumentCount(expression, minParamSize, maxParamSize, parameters.size());
        }
        Function<Publisher<?>, Publisher<?>> wrapper = metadata.createWrapper(expression);
        List<Function<ReactorQLRecord, Publisher<Object>>> mappers = createParamMappers(metadata, parameters);
        // A synchronous calculator still has a value-local error boundary. Keep the native
        // parameter stream and flatMap instead of exposing failures to row/aggregate stages.
        // fun(distinct col)
        if (function.isDistinct()) {
            return v -> Flux
                    .from(apply(metadata, v, mappers))
                    .distinct()
                    .as(wrapper);
        }
        // fun(unique col)
        if (function.isUnique()) {
            return v -> CastUtils
                    .uniqueFlux(Flux.from(apply(metadata, v, mappers)))
                    .as(wrapper);
        }

        return v -> Flux.from(apply(metadata, v, mappers));
    }

    @SuppressWarnings("all")
    protected List<Function<ReactorQLRecord, Publisher<Object>>> createParamMappers(ReactorQLMetadata metadata,
                                                                                    List<Expression> parameters) {
        return (List) parameters
                .stream()
                .map(expr -> ValueMapFeature.createMapperNow(expr, metadata))
                .collect(Collectors.toList());
    }

    public FunctionMapFeature defaultValue(Object defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }

    protected Publisher<Object> apply(ReactorQLRecord record,
                                      List<Function<ReactorQLRecord, Publisher<Object>>> mappers) {
        if (valueMapper != null && mapper == valueStreamMapper && ScalarValueMapper.class.isInstance(mappers.get(0))) {
            ScalarValueMapper parameter = ScalarValueMapper.class.cast(mappers.get(0));
            // 保持冷取值和普通 Publisher 投影；计算仍由 map 处理，保留逐值 onErrorContinue 边界。
            // 公开 mapper 被替换时不绕过其自定义参数流行为。
            return Mono.fromSupplier(() -> {
                Object value = parameter.applyScalar(record);
                return value == null ? defaultValue : value;
            }).map(valueMapper);
        }
        return mapper.apply(createParameterStream(record, mappers));
    }

    protected Publisher<Object> apply(ReactorQLMetadata metadata,
                                      ReactorQLRecord record,
                                      List<Function<ReactorQLRecord, Publisher<Object>>> mappers) {
        if (metadataMapper == null) {
            // 保留原 protected apply(record, mappers) 调用链，避免影响已有子类重写行为。
            return apply(record, mappers);
        }
        return applyMapper(metadata, createParameterStream(record, mappers));
    }

    private Flux<Object> createParameterStream(ReactorQLRecord record,
                                               List<Function<ReactorQLRecord, Publisher<Object>>> mappers) {
        if (mappers.size() == 1) {
            Function<ReactorQLRecord, Publisher<Object>> parameter = mappers.get(0);
            // 单参数无需迭代和 concatMap；defer 保持自定义 mapper 在订阅时调用。
            return Flux.defer(() -> {
                Publisher<Object> source = parameter.apply(record);
                return defaultValue == null
                        ? Flux.from(source)
                        : Mono.fromDirect(source).defaultIfEmpty(defaultValue).flux();
            });
        }
        return Flux.fromIterable(mappers)
                   // 函数参数是位置敏感的，即使参数 mapper 异步返回也必须按 SQL 参数顺序收集。
                   .concatMap(mp -> {
                       if (defaultValue != null) {
                           return Mono
                                   .fromDirect(mp.apply(record))
                                   .defaultIfEmpty(defaultValue);
                       }
                       return mp.apply(record);
                   });
    }

    private Publisher<Object> applyMapper(ReactorQLMetadata metadata, Flux<Object> stream) {
        if (metadataMapper != null) {
            return metadataMapper.apply(metadata, stream);
        }
        return mapper.apply(stream);
    }

}
