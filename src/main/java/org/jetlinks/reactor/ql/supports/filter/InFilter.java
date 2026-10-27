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
package org.jetlinks.reactor.ql.supports.filter;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.statement.select.Select;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FilterFeature;
import org.jetlinks.reactor.ql.feature.ScalarValueMapper;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.utils.CompareUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

/**
 * Compiles IN predicates, retaining streaming comparisons for dynamic or flattened values.
 * Long constant lists may share an immutable query-local index across subscriptions;
 * other value domains retain {@link CompareUtils} semantics and subclass dispatch.
 */
public class InFilter implements FilterFeature {

    private static final int MIN_INDEXED_CANDIDATES = 32;

    @Override
    public BiFunction<ReactorQLRecord, Object, Mono<Boolean>> createPredicate(Expression expression, ReactorQLMetadata metadata) {

        InExpression inExpression = ((InExpression) expression);

        Expression left = inExpression.getLeftExpression();
        Expression right = inExpression.getRightExpression();

        List<Function<ReactorQLRecord, Publisher<?>>> rightMappers = new ArrayList<>();

        if (right instanceof ExpressionList) {
            rightMappers.addAll(((ExpressionList<?>) right)
                                        .getExpressions()
                                        .stream()
                                        .map(exp -> ValueMapFeature.createMapperNow(exp, metadata))
                                        .collect(Collectors.toList()));
        } else if (right instanceof ParenthesedExpressionList) {
            rightMappers.addAll(((ParenthesedExpressionList<?>) right)
                                        .getExpressions()
                                        .stream()
                                        .map(exp -> ValueMapFeature.createMapperNow(exp, metadata))
                                        .collect(Collectors.toList()));
        } else if (right instanceof Select) {
            rightMappers.add(ValueMapFeature.createMapperNow(((Select) right), metadata));
        } else if (null != right) {
            rightMappers.add(ValueMapFeature.createMapperNow(right, metadata));
        }

        Function<ReactorQLRecord, Publisher<?>> leftMapper = ValueMapFeature.createMapperNow(left, metadata);

        boolean not = inExpression.isNot();
        // Only the builtin owns doPredicate/asFlux semantics; subclasses must retain dispatch.
        Object[] scalarCandidates = getClass() == InFilter.class
                ? scalarCandidates(leftMapper, rightMappers, metadata)
                : null;
        if (scalarCandidates != null) {
            ScalarValueMapper scalarLeft = (ScalarValueMapper) leftMapper;
            ToIntFunction<Object> candidateIndex = prepareCandidateIndex(scalarCandidates);
            return markTotalForBuiltin((ctx, column) -> {
                Object leftValue = scalarLeft.applyScalar(ctx);
                if (needsFlattening(leftValue)) {
                    // Values that represent streams/collections keep the existing subscription semantics.
                    return doPredicate(not,
                                       asFlux(Mono.just(leftValue)),
                                       asFlux(Flux.fromIterable(rightMappers)
                                                  .flatMap(mapper -> mapper.apply(ctx))));
                }
                if (candidateIndex != null) {
                    // Both the index and accepted keys are immutable builtin values. This
                    // lookup cannot invoke conversions or user callbacks, so compute before
                    // wrapping; fallbacks keep their original laziness and closure size.
                    int indexed = candidateIndex.applyAsInt(leftValue);
                    if (indexed >= 0) {
                        return Mono.just(not != (indexed == 1));
                    }
                }
                return Mono.fromSupplier(() -> {
                    // Keep the original scan at its native supplier boundary for short lists
                    // and unsupported domains, without an extra predicate dispatch per row.
                    for (Object candidate : scalarCandidates) {
                        if (candidate != null && CompareUtils.equals(candidate, leftValue)) {
                            return !not;
                        }
                    }
                    return not;
                });
            });
        }
        if (getClass() == InFilter.class
                && metadata.supportsScalarFastPath()
                && !metadata.isCheckpoint()
                && leftMapper instanceof ScalarValueMapper) {
            ScalarValueMapper scalarLeft = (ScalarValueMapper) leftMapper;
            return markTotalForBuiltin((ctx, column) -> {
                Object leftValue = scalarLeft.applyScalar(ctx);
                Flux<Object> values = asFlux(Flux.fromIterable(rightMappers)
                                                  .flatMap(mapper -> mapper.apply(ctx)));
                if (needsFlattening(leftValue)) {
                    return doPredicate(not, asFlux(Mono.just(leftValue)), values);
                }
                // A scalar left value needs no replay; keep the right source cold and cancellable.
                return values.any(value -> leftValue != null && CompareUtils.equals(value, leftValue))
                             .map(matched -> not != matched);
            });
        }
        return markTotalForBuiltin((ctx, column) ->
                doPredicate(not,
                            asFlux(leftMapper.apply(ctx)),
                            asFlux(Flux.fromIterable(rightMappers).flatMap(mapper -> mapper.apply(ctx)))
                ));
    }

    private BiFunction<ReactorQLRecord, Object, Mono<Boolean>> markTotalForBuiltin(TotalBooleanPredicate predicate) {
        // Subclasses may override asFlux/doPredicate to complete empty, so do not promise totality for them.
        return getClass() == InFilter.class ? predicate : predicate::apply;
    }

    private static Object[] scalarCandidates(Function<ReactorQLRecord, Publisher<?>> leftMapper,
                                             List<Function<ReactorQLRecord, Publisher<?>>> rightMappers,
                                             ReactorQLMetadata metadata) {
        if (!metadata.supportsScalarFastPath()
                || metadata.isCheckpoint()
                || !(leftMapper instanceof ScalarValueMapper)) {
            return null;
        }
        Object[] values = new Object[rightMappers.size()];
        for (int i = 0; i < rightMappers.size(); i++) {
            Function<ReactorQLRecord, Publisher<?>> mapper = rightMappers.get(i);
            if (!(mapper instanceof ScalarValueMapper)) {
                return null;
            }
            ScalarValueMapper scalar = (ScalarValueMapper) mapper;
            if (!scalar.isConstant()) {
                return null;
            }
            Object value = scalar.constantValue();
            if (needsFlattening(value)) {
                return null;
            }
            values[i] = value;
        }
        return values;
    }

    private static boolean needsFlattening(Object value) {
        return value instanceof Iterable
                || value instanceof Publisher
                || (value instanceof Map && ((Map<?, ?>) value).size() == 1);
    }

    // Index results: 0 = absent, 1 = present, -1 = retain the original comparison domain.
    private static ToIntFunction<Object> prepareCandidateIndex(Object[] candidates) {
        int size = 0;
        boolean integral = true;
        boolean strings = true;
        for (Object candidate : candidates) {
            if (candidate != null) {
                size++;
                integral &= isIntegral(candidate);
                strings &= candidate.getClass() == String.class;
            }
        }
        if (size >= MIN_INDEXED_CANDIDATES && integral) {
            long[] values = new long[size];
            int index = 0;
            for (Object candidate : candidates) {
                if (candidate != null) {
                    values[index++] = ((Number) candidate).longValue();
                }
            }
            Arrays.sort(values);
            // The four integral wrappers compare exactly within this domain. Other Numbers
            // can follow lossy or directional conversion rules, so retain the original scan.
            return value -> {
                if (!isIntegral(value)) {
                    return -1;
                }
                return Arrays.binarySearch(values, ((Number) value).longValue()) >= 0 ? 1 : 0;
            };
        }
        if (size >= MIN_INDEXED_CANDIDATES && strings) {
            String[] values = new String[size];
            int index = 0;
            for (Object candidate : candidates) {
                if (candidate != null) {
                    values[index++] = (String) candidate;
                }
            }
            Arrays.sort(values);
            // Only String-to-String ordering agrees with CompareUtils without conversion.
            return value -> {
                if (value == null || value.getClass() != String.class) {
                    return -1;
                }
                return Arrays.binarySearch(values, (String) value) >= 0 ? 1 : 0;
            };
        }
        return null;
    }

    private static boolean isIntegral(Object value) {
        return value instanceof Long || value instanceof Integer
                || value instanceof Short || value instanceof Byte;
    }

    protected Flux<Object> asFlux(Publisher<?> publisher) {
        return Flux.from(publisher)
                   .concatMap(v -> {
                       if (v instanceof Iterable) {
                           return Flux.fromIterable(((Iterable<?>) v));
                       }
                       if (v instanceof Publisher) {
                           return ((Publisher<?>) v);
                       }
                       if (v instanceof Map && ((Map<?, ?>) v).size() == 1) {
                           return Mono.just(((Map<?, ?>) v).values().iterator().next());
                       }
                       return Mono.just(v);
                   }, 0);
    }

    protected Mono<Boolean> doPredicate(boolean not, Flux<Object> left, Flux<Object> values) {
        // Disconnect an unfinished left source when the last comparison is cancelled.
        Flux<Object> leftCache = left.replay().refCount(1);
        return values
                .flatMap(v -> leftCache.map(l -> CompareUtils.equals(v, l)))
                .any(Boolean.TRUE::equals)
                .map(v -> not != v);
    }

    @Override
    public String getId() {
        return FeatureId.Filter.in.getId();
    }
}
