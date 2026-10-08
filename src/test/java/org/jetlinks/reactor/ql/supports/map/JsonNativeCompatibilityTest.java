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

import com.jayway.jsonpath.JsonPath;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import org.jetlinks.reactor.ql.ReactorQL;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ScalarValueMapper;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Independent native compositions check JSON argument and document-local error boundaries. */
class JsonNativeCompatibilityTest {
    private static final JsonPathFunctionMapFeature GET = JsonPathFunctionMapFeature.jsonGet("json_get", 2, 2, false);
    private static final List<String> EXPRESSIONS = Arrays.asList(
            "json_get(payload,'$.value')", "payload->'value'", "payload->>'value'");

    @Test
    void jsonFunctionsKeepPublisherBoundariesEvenWithScalarArguments() {
        for (String expression : Arrays.asList("json_get(payload,'$.value')", "json_depth(payload)",
                "json_extract(payload,'$.value')", "json_quote(payload)")) {
            DefaultReactorQLMetadata metadata = new DefaultReactorQLMetadata("select " + expression + " value from events");
            Expression value = ((SelectExpressionItem) metadata.getSql().getSelectItems().get(0)).getExpression();
            Assertions.assertFalse(ValueMapFeature.createMapperNow(value, metadata) instanceof ScalarValueMapper, expression);
        }
    }

    @Test
    void textLimitsKeepHooksContinuationAndIndependentAggregates() {
        for (String expression : EXPRESSIONS) {
            for (String columns : Arrays.asList(expression + " value", "id," + expression + " value", expression + " value,id")) {
                compare("select " + columns + " from events", row(1, "{\"value\":2222222222}"), 12, null);
            }
            for (String group : Arrays.asList("", " group by type", " group by _window(2),type", " group by type,_window(2)")) {
                compare("select sum(" + expression + ") sum,count(1) total from events" + group,
                        row(1, "{\"value\":2222222222}"), 12, null);
            }
        }
        List<Object> actual = run("select sum(json_get(payload,'$.value')) sum,count(1) total from events",
                false, true, row(1, "{\"value\":2222222222}"), 12, null);
        Assertions.assertEquals("hook/args", actual.get(0));
        Assertions.assertEquals("continue/null", actual.get(1));
        Assertions.assertEquals(rowValues("sum", 3D, "total", 2L), actual.get(2));
    }

    @Test
    void documentGetterErrorsKeepConstructionScopeAndExceptionIdentity() {
        RuntimeException failure = new IllegalStateException("document getter failed");
        Map<String, Object> bad = new AbstractMap<String, Object>() {
            @Override
            public Object get(Object key) {
                if ("payload".equals(key)) throw failure;
                return row(1, null).get(key);
            }
            @Override
            public Set<Entry<String, Object>> entrySet() { return row(1, null).entrySet(); }
        };
        for (String expression : EXPRESSIONS) {
            compare("select " + expression + " value,id from events", bad, 0, failure);
            for (String group : Arrays.asList("", " group by type", " group by _window(2),type")) {
                compare("select sum(" + expression + ") sum,count(1) total from events" + group, bad, 0, failure);
            }
        }
    }

    @Test
    void malformedMissingEmptyAndValidDocumentsKeepValuesTypesAndSignals() {
        for (String expression : EXPRESSIONS) {
            for (Object document : Arrays.asList(null, "{invalid", "{}", "{\"value\":7}",
                    Collections.singletonMap("value", 7))) {
                compare("select " + expression + " value,id from events", row(1, document), 0, null);
            }
        }
    }

    @Test
    void sourceErrorIdentityDemandContextAndCancellationRemainNative() {
        for (String expression : EXPRESSIONS) {
            for (boolean nativeChain : Arrays.asList(false, true)) {
                ReactorQL query = builder("select " + expression + " value from events", nativeChain, 0).build();
                RuntimeException failure = new IllegalStateException("source failed");
                TestPublisher<Map<String, Object>> source = TestPublisher.create();
                AtomicInteger continuation = new AtomicInteger();
                AtomicInteger contexts = new AtomicInteger();
                Flux<Map<String, Object>> output = query.start(Flux.deferContextual(context -> {
                    Assertions.assertEquals("visible", context.get("marker"));
                    contexts.incrementAndGet();
                    return source.flux();
                })).onErrorContinue((error, value) -> continuation.incrementAndGet())
                        .contextWrite(context -> context.put("marker", "visible"));
                StepVerifier.create(output, 0).then(() -> source.assertMinRequested(0))
                        .thenRequest(1).then(() -> source.next(row(1, "{\"value\":3}")))
                        .expectNext(rowValues("value", expression.contains("->>") ? "3" : 3))
                        .then(() -> source.error(failure)).expectErrorMatches(error -> error == failure).verify();
                Assertions.assertEquals(1, contexts.get());
                Assertions.assertEquals(0, continuation.get());

                TestPublisher<Map<String, Object>> cancelled = TestPublisher.create();
                StepVerifier.create(query.start(cancelled.flux()), 0).thenCancel().verify();
                cancelled.assertCancelled();
            }
        }
    }

    private static void compare(String sql, Map<String, Object> bad, int maxText, RuntimeException failure) {
        for (boolean continuation : Arrays.asList(false, true)) {
            Assertions.assertEquals(run(sql, true, continuation, bad, maxText, failure),
                    run(sql, false, continuation, bad, maxText, failure), sql + "/continue=" + continuation);
        }
    }

    private static List<Object> run(String sql, boolean nativeChain, boolean continuation,
                                  Map<String, Object> bad, int maxText, RuntimeException failure) {
        List<Object> outcome = new ArrayList<>();
        Hooks.onOperatorError("json-native-boundary", (error, value) -> {
            if (failure != null) Assertions.assertSame(failure, error);
            outcome.add("hook/" + kind(value));
            return error;
        });
        try {
            AtomicInteger consumed = new AtomicInteger();
            AtomicInteger cancelled = new AtomicInteger();
            Flux<Map<String, Object>> result = builder(sql, nativeChain, maxText).build()
                    .start(Flux.just(bad, row(2, "{\"value\":3}")).doOnNext(value -> consumed.incrementAndGet())
                            .doOnCancel(cancelled::incrementAndGet));
            if (continuation) result = result.onErrorContinue((error, value) -> {
                if (failure != null) Assertions.assertSame(failure, error);
                outcome.add("continue/" + kind(value));
            });
            StepVerifier.Step<Map<String, Object>> verifier = StepVerifier.create(result, 0)
                    .thenRequest(Long.MAX_VALUE).thenConsumeWhile(value -> {
                        outcome.add(new LinkedHashMap<>(value)); return true;
                    });
            boolean error = failure != null || maxText > 0;
            if (error && !continuation) verifier.expectErrorSatisfies(value -> {
                if (failure != null) Assertions.assertSame(failure, value);
                outcome.add("terminal/" + value.getClass().getName());
            }).verify();
            else verifier.verifyComplete();
            outcome.add("consumed/" + consumed.get());
            outcome.add("cancelled/" + cancelled.get());
            return outcome;
        } finally { Hooks.resetOnOperatorError("json-native-boundary"); }
    }

    private static ReactorQL.Builder builder(String sql, boolean nativeChain, int maxText) {
        ReactorQL.Builder builder = ReactorQL.builder().sql(nativeChain
                ? sql.replace("payload->>'value'", "native_json_text(payload)")
                     .replace("payload->'value'", "native_json_value(payload)") : sql);
        if (nativeChain) builder.feature(new NativeGet(), new NativeOperator(false), new NativeOperator(true));
        if (maxText > 0) builder.setting(JsonPathFunctionMapFeature.SETTING_MAX_JSON_TEXT_LENGTH, maxText);
        return builder;
    }

    private static String kind(Object value) {
        if (value == null) return "null";
        if (value instanceof ReactorQLRecord) return "record";
        if (value instanceof Function) return "mapper";
        if (value instanceof List) return "args";
        return value.getClass().getName();
    }

    private static Map<String, Object> row(int id, Object document) {
        return rowValues("id", id, "type", "a", "payload", document);
    }

    private static Map<String, Object> rowValues(Object... values) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int index = 0; index < values.length; index += 2) row.put((String) values[index], values[index + 1]);
        return row;
    }

    /** Retained parameter flow, independent of the production createMapper implementation. */
    private static final class NativeGet extends JsonPathFunctionMapFeature {
        private NativeGet() { super("json_get", 2, 2); }
        @Override
        protected Object evaluate(JsonFunctionContext context) { return GET.evaluate(context); }
        @Override
        public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression, ReactorQLMetadata metadata) {
            List<Expression> expressions = ((net.sf.jsqlparser.expression.Function) expression).getParameters().getExpressions();
            JsonFunctionSupport.JsonLimits limits = JsonFunctionSupport.jsonLimits(metadata);
            JsonPath[] paths = new JsonPath[expressions.size()];
            for (int index = 0; index < paths.length; index++) {
                if (GET.isJsonPathArgument(index)) paths[index] = JsonFunctionSupport.compileStaticPath(limits, expressions.get(index));
            }
            List<Function<ReactorQLRecord, Publisher<?>>> mappers = expressions.stream()
                    .map(value -> ValueMapFeature.createMapperNow(value, metadata)).collect(Collectors.toList());
            return record -> Flux.fromIterable(mappers)
                    .concatMap(mapper -> Mono.<Object>fromDirect(mapper.apply(record)).defaultIfEmpty(JsonFunctionSupport.EMPTY), 0)
                    .collectList().flatMap(args -> Mono.justOrEmpty(evaluate(new JsonFunctionContext(limits, args, paths))))
                    .as(metadata.createWrapper(expression));
        }
    }

    /** Native document-local flatMap, with the same static path and JSON implementation. */
    private static final class NativeOperator implements ValueMapFeature {
        private final boolean text;
        private NativeOperator(boolean text) { this.text = text; }
        @Override
        public String getId() { return FeatureId.ValueMap.of(text ? "native_json_text" : "native_json_value").getId(); }
        @Override
        public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression, ReactorQLMetadata metadata) {
            Expression document = ((net.sf.jsqlparser.expression.Function) expression).getParameters().getExpressions().get(0);
            Function<ReactorQLRecord, Publisher<?>> mapper = ValueMapFeature.createMapperNow(document, metadata);
            JsonFunctionSupport.JsonLimits limits = JsonFunctionSupport.jsonLimits(metadata);
            JsonPath path = JsonFunctionSupport.compilePath(limits, "$.value");
            return record -> Mono.fromDirect(mapper.apply(record)).flatMap(value -> {
                Object result = JsonFunctionSupport.readPath(limits, value, "$.value", path);
                if (result == JsonFunctionSupport.EMPTY) return Mono.empty();
                Object normalized = JsonFunctionSupport.normalize(limits, result);
                return Mono.justOrEmpty(text ? JsonFunctionSupport.stringifyScalar(limits, normalized) : normalized);
            }).as(metadata.createWrapper(expression));
        }
    }
}
