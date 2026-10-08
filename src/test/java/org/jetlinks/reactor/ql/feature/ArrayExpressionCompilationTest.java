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
package org.jetlinks.reactor.ql.feature;

import net.sf.jsqlparser.expression.ArrayExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.jetlinks.reactor.ql.DefaultReactorQLContext;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.supports.DefaultPropertyFeature;
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

/** Compares compiled array access with the original native zip boundary. */
class ArrayExpressionCompilationTest {

    @Test
    void fixedPathsPreserveFullKeyAndSuffixPriorityWithoutCachingValues() throws Exception {
        Map<String, Object> branch = row("b.c", 2, "b", row("c", 3));
        Map<String, Object> root = row("a.b.c", 1, "a", branch);
        DefaultReactorQLMetadata metadata = metadata();
        Function<ReactorQLRecord, Publisher<?>> mapper = compile("this.root['a.b.c']", metadata);
        ReactorQLRecord record = record(row("root", root));
        assertEquals(1, Mono.from(mapper.apply(record)).block());
        root.remove("a.b.c");
        assertEquals(2, Mono.from(mapper.apply(record)).block());
        branch.remove("b.c");
        assertEquals(3, Mono.from(mapper.apply(record)).block());
    }

    @Test
    void literalAndDynamicKeysMatchNativeAccessAcrossShapes() throws Exception {
        List<Object> sources = Arrays.asList(
                row("a.b", 1, "a", row("b", 2), "a..b", 3, "", 4, "size", 5),
                row("a", row("b", 2)), Arrays.asList(10, 20), new int[]{10, 20}, null);
        for (Object source : sources) {
            for (String key : Arrays.asList("a.b", "a..b", ".a", "a.", "..", "", "size",
                    "this", "*", "$", "设备.值", "\"a.b\"", "`a.b`", "a::string", "\"")) {
                String expression = "this.root['" + key.replace("'", "''") + "']";
                assertNative(expression, record(row("root", source)), metadata());
            }
            for (String index : Arrays.asList("0", "-1", "2")) {
                assertNative("this.root[" + index + "]", record(row("root", source)), metadata());
            }
            assertNative("this.root[this.key]", record(row("root", source, "key", "a.b")), metadata());
        }
        Map<String, Object> data = row("root", row("a.b", 1, "other", 2), "key", "a.b");
        ReactorQLRecord record = record(data);
        Function<ReactorQLRecord, Publisher<?>> mapper = compile("this.root[this.key]", metadata());
        assertEquals(1, Mono.from(mapper.apply(record)).block());
        data.put("key", "other");
        assertEquals(2, Mono.from(mapper.apply(record)).block());
    }

    @Test
    void customPropertyFeatureKeepsEveryLookupAtExecutionTime() throws Exception {
        AtomicInteger reads = new AtomicInteger();
        DefaultReactorQLMetadata metadata = metadata();
        metadata.addFeature((PropertyFeature) (key, source) -> "a.b".equals(key)
                ? Optional.of(reads.incrementAndGet())
                : DefaultPropertyFeature.GLOBAL.getProperty(key, source));
        Function<ReactorQLRecord, Publisher<?>> mapper = compile("this.root['a.b']", metadata);
        ReactorQLRecord record = record(row("root", row("a.b", 99)));
        assertEquals(0, reads.get());
        assertEquals(1, Mono.from(mapper.apply(record)).block());
        assertEquals(2, Mono.from(mapper.apply(record)).block());
    }

    @Test
    void nativeZipFailureKeepsErrorIdentityHookSourceAndTerminalScope() throws Exception {
        for (boolean original : Arrays.asList(false, true)) {
            RuntimeException failure = new IllegalStateException("lookup failed");
            Map<String, Object> root = row("marker", 1);
            DefaultReactorQLMetadata metadata = metadata();
            metadata.addFeature((PropertyFeature) (key, source) -> {
                if ("a.b".equals(key)) throw failure;
                return DefaultPropertyFeature.GLOBAL.getProperty(key, source);
            });
            Function<ReactorQLRecord, Publisher<?>> mapper = original
                    ? nativeMapper("this.root['a.b']", metadata)
                    : compile("this.root['a.b']", metadata);
            List<Object> hooked = new ArrayList<>();
            AtomicInteger continued = new AtomicInteger();
            Hooks.onOperatorError("array-lookup-source", (error, value) -> {
                assertSame(failure, error);
                hooked.add(value);
                return error;
            });
            try {
                StepVerifier.create(Flux.from(mapper.apply(record(row("root", root))))
                        .onErrorContinue((error, value) -> continued.incrementAndGet()), 0)
                        .thenRequest(1).expectErrorMatches(error -> error == failure).verify();
                assertEquals(0, continued.get());
                assertEquals(1, hooked.size());
                Object[] values = (Object[]) hooked.get(0);
                assertEquals("a.b", values[0]);
                assertSame(root, values[1]);
            } finally {
                Hooks.resetOnOperatorError("array-lookup-source");
            }
        }
    }

    @Test
    void defaultPropertyFailuresKeepNativeHookInputs() throws Exception {
        RuntimeException failure = new IllegalStateException("map failed");
        Map<String, Object> root = new HashMap<String, Object>() {
            @Override public Object get(Object key) { throw failure; }
        };
        Function<ReactorQLRecord, Publisher<?>> mapper = compile("this.root['a.b']", metadata());
        List<Object> hooked = new ArrayList<>();
        Hooks.onOperatorError("array-default-source", (error, value) -> { hooked.add(value); return error; });
        try {
            StepVerifier.create(Mono.from(mapper.apply(record(row("root", root)))))
                    .expectErrorMatches(error -> error == failure).verify();
            assertEquals(1, hooked.size());
            Object[] values = (Object[]) hooked.get(0);
            assertEquals("a.b", values[0]);
            assertSame(root, values[1]);
        } finally {
            Hooks.resetOnOperatorError("array-default-source");
        }
    }

    @Test
    void contextDemandEmptySourcesAndCancellationStayNative() throws Exception {
        int nativeCancellations = verifyContextAndCancellation(true);
        assertTrue(nativeCancellations > 0);
        assertEquals(nativeCancellations, verifyContextAndCancellation(false));
    }

    private static int verifyContextAndCancellation(boolean original) throws Exception {
        AtomicInteger subscribed = new AtomicInteger();
        AtomicInteger cancelled = new AtomicInteger();
        DefaultReactorQLMetadata metadata = metadata();
        metadata.addFeature(valueFeature("context_root", ignored -> Mono.deferContextual(context -> {
            subscribed.incrementAndGet();
            return Mono.just(context.get("root"));
        })));
        Function<ReactorQLRecord, Publisher<?>> mapper = original
                ? nativeMapper("context_root()['a.b']", metadata)
                : compile("context_root()['a.b']", metadata);
        Mono<Object> values = Mono.from(mapper.apply(record(row()))).cast(Object.class)
                .contextWrite(Context.of("root", row("a", row("b", 7))));
        // Reactor 3.4 subscribes zip inputs immediately, but buffers the value until demand.
        // Check the original operator too rather than importing newer Reactor semantics.
        StepVerifier.create(values, 0).then(() -> assertEquals(1, subscribed.get()))
                .thenRequest(1).expectNext(7).verifyComplete();
        StepVerifier.create(values, 0).thenCancel().verify();
        assertEquals(2, subscribed.get());
        metadata.addFeature(valueFeature("empty_root", ignored -> Mono.empty()));
        Function<ReactorQLRecord, Publisher<?>> empty = original
                ? nativeMapper("empty_root()['a.b']", metadata) : compile("empty_root()['a.b']", metadata);
        StepVerifier.create(Mono.from(empty.apply(record(row()))))
                .verifyComplete();
        metadata.addFeature(valueFeature("waiting_root", ignored -> Mono.never()
                .doOnCancel(cancelled::incrementAndGet)));
        Function<ReactorQLRecord, Publisher<?>> waiting = original
                ? nativeMapper("waiting_root()['a.b']", metadata) : compile("waiting_root()['a.b']", metadata);
        StepVerifier.create(Mono.from(waiting.apply(record(row()))), 0)
                .thenRequest(1).thenCancel().verify();
        return cancelled.get();
    }

    @Test
    void assemblyHookCanTransformLiteralIndexAfterCompilation() throws Exception {
        Function<ReactorQLRecord, Publisher<?>> mapper = compile("this.root['a.b']", metadata());
        Hooks.onEachOperator("array-index-transform", Operators.lift((scannable, actual) ->
                new CoreSubscriber<Object>() {
                    @Override public Context currentContext() { return actual.currentContext(); }
                    @Override public void onSubscribe(Subscription subscription) {
                        actual.onSubscribe(subscription);
                    }
                    @Override public void onNext(Object value) {
                        actual.onNext("a.b".equals(value) ? "other" : value);
                    }
                    @Override public void onError(Throwable error) { actual.onError(error); }
                    @Override public void onComplete() { actual.onComplete(); }
                }));
        try {
            StepVerifier.create(Mono.from(mapper.apply(record(row("root", row("a.b", 1, "other", 2)))))
                    .cast(Object.class))
                    .expectNext(2).verifyComplete();
        } finally {
            Hooks.resetOnEachOperator("array-index-transform");
        }
    }

    @Test
    void reactiveIndexReadsContextForEachSubscriptionAndCanCompleteEmpty() throws Exception {
        for (boolean original : Arrays.asList(true, false)) {
            AtomicInteger subscriptions = new AtomicInteger();
            DefaultReactorQLMetadata metadata = metadata();
            metadata.addFeature(valueFeature("context_key", ignored -> Mono.deferContextual(context -> {
                subscriptions.incrementAndGet();
                return Mono.justOrEmpty(context.getOrEmpty("key"));
            })));
            Function<ReactorQLRecord, Publisher<?>> mapper = original
                    ? nativeMapper("this.root[context_key()]", metadata)
                    : compile("this.root[context_key()]", metadata);
            Mono<Object> value = Mono.from(mapper.apply(record(row("root", row("a", row("b", 7), "other", 8)))))
                    .cast(Object.class);
            assertEquals(0, subscriptions.get());
            StepVerifier.create(value.contextWrite(Context.of("key", "a.b")))
                    .expectNext(7).verifyComplete();
            StepVerifier.create(value.contextWrite(Context.of("key", "other")))
                    .expectNext(8).verifyComplete();
            StepVerifier.create(value).verifyComplete();
            assertEquals(3, subscriptions.get());
        }
    }

    private static void assertNative(String expression, ReactorQLRecord record,
                                     DefaultReactorQLMetadata metadata) throws Exception {
        Function<ReactorQLRecord, Publisher<?>> expected = nativeMapper(expression, metadata);
        Function<ReactorQLRecord, Publisher<?>> actual = compile(expression, metadata);
        Object value;
        try {
            value = Mono.from(expected.apply(record)).block();
        } catch (RuntimeException error) {
            RuntimeException actualError = assertThrows(RuntimeException.class,
                    () -> Mono.from(actual.apply(record)).block(), expression);
            assertEquals(error.getClass(), actualError.getClass(), expression);
            assertEquals(error.getMessage(), actualError.getMessage(), expression);
            return;
        }
        assertEquals(value, Mono.from(actual.apply(record)).block(), expression);
    }

    private static Function<ReactorQLRecord, Publisher<?>> nativeMapper(
            String expression, DefaultReactorQLMetadata metadata) throws Exception {
        ArrayExpression array = (ArrayExpression) CCJSqlParserUtil.parseExpression(expression);
        Function<ReactorQLRecord, Publisher<?>> object = ValueMapFeature.createMapperNow(array.getObjExpression(), metadata);
        Function<ReactorQLRecord, Publisher<?>> index = ValueMapFeature.createMapperNow(array.getIndexExpression(), metadata);
        PropertyFeature property = metadata.getFeatureNow(PropertyFeature.ID);
        return record -> Mono.zip(Mono.from(index.apply(record)), Mono.from(object.apply(record)), property::getProperty)
                .handle((value, sink) -> value.ifPresent(sink::next));
    }

    private static Function<ReactorQLRecord, Publisher<?>> compile(
            String expression, DefaultReactorQLMetadata metadata) throws Exception {
        return ValueMapFeature.createMapperNow(CCJSqlParserUtil.parseExpression(expression), metadata);
    }

    private static ValueMapFeature valueFeature(String name, Function<ReactorQLRecord, Publisher<?>> mapper) {
        return new ValueMapFeature() {
            @Override public String getId() { return FeatureId.ValueMap.of(name).getId(); }
            @Override public Function<ReactorQLRecord, Publisher<?>> createMapper(
                    Expression expression, ReactorQLMetadata metadata) {
                return mapper;
            }
        };
    }

    private static DefaultReactorQLMetadata metadata() { return new DefaultReactorQLMetadata("select this from test"); }
    private static ReactorQLRecord record(Object value) {
        return ReactorQLRecord.newRecord("test", value, new DefaultReactorQLContext(ignored -> Flux.empty()));
    }
    private static Map<String, Object> row(Object... pairs) {
        Map<String, Object> result = new HashMap<>();
        for (int i = 0; i < pairs.length; i += 2) result.put((String) pairs[i], pairs[i + 1]);
        return result;
    }
}
