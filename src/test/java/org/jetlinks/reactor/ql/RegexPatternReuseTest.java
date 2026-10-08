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

import org.jetlinks.reactor.ql.exception.ReactorQLException;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;
import org.jetlinks.reactor.ql.supports.map.FunctionMapFeature;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

class RegexPatternReuseTest {

    @Test
    void shouldValidatePatternsWhenPublicCallbackHasNoMetadataOwner() {
        FunctionMapFeature feature = (FunctionMapFeature) new DefaultReactorQLMetadata("select 1 from dual")
                .getFeatureNow(FeatureId.ValueMap.of("regexp_like"));
        StepVerifier.create(feature.mapper.apply(Flux.just("abc", "abc")))
                    .expectNext(Boolean.TRUE)
                    .verifyComplete();
        StepVerifier.create(feature.mapper.apply(Flux.just("aaa", "(a+)+")))
                    .expectErrorSatisfies(error -> {
                        Assertions.assertTrue(error instanceof ReactorQLException);
                        Assertions.assertTrue(((ReactorQLException) error).getReason().contains("高风险嵌套量词"));
                    })
                    .verify();
        StepVerifier.create(feature.mapper.apply(Flux.just("abc", "abc")))
                    .expectNext(Boolean.TRUE)
                    .verifyComplete();
    }

    @Test
    void shouldValidateChangedPatternsBeforeConvertingFlags() {
        ReactorQL query = ReactorQL.builder()
                                   .sql("select regexp_like(text, regex, flags) matched from test")
                                   .build();
        StepVerifier.create(query.start(Flux.just(row("abc", "abc", "i"))))
                    .assertNext(value -> Assertions.assertEquals(Boolean.TRUE, value.get("matched")))
                    .verifyComplete();

        AtomicBoolean flagsConverted = new AtomicBoolean();
        Map<String, Object> unsafe = row("aaa", "(a+)+", null);
        unsafe.put("flags", new Object() {
            @Override
            public String toString() {
                flagsConverted.set(true);
                throw new IllegalStateException("flags must not precede pattern validation");
            }
        });
        StepVerifier.create(query.start(Flux.just(unsafe)))
                    .expectErrorSatisfies(error -> {
                        Assertions.assertTrue(error instanceof ReactorQLException);
                        ReactorQLException diagnostic = (ReactorQLException) error;
                        Assertions.assertEquals(ReactorQLException.INVALID_ARGUMENT, diagnostic.getI18nCode());
                        Assertions.assertTrue(diagnostic.getReason().contains("高风险嵌套量词"));
                    })
                    .verify();
        Assertions.assertFalse(flagsConverted.get());

        StepVerifier.create(query.start(Flux.just(row("ABC", "abc", "i"),
                                                 row("ABC", "abc", ""))))
                    .assertNext(value -> Assertions.assertEquals(Boolean.TRUE, value.get("matched")))
                    .assertNext(value -> Assertions.assertEquals(Boolean.FALSE, value.get("matched")))
                    .verifyComplete();
    }

    @Test
    void shouldRecheckInputLimitsForPreviouslyValidatedPattern() {
        ReactorQL query = ReactorQL.builder()
                                   .sql("select regexp_like(text, regex) matched from test")
                                   .build();
        StepVerifier.create(query.start(Flux.just(row("abc", "abc", null))))
                    .assertNext(value -> Assertions.assertEquals(Boolean.TRUE, value.get("matched")))
                    .verifyComplete();
        query.metadata().setting(DefaultReactorQLMetadata.SETTING_MAX_REGEX_INPUT_LENGTH, 2);
        StepVerifier.create(query.start(Flux.just(row("abc", "abc", null))))
                    .expectErrorSatisfies(error -> {
                        Assertions.assertTrue(error instanceof ReactorQLException);
                        Assertions.assertTrue(((ReactorQLException) error).getReason().contains("regexp input"));
                    })
                    .verify();
        query.metadata().setting(DefaultReactorQLMetadata.SETTING_MAX_REGEX_INPUT_LENGTH, 3);
        StepVerifier.create(query.start(Flux.just(row("abc", "abc", null))))
                    .assertNext(value -> Assertions.assertEquals(Boolean.TRUE, value.get("matched")))
                    .verifyComplete();
    }

    @Test
    void shouldObserveSettingsAddedAfterUsingDefaultFunctionLimits() {
        ReactorQL query = ReactorQL.builder()
                                   .sql("select replace(text, 'a', 'xx') value from test")
                                   .build();
        StepVerifier.create(query.start(Flux.just(row("abc", null, null))))
                    .assertNext(value -> Assertions.assertEquals("xxbc", value.get("value")))
                    .verifyComplete();

        query.metadata().setting(DefaultReactorQLMetadata.SETTING_MAX_GENERATED_STRING_LENGTH, 3);
        StepVerifier.create(query.start(Flux.just(row("abc", null, null))))
                    .expectError(UnsupportedOperationException.class)
                    .verify();
        query.metadata().setting(DefaultReactorQLMetadata.SETTING_MAX_GENERATED_STRING_LENGTH, 4);
        StepVerifier.create(query.start(Flux.just(row("abc", null, null))))
                    .assertNext(value -> Assertions.assertEquals("xxbc", value.get("value")))
                    .verifyComplete();

        DefaultReactorQLMetadata custom = new DefaultReactorQLMetadata(
                "select replace(text, 'a', 'xx') value from test") {
            @Override
            public Optional<Object> getSetting(String key) {
                if (SETTING_MAX_GENERATED_STRING_LENGTH.equals(key)) {
                    return Optional.of(3);
                }
                return super.getSetting(key);
            }
        };
        StepVerifier.create(new DefaultReactorQL(custom).start(Flux.just(row("abc", null, null))))
                    .expectError(UnsupportedOperationException.class)
                    .verify();
    }

    @Test
    void shouldKeepDynamicPatternsAndAllRegexFunctionsIndependent() {
        ReactorQL query = ReactorQL.builder()
                                   .sql("select regexp_like(text, regex) matched,"
                                                + "regexp_replace(text, regex, 'x') replaced,"
                                                + "regexp_extract(text, regex) extracted,"
                                                + "regexp_substr(text, regex) substring from test")
                                   .build();

        StepVerifier.create(query.start(Flux.just(row("abc", "a", null),
                                                 row("abc", "b", null),
                                                 row("abc", "a", null))))
                    .assertNext(value -> assertRegexResult(value, "xbc", "a"))
                    .assertNext(value -> assertRegexResult(value, "axc", "b"))
                    .assertNext(value -> assertRegexResult(value, "xbc", "a"))
                    .verifyComplete();
    }

    @Test
    void shouldIncludeFlagsAndRecheckLimitsOnEveryRow() {
        ReactorQL flagsQuery = ReactorQL.builder()
                                        .sql("select regexp_like(text, regex, flags) matched from test")
                                        .build();
        StepVerifier.create(flagsQuery.start(Flux.just(row("ABC", "abc", "i"),
                                                      row("ABC", "abc", ""),
                                                      row("ABC", "abc", "i"))))
                    .assertNext(value -> Assertions.assertEquals(Boolean.TRUE, value.get("matched")))
                    .assertNext(value -> Assertions.assertEquals(Boolean.FALSE, value.get("matched")))
                    .assertNext(value -> Assertions.assertEquals(Boolean.TRUE, value.get("matched")))
                    .verifyComplete();
        // Pattern.flags() reflects inline modifiers too; the cache key must use the requested flags.
        StepVerifier.create(flagsQuery.start(Flux.just(row("Ab", "a(?i)b", ""),
                                                      row("Ab", "a(?i)b", "i"),
                                                      row("Ab", "a(?i)b", ""))))
                    .assertNext(value -> Assertions.assertEquals(Boolean.FALSE, value.get("matched")))
                    .assertNext(value -> Assertions.assertEquals(Boolean.TRUE, value.get("matched")))
                    .assertNext(value -> Assertions.assertEquals(Boolean.FALSE, value.get("matched")))
                    .verifyComplete();

        ReactorQL query = ReactorQL.builder()
                                   .sql("select regexp_like(text, regex) matched from test")
                                   .build();
        StepVerifier.create(query.start(Flux.just(row("abc", "ab", null))))
                    .assertNext(value -> Assertions.assertEquals(Boolean.TRUE, value.get("matched")))
                    .verifyComplete();
        StepVerifier.create(query.start(Flux.just(row("abc", "[", null))))
                    .expectError(UnsupportedOperationException.class)
                    .verify();
        query.metadata().setting(DefaultReactorQLMetadata.SETTING_MAX_REGEX_PATTERN_LENGTH, 1);
        StepVerifier.create(query.start(Flux.just(row("abc", "ab", null))))
                    .expectError(UnsupportedOperationException.class)
                    .verify();
    }

    @Test
    void shouldKeepConcurrentSubscriptionsAndReactorContext() {
        ReactorQL query = ReactorQL.builder()
                                   .sql("select regexp_like(text, regex) matched from test")
                                   .build();
        Mono<?> left = query.start(Flux.range(0, 500)
                                      .map(ignore -> row("abc", "^a", null)))
                            .subscribeOn(Schedulers.parallel())
                            .collectList();
        Mono<?> right = query.start(Flux.range(0, 500)
                                       .map(ignore -> row("abc", "^z", null)))
                             .subscribeOn(Schedulers.parallel())
                             .collectList();
        StepVerifier.create(Mono.zip(left, right))
                    .assertNext(pair -> {
                        Assertions.assertEquals(500, ((java.util.List<?>) pair.getT1()).size());
                        Assertions.assertEquals(500, ((java.util.List<?>) pair.getT2()).size());
                        Assertions.assertTrue(((java.util.List<?>) pair.getT1()).stream()
                                                 .allMatch(value -> Boolean.TRUE.equals(((Map<?, ?>) value).get("matched"))));
                        Assertions.assertTrue(((java.util.List<?>) pair.getT2()).stream()
                                                 .allMatch(value -> Boolean.FALSE.equals(((Map<?, ?>) value).get("matched"))));
                    })
                    .verifyComplete();

        AtomicBoolean contextSeen = new AtomicBoolean();
        StepVerifier.create(query.start(Flux.deferContextual(context -> {
                                     contextSeen.set("visible".equals(context.get("marker")));
                                     return Flux.just(row("abc", "^a", null));
                                 }))
                                 .contextWrite(context -> context.put("marker", "visible")))
                    .assertNext(value -> Assertions.assertEquals(Boolean.TRUE, value.get("matched")))
                    .verifyComplete();
        Assertions.assertTrue(contextSeen.get());
    }

    private static void assertRegexResult(Map<String, Object> value,
                                          String replaced,
                                          String extracted) {
        Assertions.assertEquals(4, value.size());
        Assertions.assertEquals(Boolean.TRUE, value.get("matched"));
        Assertions.assertEquals(replaced, value.get("replaced"));
        Assertions.assertEquals(extracted, value.get("extracted"));
        Assertions.assertEquals(extracted, value.get("substring"));
    }

    private static Map<String, Object> row(String text, String regex, String flags) {
        Map<String, Object> value = new HashMap<>();
        value.put("text", text);
        value.put("regex", regex);
        value.put("flags", flags);
        return value;
    }
}
