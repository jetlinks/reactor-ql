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

import org.jetlinks.reactor.ql.ReactorQL;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

class FunctionMapFeatureCompatibilityTest {

    @Test
    void testLegacyProtectedApplyOverrideIsPreserved() {
        LegacyApplyFeature feature = new LegacyApplyFeature();

        ReactorQL
                .builder()
                .feature(feature)
                .sql("select legacy_apply(this) v from test")
                .build()
                .start(Flux.just("value"))
                .as(StepVerifier::create)
                .assertNext(result -> Assertions.assertEquals("legacy:value", result.get("v")))
                .verifyComplete();

        Assertions.assertEquals(1, feature.applyCount.get());
    }

    @Test
    void testMetadataAwareMapperReceivesMetadataForParameterFunctions() {
        FunctionMapFeature feature = new FunctionMapFeature(
                "metadata_echo",
                1,
                1,
                (metadata, values) -> values.map(value -> String.valueOf(metadata.getSetting("prefix").orElse("missing")) + ":" + value)
        );

        ReactorQL
                .builder()
                .setting("prefix", "p")
                .feature(feature)
                .sql("select metadata_echo(this) v from test")
                .build()
                .start(Flux.just("value"))
                .as(StepVerifier::create)
                .assertNext(result -> Assertions.assertEquals("p:value", result.get("v")))
                .verifyComplete();
    }

    @Test
    void testNoParameterAndDefaultValueBranchesRemainUsable() {
        FunctionMapFeature constant = new FunctionMapFeature(
                "legacy_const",
                0,
                0,
                values -> values.defaultIfEmpty("empty").next()
        );
        FunctionMapFeature metadataConstant = new FunctionMapFeature(
                "metadata_const",
                0,
                0,
                (metadata, values) -> Mono.just(metadata.getSetting("marker").orElse("missing"))
        );
        FunctionMapFeature collectWithDefault = new FunctionMapFeature(
                "collect_defaults",
                2,
                2,
                Flux::collectList
        ).defaultValue("fallback");

        ReactorQL
                .builder()
                .setting("marker", "ok")
                .feature(constant, metadataConstant, collectWithDefault)
                .sql("select legacy_const() legacyVal, metadata_const() metadataVal, collect_defaults(present, missing) defaultsVal from test")
                .build()
                .start(Flux.just(Collections.singletonMap("present", "value")))
                .as(StepVerifier::create)
                .assertNext(result -> {
                    Assertions.assertEquals("empty", result.get("legacyVal"));
                    Assertions.assertEquals("ok", result.get("metadataVal"));
                    Assertions.assertEquals(Arrays.asList("value", "fallback"), result.get("defaultsVal"));
                })
                .verifyComplete();
    }

    @Test
    void testParameterGuardAndDistinctUniqueWrappers() {
        FunctionMapFeature emitValues = new FunctionMapFeature(
                "emit_values",
                3,
                1,
                values -> values
        );
        FunctionMapFeature needArg = new FunctionMapFeature(
                "need_arg",
                1,
                1,
                Flux::next
        );

        Assertions.assertThrows(UnsupportedOperationException.class, () -> ReactorQL
                .builder()
                .feature(needArg)
                .sql("select need_arg() v from dual")
                .build());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> ReactorQL
                .builder()
                .feature(needArg)
                .sql("select need_arg(1, 2) v from dual")
                .build());

        ReactorQL
                .builder()
                .feature(emitValues)
                .sql("select emit_values(distinct a, b, c) distinctValue, emit_values(unique a, b, c) uniqueValue from test")
                .build()
                .start(Flux.just(row(1, 2, 1)))
                .as(StepVerifier::create)
                .assertNext(result -> {
                    Assertions.assertEquals(1, result.get("distinctValue"));
                    Assertions.assertEquals(2, result.get("uniqueValue"));
                })
                .verifyComplete();
    }

    private static Map<String, Object> row(Object a, Object b, Object c) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("a", a);
        row.put("b", b);
        row.put("c", c);
        return row;
    }

    private static class LegacyApplyFeature extends FunctionMapFeature {

        private final AtomicInteger applyCount = new AtomicInteger();

        private LegacyApplyFeature() {
            super("legacy_apply", 1, 1, values -> Mono.error(new IllegalStateException("legacy apply override was not used")));
        }

        @Override
        protected Publisher<Object> apply(ReactorQLRecord record,
                                          List<Function<ReactorQLRecord, Publisher<Object>>> mappers) {
            applyCount.incrementAndGet();
            return Mono
                    .fromDirect(mappers.get(0).apply(record))
                    .map(value -> "legacy:" + value);
        }
    }
}
