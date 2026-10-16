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

import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.ReactorQL;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

import java.util.Collections;
import java.util.Locale;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

class CastFeatureNormalizationTest {

    @Test
    void directCallStillNormalizesType() {
        assertEquals(12.5D, CastFeature.castValue("12.5", " DOUBLE   PRECISION (12,2) "));
        assertEquals(12L, CastFeature.castValue("12", " BIGINT "));
        assertEquals("value", CastFeature.castValue("value", " UNKNOWN "));
    }

    @Test
    void directCallMatchesPreviousNormalizationForValuesAndTypes() {
        String[] types = {"STRING", "VARCHAR", "INT", "SIGNED", "BIGINT", "DOUBLE PRECISION",
                "FLOAT8", "NUMERIC", "BOOL", "TINYINT", "SMALLINT", "FLOAT4", "UNKNOWN"};
        String[] whitespace = {" ", "  ", "\t", "\n", "\r\n", "\u000B\f"};
        for (String type : types) {
            for (String space : whitespace) {
                for (String suffix : new String[]{"", " (12,2)", "(12,2) ignored"}) {
                    String inputType = "\0\t" + type.replace(" ", space) + suffix + "\r\0";
                    Object expected = CastFeature.castValue("12.5", previousNormalizeType(inputType));
                    Object actual = CastFeature.castValue("12.5", inputType);
                    assertEquals(expected, actual, inputType);
                    assertEquals(expected.getClass(), actual.getClass(), inputType);
                }
            }
        }
    }

    @Test
    void everyUtf16SeparatorKeepsOriginalCastResolution() {
        String value = "12.5";
        for (int separator = Character.MIN_VALUE; separator <= Character.MAX_VALUE; separator++) {
            String type = "DOUBLE" + (char) separator + "PRECISION";
            Object expected = CastFeature.castValue(value, previousNormalizeType(type));
            Object actual = CastFeature.castValue(value, type);
            String message = "separator U+" + Integer.toHexString(separator);
            assertEquals(expected, actual, message);
            assertEquals(expected.getClass(), actual.getClass(), message);
            if (expected == value) {
                assertSame(value, actual, message);
            }
        }
    }

    @Test
    void defaultRegexWhitespaceAndUnknownTypeIdentityRemainUnchanged() {
        Object value = new Object();
        // Default Java \\s is ASCII-only; Unicode whitespace must not become a type separator.
        for (String separator : new String[]{"\u0085", "\u00A0", "\u1680", "\u2003", "\u2028", "\u2029"}) {
            assertSame(value, CastFeature.castValue(value, "DOUBLE" + separator + "PRECISION"));
        }
        for (String type : new String[]{null, "", "\t\r\n", "UNKNOWN(10)", "(INT)", "( INT (10)"}) {
            assertSame(value, CastFeature.castValue(value, type));
        }
        assertNull(CastFeature.castValue(null, null));
        assertEquals(12.5D, CastFeature.castValue("12.5", " DOUBLE\t\n\u000B\f\r PRECISION "));
    }

    @Test
    void normalizationPreservesConversionFailures() {
        RuntimeException expected = assertThrows(RuntimeException.class,
                () -> CastFeature.castValue("not-a-number", "int"));
        RuntimeException actual = assertThrows(RuntimeException.class,
                () -> CastFeature.castValue("not-a-number", "\t INT (10) \r"));
        assertEquals(expected.getClass(), actual.getClass());
        assertEquals(expected.getMessage(), actual.getMessage());
    }

    private static String previousNormalizeType(String type) {
        if (type == null) {
            return "";
        }
        String normalized = type.trim().toLowerCase(Locale.ENGLISH);
        int argIndex = normalized.indexOf('(');
        if (argIndex > 0) {
            normalized = normalized.substring(0, argIndex).trim();
        }
        return normalized.replaceAll("\\s+", " ");
    }

    @Test
    void compiledCastKeepsAsyncValueAndContext() {
        ValueMapFeature asyncValue = new ValueMapFeature() {
            @Override
            public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression,
                                                                         ReactorQLMetadata metadata) {
                return record -> Mono.deferContextual(context -> Mono.just(context.get("value")));
            }

            @Override
            public String getId() {
                return FeatureId.ValueMap.of("async_value").getId();
            }
        };
        ReactorQL query = ReactorQL.builder()
                                  .feature(asyncValue)
                                  .sql("select cast(async_value() as DOUBLE PRECISION) value from test")
                                  .build();
        StepVerifier.create(query.start(Flux.just(Collections.singletonMap("id", 1)))
                                 .contextWrite(Context.of("value", "12.5")))
                    .expectNext(Collections.singletonMap("value", 12.5D))
                    .verifyComplete();
        // The query-level conversion function must not retain a previous subscription's parameter.
        StepVerifier.create(query.start(Flux.just(Collections.singletonMap("id", 2)))
                                 .contextWrite(Context.of("value", "8.25")))
                    .expectNext(Collections.singletonMap("value", 8.25D))
                    .verifyComplete();
    }
}
