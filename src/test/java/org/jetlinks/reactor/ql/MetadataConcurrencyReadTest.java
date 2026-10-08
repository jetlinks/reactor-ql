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

import org.jetlinks.reactor.ql.exception.TypeCastException;
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.util.concurrent.Queues;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/** Concurrency reads remain dynamic and use the public metadata extension and conversion boundary. */
class MetadataConcurrencyReadTest {

    @Test
    void readsDefaultAndConfiguredValuesWithoutChangingConversions() {
        ReactorQLMetadata metadata = new DefaultReactorQLMetadata("select this from test");
        Assertions.assertEquals(Queues.SMALL_BUFFER_SIZE, metadata.getConcurrency());
        Object[] values = {1, 2L, 3.9D, "4", "0x10", true, false, 'a'};
        int[] expected = {1, 2, 3, 4, 16, 1, 0, 97};
        for (int i = 0; i < values.length; i++) {
            metadata.setting("concurrency", values[i]);
            Assertions.assertEquals(expected[i], metadata.getConcurrency());
        }
    }

    @Test
    void callsOverriddenSettingGetterOncePerReadAndDoesNotCacheItsValues() {
        AtomicInteger reads = new AtomicInteger();
        ReactorQLMetadata metadata = new DefaultReactorQLMetadata("select this from test") {
            @Override
            public Optional<Object> getSetting(String key) {
                return "concurrency".equals(key)
                        ? Optional.of(reads.incrementAndGet())
                        : super.getSetting(key);
            }
        };
        // Construction can inspect other settings; each explicit read still goes through the getter.
        reads.set(0);
        Assertions.assertEquals(1, metadata.getConcurrency());
        Assertions.assertEquals(2, metadata.getConcurrency());
        Assertions.assertEquals(2, reads.get());
    }

    @Test
    void preservesInvalidValueAndNumberConversionFailures() {
        ReactorQLMetadata metadata = new DefaultReactorQLMetadata("select this from test");
        metadata.setting("concurrency", "not a number");
        Assertions.assertThrows(TypeCastException.class, metadata::getConcurrency);
        RuntimeException failure = new IllegalStateException("number conversion failed");
        AtomicInteger conversions = new AtomicInteger();
        metadata.setting("concurrency", new Number() {
            @Override public int intValue() { conversions.incrementAndGet(); throw failure; }
            @Override public long longValue() { throw failure; }
            @Override public float floatValue() { throw failure; }
            @Override public double doubleValue() { throw failure; }
        });
        Assertions.assertSame(failure, Assertions.assertThrows(
                IllegalStateException.class, metadata::getConcurrency));
        Assertions.assertEquals(1, conversions.get());
    }
}
