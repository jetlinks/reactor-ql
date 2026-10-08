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
package org.jetlinks.reactor.ql.supports;

import com.google.common.collect.Sets;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.beanutils.BeanUtilsBean;
import org.apache.commons.beanutils.ConvertUtilsBean;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.beanutils.PropertyUtilsBean;
import org.jetlinks.reactor.ql.ReactorQL;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

class DefaultPropertyFeatureTest {

    @Test
    void shouldPreserveConfiguredBeanUtilsLookupAfterQueryCompilation() throws Exception {
        TestData bean = new TestData();
        bean.setName("source-name");
        TestData child = new TestData();
        child.setName("child-name");
        bean.setNest(child);
        ReactorQL query = ReactorQL.builder()
                .sql("select name simple,nest.name nested from test")
                .build();
        ReactorQL aggregate = ReactorQL.builder()
                .sql("select name,count(1) total,sum(age) sum,avg(age) avg,max(age) max "
                        + "from test group by name")
                .build();
        java.util.concurrent.atomic.AtomicInteger ageLookups = new java.util.concurrent.atomic.AtomicInteger();
        BeanUtilsBean previous = BeanUtilsBean.getInstance();
        PropertyUtilsBean properties = new PropertyUtilsBean() {
            @Override
            public Object getProperty(Object source, String name)
                    throws java.lang.IllegalAccessException, java.lang.reflect.InvocationTargetException,
                    java.lang.NoSuchMethodException {
                if ("name".equals(name)) {
                    return "simple-override";
                }
                if ("nest.name".equals(name)) {
                    return "full-path-override";
                }
                if ("age".equals(name)) {
                    ageLookups.incrementAndGet();
                    return 9;
                }
                return super.getProperty(source, name);
            }
        };
        // BeanUtils exposes a context-classloader-scoped configurable lookup. Keep the original
        // delegate and full path; simple-getter or pre-split replacement would bypass this contract.
        try {
            BeanUtilsBean.setInstance(new BeanUtilsBean(new ConvertUtilsBean(), properties));
            Map<String, Object> expected = new HashMap<>();
            expected.put("simple", "simple-override");
            expected.put("nested", "full-path-override");
            assertEquals(Collections.singletonList(expected), query.start(Flux.just(bean)).collectList().block());
            assertEquals("source-name", PropertyUtils.getSimpleProperty(bean, "name"));
            Object split = DefaultPropertyFeature.GLOBAL.getPropertyValue("nest", bean);
            assertEquals("simple-override", DefaultPropertyFeature.GLOBAL.getPropertyValue("name", split));
            Map<String, Object> aggregateExpected = new HashMap<>();
            aggregateExpected.put("name", "simple-override");
            aggregateExpected.put("total", 1L);
            aggregateExpected.put("sum", 9D);
            aggregateExpected.put("avg", 9D);
            aggregateExpected.put("max", 9);
            assertEquals(Collections.singletonList(aggregateExpected),
                    aggregate.start(Flux.just(bean)).collectList().block());
            // Different aggregate arguments still perform their own public property evaluation.
            assertEquals(3, ageLookups.get());
        } finally {
            BeanUtilsBean.setInstance(previous);
        }
    }

    @Test
    void shouldSplitFirstDotLikePreviousPattern() {
        DefaultPropertyFeature feature = new DefaultPropertyFeature();
        Pattern previous = Pattern.compile("[.]");
        for (String path : new String[]{"", "a", ".", ".a", "a.", "a..b", "a.b.c", ".."}) {
            assertArrayEquals(previous.split(path, 2), feature.splitDot(path, 2), path);
        }
        assertArrayEquals(previous.split("a.b.c", 3), feature.splitDot("a.b.c", 3));
    }

    @Test
    void shouldMatchDynamicLookupForPreparedNestedPaths() {
        DefaultPropertyFeature feature = DefaultPropertyFeature.GLOBAL;
        Map<String, Object> nested = new HashMap<>();
        nested.put("value", 7);
        nested.put("deep", Collections.singletonMap("value", 9));
        nested.put("a.b", 11);
        nested.put("a", Collections.singletonMap("b", 12));
        Map<String, Object> source = new HashMap<>();
        source.put("nested", nested);
        source.put("nested.value", 8);
        source.put("arr", Collections.singletonList(Collections.singletonMap("a", 13)));
        Map<String, Object> withoutDirectKey = new HashMap<>(source);
        withoutDirectKey.remove("nested.value");
        Map<String, Object> nullDirectKey = new HashMap<>(source);
        nullDirectKey.put("nested.value", null);

        for (String property : new String[]{"nested.value", "nested.deep.value", "nested.a.b",
                "arr.[0].a", "this.size", "missing.value", "nested.value::string"}) {
            Function<Object, Object> prepared = feature.preparePropertyValue(property);
            assertEquals(feature.getPropertyValue(property, source), prepared.apply(source), property);
            assertEquals(feature.getPropertyValue(property, withoutDirectKey),
                         prepared.apply(withoutDirectKey), property);
            assertNull(prepared.apply(null), property);
        }
        assertEquals(8, feature.preparePropertyValue("nested.value").apply(source));
        assertEquals(7, feature.preparePropertyValue("nested.value").apply(withoutDirectKey));
        assertEquals(feature.getPropertyValue("nested.value", nullDirectKey),
                     feature.preparePropertyValue("nested.value").apply(nullDirectKey));
        assertEquals(11, feature.preparePropertyValue("nested.a.b").apply(source));

        TestData bean = new TestData();
        TestData child = new TestData();
        child.setName("nested");
        bean.setNest(child);
        assertEquals(feature.getPropertyValue("nest.name", bean),
                     feature.preparePropertyValue("nest.name").apply(bean));
    }

    @Test
    void preparedFallbackCleansQuotedNamesOnlyOnce() {
        DefaultPropertyFeature feature = new DefaultPropertyFeature();
        Map<String, Object> source = new HashMap<>();
        source.put("key", 1);
        source.put("`key`", 2);
        source.put("\"key\"", 3);
        source.put("amount", "11");
        source.put("`amount", "12");
        source.put("\"amount", "13");
        for (String name : new String[]{null, "key", "`key`", "``key``", "\"\"key\"\"",
                "amount::int", "`amount::int`", "``amount::int``", "\"\"amount::int\"\""}) {
            Function<Object, Object> prepared = feature.preparePropertyValue(name);
            Object expected = feature.getPropertyValue(name, source);
            Object actual = prepared.apply(source);
            assertEquals(expected, actual, name);
            if (expected != null) {
                assertEquals(expected.getClass(), actual.getClass(), name);
            }
            assertNull(prepared.apply(null), name);
        }
        assertEquals(2, feature.preparePropertyValue("``key``").apply(source));
        assertEquals("12", feature.preparePropertyValue("``amount::int``").apply(source));
    }

    @Test
    void preparedFallbackStillInvokesDynamicExtensionWithOriginalName() {
        Object value = new Object();
        java.util.concurrent.atomic.AtomicReference<Object> observed =
                new java.util.concurrent.atomic.AtomicReference<>();
        DefaultPropertyFeature feature = new DefaultPropertyFeature() {
            @Override
            public Object getPropertyValue(Object property, Object source) {
                observed.set(property);
                return value;
            }
        };
        for (String name : new String[]{"``key``", "``amount::int``", "nest.amount::int"}) {
            Function<Object, Object> prepared = feature.preparePropertyValue(name);
            assertSame(value, prepared.apply(Collections.emptyMap()));
            assertSame(name, observed.get());
        }
    }


    @Test
    void testCast() {
        DefaultPropertyFeature feature = new DefaultPropertyFeature();

        TestData data = new TestData();
        data.setName("test");
        data.setAge(10);

        assertEquals("10", feature.getProperty("age::string", data).orElse(null));

    }

    @Test
    void test() {
        DefaultPropertyFeature feature = new DefaultPropertyFeature();

        TestData data = new TestData();
        data.setName("test");
        data.setAge(10);

        TestData nest = new TestData();
        nest.setName("nest");
        nest.setAge(20);
        data.setNest(nest);

        assertEquals("test", feature.getProperty("name", data).orElse(null));
        assertEquals(10, feature.getProperty("age", data).orElse(null));
        assertEquals("nest", feature.getProperty("nest.name", data).orElse(null));
        assertEquals(20, feature.getProperty("nest.age::int", data).orElse(null));

        assertNull(feature.getProperty("nest.aa", data).orElse(null));
        assertNull(feature.getProperty("nest.aa", null).orElse(null));


    }

    @Test
    void testMap() {
        DefaultPropertyFeature feature = new DefaultPropertyFeature();

        Map<String, Object> val = new HashMap<>();
        val.put("name", "123");
        val.put("nest.a", "123");
        val.put("nest2", Collections.singletonMap("a", "123"));
        val.put("nest3", Collections.singletonMap("a.b", "123"));
        val.put("arr", Collections.singletonList(Collections.singletonMap("a", "123")));

        assertEquals("123", feature.getProperty("name", val).orElse(null));
        assertEquals("123", feature.getProperty("nest.a", val).orElse(null));
        assertEquals("123", feature.getProperty("nest2.a", val).orElse(null));
        assertEquals("123", feature.getProperty("nest2.a.this", val).orElse(null));
        assertEquals("123", feature.getProperty("arr.[0].a", val).orElse(null));
        assertEquals("123", feature.getProperty("nest3.a.b", val).orElse(null));

        assertEquals(5, feature.getProperty("this.size", val).orElse(null));
        assertEquals(5, feature.getProperty("this.$size", val).orElse(null));
        assertEquals(false, feature.getProperty("this.empty", val).orElse(null));
        assertEquals(false, feature.getProperty("this.$empty", val).orElse(null));
        assertEquals(val.keySet(), feature.getProperty("this.keys", val).orElse(null));
        assertEquals(val.keySet(), feature.getProperty("this.$keys", val).orElse(null));
        assertEquals(val.values(), feature.getProperty("this.values", val).orElse(null));
        assertEquals(val.values(), feature.getProperty("this.$values", val).orElse(null));
        assertEquals(val.size(), feature.getProperty("this.$entries.size", val).orElse(null));
        assertEquals(val.size(), feature.getProperty("this.entries.size", val).orElse(null));


    }

    @Test
    void shouldPreserveMissingAndNestedPropertySemantics() {
        DefaultPropertyFeature feature = new DefaultPropertyFeature();
        Map<String, Object> nested = Collections.singletonMap("value", 7);
        Map<String, Object> source = new HashMap<>();
        source.put("nested", nested);
        source.put("nested.value", 8);

        assertNull(feature.getPropertyValue("missing", source));
        assertNull(feature.getPropertyValue("missing::string", source));
        assertEquals(8, feature.getPropertyValue("nested.value", source));
        assertEquals(7, feature.getPropertyValue("nested.value", Collections.singletonMap("nested", nested)));
        assertNull(feature.getPropertyValue("nested.missing", source));
    }

    @Test
    void testList() {
        DefaultPropertyFeature feature = new DefaultPropertyFeature();

        Map<String, Object> val = new HashMap<>();
        val.put("arr", Collections.singletonList(Collections.singletonMap("a", "123")));
        val.put("set", Collections.singleton(Collections.singletonMap("a", "123")));

        val.put("mset", Sets.newHashSet(1,2,3,4));

        assertEquals(1, feature.getProperty("arr.size", val).orElse(null));
        assertEquals(1, feature.getProperty("arr.$size", val).orElse(null));
        assertEquals(false, feature.getProperty("arr.empty", val).orElse(null));
        assertEquals(false, feature.getProperty("arr.$empty", val).orElse(null));

        assertEquals(1, feature.getProperty("set.size", val).orElse(null));
        assertEquals(false, feature.getProperty("set.empty", val).orElse(null));

        assertEquals("123", feature.getProperty("arr.0.a", val).orElse(null));
        assertEquals("123", feature.getProperty("set.0.a", val).orElse(null));

        assertEquals(1, feature.getProperty("mset.0", val).orElse(null));
        assertEquals(4, feature.getProperty("mset.-1", val).orElse(null));
        assertEquals(3, feature.getProperty("mset.-2", val).orElse(null));


    }

    @Getter
    @Setter
    public static class TestData {

        private String name;

        private int age;

        private TestData nest;

    }

}
