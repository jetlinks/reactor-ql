package org.jetlinks.reactor.ql.supports;

import com.google.common.collect.Sets;
import lombok.Getter;
import lombok.Setter;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DefaultPropertyFeatureTest {


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
        assertEquals(val.keySet(), feature.getProperty("this.$keys", val).orElse(null));
        assertEquals(val.values(), feature.getProperty("this.$values", val).orElse(null));
        assertEquals(val.size(), feature.getProperty("this.$entries.size", val).orElse(null));


    }

    @Test
    void testList() {
        DefaultPropertyFeature feature = new DefaultPropertyFeature();

        Map<String, Object> val = new HashMap<>();
        val.put("arr", Collections.singletonList(Collections.singletonMap("a", "123")));
        val.put("set", Collections.singleton(Collections.singletonMap("a", "123")));

        val.put("mset", Sets.newHashSet(1,2,3,4));

        assertEquals(1, feature.getProperty("arr.size", val).orElse(null));
        assertEquals(false, feature.getProperty("arr.empty", val).orElse(null));

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