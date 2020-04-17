package org.jetlinks.reactor.ql.supports;

import lombok.Getter;
import lombok.Setter;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DefaultPropertyFeatureTest {


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

        assertEquals(feature.getProperty("name",data).orElse(null),"test");
        assertEquals(feature.getProperty("age",data).orElse(null),10);
        assertEquals(feature.getProperty("nest.name",data).orElse(null),"nest");
        assertEquals(feature.getProperty("nest.age",data).orElse(null),20);

        assertNull(feature.getProperty("nest.aa", data).orElse(null));
        assertNull(feature.getProperty("nest.aa", null).orElse(null));


    }

    @Test
    void testMap(){
        DefaultPropertyFeature feature = new DefaultPropertyFeature();

        Map<String,Object> val = new HashMap<>();
        val.put("name","123");
        val.put("nest.a","123");
        val.put("nest2", Collections.singletonMap("a","123"));

        assertEquals(feature.getProperty("name",val).orElse(null),"123");
        assertEquals(feature.getProperty("nest.a",val).orElse(null),"123");
        assertEquals(feature.getProperty("nest2.a",val).orElse(null),"123");

    }

    @Getter
    @Setter
    public static class TestData {

        private String name;

        private int age;

        private TestData nest;

    }

}