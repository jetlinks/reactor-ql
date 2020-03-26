package org.jetlinks.reactor.ql;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ReactorQLTest {

    @Test
    void testLimit() {

        ReactorQL.builder()
                .sql("select * from test limit 0,10")
                .build()
                .source(Flux.range(0, 20))
                .start()
                .as(StepVerifier::create)
                .expectNextCount(10)
                .verifyComplete();

    }

    @Test
    void testCount() {

        ReactorQL.builder()
                .sql("select count(1) total from test")
                .build()
                .source(Flux.range(0, 100))
                .start()
                .as(StepVerifier::create)
                .expectNext(Collections.singletonMap("total", 100L))
                .verifyComplete();

    }

    @Test
    void testMax() {

        ReactorQL.builder()
                .sql("select max(this) val from test")
                .build()
                .source(Flux.range(1, 100))
                .start()
                .as(StepVerifier::create)
                .expectNext(Collections.singletonMap("val", 100D))
                .verifyComplete();

    }

    @Test
    void testMin() {

        ReactorQL.builder()
                .sql("select min(this) val from test")
                .build()
                .source(Flux.range(1, 100))
                .start()
                .as(StepVerifier::create)
                .expectNext(Collections.singletonMap("val", 1D))
                .verifyComplete();

    }

    @Test
    void testAvg() {

        ReactorQL.builder()
                .sql("select avg(this) val from test")
                .build()
                .source(Flux.range(1, 100))
                .start()
                .as(StepVerifier::create)
                .expectNext(Collections.singletonMap("val", 50.5D))
                .verifyComplete();

    }

    @Test
    void testSum() {

        ReactorQL.builder()
                .sql("select sum(val) total from test")
                .build()
                .source(Flux.range(1, 100).map(v -> Collections.singletonMap("val", v)))
                .start()
                .as(StepVerifier::create)
                .expectNext(Collections.singletonMap("total", 5050D))
                .verifyComplete();


        ReactorQL.builder()
                .sql("select sum(1) total from test")
                .build()
                .source(Flux.range(1, 100).map(v -> Collections.singletonMap("val", v)))
                .start()
                .as(StepVerifier::create)
                .expectNext(Collections.singletonMap("total", 100D))
                .verifyComplete();

    }

    @Test
    void testGroup() {

        ReactorQL.builder()
                .sql("select count(1) total,type from test group by type")
                .build()
                .source(Flux.range(0, 100).map(v -> Collections.singletonMap("type", v / 10)))
                .start()
                .doOnNext(System.out::println)
                .as(StepVerifier::create)
                .expectNextCount(10)
                .verifyComplete();

    }

    @Test
    void testGroupByTime() {

        ReactorQL.builder()
                .sql("select avg(this) total from test group by interval('1s')")
                .build()
                .source(Flux.range(0, 10).delayElements(Duration.ofMillis(500)))
                .start()
                .doOnNext(System.out::println)
                .as(StepVerifier::create)
                .expectNextCount(6)
                .verifyComplete();

    }

    @Test
    void testGroupByTimeHaving() {

        ReactorQL.builder()
                .sql("select avg(this) total from test group by interval('1s') having total > 2")
                .build()
                .source(Flux.range(0, 10).delayElements(Duration.ofMillis(500)))
                .start()
                .doOnNext(System.out::println)
                .as(StepVerifier::create)
                .expectNextCount(4)
                .verifyComplete();

    }

    @Test
    void testCase() {

        ReactorQL.builder()
                .sql("select case this when 1 then '一' when 2 then '二' when 3 then '三' else type end type from test")
                .build()
                .source(Flux.range(0, 4).delayElements(Duration.ofMillis(500)))
                .start()
                .doOnNext(System.out::println)
                .cast(Map.class)
                .map(map -> map.get("type"))
                .as(StepVerifier::create)
                .expectNext(0, "一", "二", "三")
                .verifyComplete();

    }

    @Test
    void testSimpleMap() {

        ReactorQL.builder()
                .sql("select _name name from test")
                .build()
                .source(Flux.just(Collections.singletonMap("_name", "test")))
                .start()
                .as(StepVerifier::create)
                .expectNext(Collections.singletonMap("name", "test"))
                .verifyComplete();

    }


}