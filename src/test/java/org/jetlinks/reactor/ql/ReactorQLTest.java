package org.jetlinks.reactor.ql;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class ReactorQLTest {

    @Test
    void testLimit() {

        ReactorQL.builder()
                .sql("select * from test limit 0,10")
                .build()
                .start(Flux.range(0, 20))
                .as(StepVerifier::create)
                .expectNextCount(10)
                .verifyComplete();

    }

    @Test
    void testCount() {

        ReactorQL.builder()
                .sql("select count(1) total from test")
                .build()
                .start(Flux.range(0, 100))
                .as(StepVerifier::create)
                .expectNext(Collections.singletonMap("total", 100L))
                .verifyComplete();

    }

    @Test
    void testWhere() {

        ReactorQL.builder()
                .sql("select count(1) total from test where this > 10 and (this <=90 or this >95)")
                .build()
                .start(Flux.range(1, 100))
                .as(StepVerifier::create)
                .expectNext(Collections.singletonMap("total", 85L))
                .verifyComplete();

    }

    @Test
    void testIn() {

        ReactorQL.builder()
                .sql("select this from test where this in (1,2,3,4)")
                .build()
                .start(Flux.range(0, 20))
                .as(StepVerifier::create)
                .expectNextCount(4)
                .verifyComplete();

    }

    @Test
    void testCalculate() {

        ReactorQL.builder()
                .sql("select this + 10 \"add\",this-10 sub,this*10 mul,this/10 adv from test")
                .build()
                .start(Flux.range(0, 100))
                .doOnNext(System.out::println)
                .cast(Map.class)
                .map(map -> map.get("add"))
                .cast(Long.class)
                .reduce(Math::addExact)
                .as(StepVerifier::create)
                .expectNext(5950L)
                .verifyComplete();

    }

    @Test
    void testMax() {

        ReactorQL.builder()
                .sql("select max(this) val from test")
                .build()
                .start(Flux.range(1, 100))
                .as(StepVerifier::create)
                .expectNext(Collections.singletonMap("val", 100))
                .verifyComplete();

    }

    @Test
    void testMin() {

        ReactorQL.builder()
                .sql("select min(this) val from test")
                .build()
                .start(Flux.range(1, 100))
                .as(StepVerifier::create)
                .expectNext(Collections.singletonMap("val", 1))
                .verifyComplete();

    }

    @Test
    void testAvg() {

        ReactorQL.builder()
                .sql("select avg(this) val from test")
                .build()
                .start(Flux.range(1, 100))
                .as(StepVerifier::create)
                .expectNext(Collections.singletonMap("val", 50.5D))
                .verifyComplete();

    }

    @Test
    void testSumAndCount() {

        ReactorQL.builder()
                .sql("select sum(val) total,count(1) count from test")
                .build()
                .start(Flux.range(1, 100).map(v -> Collections.singletonMap("val", v)))
                .as(StepVerifier::create)
                .expectNext(new HashMap<String, Object>() {
                    {
                        put("total", 5050D);
                        put("count", 100L);
                    }
                })
                .verifyComplete();

    }


    @Test
    void testGroup() {

        ReactorQL.builder()
                .sql("select count(1) total,type from test group by type")
                .build()
                .start(Flux.range(0, 100).map(v -> Collections.singletonMap("type", v / 10)))
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
                .start(Flux.range(0, 10).delayElements(Duration.ofMillis(500)))
                .doOnNext(System.out::println)
                .as(StepVerifier::create)
                .expectNextCount(6)
                .verifyComplete();

    }

    @Test
    void testGroupByColumns() {

        ReactorQL.builder()
                .sql("select productId,deviceId,count(1) total from test group by productId,deviceId")
                .build()
                .start(Flux.range(0, 10).map(v ->
                        new HashMap<String, Object>() {
                            {
                                put("val", v);
                                put("deviceId", "dev-" + v / 2);
                                put("productId", "prod-" + v / 4);
                            }
                        }))
                .doOnNext(System.out::println)
                .cast(Map.class)
                .map(map -> map.get("total"))
                .cast(Long.class)
                .reduce(Math::addExact)
                .as(StepVerifier::create)
                .expectNext(10L)
                .verifyComplete();

    }

    @Test
    void testGroupByTimeHaving() {

        ReactorQL.builder()
                .sql("select avg(this) total from test group by interval('1s') having total > 2")
                .build()
                .start(Flux.range(0, 10).delayElements(Duration.ofMillis(500)))
                .doOnNext(System.out::println)
                .as(StepVerifier::create)
                .expectNextCount(4)
                .verifyComplete();

    }

    @Test
    void testCase() {

        ReactorQL.builder()
                .sql("select case this when 1 then '一' when 2 then '二' when 2+1 then '三' else this end type from test")
                .build()
                .start(Flux.range(0, 4).delayElements(Duration.ofMillis(500)))
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
                .start(Flux.just(Collections.singletonMap("_name", "test")))
                .as(StepVerifier::create)
                .expectNext(Collections.singletonMap("name", "test"))
                .verifyComplete();

    }

    @Test
    void testNow(){
        ReactorQL.builder()
                .sql("select now('yyyy-MM-dd') now from dual")
                .build()
                .start(Flux.just(1))
                .as(StepVerifier::create)
                .expectNext(Collections.singletonMap("now", DateTimeFormatter.ofPattern("yyyy-MM-dd").format(LocalDateTime.now())))
                .verifyComplete();
    }

    @Test
    void testCast(){
        ReactorQL.builder()
                .sql("select cast(this as string) val from dual")
                .build()
                .start(Flux.just(1))
                .as(StepVerifier::create)
                .expectNext(Collections.singletonMap("val", "1"))
                .verifyComplete();
    }

    @Test
    void testDateFormat(){
        ReactorQL.builder()
                .sql("select date_format(this,'yyyy-MM-dd','Asia/Shanghai') now from dual")
                .build()
                .start(Flux.just(System.currentTimeMillis()))
                .as(StepVerifier::create)
                .expectNext(Collections.singletonMap("now", DateTimeFormatter.ofPattern("yyyy-MM-dd").format(LocalDateTime.now())))
                .verifyComplete();
    }

}