package org.jetlinks.reactor.ql;

import org.hswebframework.utils.time.DateFormatter;
import org.jetlinks.reactor.ql.supports.map.SingleParameterFunctionMapFeature;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

class ReactorQLTest {

    @Test
    void testLimit() {

        ReactorQL.builder()
                 .sql("select * from test limit 0,10")
                 .build()
                 .start(Flux.range(0, 20))
                 .doOnNext(System.out::println)
                 .as(StepVerifier::create)
                 .expectNextCount(10)
                 .verifyComplete();

    }

    @Test
    void testOrderBy() {

        ReactorQL.builder()
                 .sql("select this val from test order by this")
                 .build()
                 .start(Flux.just(0, 3, 2, 1, 6))
                 .map(map -> map.get("val"))
                 .as(StepVerifier::create)
                 .expectNext(0, 1, 2, 3, 6)
                 .verifyComplete();

        ReactorQL.builder()
                 .sql("select this val from test order by this desc")
                 .build()
                 .start(Flux.just(0, 3, 2, 1, 6))
                 .map(map -> map.get("val"))
                 .as(StepVerifier::create)
                 .expectNext(6, 3, 2, 1, 0)
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
                 .sql("select /*+ concurrency(0) */ count(1) total from test where (this > 10 and this > 10.0) and (this <=90 or this >95) and this is not null")
                 .build()
                 .start(Flux.range(1, 100))
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("total", 85L))
                 .verifyComplete();

    }

    @Test
    void testBoolean() {

        ReactorQL.builder()
                 .sql("select count(1) total from test where not this")
                 .build()
                 .start(Flux.just(false))
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("total", 1L))
                 .verifyComplete();

        ReactorQL.builder()
                 .sql("select count(1) total from test where (this is true) and (this is not false)")
                 .build()
                 .start(Flux.just(true))
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("total", 1L))
                 .verifyComplete();

        ReactorQL.builder()
                 .sql("select count(1) total from test where (this is false) and (this is not true)")
                 .build()
                 .start(Flux.just(false))
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("total", 1L))
                 .verifyComplete();

        ReactorQL.builder()
                 .sql("select count(1) total from test where (this is false) or (this is not true)")
                 .build()
                 .start(Flux.just(true))
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("total", 0L))
                 .verifyComplete();
    }

    @Test
    void testLike() {

        ReactorQL.builder()
                 .sql("select count(1) total from test where this like 'abc%'")
                 .build()
                 .start(Flux.just("abcdefg"))
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("total", 1L))
                 .verifyComplete();

        ReactorQL.builder()
                 .sql("select count(1) total from test where this not like 'abc%'")
                 .build()
                 .start(Flux.just("abcdefg"))
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("total", 0L))
                 .verifyComplete();

        ReactorQL.builder()
                 .sql("select count(1) total from test where str_like(this,'abc%')")
                 .build()
                 .start(Flux.just("abcdefg"))
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("total", 1L))
                 .verifyComplete();

        ReactorQL.builder()
                 .sql("select count(1) total from test where not str_like(this,'abc%')")
                 .build()
                 .start(Flux.just("abcdefg"))
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("total", 0L))
                 .verifyComplete();

        ReactorQL.builder()
                 .sql("select str_nlike(this,'xxx%') nlike from test")
                 .build()
                 .start(Flux.just("abcdefg"))
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("nlike", true))
                 .verifyComplete();


    }

    @Test
    void testArray() {
        ReactorQL.builder()
                 .sql("select this['name.[0]'] name from i")
                 .build()
                 .start(Flux.just(Collections.singletonMap("name", Collections.singletonList("123"))))
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("name", "123"))
                 .verifyComplete();
    }

    @Test
    void testDotProperty() {
        ReactorQL.builder()
                 .sql("select this.headers['name.value'] name from i")
                 .build()
                 .start(Flux.just(Collections.singletonMap("headers", Collections.singletonMap("name.value", "123"))))
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("name", "123"))
                 .verifyComplete();
    }

    @Test
    void testNest() {
        ReactorQL.builder()
                 .sql("select this.name.info name from i")
                 .build()
                 .start(Flux.just(Collections.singletonMap("name", Collections.singletonMap("info", "123"))))
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("name", "123"))
                 .verifyComplete();
    }

    @Test
    void testBind() {

        ReactorQL.builder()
                 .sql("select count(1) total from test where this > ? and this <= :val and this>:0 and :3 is null and :1 is not null")
                 .build()
                 .start(ReactorQLContext
                                .ofDatasource(v -> Flux.range(1, 100))
                                .bind(10)
                                .bind(1, 10)
                                .bind("val", 95)
                 )
                 .map(ReactorQLRecord::asMap)
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("total", 85L))
                 .verifyComplete();

    }

    @Test
    void testSelectBind() {
        ReactorQL.builder()
                 .sql("select ? value from dual")
                 .build()
                 .start(ReactorQLContext
                                .ofDatasource(v -> Flux.just(1))
                                .bind(10)
                 )
                 .map(ReactorQLRecord::asMap)
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("value", 10))
                 .verifyComplete();

        ReactorQL.builder()
                 .sql("select :0 value from dual")
                 .build()
                 .start(ReactorQLContext
                                .ofDatasource(v -> Flux.just(1))
                                .bind(10)
                 )
                 .map(ReactorQLRecord::asMap)
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("value", 10))
                 .verifyComplete();
    }

    @Test
    void testDataSource() {
        ReactorQL.builder()
                 .sql("select (select this name from a) a,(select this name from v) v from t")
                 .build()
                 .start(name -> Flux.just("t_" + name))
                 .as(StepVerifier::create)
                 .expectNext(new HashMap<String, Object>() {{
                     put("a", Collections.singletonMap("name", "t_a"));
                     put("v", Collections.singletonMap("name", "t_v"));
                 }})
                 .verifyComplete();
    }

    @Test
    void testBetween() {

        ReactorQL.builder()
                 .sql("select count(1) total from test where this between 1 and 20 and {ts '2020-02-04 00:00:00'} between {d '2010-01-01'} and now()")
                 .build()
                 .start(Flux.range(1, 100))
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("total", 20L))
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

        ReactorQL.builder()
                 .sql("select this from test where this not in (1,2,3,4)")
                 .build()
                 .start(Flux.range(0, 10))
                 .as(StepVerifier::create)
                 .expectNextCount(6)
                 .verifyComplete();

        ReactorQL.builder()
                 .sql("select this v from test where 10 in (1,2,3,list)")
                 .build()
                 .start(Flux.just(Collections.singletonMap("list", Arrays.asList(1, 2, 3)),
                                  Collections.singletonMap("list", Arrays.asList(10, 20, 30))))
                 .doOnNext(System.out::println)
                 .as(StepVerifier::create)
                 .expectNextCount(1)
                 .verifyComplete();

        ReactorQL.builder()
                 .sql("select this from test where this in (select * from (values(6)) t(v) )")
                 .build()
                 .start(Flux.range(0, 10))
                 .as(StepVerifier::create)
                 .expectNextCount(1)
                 .verifyComplete();

    }

    @Test
    void testContains() {

        ReactorQL.builder()
                 .sql("select contains_all(this,?) isIn from dual")
                 .build()
                 .start(ReactorQLContext
                                .ofDatasource((s) -> Flux.just(Arrays.asList(1, 2, 3, 4)))
                                .bind(0, Arrays.asList(2, "3")))
                 .doOnNext(System.out::println)
                 .map(e -> e.asMap().get("isIn"))
                 .as(StepVerifier::create)
                 .expectNext(true)
                 .verifyComplete();
//
        ReactorQL.builder()
                 .sql("select contains_any(this,?) isIn from dual")
                 .build()
                 .start(ReactorQLContext
                                .ofDatasource((s) -> Flux.just(Arrays.asList(1, 2, 3, 4)))
                                .bind(0, Arrays.asList(2, 9)))
                 .doOnNext(System.out::println)
                 .map(e -> e.asMap().get("isIn"))
                 .as(StepVerifier::create)
                 .expectNext(true)
                 .verifyComplete();

        {
            ReactorQL.builder()
                     .sql("select not_contains(this,?) isIn from dual")
                     .build()
                     .start(ReactorQLContext
                                    .ofDatasource((s) -> Flux.just(Arrays.asList(1, 2, 3, 4)))
                                    .bind(0, Arrays.asList(0, 9)))
                     .doOnNext(System.out::println)
                     .map(e -> e.asMap().get("isIn"))
                     .as(StepVerifier::create)
                     .expectNext(true)
                     .verifyComplete();
            ReactorQL.builder()
                     .sql("select not_contains(this,?) isIn from dual")
                     .build()
                     .start(ReactorQLContext
                                    .ofDatasource((s) -> Flux.just(Arrays.asList(1, 2, 3, 4)))
                                    .bind(0, Arrays.asList(9, 1)))
                     .doOnNext(System.out::println)
                     .map(e -> e.asMap().get("isIn"))
                     .as(StepVerifier::create)
                     .expectNext(false)
                     .verifyComplete();
        }


    }

    @Test
    void testCalculate() {

        ReactorQL.builder()
                 .sql(
                         "select math.plus(this,10) plus,",
                         "math.sub(this,10) sub1,",
                         "math.mul(this,10) mul1,",
                         "math.divi(this,10) div1,",
                         "math.mod(this,10) mod1,",
                         "this + 10 \"v\",",
                         "this-10 sub,",
                         "this*10 mul,",
                         "this/10 divi,",
                         "math.max(1,2) mx,",
                         "math.min(1,3) min,",
                         "math.avg(1,3) avg,",

                         "this%2 mod from test"
                 )
                 .build()
                 .start(Flux.range(0, 100))
                 .doOnNext(System.out::println)
                 .cast(Map.class)
                 .map(map -> map.get("v"))
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

        ReactorQL.builder()
                 .sql("select avg(distinct this) val from test")
                 .build()
                 .start(Flux.just(1, 1, 2, 2, 3))
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("val", 2.0D))
                 .verifyComplete();

        ReactorQL.builder()
                 .sql("select avg(unique this) val from test")
                 .build()
                 .start(Flux.just(1, 1, 2, 2, 3, 5))
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("val", 4.0D))
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
                 .sql("select count(1) total,type,_group_by_key from test group by type")
                 .build()
                 .start(Flux.range(0, 100).map(v -> Collections.singletonMap("type", v / 10)))
                 .doOnNext(System.out::println)
                 .as(StepVerifier::create)
                 .expectNextCount(10)
                 .verifyComplete();

    }


    @Test
    void testGroupWhere() {

        ReactorQL.builder()
                 .sql("select count(1) total,type from test where type=1 group by type")
                 .build()
                 .start(Flux.range(0, 100).map(v -> Collections.singletonMap("type", v / 10)))
                 .doOnNext(System.out::println)
                 .as(StepVerifier::create)
                 .expectNextCount(1)
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
    void testGroupByBinary() {
        ReactorQL.builder()
                 .sql("select avg(this) total,_group_by_key from test group by this/2")
                 .build()
                 .start(Flux.range(0, 10))
                 .doOnNext(System.out::println)
                 .as(StepVerifier::create)
                 .expectNextCount(5)
                 .verifyComplete();
    }

    @Test
    void testGroupByWindowEmpty() {
        ReactorQL.builder()
                 .sql("select count(this) total from test group by interval(500)")
                 .build()
                 .start(Flux.range(0, 2).delayElements(Duration.ofSeconds(1)))
                 .doOnNext(System.out::println)
                 .as(StepVerifier::create)
                 .expectNextCount(5)
                 .verifyComplete();

    }

    @Test
    void testZip() {
        ReactorQL.builder()
                 .sql("select ",
                      "t3.t1.v1,",
                      "t3.t2.v2 ",
                      "from zip(",
                      "   (select this v1 from t1),",
                      "   (select this+1 v2 from t2)",
                      ") t3")
                 .build()
                 .start(Flux.range(0, 2))
                 .doOnNext(System.out::println)
                 .as(StepVerifier::create)
                 .expectNextCount(2)
                 .verifyComplete();
    }

    @Test
    void testCombine() {
        ReactorQL.builder()
                 .sql("select ",
                      "t3.t1.v1,",
                      "t3.t2.v2 ",
                      "from combine(",
                      "   (select this v1 from t1),",
                      "   (select this+1 v2 from t2)",
                      ") t3")
                 .build()
                 .start(table -> table.equals("t1") ?
                         Flux.interval(Duration.ofMillis(10)).take(5) :
                         Flux.interval(Duration.ofMillis(20)).take(5))
                 .take(5)
                 .doOnNext(System.out::println)
                 .as(StepVerifier::create)
                 .expectNextCount(5)
                 .verifyComplete();
    }


    @Test
    void testValues() {

        ReactorQL.builder()
                 .sql("select",
                      "sum(t.a+t.b) sum ",
                      "from ( values (1,2,3),(1,2,3) ) t(a,b,c) ")
                 .build()
                 .start(Flux.range(0, 2))
                 .doOnNext(System.out::println)
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("sum", 6D))
                 .verifyComplete();

    }

    @Test
    void testQ() {
        ReactorQL.builder()
                 .sql("select",
                      "sum(a.v) sum ",
                      "from ( select this v from t ) a ")
                 .build()
                 .start(Flux.range(0, 2))
                 .doOnNext(System.out::println)
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("sum", 1D))
                 .verifyComplete();

    }


    @Test
    void testGroupByWindow() {

        ReactorQL.builder()
                 .sql("select avg(this) total from test group by _window(2)")
                 .build()
                 .start(Flux.range(0, 10).delayElements(Duration.ofMillis(100)))
                 .doOnNext(System.out::println)
                 .as(StepVerifier::create)
                 .expectNextCount(5)
                 .verifyComplete();
        System.out.println();


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
                 .sql("select "
                         , "case this"
                         , "when 3.1 then '3.1'"
                         , "when range(this,10,11) then '1'"
                         , "when null then ''"
                         , "when 'null' then ''"
                         , "when gt(this,10) then ''"
                         , "when {ts '2020-01-01 12:00:00'} then '2020-01-01 12:00:00'"
                         , "when {t '12:00:00'}then '12:00:00'"
                         , "when {d '2020-01-01'}then '2020-01-01'"
                         , "when 1 then '一'"
                         , "when 2 then '二'"
                         , "when 2+1 then '三'"
                         , "when this then this"
                         , "else this end type from test")
                 .build()
                 .start(Flux.range(0, 4))
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
    void testExists() {

        ReactorQL.builder()
                 .sql("select _name name from t where exists(select 'test' v from a where t._name = a._name ) ")
                 .build()
                 .start(Flux.just(Collections.singletonMap("_name", "test")))
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("name", "test"))
                 .verifyComplete();


        ReactorQL.builder()
                 .sql("select _name name from t where not exists(select 'test' v from a where t._name != a._name ) ")
                 .build()
                 .start(Flux.just(Collections.singletonMap("_name", "test")))
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("name", "test"))
                 .verifyComplete();

    }


    @Test
    void testWhereFromSelect() {
        ReactorQL.builder()
                 .sql("select _name name from t where _name = (select 'test' v ) and (select 'test' v ) = _name")
                 .build()
                 .start(Flux.just(Collections.singletonMap("_name", "test")))
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("name", "test"))
                 .verifyComplete();
    }

    @Test
    void testNow() {
        ReactorQL.builder()
                 .sql("select now('yyyy-MM-dd') now from dual")
                 .build()
                 .start(Flux.just(1))
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("now", DateTimeFormatter
                         .ofPattern("yyyy-MM-dd")
                         .format(LocalDateTime.now())))
                 .verifyComplete();
    }


    @Test
    void testNestGroup() {
        ReactorQL.builder()
                 .sql(
                         "select sum(val+val2) v,avg(val+val2) avg,val2/2 g from (",

                         "select ",
                         "10 val,",
                         "this val2",
                         "from dual",

                         ") group by val2/2"
                 )
                 .build()
                 .start(Flux.range(0, 10))
                 .doOnNext(System.out::println)
                 .as(StepVerifier::create)
                 .expectNextCount(5)
                 .verifyComplete();

    }

    @Test
    void testCalculatePriority() {
        ReactorQL.builder()
                 .sql(
                         "select ",
                         "3+2-5*0 val",
                         ",3+2*2 val2",
                         ",2*(3+2) val3",
                         ",math.floor(math.log(32.2)) log",
                         ",math.max(1,2) max",
                         "from dual"
                 )
                 .build()
                 .start(Flux.just(1))
                 .as(StepVerifier::create)
                 .expectNext(new HashMap<String, Object>() {{
                     put("val", 5L);
                     put("val2", 7L);
                     put("val3", 10L);
                     put("log", 3.0);
                     put("max", 2L);
                 }})
                 .verifyComplete();
    }

    @Test
    void testCast() {
        ReactorQL.builder()
                 .sql(
                         "select ",
                         "cast(this as string) val",
                         ",'y'::bool bool0",
                         ",cast('y' as boolean) bool1",
                         ",cast('1' as bool) bool2",
                         ",cast('false' as bool) bool3",
                         ",cast(100.2 as int) int",
                         ",cast('100.3' as double) d",
                         ",cast(101.3-this as float) float",
                         ",cast('2020-01-01' as date) date",
                         ",cast('1.0E32' as decimal) decimal",
                         ",cast('100' as long) long",
                         ",cast('a' as unknown) unknown",

                         "from dual"
                 )
                 .build()
                 .start(Flux.just(1))
                 .doOnNext(System.out::println)
                 .as(StepVerifier::create)
                 .expectNext(new HashMap<String, Object>() {{
                     put("val", "1");
                     put("int", 100);
                     put("d", 100.3D);
                     put("float", 100.3F);
                     put("bool0", true);
                     put("bool1", true);
                     put("bool2", true);
                     put("bool3", false);
                     put("date", DateFormatter.fromString("2020-01-01"));
                     put("decimal", new BigDecimal("1.0E32"));
                     put("long", 100L);
                     put("unknown", "a");

                 }})
                 .verifyComplete();
    }

    @Test
    void testDateFormat() {
        ReactorQL.builder()
                 .sql("select date_format(this,'yyyy-MM-dd','Asia/Shanghai') now from dual")
                 .build()
                 .start(Flux.just(System.currentTimeMillis()))
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("now", DateTimeFormatter
                         .ofPattern("yyyy-MM-dd")
                         .format(LocalDateTime.now(ZoneId.of("Asia/Shanghai")))))
                 .verifyComplete();

        try {
            ReactorQL.builder()
                     .sql("select date_format(this,'yyyy-MM-dd','aaa') now from dual")
                     .build();
            Assertions.fail("");
        } catch (UnsupportedOperationException ignore) {

        }
        try {
            ReactorQL.builder()
                     .sql("select date_format(this) now from dual")
                     .build();
            Assertions.fail("");
        } catch (UnsupportedOperationException ignore) {

        }
        try {
            ReactorQL.builder()
                     .sql("select date_format(this,'aaa') now from dual")
                     .build();
            Assertions.fail("");
        } catch (UnsupportedOperationException ignore) {

        }
        try {
            ReactorQL.builder()
                     .sql("select date_format(this,123) now from dual")
                     .build();
            Assertions.fail("");
        } catch (UnsupportedOperationException ignore) {

        }
    }

    @Test
    void testNestArrayGet() {
        ReactorQL.builder()
                 .sql("select t.val val from (select this val from a) t")
                 .build()
                 .start(Flux.just(1))
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("val", 1))
                 .verifyComplete();

        ReactorQL.builder()
                 .sql("select t['val'] val from (select this val from a) t")
                 .build()
                 .start(Flux.just(1))
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("val", 1))
                 .verifyComplete();
    }

    @Test
    void testNestQuery() {
        ReactorQL.builder()
                 .sql("select (select date_format(n.t,'yyyy-MM-dd','Asia/Shanghai') now from (select now() t) n) now_obj")
                 .build()
                 .start(Flux.just(System.currentTimeMillis()))
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("now_obj", Collections.singletonMap("now", DateTimeFormatter
                         .ofPattern("yyyy-MM-dd")
                         .format(LocalDateTime.now(ZoneId.of("Asia/Shanghai"))))))
                 .verifyComplete();
    }

    @Test
    void testConcat() {
        ReactorQL.builder()
                 .sql("select concat(1,2,3,4) v, 1||2 v2 ,concat(row_to_array((select 1 a1))) v3 from dual")
                 .build()
                 .start(Flux.just(System.currentTimeMillis()))
                 .as(StepVerifier::create)
                 .expectNext(new HashMap<String, Object>() {{
                     put("v", "1234");
                     put("v2", "12");
                     put("v3", "1");
                 }})
                 .verifyComplete();
    }


    @Test
    void testBit() {
        ReactorQL.builder()
                 .sql("select ",
                      "1^3", ",bit_mutex(2,4)",
                      ",0x616263 hex",
                      ",1&3", ",bit_and(3,8)",
                      ",1|3", ",bit_or(3,9)",
                      ",1<<3", ",bit_left_shift(1,3)",
                      ",1>>3", ",bit_right_shift(1,3)",
                      ",bit_unsigned_shift(1,3)",//1 >>>3
                      ",~3,-3,-(+3)", //sign
                      ",bit_not(3)",
                      ",bit_count(30)",
                      " from dual")
                 .build()
                 .start(Flux.just(System.currentTimeMillis()))
                 .doOnNext(System.out::println)
                 .as(StepVerifier::create)
                 .expectNextCount(1)
                 .verifyComplete();
    }


    @Test
    void testNewMap() {
        ReactorQL.builder()
                 .sql("select new_map('1',1,'2',2) v from dual")
                 .build()
                 .start(Flux.just(System.currentTimeMillis()))
                 .as(StepVerifier::create)
                 .expectNext(new HashMap<String, Object>() {{
                     put("v", new HashMap<Object, Object>() {
                         {
                             put("1", 1L);
                             put("2", 2L);
                         }
                     });
                 }})
                 .verifyComplete();
    }

    @Test
    void testNewArray() {
        ReactorQL.builder()
                 .sql("select new_array(1,2,3,4) v from dual")
                 .build()
                 .start(Flux.just(System.currentTimeMillis()))
                 .as(StepVerifier::create)
                 .expectNext(new HashMap<String, Object>() {{
                     put("v", Arrays.asList(1L, 2L, 3L, 4L));
                 }})
                 .verifyComplete();
    }

    @Test
    void testJoin() {
        ReactorQL.builder()
                 .sql("select t1.name,t2.name from t1,t2")
                 .build()
                 .start(t -> {
                     return Flux.just(Collections.singletonMap("name", t));
                 })
                 .as(StepVerifier::create)
                 .expectNext(new HashMap<String, Object>() {{
                     put("t1.name", "t1");
                     put("t2.name", "t2");
                 }})
                 .verifyComplete();
    }

    @Test
    void testCustomFunction() {
        ReactorQL.builder()
                 .sql("select upper('name') name from t1")
                 .feature(new SingleParameterFunctionMapFeature("upper", v -> String.valueOf(v).toUpperCase()))
                 .build()
                 .start(Flux.just(1))
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("name", "NAME"))
                 .verifyComplete();
    }


    @Test
    void testWhereCase() {
        ReactorQL.builder()
                 .sql("select t.key",
                      "from t",
                      "where (",
                      "   case ",
                      "       when t.key = '1' then t.value = '2'",
                      "       when t.key = '2' then t.value = '3'",
                      "end",
                      " )")
                 .build()
                 .start(Flux.just(
                         new HashMap<String, Object>() {
                             {
                                 put("key", "1");
                                 put("value", "2");
                             }
                         },
                         new HashMap<String, Object>() {
                             {
                                 put("key", "2");
                                 put("value", "2");
                             }
                         }
                 ))
                 .as(StepVerifier::create)
                 .expectNext(new HashMap<String, Object>() {{
                     put("t.key", "1");
                 }})
                 .verifyComplete();
    }


    @Test
    void testRightJoin() {
        ReactorQL.builder()
                 .sql(
                         "select t1.name,t2.name,t1.v,t2.v from t1 ",
                         "right join t2 on t1.v=t2.v"
                 )
                 .build()
                 .start(t -> Flux.range(0, 2)
                                 .map(v -> new HashMap<String, Object>() {
                                     {
                                         put("name", t);
                                         put("v", v);
                                     }
                                 }))
                 .doOnNext(System.out::println)
                 .as(StepVerifier::create)
                 .expectNextCount(4)
                 .verifyComplete();
    }

    @Test
    void testLeftJoin() {
        ReactorQL.builder()
                 .sql(
                         "select t1.name,t2.name,t1.v,t2.v,t3.name,t3.v from t1 ",
                         "left join t2 on t1.v=t2.v",
                         "left join t3 on t3.v=t2.v"
//                        "where t1.v=t2.v and t3.v=t2.v"
                 )
                 .build()
                 .start(t -> Flux.range(0, t.equals("t1") ? 3 : 2)
                                 .map(v -> new HashMap<String, Object>() {
                                     {
                                         put("name", t);
                                         put("v", v);
                                     }
                                 }))
                 .doOnNext(System.out::println)
                 .as(StepVerifier::create)
                 .expectNextCount(3)
                 .verifyComplete();
    }

    @Test
    void testSubJoinParam() {
        ReactorQL.builder()
                 .sql(
                         "select t1.name,t2.name from t1 ",
                         "left join (select name from ( values (1),(t1.name) ) t(name) ) t2"
                 )
                 .build()
                 .start(t -> Flux.range(0, 2)
                                 .map(v -> new HashMap<String, Object>() {
                                     {
                                         put("name", t);
                                     }
                                 }))
                 .doOnNext(System.out::println)
                 .as(StepVerifier::create)
                 .expectNextCount(4)
                 .verifyComplete();

    }

    @Test
    void testSubJoin() {
        ReactorQL.builder()
                 .sql(
                         "select t1.name,t2.name,t1.v,t2.v from t1 ",
                         "left join (select this.v v ,name from t2 ) t2 on t1.v=t2.v"
                 )
                 .build()
                 .start(t -> Flux.range(0, 2)
                                 .map(v -> new HashMap<String, Object>() {
                                     {
                                         put("name", t);
                                         put("v", v);
                                     }
                                 }))
                 .doOnNext(System.out::println)
                 .as(StepVerifier::create)
                 .expectNext(new HashMap<String, Object>() {{
                     put("t1.name", "t1");
                     put("t2.name", "t2");
                     put("t1.v", 0);
                     put("t2.v", 0);

                 }}, new HashMap<String, Object>() {{
                     put("t1.name", "t1");
                     put("t2.name", "t2");
                     put("t1.v", 1);
                     put("t2.v", 1);
                 }})
                 .verifyComplete();
    }

    @Test
    void testJoinWhere() {
        ReactorQL.builder()
                 .sql("select t1.name,t2.name,t1.v,t2.v from t1,t2 where t1.v=t2.v")
                 .build()
                 .start(t -> Flux.range(0, 2)
                                 .map(v -> new HashMap<String, Object>() {
                                     {
                                         put("name", t);
                                         put("v", v);
                                     }
                                 }))
                 .doOnNext(System.out::println)
                 .as(StepVerifier::create)
                 .expectNext(new HashMap<String, Object>() {{
                     put("t1.name", "t1");
                     put("t2.name", "t2");
                     put("t1.v", 0);
                     put("t2.v", 0);

                 }}, new HashMap<String, Object>() {{
                     put("t1.name", "t1");
                     put("t2.name", "t2");
                     put("t1.v", 1);
                     put("t2.v", 1);
                 }})
                 .verifyComplete();
    }

    @Test
    void testQuo() {
        ReactorQL.builder()
                 .sql("select  \"this\" \"t\" from \"table\" ")
                 .build()
                 .start(Flux::just)
                 .doOnNext(System.out::println)
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("t", "table"))
                 .verifyComplete();
    }

    @Test
    void testDistinctCount() {
        ReactorQL.builder()
                 .sql("select distinct_count(this) t from \"table\" ")
                 .build()
                 .start(Flux.just(1, 2, 3, 3, 4, 5, 6, 6, 6, 7))
                 .doOnNext(System.out::println)
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("t", 7L))
                 .verifyComplete();

        ReactorQL.builder()
                 .sql("select count(distinct this) t from \"table\" ")
                 .build()
                 .start(Flux.just(1, 2, 3, 3, 4, 5, 6, 6, 6, 7))
                 .doOnNext(System.out::println)
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("t", 7L))
                 .verifyComplete();
    }

    @Test
    void testUniqueCount() {

        ReactorQL.builder()
                 .sql("select count(unique this) t from \"table\" ")
                 .build()
                 .start(Flux.just(1, 2, 3, 3, 4, 5, 6, 6, 6, 7))
                 .doOnNext(System.out::println)
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("t", 5L))
                 .verifyComplete();

        Duration time = Flux
                .range(0, 1000000)
                .collect(Collectors.groupingBy(Function.identity(),
                                               ConcurrentHashMap::new,
                                               Collectors.counting()))
                .as(StepVerifier::create)
                .expectNextCount(1)
                .verifyComplete();

        System.out.println(time);

        time = ReactorQL
                .builder()
                .sql("select count(unique this) t from \"table\" ")
                .build()
                .start(Flux.range(0, 1000000))
                .doOnNext(System.out::println)
                .as(StepVerifier::create)
                .expectNext(Collections.singletonMap("t", 1000000L))
                .verifyComplete();
        System.out.println(time);
    }

    @Test
    void testDistinct() {
        ReactorQL.builder()
                 .sql("select distinct this from \"table\" ")
                 .build()
                 .start(Flux.just(1, 2, 3, 3, 4, 5, 6, 6, 6, 7))
                 .doOnNext(System.out::println)
                 .as(StepVerifier::create)
                 .expectNextCount(7)
                 .verifyComplete();
    }

    @Test
    void testDistinctColumn() {
        ReactorQL.builder()
                 .sql("select distinct on(this) this from \"table\" ")
                 .build()
                 .start(Flux.just(1, 2, 3, 3, 4, 5, 6, 6, 6, 7))
                 .doOnNext(System.out::println)
                 .as(StepVerifier::create)
                 .expectNextCount(7)
                 .verifyComplete();
    }

    @Test
    void testDistinctOnAll() {
        ReactorQL.builder()
                 .sql("select distinct on(*) this from \"table\" ")
                 .build()
                 .start(Flux.just(1, 2, 3, 3, 4, 5, 6, 6, 6, 7))
                 .doOnNext(System.out::println)
                 .as(StepVerifier::create)
                 .expectNextCount(7)
                 .verifyComplete();
    }

    @Test
    void testDistinctOnTable() {
        ReactorQL.builder()
                 .sql("select distinct on(t.*) this from \"table\" t ")
                 .build()
                 .start(Flux.just(1, 2, 3, 3, 4, 5, 6, 6, 6, 7))
                 .doOnNext(System.out::println)
                 .as(StepVerifier::create)
                 .expectNextCount(7)
                 .verifyComplete();
    }


    @Test
    void testAllColumn() {
        ReactorQL.builder()
                 .sql("select * from \"table\" t ")
                 .build()
                 .start(Flux.just(Collections.singletonMap("a", "b")))
                 .doOnNext(System.out::println)
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("a", "b"))
                 .verifyComplete();
    }

    @Test
    void testAllColumnTable() {
        ReactorQL.builder()
                 .sql("select t.* from \"table\" t  ")
                 .build()
                 .start(Flux.just(Collections.singletonMap("a", "b")))
                 .doOnNext(System.out::println)
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("a", "b"))
                 .verifyComplete();
    }

    @Test
    void testColumnToRow() {
        ReactorQL.builder()
                 .sql("select prop $this from dual")
                 .build()
                 .start(Flux.just(Collections.singletonMap("prop", Collections.singletonMap("v", 1))))
                 .doOnNext(System.out::println)
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("v", 1))
                 .verifyComplete();
    }

    @Test
    void testGroupTakeFirst() {
        ReactorQL.builder()
                 .sql("select take(this) v,count(1) total from test group by _window(5)")
                 .build()
                 .start(Flux.range(0, 11))
                 .doOnNext(System.out::println)
                 .map(map -> map.get("v"))
                 .as(StepVerifier::create)
                 .expectNext(0, 5, 10)
                 .verifyComplete();
    }

    @Test
    void testGroupTake2() {
        ReactorQL.builder()
                 .sql("select take(this,2) v from test group by _window(5)")
                 .build()
                 .start(Flux.range(0, 11))
                 .doOnNext(System.out::println)
                 .map(map -> map.get("v"))
                 .as(StepVerifier::create)
                 .expectNext(0, 1, 5, 6, 10)
                 .verifyComplete();
    }

    @Test
    void testTakeLast() {
        ReactorQL.builder()
                 .sql("select take(this,-1) v from test group by _window(5)")
                 .build()
                 .start(Flux.range(0, 11))
                 .doOnNext(System.out::println)
                 .map(map -> map.get("v"))
                 .as(StepVerifier::create)
                 .expectNext(4, 9, 10)
                 .verifyComplete();

        ReactorQL.builder()
                 .sql("select this v from test group by _window(5), take(-1)")
                 .build()
                 .start(Flux.range(0, 11))
                 .doOnNext(System.out::println)
                 .map(map -> map.get("v"))
                 .as(StepVerifier::create)
                 .expectNext(4, 9, 10)
                 .verifyComplete();
    }

    @Test
    void testTake3Last2() {
        ReactorQL.builder()
                 .sql("select take(this,3,-2) v from test group by _window(5)")
                 .build()
                 .start(Flux.range(0, 11))
                 .doOnNext(System.out::println)
                 .map(map -> map.get("v"))
                 .as(StepVerifier::create)
                 .expectNext(1, 2, 6, 7, 10)
                 .verifyComplete();

        ReactorQL.builder()
                 .sql("select take(this,3,-2) v from test group by _window(5),take(3,-2)")
                 .build()
                 .start(Flux.range(0, 11))
                 .doOnNext(System.out::println)
                 .map(map -> map.get("v"))
                 .as(StepVerifier::create)
                 .expectNext(1, 2, 6, 7, 10)
                 .verifyComplete();
    }

    @Test
    void testLast3Tak2() {
        ReactorQL.builder()
                 .sql("select take(this,-3,2) v  from test group by _window(5)")
                 .build()
                 .start(Flux.range(0, 11))
                 .doOnNext(System.out::println)
                 .map(map -> map.get("v"))
                 .as(StepVerifier::create)
                 .expectNext(2, 3, 7, 8, 10)
                 .verifyComplete();

        ReactorQL.builder()
                 .sql("select take(this,3,-2) v from test group by _window(5),take(3,-2)")
                 .build()
                 .start(Flux.range(0, 11))
                 .doOnNext(System.out::println)
                 .map(map -> map.get("v"))
                 .as(StepVerifier::create)
                 .expectNext(1, 2, 6, 7, 10)
                 .verifyComplete();
    }

    @Test
    void testGroupTimeProperty() {
        String[] sql = {""
                // ,"select * from("
                , "select deviceId,count(1) total from dual"
                , "group by deviceId,interval('1s')"
                , "having total = 0"
                // ,")"
        };

        Flux<Map<String, Object>> data = Flux.create(sink -> {

            sink.next(Collections.singletonMap("deviceId", 1));
            sink.next(Collections.singletonMap("deviceId", 2));
            sink.next(Collections.singletonMap("deviceId", 1));
            sink.next(Collections.singletonMap("deviceId", 2));

        });

        ReactorQL.builder()
                 .sql(sql)
                 .build()
                 .start(data)
                 .doOnNext(System.out::println)
                 .log("testGroupTimeProperty")
                 .map(map -> map.get("deviceId"))
                 .take(4)
                 .as(StepVerifier::create)
                 .expectNextCount(4)
                 .verifyComplete();

    }

    @Test
    void testTraceInfo() {
        ReactorQL.builder()
                 .sql("select row.index rownum,row.elapsed elapsed from dual")
                 .build()
                 .start(Flux.range(0, 5))
                 .doOnNext(System.out::println)
                 .map(v -> v.get("rownum"))
                 .as(StepVerifier::create)
                 .expectNext(1L, 2L, 3L, 4L, 5L)
                 .verifyComplete()
        ;
    }


    @Test
    void testGroupTraceInfo() {
        ReactorQL.builder()
                 .sql("select row.index rownum,row.elapsed elapsed from dual group by _window(3),trace(),take(1)")
                 .build()
                 .start(Flux.range(0, 6))
                 .doOnNext(System.out::println)
                 .map(v -> v.get("rownum"))
                 .as(StepVerifier::create)
                 .expectNext(1L, 1L)
                 .verifyComplete();
    }

    @Test
    void testIfFunction() {
        String[] sql = {
                "   select ",
                "       val,",
                "       if(val<1,1,0) lt1ms,",
                "       if(val between 1 and 10,1,0) gt1ms,",
                "       if(val>=10,1,0) gt10ms",
                "   from dual ",
        };
        System.out.println(String.join("\n", sql));
        ReactorQL.builder()
                 .sql(sql)
                 .build()
                 .start(Flux.range(1, 20)
                            .map(i -> Collections.singletonMap("val", i)))
                 .doOnNext(System.out::println)
                 .as(StepVerifier::create)
                 .expectNextCount(20)
                 .verifyComplete();
    }

    @Test
    void testCollectList() {
        String[] sql = {
                "select collect_list(val) row from dual"
        };
        ReactorQL.builder()
                 .sql(sql)
                 .build()
                 .start(Flux.range(1, 20)
                            .map(i -> Collections.singletonMap("val", i)))
                 .doOnNext(System.out::println)
                 .as(StepVerifier::create)
                 .expectNextCount(1)
                 .verifyComplete();

    }

    @Test
    void testCollectListDistinct() {
        ReactorQL.builder()
                 .sql(
                         "select collect_list(distinct val) row from dual"
                 )
                 .build()
                 .start(Flux.just(1, 2, 2, 3, 3, 4)
                            .map(i -> Collections.singletonMap("val", i)))
                 .doOnNext(System.out::println)
                 .flatMapIterable(row -> CastUtils.castArray(row.get("row")))
                 .as(StepVerifier::create)
                 .expectNextCount(4)
                 .verifyComplete();
    }

    @Test
    void testCollectListUnique() {
        ReactorQL.builder()
                 .sql(
                         "select collect_list(unique val) row from dual"
                 )
                 .build()
                 .start(Flux.just(1, 2, 2, 3, 3, 4)
                            .map(i -> Collections.singletonMap("val", i)))
                 .doOnNext(System.out::println)
                 .flatMapIterable(row -> CastUtils.castArray(row.get("row")))
                 .as(StepVerifier::create)
                 .expectNextCount(2)
                 .verifyComplete();

    }

    @Test
    void testCollectRowMap() {
        String[] sql = {
                "select collect_row(name,val) $this from (",
                "select val,",
                "case",
                " when val<10 then '<10ms'",
                " when range(val,10,100) then '10ms-100ms'",
                " when val>100 then '>100ms'",
                " else '>100ms'",
                " end name",
                "from dual group by _window(10)",
                ") t "
        };
        System.out.println(String.join("\n", sql));
        ReactorQL.builder()
                 .sql(sql)
                 .build()
                 .start(Flux.range(0, 130).map(i -> Collections.singletonMap("val", i)))
                 .doOnNext(System.out::println)
                 .as(StepVerifier::create)
                 .expectNextCount(1)
                 .verifyComplete();
    }

    @Test
    void testArrayToRow() {
        String[] sql = {
                "select array_to_row(this.arr,'name','value') row from dual"
        };

        Map<String, Object> value1 = new HashMap<>();
        value1.put("name", "1");
        value1.put("value", 1);

        Map<String, Object> value2 = new HashMap<>();
        value2.put("name", "2");
        value2.put("value", 2);

        Map<String, Object> value3 = new HashMap<>();

        Map<String, Object> expect = new HashMap<>();
        expect.put("1", 1);
        expect.put("2", 2);


        ReactorQL.builder()
                 .sql(sql)
                 .build()
                 .start(Flux.just(Collections.singletonMap("arr", Arrays.asList(value1, value2, value3))))
                 .doOnNext(System.out::println)
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("row", expect))
                 .verifyComplete();
    }

    @Test
    void testFlatArray() {

        String sql = "select flat_array(this.arr) each from dual";

        ReactorQL.builder()
                 .sql(sql)
                 .build()
                 .start(Flux.just(Collections.singletonMap("arr", Arrays.asList(1, 2, 3))))
                 .doOnNext(System.out::println)
                 .map(map -> map.get("each"))
                 .as(StepVerifier::create)
                 .expectNext(1, 2, 3)
                 .verifyComplete();

        ReactorQL.builder()
                 .sql(sql)
                 .build()
                 .start(Flux.just(Collections.singletonMap("arr", Flux.just(1, 2, 3))))
                 .doOnNext(System.out::println)
                 .map(map -> map.get("each"))
                 .as(StepVerifier::create)
                 .expectNext(1, 2, 3)
                 .verifyComplete();

        ReactorQL.builder()
                 .sql(sql)
                 .build()
                 .start(Flux.just(Collections.singletonMap("arr", 1)))
                 .doOnNext(System.out::println)
                 .map(map -> map.get("each"))
                 .as(StepVerifier::create)
                 .expectNext(1)
                 .verifyComplete();
    }

    @Test
    void testMath() {
        String sql = String.join(" ", "select"
                , String.join(","
                        , "math.log(val) log"
                        , "math.log1p(val) log1p"
                        , "math.log10(val) log10"
                        , "math.exp(val) exp"
                        , "math.expm1(val) expm1"
                        , "math.rint(val) rint"
                        , "math.sin(val) sin"
                        , "math.asin(val) asin"
                        , "math.sinh(val) sinh"
                        , "math.cos(val) cos"
                        , "math.cosh(val) cosh"
                        , "math.acos(val) acos"
                        , "math.tan(val) tan"
                        , "math.tanh(val) tanh"
                        , "math.atan(val) atan"
                        , "math.ceil(val) ceil"
                        , "math.round(val) round"
                        , "math.floor(val) floor"
                        , "math.abs(val) abs"
                        , "math.degrees(val) degrees"
                        , "math.radians(val) radians"
                )
                , "from dual");

        ReactorQL.builder()
                 .sql(sql)
                 .build()
                 .start(Flux.just(Collections.singletonMap("val", 10)))
                 .doOnNext(System.out::println)
                 .as(StepVerifier::create)
                 .expectNextCount(1)
                 .verifyComplete();
    }

    @Test
    void testMathMax() {

        String sql = "select math.max(this.arr) max from dual";

        ReactorQL.builder()
                 .sql(sql)
                 .build()
                 .start(Flux.just(Collections.singletonMap("arr", Arrays.asList(1, 2, 3))))
                 .doOnNext(System.out::println)
                 .map(map -> map.get("max"))
                 .as(StepVerifier::create)
                 .expectNext(3)
                 .verifyComplete();
    }

    @Test
    void testFiledExtract() {
        String sql = "select deviceNum,this.payload.current.gpsSpeed as gpsSpeed from dual where this.payload.current.gpsSpeed>22";

        ReactorQL.builder()
                 .sql(sql)
                 .build()
                 .start(Flux.just(new HashMap<String, Object>() {
                     {
                         put("deviceNum", "12131221");
                         put("payload", new HashMap<String, Object>() {
                                 {
                                     put("previous", new HashMap<String, Object>() {{
                                         put("gpsSpeed", 22.3);
                                         put("oilTemperature", 34.5);
                                     }});
                                     put("current", new HashMap<String, Object>() {{
                                         put("gpsSpeed", 34.3);
                                         put("oilTemperature", 45.5);
                                     }});
                                 }
                             }
                         );
                     }
                 }))
                 .doOnNext(System.out::println)
                 .map(map -> map.get("gpsSpeed"))
                 .as(StepVerifier::create)
                 .expectNext(34.3)
                 .verifyComplete();
    }

    @Test
    void testCoalesce() {
        String sql = "select coalesce(this.name,'test') val from dual";
        ReactorQL.builder()
                 .sql(sql)
                 .build()
                 .start(Flux.just(1))
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("val", "test"))
                 .verifyComplete();

    }

    @Test
    void testFunctionIn() {
        {
            String sql = "select in(this,0,2,3,1) val from dual";
            ReactorQL.builder()
                     .sql(sql)
                     .build()
                     .start(Flux.just(1))
                     .as(StepVerifier::create)
                     .expectNext(Collections.singletonMap("val", true))
                     .verifyComplete();
        }

        {
            String sql = "select nin(this,0,2,3,1) val from dual";
            ReactorQL.builder()
                     .sql(sql)
                     .build()
                     .start(Flux.just(1))
                     .as(StepVerifier::create)
                     .expectNext(Collections.singletonMap("val", false))
                     .verifyComplete();
        }

        {

            String sql = "select in(arg,data) val from dual";
            ReactorQL.builder()
                     .sql(sql)
                     .build()
                     .start(Flux.just(new HashMap<String, Object>() {{
                         put("data", Arrays.asList(1, 2, 3));
                         put("arg", 2);
                     }}))
                     .as(StepVerifier::create)
                     .expectNext(Collections.singletonMap("val", true))
                     .verifyComplete();
        }

        {

            String sql = "select nin(arg,data) val from dual";
            ReactorQL.builder()
                     .sql(sql)
                     .build()
                     .start(Flux.just(new HashMap<String, Object>() {{
                         put("data", Arrays.asList(1, 2, 3));
                         put("arg", 2);
                     }}))
                     .as(StepVerifier::create)
                     .expectNext(Collections.singletonMap("val", false))
                     .verifyComplete();
        }

    }

    @Test
    void testFunctionBetween() {
        {
            String sql = "select btw(this,0,10) val from dual";
            ReactorQL.builder()
                     .sql(sql)
                     .build()
                     .start(Flux.just(4))
                     .as(StepVerifier::create)
                     .expectNext(Collections.singletonMap("val", true))
                     .verifyComplete();
        }

        {
            String sql = "select nbtw(this,0,10) val from dual";
            ReactorQL.builder()
                     .sql(sql)
                     .build()
                     .start(Flux.just(5))
                     .as(StepVerifier::create)
                     .expectNext(Collections.singletonMap("val", false))
                     .verifyComplete();
        }

        {

            String sql = "select btw(arg,data) val from dual";
            ReactorQL.builder()
                     .sql(sql)
                     .build()
                     .start(Flux.just(new HashMap<String, Object>() {{
                         put("data", Arrays.asList(1, 3));
                         put("arg", 2);
                     }}))
                     .as(StepVerifier::create)
                     .expectNext(Collections.singletonMap("val", true))
                     .verifyComplete();
        }

        {

            String sql = "select nbtw(arg,data) val from dual";
            ReactorQL.builder()
                     .sql(sql)
                     .build()
                     .start(Flux.just(new HashMap<String, Object>() {{
                         put("data", Arrays.asList(1, 2, 3));
                         put("arg", 2);
                     }}))
                     .as(StepVerifier::create)
                     .expectNext(Collections.singletonMap("val", false))
                     .verifyComplete();
        }

    }

    @Test
    void testStr() {

        BiConsumer<String, String> substrTest = (func, expect) -> {
            String sql = "select " + func + " val from dual";
            ReactorQL.builder()
                     .sql(sql)
                     .build()
                     .start(Flux.just(1))
                     .as(StepVerifier::create)
                     .expectNext(Collections.singletonMap("val", expect))
                     .verifyComplete();
        };
        substrTest.accept("substr('abc',1)", "bc");
        substrTest.accept("substr('abc',0,1)", "a");
        substrTest.accept("substr('abc',-1)", "c");

        substrTest.accept("substr('abc',0,10)", "abc");

        substrTest.accept("substr('abc',10)", "");
        substrTest.accept("substr('abc',-10)", "");

    }

    @Test
    void testNullFunction() {
        ReactorQL.builder()
                 .sql("select isnull(this.name) nameNull from dual")
                 .build()
                 .start(Flux.just(Collections.emptyMap()))
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("nameNull", true))
                 .verifyComplete();

        ReactorQL.builder()
                 .sql("select notnull(this.name) nameNotnull from dual")
                 .build()
                 .start(Flux.just(Collections.singletonMap("name", 1)))
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("nameNotnull", true))
                 .verifyComplete();
    }

    @Test
    void testAllMatch() {
        ReactorQL.builder()
                 .sql("select all_match(1,this.bool) allMatch from dual")
                 .build()
                 .start(Flux.just(Collections.emptyMap()))
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("allMatch", false))
                 .verifyComplete();

        ReactorQL.builder()
                 .sql("select all_match(1,this.bool) allMatch from dual")
                 .build()
                 .start(Flux.just(Collections.singletonMap("bool", true)))
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("allMatch", true))
                 .verifyComplete();

    }

    @Test
    void testAnyMatch() {

        ReactorQL.builder()
                 .sql("select any_match(this.bool) anyMatch from dual")
                 .build()
                 .start(Flux.just(Collections.emptyMap()))
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("anyMatch", false))
                 .verifyComplete();

        ReactorQL.builder()
                 .sql("select any_match(this.bool,1) anyMatch from dual")
                 .build()
                 .start(Flux.just(Collections.emptyMap()))
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("anyMatch", true))
                 .verifyComplete();

        ReactorQL.builder()
                 .sql("select any_match(0,this.bool) anyMatch from dual")
                 .build()
                 .start(Flux.just(Collections.singletonMap("bool", false)))
                 .as(StepVerifier::create)
                 .expectNext(Collections.singletonMap("anyMatch", false))
                 .verifyComplete();

    }

    @Test
    void testTime() {
        LocalDateTime now = LocalDateTime.now();
        Map<String, Object> data = new LinkedHashMap<>();

        data.put("year", now.getYear());
        data.put("month", now.getMonthValue());
        data.put("day", now.getDayOfMonth());
        data.put("day2", now.getDayOfYear());
        data.put("week", now.getDayOfWeek().getValue());
        data.put("hour", now.getHour());
        data.put("minute", now.getMinute());
        data.put("second", now.getSecond());


        ReactorQL.builder()
                 .sql("select ",
                      "year(now) `year`,",
                      "month(now) `month`,",
                      "day_of_month(now) `day`,",
                      "day_of_year(now) `day2`,",
                      "day_of_week(now) `week`,",
                      "hour(now) `hour`,",
                      "minute(now) `minute`,",
                      "second(now) `second`",
                      " from `dual`")
                 .build()
                 .start(Flux.just(Collections.singletonMap("now", now)))
                 .doOnNext(System.out::println)
                 .as(StepVerifier::create)
                 .expectNext(data)
                 .verifyComplete();
    }

    @Test
    void testChoose() {
        ReactorQL
                .builder()
                .sql("select choose(null) v from `dual`")
                .build()
                .start(Flux.just(Collections.emptyMap()))
                .as(StepVerifier::create)
                .expectNext(Collections.emptyMap())
                .verifyComplete();

        ReactorQL
                .builder()
                .sql("select choose(0) v from `dual`")
                .build()
                .start(Flux.just(Collections.emptyMap()))
                .as(StepVerifier::create)
                .expectNext(Collections.emptyMap())
                .verifyComplete();

        ReactorQL
                .builder()
                .sql("select choose(2) v from `dual`")
                .build()
                .start(Flux.just(Collections.emptyMap()))
                .as(StepVerifier::create)
                .expectNext(Collections.emptyMap())
                .verifyComplete();

        ReactorQL
                .builder()
                .sql("select choose(2,1,'2',3,4,5) v from `dual`")
                .build()
                .start(Flux.just(Collections.emptyMap()))
                .as(StepVerifier::create)
                .expectNext(Collections.singletonMap("v", "2"))
                .verifyComplete();
    }

    @Test
    void testSelectBoolean() {
        ReactorQL
                .builder()
                .sql("select new_map('bool',true) v from `dual`")
                .build()
                .start(Flux.just(Collections.emptyMap()))
                .doOnNext(System.out::println)
                .as(StepVerifier::create)
                .expectNext(Collections.singletonMap("v", Collections.singletonMap("bool", true)))
                .verifyComplete();

        ReactorQL
                .builder()
                .sql("select new_map('bool',false) v from `dual`")
                .build()
                .start(Flux.just(Collections.emptyMap()))
                .doOnNext(System.out::println)
                .as(StepVerifier::create)
                .expectNext(Collections.singletonMap("v", Collections.singletonMap("bool", false)))
                .verifyComplete();
    }

    @Test
    void testErrorMatch() {
        Map<String, Object> data = new HashMap<>();
        data.put("left", 1);
        data.put("right", "");
        ReactorQL
                .builder()
                .sql("select 1 from dual where left>right")
                .build()
                .start(Flux.range(0,100_0000).map(ignore->data))
                .doOnNext(System.out::println)
                .as(StepVerifier::create)
                .expectNextCount(0)
                .verifyComplete();
    }
}