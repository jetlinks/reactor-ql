package org.jetlinks.reactor.ql.examples;

import org.jetlinks.reactor.ql.ReactorQL;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.function.Function;

class GroupByWindowTest {

    @Test
    void testGroupByTimeWindow() {
        //时间窗口
        //场景: 统计每500ms温度传感器的实时平均温度

        //每200毫秒输入一条数据, 1,2,3,4,5,6
        //每500ms收集为一组数据
        // 1,2
        // 3,4
        // 4,6

        ReactorQL.builder()
                .sql("select avg(this) total from test group by _window('500ms')")
                .build()
                .start(Flux.just(1, 2, 3, 4, 5, 6).delayElements(Duration.ofMillis(200)))
                .doOnNext(System.out::println)
                .map(map -> map.get("total"))
                .as(StepVerifier::create)
                .expectNext(1.5D, 3.5D, 5.5D)
                .verifyComplete();
    }




    @Test
    void testGroupByTimeoutWindow() {
        //时间窗口
        //场景: 统计每3条并且300ms之内的数据的合计值

        //连续输入4条数据后每间隔200ms输入一个数据
        //输入 1,2,3,4,..5,..6,..7,..8
        // 1,2,3
        // 4,5,6
        // 7,8

        ReactorQL.builder()
                .sql("select sum(this) total from test group by _window(3,'300ms')")
                .build()
                .start(Flux.concat(Flux.just(1,2,3,4),Flux.just(5,6,7,8).delayElements(Duration.ofMillis(100))))
                .doOnNext(System.out::println)
                .map(map -> map.get("total"))
                .as(StepVerifier::create)
                .expectNext(6D,15D,15D)
                .verifyComplete();
    }

    @Test
    void testGroupByTimeSlidingWindow() {
        //时间滑动窗口
        //场景: 统计400ms内,每200ms收集的平均值

        //每200毫秒输入一条数据, 1,2,3,4,5,6
        // [[1],[1,2],[2,3],[3,4],[4,5],[5,6],[6]]

        ReactorQL.builder()
                .sql("select max(this) max, min(this) min, sum(this)total from test group by _window('400ms','200ms')")
                .build()
                .start(Flux.just(1, 2, 3, 4, 5, 6).delayElements(Duration.ofMillis(200)))
                .doOnNext(System.out::println)
                .map(map -> map.get("total"))
                .as(StepVerifier::create)
                .expectNextCount(7)
                .verifyComplete();
    }

    @Test
    void testGroupByWindow() {
        //数量窗口
        //场景: 每3条数据统计总数
        //输入 1,2,3,4,5,6
        //分组为 [[1,2,3],[4,5,6]]
        ReactorQL.builder()
                .sql("select sum(this) total from test group by _window(3)")
                .build()
                .start(Flux.just(1, 2, 3, 4, 5, 6))
                .doOnNext(System.out::println)
                .map(map -> map.get("total"))
                .as(StepVerifier::create)
                .expectNext(
                        6D, // 1,2,3
                        15D // 4,5,6
                        )
                .verifyComplete();

    }
    @Test
    void testGroupBySlidingWindow() {
        //滑动窗口
        //场景: 每3条滑动1条数据统计总数
        //输入 1,2,3,4,5,6
        //分组为: [[1,2,3],[2,3,4],[3,4,5],[4,5,6],[5,6],[6]]

        ReactorQL.builder()
                .sql("select sum(this) total from test group by _window(3,1)")
                .build()
                .start(Flux.just(1, 2, 3, 4, 5, 6))
                .doOnNext(System.out::println)
                .map(map -> map.get("total"))
                .as(StepVerifier::create)
                .expectNext(
                        6D, // 1,2,3
                        9D, // 2,3,4
                        12D,// 3,4,5
                        15D,// 4,5,6
                        11D,// 5,6
                        6D) // 6
                .verifyComplete();

    }



    @Test
    void testGroupByTimeIllegalParameter() {
        //错误的分组参数

        try {
            ReactorQL.builder()
                    .sql("select avg(this) total from test group by _window()")
                    .build();
            Assertions.fail("error");
        }catch (UnsupportedOperationException ignore){}
        try {
            ReactorQL.builder()
                    .sql("select avg(this) total from test group by _window(eq(this,1))")
                    .build();
            Assertions.fail("error");
        }catch (UnsupportedOperationException ignore){}

        try {
            ReactorQL.builder()
                    .sql("select avg(this) total from test group by _window(1,2,3)")
                    .build();
            Assertions.fail("error");
        }catch (UnsupportedOperationException ignore){}

        try {
            ReactorQL.builder()
                    .sql("select avg(this) total from test group by _window(0)")
                    .build();
            Assertions.fail("error");
        }catch (UnsupportedOperationException ignore){}
        try {
            ReactorQL.builder()
                    .sql("select avg(this) total from test group by _window('0s')")
                    .build();
            Assertions.fail("error");
        }catch (UnsupportedOperationException ignore){}
        try {
            ReactorQL.builder()
                    .sql("select avg(this) total from test group by _window(0,'0s')")
                    .build();
            Assertions.fail("error");
        }catch (UnsupportedOperationException ignore){}


        try {
            ReactorQL.builder()
                    .sql("select avg(this) total from test group by _window(10,'0s')")
                    .build();
            Assertions.fail("error");
        }catch (UnsupportedOperationException ignore){}


    }
}
