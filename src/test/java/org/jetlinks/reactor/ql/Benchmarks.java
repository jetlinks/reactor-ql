package org.jetlinks.reactor.ql;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Collections;

public class Benchmarks {


    @Test
    void testCount() {
        System.out.println(ReactorQL.builder()
                .sql("select count(1) total from t")
                .build()
                .start(Flux.range(0, 1000000))
                .doOnNext(System.out::println)
                .as(StepVerifier::create)
                .expectNext(Collections.singletonMap("total", 1000000L))
                .verifyComplete());
    }

    @Test
    void testWhere() {
        System.out.println(ReactorQL.builder()
                .sql("select count(1) total from t where this >= 0 and this < 1000000")
                .build()
                .start(Flux.range(0, 1000000))
                .as(StepVerifier::create)
                .expectNext(Collections.singletonMap("total", 1000000L))
                .verifyComplete());
    }
}
