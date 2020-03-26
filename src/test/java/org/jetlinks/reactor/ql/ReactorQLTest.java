package org.jetlinks.reactor.ql;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Collections;

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
                .source(Flux.range(0,100))
                .start()
                .as(StepVerifier::create)
                .expectNext(Collections.singletonMap("total",100L))
                .verifyComplete();

    }

    @Test
    void testSimpleMap() {

        ReactorQL.builder()
                .sql("select _name name from test")
                .build()
                .source(Flux.just(Collections.singletonMap("_name","test")))
                .start()
                .as(StepVerifier::create)
                .expectNext(Collections.singletonMap("name","test"))
                .verifyComplete();

    }


}