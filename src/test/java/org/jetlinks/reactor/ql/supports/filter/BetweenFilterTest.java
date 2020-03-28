package org.jetlinks.reactor.ql.supports.filter;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

class BetweenFilterTest {

    @Test
    void test() {
        BetweenFilter filter = new BetweenFilter();

        assertFalse(filter.predicate(null, 1, 2));

        assertTrue(filter.predicate(1, 1, 2));

        assertTrue(filter.predicate("2","1","3"));

        assertTrue(filter.predicate(new Date(System.currentTimeMillis()-1000),new Date(System.currentTimeMillis()-500),new Date(System.currentTimeMillis()-2000)));

    }

}