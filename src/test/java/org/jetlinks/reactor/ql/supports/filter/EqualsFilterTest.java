package org.jetlinks.reactor.ql.supports.filter;

import org.junit.jupiter.api.Test;

import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

class EqualsFilterTest {

    @Test
    void test() {

        EqualsFilter filter = new EqualsFilter("=", false);

        assertTrue(filter.doPredicate("1", "1"));
        assertTrue(filter.doPredicate("1", 1));
        assertTrue(filter.doPredicate(1, 1));
        assertTrue(filter.doPredicate(1L, 1D));
        assertTrue(filter.doPredicate(1L, "1"));
        assertTrue(filter.doPredicate(1F, "1.0E0"));

        long now = System.currentTimeMillis();

        assertTrue(filter.doPredicate(now, new Date(now)));
        assertTrue(filter.doPredicate(new Date(now), now));


        assertFalse(filter.doPredicate("1", "1D"));


    }

    @Test
    void testNot() {

        EqualsFilter filter = new EqualsFilter("!=", true);

        assertFalse(filter.doPredicate("1", "1"));
        assertFalse(filter.doPredicate("1", 1));
        assertFalse(filter.doPredicate(1, 1));
        assertFalse(filter.doPredicate(1L, 1D));
        assertFalse(filter.doPredicate(1L, "1"));
        assertFalse(filter.doPredicate(1F, "1.0E0"));

        long now = System.currentTimeMillis();

        assertFalse(filter.doPredicate(now, new Date(now)));
        assertFalse(filter.doPredicate(new Date(now), now));


        assertTrue(filter.doPredicate("1", "1D"));


    }

}