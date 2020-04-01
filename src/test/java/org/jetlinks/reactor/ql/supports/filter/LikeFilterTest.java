package org.jetlinks.reactor.ql.supports.filter;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LikeFilterTest {


    @Test
    void testLike() {
        LikeFilter filter = new LikeFilter();

        assertTrue(filter.doTest(false,"abc", "%bc"));

        assertTrue(filter.doTest(false,"abc", "ab%"));

        assertTrue(filter.doTest(false,12345, "123%"));

        assertTrue(filter.doTest(false,12345, "1%5"));
    }

    @Test
    void testNotLike() {
        LikeFilter filter = new LikeFilter();

        assertFalse(filter.doTest(true,"abc", "%bc"));

        assertFalse(filter.doTest(true,"abc", "ab%"));

        assertFalse(filter.doTest(true,12345, "123%"));

        assertFalse(filter.doTest(true,12345, "1%5"));
    }
}