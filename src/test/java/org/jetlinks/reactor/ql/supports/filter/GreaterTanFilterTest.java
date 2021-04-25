package org.jetlinks.reactor.ql.supports.filter;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class GreaterTanFilterTest {

    @Test
    void test() {
        GreaterTanFilter filter = new GreaterTanFilter(">");

        assertFalse(filter.test(1, 1));
        assertFalse(filter.test(1, Arrays.asList(1, 2)));

        assertTrue(filter.test(3, 2));

        assertTrue(filter.test(3, "2"));
        assertTrue(filter.test("3", "2"));

        assertTrue(filter.test('3', '2'));

        assertTrue(filter.test(System.currentTimeMillis() + 1000, LocalDateTime.now()));

    }

}