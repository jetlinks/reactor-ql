/*
 * Copyright 2025 JetLinks https://www.jetlinks.cn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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