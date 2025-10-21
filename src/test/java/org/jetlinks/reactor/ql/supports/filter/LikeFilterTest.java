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

    @Test
    void testChinese() {
        LikeFilter filter = new LikeFilter();

        assertFalse(filter.doTest(true,"你好", "%好"));

        assertFalse(filter.doTest(true,"你好", "你%"));

        assertFalse(filter.doTest(true,"你好", "你好"));

    }
}