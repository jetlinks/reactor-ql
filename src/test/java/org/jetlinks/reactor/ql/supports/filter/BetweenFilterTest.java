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