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

import org.jetlinks.reactor.ql.ReactorQL;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

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

    @Test
    void literalLikeAndNotLikeMustMatchExistingRegexSemantics() {
        List<String> inputs = Arrays.asList(
                null, "", "ab", "abc", "xbc", "abx", "a_b", "a.b", "aXb", "a.bc",
                "ab\nc", "ab\rc", "ab\u0085c", "ab\u2028c", "ab\u2029c",
                "\nabc", "abc\n", "a\nbc", "你好", "x你好x");
        List<String> patterns = Arrays.asList(
                "", "ab", "ab%", "%bc", "%b%", "%", "a_b", "a.b%",
                "ab.*", "a%bc", "a%%", "^(ab|ac)%", "你好%", "%好%");

        assertRegexSemantics(inputs, patterns);
    }

    @Test
    void matchedLiteralRegionsKeepUnicodeAndLineTerminatorSemantics() {
        String literal = "设备-very-long-literal-section";
        List<String> inputs = new ArrayList<>(Arrays.asList(
                null, "", literal, literal + "tail", "head" + literal,
                "head" + literal + "tail", literal + literal, "😀" + literal + "😀",
                "line\ntail", "headline\n", "headline\ntail"));
        for (String terminator : Arrays.asList("\n", "\r", "\u0085", "\u2028", "\u2029", "\r\n")) {
            inputs.add(terminator + literal);
            inputs.add(literal + terminator);
            inputs.add("head" + terminator + literal + "tail");
            inputs.add("head" + literal + terminator + "tail");
            inputs.add(literal + terminator + literal);
            inputs.add(terminator + literal + terminator);
        }
        assertRegexSemantics(inputs, Arrays.asList(literal + "%", "%" + literal,
                "%" + literal + "%", "%", "%%", "line\n%", "%line\n", "%line\n%"));
    }

    private static void assertRegexSemantics(List<String> inputs, List<String> patterns) {

        for (String pattern : patterns) {
            Pattern regex = Pattern.compile(pattern.replace("%", ".*"));
            for (boolean not : new boolean[]{false, true}) {
                ReactorQL query = ReactorQL.builder()
                                           .sql("select text matched_text from test where text "
                                                   + (not ? "not like " : "like ")
                                                   + "'" + pattern + "'")
                                           .build();
                List<String> expected = new ArrayList<>();
                for (String input : inputs) {
                    if (input != null && (regex.matcher(input).matches() != not)) {
                        expected.add(input);
                    }
                }
                List<String> actual = query.start(Flux.fromIterable(rows(inputs)))
                                           .map(row -> (String) row.get("matched_text"))
                                           .collectList()
                                           .block();
                assertEquals(expected, actual, "pattern=" + pattern + ", not=" + not);
            }
        }
    }

    @Test
    void dynamicLikePatternMustRemainRowSpecific() {
        List<Map<String, Object>> rows = new ArrayList<>();
        rows.add(row("abc", "ab%"));
        rows.add(row("xbc", "%bc"));
        rows.add(row("aXb", "a.b"));
        rows.add(row("ab\nc", "ab%"));
        rows.add(row("abc", null));
        rows.add(row(null, "%"));

        ReactorQL query = ReactorQL.builder()
                                   .sql("select text matched_text from test where text like pattern")
                                   .build();
        List<String> actual = query.start(Flux.fromIterable(rows))
                                   .map(result -> (String) result.get("matched_text"))
                                   .collectList()
                                   .block();
        assertEquals(Arrays.asList("abc", "xbc", "aXb"), actual);
    }

    private static List<Map<String, Object>> rows(List<String> inputs) {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (String input : inputs) {
            rows.add(row(input, null));
        }
        return rows;
    }

    private static Map<String, Object> row(String text, String pattern) {
        Map<String, Object> row = new HashMap<>();
        row.put("text", text);
        row.put("pattern", pattern);
        return row;
    }
}
