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
package org.jetlinks.reactor.ql;

import org.jetlinks.reactor.ql.exception.ReactorQLException;
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;

class ReactorQLExceptionTest {

    @Test
    void testUnsupportedExpressionDiagnostic() {
        ReactorQLException error = Assertions.assertThrows(ReactorQLException.class, () -> ReactorQL
                .builder()
                .sql("select missing_func(value) v from test")
                .build()
                .start(Flux.just(1))
                .blockLast());

        Assertions.assertEquals(ReactorQLException.UNSUPPORTED_EXPRESSION, error.getI18nCode());
        Assertions.assertTrue(error.getLine() == null || error.getLine() > 0);
        Assertions.assertTrue(error.getColumn() == null || error.getColumn() > 0);
        Assertions.assertEquals("missing_func(value)", error.getExpression());
        Assertions.assertTrue(error.getMessage().contains("Reason:"));
        Assertions.assertTrue(error.getMessage().contains("Suggestion:"));
        Assertions.assertTrue(error.getMessage().contains("Example:"));
        Assertions.assertEquals(7, error.getI18nArgs().length);
        assertPublicSuggestion(error);
    }

    @Test
    void testFunctionArgumentDiagnostic() {
        ReactorQLException error = Assertions.assertThrows(ReactorQLException.class, () -> ReactorQL
                .builder()
                .sql("select substring('abc') v from dual")
                .build()
                .start(Flux.just(1))
                .blockLast());

        Assertions.assertEquals(ReactorQLException.FUNCTION_ARGUMENT_COUNT, error.getI18nCode());
        Assertions.assertTrue(error.getReason().contains("期望 2..3"));
        Assertions.assertTrue(error.getMessage().contains("substring('abc')"));
        Assertions.assertTrue(error instanceof UnsupportedOperationException);
    }

    @Test
    void testSyntaxDiagnosticAndLocalizedMessage() {
        ReactorQLException error = Assertions.assertThrows(ReactorQLException.class,
                                                           () -> new DefaultReactorQLMetadata("select from"));

        Assertions.assertEquals(ReactorQLException.SYNTAX_ERROR, error.getI18nCode());
        Assertions.assertNotNull(error.getReason());
        Locale old = Locale.getDefault();
        try {
            Locale.setDefault(Locale.ENGLISH);
            Assertions.assertTrue(error.getLocalizedMessage().contains("ReactorQL query error"));
            Assertions.assertTrue(error.getLocalizedMessage().contains("SQL parse failed"));
            Locale.setDefault(Locale.SIMPLIFIED_CHINESE);
            Assertions.assertTrue(error.getLocalizedMessage().contains("ReactorQL查询错误"));
            Assertions.assertTrue(error.getLocalizedMessage().contains("SQL解析失败"));
        } finally {
            Locale.setDefault(old);
        }
    }

    @Test
    void testLexicalSyntaxDiagnosticKeepsPosition() {
        ReactorQLException error = Assertions.assertThrows(ReactorQLException.class,
                                                           () -> new DefaultReactorQLMetadata("select * from t where name = '"));

        Assertions.assertEquals(ReactorQLException.SYNTAX_ERROR, error.getI18nCode());
        Assertions.assertEquals(1, error.getLine());
        Assertions.assertEquals(31, error.getColumn());
    }

    @Test
    void testNonSelectStatementKeepsSelectOnlyCompatibility() {
        Assertions.assertThrows(ClassCastException.class, () -> new DefaultReactorQLMetadata("delete from t"));
    }

    @Test
    void testNestedStructuredDiagnosticIsNotWrapped() {
        ReactorQLException error = Assertions.assertThrows(ReactorQLException.class, () -> ReactorQL
                .builder()
                .sql("select date_format(missing_func(value), 'yyyy-MM-dd') v from dual")
                .build());

        Assertions.assertEquals(ReactorQLException.UNSUPPORTED_EXPRESSION, error.getI18nCode());
        Assertions.assertEquals("missing_func(value)", error.getExpression());
    }

    @Test
    void testJsonUnsafePathDiagnostic() {
        ReactorQLException error = Assertions.assertThrows(ReactorQLException.class, () -> ReactorQL
                .builder()
                .sql("select json_get(this, '$..*') v from dual")
                .build());

        Assertions.assertEquals(ReactorQLException.INVALID_ARGUMENT, error.getI18nCode());
        Assertions.assertTrue(error.getReason().contains("JSONPath"));
        assertPublicSuggestion(error);

        Map<String, Object> row = new HashMap<>();
        row.put("path", "$[?(@.a)]");
        ReactorQL
                .builder()
                .sql("select json_get(this, path) v from dual")
                .build()
                .start(Flux.just(row))
                .as(StepVerifier::create)
                .expectErrorMatches(err -> err instanceof ReactorQLException
                        && ((ReactorQLException) err).getI18nCode().equals(ReactorQLException.INVALID_ARGUMENT)
                        && ((ReactorQLException) err).getReason().contains("JSONPath"))
                .verify();
    }

    @Test
    void testOrderByResourceLimitDiagnostic() {
        ReactorQL
                .builder()
                .sql("select this val from test order by this")
                .setting(DefaultReactorQL.SETTING_ORDER_BY_MAX_ROWS, 1)
                .build()
                .start(Flux.just(2, 1))
                .as(StepVerifier::create)
                .expectErrorMatches(err -> err instanceof ReactorQLException
                        && ((ReactorQLException) err).getI18nCode().equals(ReactorQLException.RESOURCE_LIMIT)
                        && ((ReactorQLException) err).getSuggestion().contains("LIMIT"))
                .verify();
    }

    @Test
    void testDateFormatAndWindowArgumentDiagnostic() {
        ReactorQLException dateError = Assertions.assertThrows(ReactorQLException.class, () -> ReactorQL
                .builder()
                .sql("select format_datetime(this, 'yyyy-MM-dd ii') v from dual")
                .build());
        Assertions.assertEquals(ReactorQLException.INVALID_ARGUMENT, dateError.getI18nCode());
        Assertions.assertTrue(dateError.getSuggestion().contains("yyyy-MM-dd HH:mm:ss"));
        assertPublicSuggestion(dateError);

        ReactorQLException windowError = Assertions.assertThrows(ReactorQLException.class, () -> ReactorQL
                .builder()
                .sql("select count(1) total from test group by _window(0)")
                .build());
        Assertions.assertEquals(ReactorQLException.INVALID_ARGUMENT, windowError.getI18nCode());
        Assertions.assertTrue(windowError.getSuggestion().contains("_window"));
        assertPublicSuggestion(windowError);
    }

    @Test
    void testI18nResourceKeysExist() {
        ResourceBundle zh = ResourceBundle.getBundle("i18n.reactorql.messages", Locale.SIMPLIFIED_CHINESE);
        ResourceBundle en = ResourceBundle.getBundle("i18n.reactorql.messages", Locale.ENGLISH);
        String[] keys = {
                ReactorQLException.SYNTAX_ERROR,
                ReactorQLException.UNSUPPORTED_EXPRESSION,
                ReactorQLException.UNSUPPORTED_CONDITION,
                ReactorQLException.UNSUPPORTED_GROUP_EXPRESSION,
                ReactorQLException.UNSUPPORTED_FROM,
                ReactorQLException.UNSUPPORTED_FLAT_MAP,
                ReactorQLException.FUNCTION_ARGUMENT_COUNT,
                ReactorQLException.INVALID_ARGUMENT,
                ReactorQLException.RESOURCE_LIMIT
        };
        for (String key : keys) {
            Assertions.assertTrue(zh.containsKey(key), key);
            Assertions.assertTrue(en.containsKey(key), key);
        }
    }

    @Test
    void testFactoryDiagnosticsUsePublicSuggestions() {
        ReactorQLException condition = ReactorQLException.unsupportedCondition(
                "a ~= b",
                "where 条件请使用 =、<>、>、>=、<、<=、between、in、like、and、or 或已支持的布尔函数。",
                "select * from test where value >= 10"
        );
        Assertions.assertEquals(ReactorQLException.UNSUPPORTED_CONDITION, condition.getI18nCode());
        assertPublicSuggestion(condition);

        ReactorQLException group = ReactorQLException.unsupportedGroupExpression("payload->'id'");
        Assertions.assertEquals(ReactorQLException.UNSUPPORTED_GROUP_EXPRESSION, group.getI18nCode());
        assertPublicSuggestion(group);

        ReactorQLException from = ReactorQLException.unsupportedFrom("call_func()");
        Assertions.assertEquals(ReactorQLException.UNSUPPORTED_FROM, from.getI18nCode());
        assertPublicSuggestion(from);

        ReactorQLException flatMap = ReactorQLException.unsupportedFlatMap("lower(name)");
        Assertions.assertEquals(ReactorQLException.UNSUPPORTED_FLAT_MAP, flatMap.getI18nCode());
        assertPublicSuggestion(flatMap);
    }

    @Test
    void testSettingDiagnostics() {
        ReactorQLException jsonSettingError = Assertions.assertThrows(ReactorQLException.class, () -> ReactorQL
                .builder()
                .sql("select json_get(this, '$.id') v from dual")
                .setting("function.json.maxPathLength", "bad")
                .build());
        Assertions.assertEquals(ReactorQLException.INVALID_ARGUMENT, jsonSettingError.getI18nCode());
        Assertions.assertTrue(jsonSettingError.getReason().contains("JSON setting"));
        assertPublicSuggestion(jsonSettingError);

        ReactorQLException orderSettingError = Assertions.assertThrows(ReactorQLException.class, () -> ReactorQL
                .builder()
                .sql("select this v from dual order by this")
                .setting(DefaultReactorQL.SETTING_ORDER_BY_MAX_ROWS, "bad")
                .build());
        Assertions.assertEquals(ReactorQLException.INVALID_ARGUMENT, orderSettingError.getI18nCode());
        Assertions.assertTrue(orderSettingError.getReason().contains("ORDER BY setting"));
        assertPublicSuggestion(orderSettingError);
    }

    private void assertPublicSuggestion(ReactorQLException error) {
        String suggestion = error.getSuggestion();
        Assertions.assertNotNull(suggestion);
        Assertions.assertFalse(suggestion.contains("Feature"), suggestion);
        Assertions.assertFalse(suggestion.contains("metadata"), suggestion);
        Assertions.assertFalse(suggestion.contains("AST"), suggestion);
        Assertions.assertFalse(suggestion.contains("Token"), suggestion);
        Assertions.assertFalse(suggestion.contains("内部"), suggestion);
    }
}
