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
package org.jetlinks.reactor.ql.exception;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.ASTNodeAccess;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.parser.SimpleNode;
import net.sf.jsqlparser.parser.Token;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

/**
 * ReactorQL 查询诊断异常。
 *
 * <p>该异常保留 {@link UnsupportedOperationException} 兼容性，同时携带稳定错误码、表达式位置、原因、建议和示例。
 * 上层平台可通过 {@link #getI18nCode()} 与 {@link #getI18nArgs()} 做国际化渲染。</p>
 */
public class ReactorQLException extends UnsupportedOperationException {

    private static final long serialVersionUID = -5983596320400609489L;

    private static final String BUNDLE_NAME = "i18n.reactorql.messages";
    private static final String MESSAGE_TEMPLATE_KEY = "reactorql.error.format";
    private static final String DEFAULT_TEMPLATE = "ReactorQL query error [{0}]\n"
            + "Location: line {1}, column {2}\n"
            + "Expression: {3}\n"
            + "Reason: {4}\n"
            + "Suggestion: {5}\n"
            + "Example: {6}";

    public static final String SYNTAX_ERROR = "error.reactorql.syntax";
    public static final String UNSUPPORTED_EXPRESSION = "error.reactorql.unsupported_expression";
    public static final String UNSUPPORTED_CONDITION = "error.reactorql.unsupported_condition";
    public static final String UNSUPPORTED_GROUP_EXPRESSION = "error.reactorql.unsupported_group_expression";
    public static final String UNSUPPORTED_FROM = "error.reactorql.unsupported_from";
    public static final String UNSUPPORTED_FLAT_MAP = "error.reactorql.unsupported_flat_map";
    public static final String FUNCTION_ARGUMENT_COUNT = "error.reactorql.function_argument_count";
    public static final String INVALID_ARGUMENT = "error.reactorql.invalid_argument";
    public static final String RESOURCE_LIMIT = "error.reactorql.resource_limit";

    private final String i18nCode;
    private final Object[] i18nArgs;
    private final Integer line;
    private final Integer column;
    private final String expression;
    private final String reason;
    private final String suggestion;
    private final String example;

    private ReactorQLException(Builder builder) {
        super(format(DEFAULT_TEMPLATE, builder.toMessageArgs()), builder.cause);
        this.i18nCode = builder.i18nCode;
        this.i18nArgs = builder.toMessageArgs();
        this.line = builder.line;
        this.column = builder.column;
        this.expression = builder.expression;
        this.reason = builder.reason;
        this.suggestion = builder.suggestion;
        this.example = builder.example;
    }

    public static ReactorQLException syntax(String sql, Throwable cause) {
        Builder builder = builder(SYNTAX_ERROR)
                .expression(sql)
                .reason("SQL 语法解析失败: " + rootMessage(cause))
                .suggestion("检查 SQL 关键字、函数参数、括号和逗号是否完整；如果使用非 ASCII 别名，请使用 as \"别名\"。")
                .example("select count(1) total from test where value > 0")
                .cause(cause);
        fillParserPosition(builder, cause);
        return builder.build();
    }

    public static ReactorQLException unsupportedExpression(Object expression, String suggestion, String example) {
        return builder(UNSUPPORTED_EXPRESSION)
                .expression(expression)
                .reason("当前 SQL 表达式不在支持范围内")
                .suggestion(suggestion)
                .example(example)
                .build();
    }

    public static ReactorQLException unsupportedCondition(Object expression, String suggestion, String example) {
        return builder(UNSUPPORTED_CONDITION)
                .expression(expression)
                .reason("当前条件表达式未匹配到可执行的过滤器或布尔值函数")
                .suggestion(suggestion)
                .example(example)
                .build();
    }

    public static ReactorQLException unsupportedGroupExpression(Object expression) {
        return builder(UNSUPPORTED_GROUP_EXPRESSION)
                .expression(expression)
                .reason("group by 仅支持列、常用分组函数、常用行级函数或二元计算表达式")
                .suggestion("如需按计算结果分组，可使用 timestamp - timestamp % 60000、date_trunc('minute', timestamp) 或 time_bucket('1m', timestamp)。")
                .example("select count(1) total from test group by date_trunc('minute', timestamp)")
                .build();
    }

    public static ReactorQLException functionArgumentCount(Object expression, int min, int max, int actual) {
        String expected = min == max ? String.valueOf(min) : min + ".." + max;
        return builder(FUNCTION_ARGUMENT_COUNT)
                .expression(expression)
                .reason("函数参数数量不匹配，期望 " + expected + " 个，实际 " + actual + " 个")
                .suggestion("检查函数签名，补齐必需参数或删除多余参数；参数为 NULL 时仍会参与数量校验。")
                .example("select substring(name, 1, 3) part from test")
                .build();
    }

    public static ReactorQLException invalidArgument(String reason, String suggestion, String example) {
        return builder(INVALID_ARGUMENT)
                .reason(reason)
                .suggestion(suggestion)
                .example(example)
                .build();
    }

    public static ReactorQLException invalidArgument(Object expression, String reason, String suggestion, String example) {
        return builder(INVALID_ARGUMENT)
                .expression(expression)
                .reason(reason)
                .suggestion(suggestion)
                .example(example)
                .build();
    }

    public static ReactorQLException resourceLimit(String reason, String suggestion, String example) {
        return builder(RESOURCE_LIMIT)
                .reason(reason)
                .suggestion(suggestion)
                .example(example)
                .build();
    }

    public static ReactorQLException unsupportedFrom(Object expression) {
        return builder(UNSUPPORTED_FROM)
                .expression(expression)
                .reason("当前 FROM 表达式不在支持范围内")
                .suggestion("使用普通表名、子查询、values，或使用当前运行环境已经开放的表函数。")
                .example("select * from test 或 select * from (select a from t1) t")
                .build();
    }

    public static ReactorQLException unsupportedFlatMap(Object expression) {
        return builder(UNSUPPORTED_FLAT_MAP)
                .expression(expression)
                .reason("当前表达式不能作为 flatMap/列转行函数使用")
                .suggestion("列转行仅支持当前运行环境开放的展开函数，例如数组展开；普通函数应放在 select 值表达式中。")
                .example("select each(items) item from test")
                .build();
    }

    public static Builder builder(String i18nCode) {
        return new Builder(i18nCode);
    }

    public String getI18nCode() {
        return i18nCode;
    }

    public Object[] getI18nArgs() {
        return Arrays.copyOf(i18nArgs, i18nArgs.length);
    }

    public Integer getLine() {
        return line;
    }

    public Integer getColumn() {
        return column;
    }

    public String getExpression() {
        return expression;
    }

    public String getReason() {
        return reason;
    }

    public String getSuggestion() {
        return suggestion;
    }

    public String getExample() {
        return example;
    }

    @Override
    public String getLocalizedMessage() {
        try {
            ResourceBundle bundle = ResourceBundle.getBundle(BUNDLE_NAME, Locale.getDefault());
            return format(bundle.getString(MESSAGE_TEMPLATE_KEY), i18nArgs);
        } catch (MissingResourceException e) {
            return getMessage();
        }
    }

    private static String format(String template, Object[] args) {
        return MessageFormat.format(template, args);
    }

    private static String rootMessage(Throwable error) {
        Throwable current = error;
        while (current != null && current.getCause() != null) {
            current = current.getCause();
        }
        return current == null || current.getMessage() == null ? String.valueOf(error) : current.getMessage();
    }

    private static void fillParserPosition(Builder builder, Throwable error) {
        Throwable current = error;
        while (current != null) {
            if (current instanceof JSQLParserException) {
                current = current.getCause();
                continue;
            }
            if (current instanceof ParseException) {
                ParseException parseException = (ParseException) current;
                Token token = parseException.currentToken == null ? null : parseException.currentToken.next;
                if (token == null) {
                    token = parseException.currentToken;
                }
                if (token != null) {
                    builder.position(token.beginLine, token.beginColumn);
                }
                return;
            }
            current = current.getCause();
        }
    }

    private static void fillExpressionPosition(Builder builder, Object expression) {
        if (!(expression instanceof ASTNodeAccess)) {
            return;
        }
        SimpleNode node = ((ASTNodeAccess) expression).getASTNode();
        Token token = node == null ? null : node.jjtGetFirstToken();
        if (token != null) {
            builder.position(token.beginLine, token.beginColumn);
        }
    }

    public static final class Builder {
        private final String i18nCode;
        private Integer line;
        private Integer column;
        private String expression;
        private String reason;
        private String suggestion;
        private String example;
        private Throwable cause;

        private Builder(String i18nCode) {
            this.i18nCode = i18nCode;
        }

        public Builder position(Integer line, Integer column) {
            this.line = line;
            this.column = column;
            return this;
        }

        public Builder expression(Object expression) {
            if (expression != null) {
                this.expression = String.valueOf(expression);
                fillExpressionPosition(this, expression);
            }
            return this;
        }

        public Builder reason(String reason) {
            this.reason = reason;
            return this;
        }

        public Builder suggestion(String suggestion) {
            this.suggestion = suggestion;
            return this;
        }

        public Builder example(String example) {
            this.example = example;
            return this;
        }

        public Builder cause(Throwable cause) {
            this.cause = cause;
            return this;
        }

        public ReactorQLException build() {
            return new ReactorQLException(this);
        }

        private Object[] toMessageArgs() {
            return new Object[]{
                    i18nCode,
                    line == null ? "-" : line,
                    column == null ? "-" : column,
                    expression == null ? "-" : expression,
                    reason == null ? "-" : reason,
                    suggestion == null ? "-" : suggestion,
                    example == null ? "-" : example
            };
        }
    }
}
