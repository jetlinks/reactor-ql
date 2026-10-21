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

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FilterFeature;
import org.jetlinks.reactor.ql.feature.ScalarValueMapper;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.jetlinks.reactor.ql.internal.FirstValueMapper;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public class LikeFilter implements FilterFeature {

    private final String id;

    public LikeFilter() {
        this("like");
    }

    public LikeFilter(String type) {
        id = FeatureId.Filter.of(type).getId();
    }

    @Override
    public BiFunction<ReactorQLRecord, Object, Mono<Boolean>> createPredicate(Expression expression, ReactorQLMetadata metadata) {
        Tuple2<Function<ReactorQLRecord, Publisher<?>>,
                Function<ReactorQLRecord, Publisher<?>>> tuple2 = ValueMapFeature.createBinaryMapper(expression, metadata);

        Function<ReactorQLRecord, Publisher<?>> leftMapper = tuple2.getT1();
        Function<ReactorQLRecord, Publisher<?>> rightMapper = tuple2.getT2();

        LikeExpression like = ((LikeExpression) expression);
        boolean not = like.isNot();
        if (like.isCaseInsensitive() || like.getEscape() != null) {
            return createDialectPredicate(like, metadata, leftMapper, rightMapper);
        }

        // Missing operands must complete empty; a primitive predicate cannot preserve that signal.
        Function<Object[], Boolean> zipper = values -> doTest(not, values[0], values[1]);
        if (rightMapper instanceof ScalarValueMapper && !(rightMapper instanceof FirstValueMapper)
                && ((ScalarValueMapper) rightMapper).isConstant()) {
            Object constant = ((ScalarValueMapper) rightMapper).constantValue();
            if (constant instanceof String) {
                AtomicReference<Predicate<String>> prepared = new AtomicReference<>();
                zipper = values -> {
                    Predicate<String> matcher = prepared.get();
                    if (matcher == null) {
                        // Prepare only when both operands emit, preserving invalid-pattern timing.
                        matcher = createLiteralMatcher((String) constant);
                        prepared.compareAndSet(null, matcher);
                    }
                    return not != matcher.test(String.valueOf(values[0]));
                };
            }
        }
        Function<Object[], Boolean> preparedZipper = zipper;
        return (row, column) -> Mono
                .zip(preparedZipper,
                     FirstValueMapper.first(leftMapper, row),
                     FirstValueMapper.first(rightMapper, row));
    }

    public static boolean doTest(boolean not, Object left, Object right) {
        return not != matches(left, right);
    }

    private BiFunction<ReactorQLRecord, Object, Mono<Boolean>> createDialectPredicate(
            LikeExpression expression, ReactorQLMetadata metadata,
            Function<ReactorQLRecord, Publisher<?>> left,
            Function<ReactorQLRecord, Publisher<?>> right) {
        Function<ReactorQLRecord, Publisher<?>> escape = expression.getEscape() == null
                ? ScalarValueMapper.constant("\\")
                : ValueMapFeature.createMapperNow(expression.getEscape(), metadata);
        Pattern fixed = null;
        if (right instanceof ScalarValueMapper && ((ScalarValueMapper) right).isConstant()
                && escape instanceof ScalarValueMapper && ((ScalarValueMapper) escape).isConstant()) {
            Object patternValue = ((ScalarValueMapper) right).constantValue();
            Object escapeValue = ((ScalarValueMapper) escape).constantValue();
            if (patternValue != null && escapeValue != null) {
                fixed = compileDialectPattern(String.valueOf(patternValue), String.valueOf(escapeValue),
                        expression.isCaseInsensitive());
            }
        }
        Pattern constantPattern = fixed;
        Function<Object[], Boolean> compare = values -> {
            Pattern pattern = constantPattern == null
                    ? compileDialectPattern(String.valueOf(values[1]), String.valueOf(values[2]),
                            expression.isCaseInsensitive())
                    : constantPattern;
            return expression.isNot() != pattern.matcher(String.valueOf(values[0])).matches();
        };
        return (row, ignored) -> Mono.zip(compare, FirstValueMapper.first(left, row),
                FirstValueMapper.first(right, row), FirstValueMapper.first(escape, row));
    }

    private static Pattern compileDialectPattern(String value, String escape, boolean ignoreCase) {
        if (escape.length() > 1) {
            throw new IllegalArgumentException("LIKE ESCAPE must contain at most one character");
        }
        StringBuilder regex = new StringBuilder(value.length() * 2);
        for (int index = 0; index < value.length(); index++) {
            char ch = value.charAt(index);
            if (!escape.isEmpty() && ch == escape.charAt(0)) {
                if (++index == value.length()) {
                    throw new IllegalArgumentException("LIKE pattern ends with its escape character");
                }
                regex.append(Pattern.quote(String.valueOf(value.charAt(index))));
            } else if (ch == '%') {
                regex.append(".*");
            } else if (ch == '_') {
                regex.append('.');
            } else {
                regex.append(Pattern.quote(String.valueOf(ch)));
            }
        }
        int flags = Pattern.DOTALL | (ignoreCase ? Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE : 0);
        return Pattern.compile(regex.toString(), flags);
    }

    private static boolean matches(Object left, Object right) {
        return compilePattern(String.valueOf(right))
                .matcher(String.valueOf(left))
                .matches();
    }

    private static Pattern compilePattern(String value) {
        return Pattern.compile(value.replace("%", ".*"));
    }

    private static Predicate<String> createLiteralMatcher(String value) {
        Pattern regex = compilePattern(value);
        Predicate<String> fallback = input -> regex.matcher(input).matches();
        if (hasRegexSyntax(value)) {
            return fallback;
        }
        int firstWildcard = value.indexOf('%');
        if (firstWildcard < 0) {
            return value::equals;
        }
        // Java regex ".*" does not cross line terminators without DOTALL.
        if (hasLineTerminator(value)) {
            return fallback;
        }
        return createWildcardMatcher(value, firstWildcard, fallback);
    }

    private static Predicate<String> createWildcardMatcher(String value,
                                                           int firstWildcard,
                                                           Predicate<String> fallback) {
        int lastWildcard = value.lastIndexOf('%');
        if (firstWildcard == lastWildcard && lastWildcard == value.length() - 1) {
            String prefix = value.substring(0, lastWildcard);
            return input -> input.startsWith(prefix) && !hasLineTerminator(input);
        }
        if (firstWildcard == lastWildcard && firstWildcard == 0) {
            String suffix = value.substring(1);
            return input -> input.endsWith(suffix) && !hasLineTerminator(input);
        }
        if (firstWildcard == 0 && lastWildcard == value.length() - 1
                && value.indexOf('%', 1) == lastWildcard) {
            String part = value.substring(1, lastWildcard);
            return input -> input.contains(part) && !hasLineTerminator(input);
        }
        return fallback;
    }

    private static boolean hasRegexSyntax(String value) {
        for (int i = 0; i < value.length(); i++) {
            switch (value.charAt(i)) {
                case '\\':
                case '.':
                case '^':
                case '$':
                case '|':
                case '?':
                case '*':
                case '+':
                case '(':
                case ')':
                case '[':
                case ']':
                case '{':
                case '}':
                    return true;
                default:
                    break;
            }
        }
        return false;
    }

    private static boolean hasLineTerminator(String value) {
        for (int i = 0; i < value.length(); i++) {
            switch (value.charAt(i)) {
                case '\n':
                case '\r':
                case '\u0085':
                case '\u2028':
                case '\u2029':
                    return true;
                default:
                    break;
            }
        }
        return false;
    }

    @Override
    public String getId() {
        return id;
    }
}
