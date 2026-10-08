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
import org.jetlinks.reactor.ql.feature.RawScalarFilter;
import org.jetlinks.reactor.ql.feature.RawScalarValueMapper;
import org.jetlinks.reactor.ql.feature.ScalarFilter;
import org.jetlinks.reactor.ql.feature.ScalarValueMapper;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public class LikeFilter implements FilterFeature {

    private static final String ID = FeatureId.Filter.of("like").getId();

    @Override
    public BiFunction<ReactorQLRecord, Object, Mono<Boolean>> createPredicate(Expression expression, ReactorQLMetadata metadata) {
        Tuple2<Function<ReactorQLRecord, Publisher<?>>,
                Function<ReactorQLRecord, Publisher<?>>> tuple2 = ValueMapFeature.createBinaryMapper(expression, metadata);

        Function<ReactorQLRecord, Publisher<?>> leftMapper = tuple2.getT1();
        Function<ReactorQLRecord, Publisher<?>> rightMapper = tuple2.getT2();

        LikeExpression like = ((LikeExpression) expression);
        boolean not = like.isNot();

        if (leftMapper instanceof ScalarValueMapper && rightMapper instanceof ScalarValueMapper) {
            ScalarValueMapper leftScalar = (ScalarValueMapper) leftMapper;
            ScalarValueMapper rightScalar = (ScalarValueMapper) rightMapper;
            Predicate<String> literalMatcher = like.getRightExpression() instanceof StringValue
                    ? createLiteralMatcher(((StringValue) like.getRightExpression()).getValue())
                    : null;
            ScalarFilter recordFilter = (row, column) -> testScalar(
                    not, leftScalar.applyScalar(row), rightScalar.applyScalar(row), literalMatcher);
            if (metadata.supportsScalarFastPath()
                    && !metadata.isCheckpoint()
                    && leftMapper instanceof RawScalarValueMapper
                    && rightMapper instanceof RawScalarValueMapper) {
                RawScalarValueMapper leftRaw = (RawScalarValueMapper) leftMapper;
                RawScalarValueMapper rightRaw = (RawScalarValueMapper) rightMapper;
                return new RawScalarFilter() {
                    @Override
                    public ScalarFilter recordFilter() {
                        return recordFilter;
                    }

                    @Override
                    public boolean acceptsSource(String alias) {
                        return leftRaw.acceptsSource(alias) && rightRaw.acceptsSource(alias);
                    }

                    @Override
                    public boolean acceptsAnyRow() {
                        return leftRaw.acceptsAnyRow() && rightRaw.acceptsAnyRow();
                    }

                    @Override
                    public boolean testRaw(Object row) {
                        return testScalar(not, leftRaw.applyRaw(row), rightRaw.applyRaw(row), literalMatcher);
                    }

                    @Override
                    public boolean test(ReactorQLRecord row, Object column) {
                        return recordFilter.test(row, column);
                    }
                };
            }
            return recordFilter;
        }

        return (row, column) -> Mono
                .zip(Mono.from(leftMapper.apply(row)),
                     Mono.from(rightMapper.apply(row)),
                     (left, right) -> doTest(not, left, right));
    }

    private static boolean testScalar(boolean not, Object left, Object right, Predicate<String> literalMatcher) {
        if (left == null || right == null) {
            return false;
        }
        boolean matched = literalMatcher == null
                ? matches(left, right)
                : literalMatcher.test(String.valueOf(left));
        return not != matched;
    }

    public static boolean doTest(boolean not, Object left, Object right) {
        return not != matches(left, right);
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
        return ID;
    }
}
