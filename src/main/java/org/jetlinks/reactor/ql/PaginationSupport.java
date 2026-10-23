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

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.statement.select.Fetch;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Top;
import org.jetlinks.reactor.ql.exception.ReactorQLException;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.jetlinks.reactor.ql.utils.ExpressionUtils;
import reactor.core.publisher.Flux;

import java.util.function.BiFunction;

/**
 * Immutable pagination expressions shared by sorting and final pagination for one query plan.
 * SQL dialects resolve to the same count/offset definitions; bound values remain subscription-local.
 */
final class PaginationSupport {

    private final Expression count;
    private final Expression offset;

    private PaginationSupport(Expression count, Expression offset) {
        this.count = count;
        this.offset = offset;
    }

    static PaginationSupport create(PlainSelect sql) {
        Limit limit = sql.getLimit();
        Expression count = limit == null ? null : limit.getRowCount();
        if (sql.getTop() != null) {
            Top top = sql.getTop();
            if (top.isPercentage() || top.isWithTies()) {
                throw ReactorQLException.unsupportedExpression(top,
                        "流式分页不支持 TOP PERCENT 或 WITH TIES，请指定明确的行数。", "select top 10 * from test");
            }
            count = top.getExpression();
        }
        if (sql.getFetch() != null) {
            Fetch fetch = sql.getFetch();
            if (fetch.getFetchParameters().stream().anyMatch(value -> "TIES".equalsIgnoreCase(value.trim())
                    || "WITH TIES".equalsIgnoreCase(value.trim()))) {
                throw ReactorQLException.unsupportedExpression(fetch,
                        "流式分页不支持 FETCH WITH TIES，请使用 ONLY。", "select * from test fetch first 10 rows only");
            }
            count = fetch.getExpression();
            if (count == null) {
                count = new LongValue(fetch.getRowCount());
            }
        }
        Expression offset = limit == null ? null : limit.getOffset();
        if (sql.getOffset() != null) {
            offset = sql.getOffset().getOffset();
        }
        return new PaginationSupport(count, offset);
    }

    Long count(ReactorQLContext context) {
        return resolve(count, context);
    }

    Long offset(ReactorQLContext context) {
        return resolve(offset, context);
    }

    BiFunction<ReactorQLContext, Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> limit() {
        return paginate(count, false);
    }

    BiFunction<ReactorQLContext, Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> offset() {
        return paginate(offset, true);
    }

    private static Long resolve(Expression expression, ReactorQLContext context) {
        return expression == null ? null : ExpressionUtils.getSimpleValue(expression, context)
                .map(value -> CastUtils.castNumber(value).longValue()).orElse(null);
    }

    private static BiFunction<ReactorQLContext, Flux<ReactorQLRecord>, Flux<ReactorQLRecord>> paginate(
            Expression expression, boolean skip) {
        if (expression == null) {
            return (context, source) -> source;
        }
        if (expression instanceof LongValue) {
            long value = ((LongValue) expression).getValue();
            return (context, source) -> skip ? source.skip(value) : source.take(value);
        }
        // Reusing a compiled query must not freeze bound parameters from an earlier subscription.
        return (context, source) -> Flux.defer(() -> {
            Long value = resolve(expression, context);
            return value == null ? source : skip ? source.skip(value) : source.take(value);
        });
    }
}
