package org.jetlinks.reactor.ql.utils;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import org.jetlinks.reactor.ql.ReactorQLContext;
import org.jetlinks.reactor.ql.supports.ExpressionVisitorAdapter;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

public class ExpressionUtils {

    public static List<Expression> getFunctionParameter(Function function) {
        ExpressionList list = function.getParameters();
        List<Expression> expressions;
        if (list != null) {
            expressions = list.getExpressions();
        } else {
            expressions = Collections.emptyList();
        }
        return expressions;
    }

    public static Optional<Object> getSimpleValue(Expression expr, ReactorQLContext context) {
        AtomicReference<Object> ref = new AtomicReference<>();
        expr.accept(new ExpressionVisitorAdapter() {
            @Override
            public void visit(LongValue longValue) {
                ref.set(longValue.getValue());
            }

            @Override
            public void visit(DoubleValue doubleValue) {
                ref.set(doubleValue.getValue());
            }

            @Override
            public void visit(SignedExpression signedExpression) {
                Expression expr = signedExpression.getExpression();
                Number val = getSimpleValue(expr)
                        .map(CastUtils::castNumber)
                        .orElseThrow(() -> new UnsupportedOperationException("unsupported simple expression:" + signedExpression));

                switch (signedExpression.getSign()) {
                    case '-':
                        ref.set(CastUtils.castNumber(val
                                , i -> -i
                                , l -> -l
                                , d -> -d
                                , f -> -f
                                , d -> -d.doubleValue()
                        ));
                        break;
                    case '~':
                        ref.set(~val.longValue());
                        break;
                    default:
                        ref.set(val);
                        break;
                }
            }

            @Override
            public void visit(DateValue dateValue) {
                ref.set(dateValue.getValue());
            }

            @Override
            public void visit(TimeValue timeValue) {
                ref.set(timeValue.getValue());
            }

            @Override
            public void visit(StringValue function) {
                ref.set(function.getValue());
            }

            @Override
            public void visit(NumericBind bind) {
                if (null != context) {
                    context.getParameter(bind.getBindId()).ifPresent(ref::set);
                }
            }

            @Override
            public void visit(JdbcParameter parameter) {
                if (null != context) {
                    context.getParameter(parameter.getIndex()).ifPresent(ref::set);
                }
            }

            @Override
            public void visit(JdbcNamedParameter parameter) {
                if (null != context) {
                    context.getParameter(parameter.getName()).ifPresent(ref::set);
                }
            }

            @Override
            public void visit(UserVariable var) {
                if (null != context) {
                    context.getParameter(var.getName())
                           .ifPresent(ref::set);
                }
            }
        });

        return Optional.ofNullable(ref.get());
    }

    public static Optional<Object> getSimpleValue(Expression expr) {
        return getSimpleValue(expr, null);
    }

}
