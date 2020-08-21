package org.jetlinks.reactor.ql.utils;

import net.sf.jsqlparser.expression.*;
import org.jetlinks.reactor.ql.supports.ExpressionVisitorAdapter;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class ExpressionUtils {

    public static Optional<Object> getSimpleValue(Expression expr) {
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

        });

        return Optional.ofNullable(ref.get());
    }

}
