package org.jetlinks.reactor.ql.supports;

import java.util.Date;

public class EqualsFilter extends AbstractFilterFeature {

    public EqualsFilter() {
        super("=");
    }

    @Override
    protected boolean doPredicate(Number left, Number right) {
        return left.equals(right) || left.doubleValue() == right.doubleValue();
    }

    @Override
    protected boolean doPredicate(Date left, Date right) {
        return left.equals(right);
    }

    @Override
    protected boolean doPredicate(String left, String right) {
        return left.equals(right);
    }

    @Override
    protected boolean doPredicate(Object left, Object right) {
        return left.equals(right);
    }
}
