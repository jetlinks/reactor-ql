package org.jetlinks.reactor.ql.supports.filter;

import java.util.Date;

public class GreaterTanFilter extends AbstractFilterFeature {

    public GreaterTanFilter() {
        super(">");
    }

    @Override
    protected boolean doPredicate(Number left, Number right) {
        return left.doubleValue() > right.doubleValue();
    }

    @Override
    protected boolean doPredicate(Date left, Date right) {
        return left.getTime() > right.getTime();
    }

    @Override
    protected boolean doPredicate(String left, String right) {
        return left.compareTo(right) > 0;
    }

    @Override
    @SuppressWarnings("all")
    protected boolean doPredicate(Object left, Object right) {
        if (left instanceof Comparable) {
            return ((Comparable) left).compareTo(right) > 0;
        }
        return false;
    }
}
