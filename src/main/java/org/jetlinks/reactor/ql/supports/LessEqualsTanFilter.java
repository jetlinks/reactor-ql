package org.jetlinks.reactor.ql.supports;

import java.util.Date;

public class LessEqualsTanFilter extends AbstractFilterFeature {

    public LessEqualsTanFilter() {
        super("<=");
    }

    @Override
    protected boolean doPredicate(Number left, Number right) {
        return left.doubleValue() <= right.doubleValue();
    }

    @Override
    protected boolean doPredicate(Date left, Date right) {
        return left.getTime() <= right.getTime();
    }

    @Override
    protected boolean doPredicate(String left, String right) {
        return left.compareTo(right) <= 0;
    }

    @Override
    protected boolean doPredicate(Object left, Object right) {
        if (left instanceof Comparable) {
            return ((Comparable) left).compareTo(right) <= 0;
        }
        return false;
    }
}
