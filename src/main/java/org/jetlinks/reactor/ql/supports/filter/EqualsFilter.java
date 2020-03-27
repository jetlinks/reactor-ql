package org.jetlinks.reactor.ql.supports.filter;

import org.jetlinks.reactor.ql.utils.CompareUtils;

import java.util.Date;

public class EqualsFilter extends BinaryFilterFeature {

    private boolean not;

    public EqualsFilter(String type, boolean not) {
        super(type);
        this.not = not;
    }

    @Override
    protected boolean doPredicate(Number left, Number right) {
        return not != CompareUtils.compare(left, right);
    }

    @Override
    protected boolean doPredicate(Date left, Date right) {
        return not != CompareUtils.compare(left, right);
    }

    @Override
    protected boolean doPredicate(String left, String right) {
        return not != CompareUtils.compare(left, right);
    }

    @Override
    protected boolean doPredicate(Object left, Object right) {
        return not != CompareUtils.compare(left, right);
    }
}
