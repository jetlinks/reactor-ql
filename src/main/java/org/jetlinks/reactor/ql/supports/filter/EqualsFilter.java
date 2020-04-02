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
    protected boolean doTest(Number left, Number right) {
        return not != CompareUtils.equals(left, right);
    }

    @Override
    protected boolean doTest(Date left, Date right) {
        return not != CompareUtils.equals(left, right);
    }

    @Override
    protected boolean doTest(String left, String right) {
        return not != CompareUtils.equals(left, right);
    }

    @Override
    protected boolean doTest(Object left, Object right) {
        return not != CompareUtils.equals(left, right);
    }
}
