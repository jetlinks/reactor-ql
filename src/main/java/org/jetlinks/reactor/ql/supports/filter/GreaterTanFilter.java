package org.jetlinks.reactor.ql.supports.filter;

import org.jetlinks.reactor.ql.utils.CompareUtils;

import java.util.Date;

public class GreaterTanFilter extends BinaryFilterFeature {

    public GreaterTanFilter(String type) {
        super(type);
    }

    @Override
    protected boolean doTest(Number left, Number right) {
        return CompareUtils.compare(left, right) > 0;
    }

    @Override
    protected boolean doTest(Date left, Date right) {
        return left.getTime() > right.getTime();
    }

    @Override
    protected boolean doTest(String left, String right) {
        return left.compareTo(right) > 0;
    }

    @Override
    @SuppressWarnings("all")
    protected boolean doTest(Object left, Object right) {

        return CompareUtils.compare(left, right) > 0;
    }
}
