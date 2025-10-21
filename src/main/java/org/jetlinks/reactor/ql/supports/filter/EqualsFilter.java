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

import org.jetlinks.reactor.ql.utils.CompareUtils;

import java.util.Date;

public class EqualsFilter extends BinaryFilterFeature {

    private final boolean not;

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
