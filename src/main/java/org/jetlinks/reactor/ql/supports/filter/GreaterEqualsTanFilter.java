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

public class GreaterEqualsTanFilter extends BinaryFilterFeature {

    public GreaterEqualsTanFilter(String type) {
        super(type);
    }

    @Override
    protected boolean doTest(Number left, Number right) {
        return CompareUtils.compare(left, right) >= 0;
    }

    @Override
    protected boolean doTest(Date left, Date right) {
        return left.getTime() >= right.getTime();
    }

    @Override
    protected boolean doTest(String left, String right) {
        return left.compareTo(right) >= 0;
    }

    @Override
    @SuppressWarnings("all")
    protected boolean doTest(Object left, Object right) {
        return CompareUtils.compare(left, right) >= 0;
    }
}
