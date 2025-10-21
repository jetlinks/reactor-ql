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

import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import org.jetlinks.reactor.ql.supports.fmap.ArrayValueFlatMapFeature;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class IfValueMapFeatureTest {

    @Test
    void test(){

        IfValueMapFeature feature=new IfValueMapFeature();
        assertThrows(Throwable.class,()->feature.createMapper(new Function(), null));

        Function function=new Function();
        function.setParameters(new ExpressionList());
        assertThrows(Throwable.class,()->feature.createMapper(new Function(),null));

    }
}