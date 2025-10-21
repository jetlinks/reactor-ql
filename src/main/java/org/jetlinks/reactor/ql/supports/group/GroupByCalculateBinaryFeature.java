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
package org.jetlinks.reactor.ql.supports.group;

import org.jetlinks.reactor.ql.utils.CastUtils;

import java.util.function.BiFunction;

/**
 * 根据数学运算来分组
 *
 * @author zhouhao
 * @since 1.0
 */
public class GroupByCalculateBinaryFeature extends GroupByBinaryFeature {

    public GroupByCalculateBinaryFeature(String type, BiFunction<Number, Number, Object> mapper) {

        super(type, (left, right) -> mapper.apply(CastUtils.castNumber(left), CastUtils.castNumber(right)));
    }


}
