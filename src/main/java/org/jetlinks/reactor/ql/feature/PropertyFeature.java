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
package org.jetlinks.reactor.ql.feature;

import java.util.Optional;

/**
 * 属性获取支持, 用于从指定的对象中获取属性值
 *
 * @author zhouhao
 * @see org.jetlinks.reactor.ql.supports.DefaultPropertyFeature
 * @since 1.0
 */
public interface PropertyFeature extends Feature {


    String ID_STR = "property-resolver";

    FeatureId<PropertyFeature> ID = FeatureId.of(ID_STR);

    /**
     * 从参数source中获取获取属性值
     * <pre>{@code
     * //普通获取
     * getProperty("a",{a:"value"}); => "value"
     * //从数组中获取值
     * getProperty(0,[1,2,3]); => 1
     * //嵌套值
     * getProperty("a.b.c",{a:{b:{c:"value"}}}); => "value"
     *
     * }</pre>
     *
     * @param property 属性
     * @param source   数据
     * @return 值
     */
    Optional<Object> getProperty(Object property, Object source);

    @Override
    default String getId() {
        return ID_STR;
    }
}
