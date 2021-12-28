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
