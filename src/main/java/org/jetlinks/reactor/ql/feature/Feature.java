package org.jetlinks.reactor.ql.feature;

/**
 * QL特性接口,表示支持到某个功能,如函数,聚合等
 *
 * @author zhouhao
 * @see FeatureId
 * @see FilterFeature
 * @see ValueMapFeature
 * @see GroupFeature
 * @since 1.0
 */
public interface Feature {

    /**
     * 特性ID
     *
     * @return ID
     */
    String getId();

}
