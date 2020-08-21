package org.jetlinks.reactor.ql;

import net.sf.jsqlparser.statement.select.PlainSelect;
import org.jetlinks.reactor.ql.feature.Feature;
import org.jetlinks.reactor.ql.feature.FeatureId;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * 元数据,用于管理特性,进行配置等操作
 *
 * @author zhouhao
 */
public interface ReactorQLMetadata {

    /**
     * 获取特性
     *
     * @param featureId 特性ID
     * @param <T>       特性类型
     * @return 特性
     */
    <T extends Feature> Optional<T> getFeature(FeatureId<T> featureId);

    /**
     * 获取设置
     * @param key
     * @return
     */
    Optional<Object> getSetting(String key);

    ReactorQLMetadata setting(String key,Object value);

    PlainSelect getSql();

    /**
     * 获取特性,如果不存在则抛出异常
     *
     * @param featureId 特性ID
     * @param <T>       特性类型
     * @return 特性
     */
    default <T extends Feature> T getFeatureNow(FeatureId<T> featureId) {
        return getFeatureNow(featureId, featureId::getId);
    }

    /**
     * 获取特性,如果特性不存在则使用指定等错误消息抛出异常
     *
     * @param featureId    特性ID
     * @param errorMessage 错误消息
     * @param <T>          特性类型
     * @return 特性
     */
    default <T extends Feature> T getFeatureNow(FeatureId<T> featureId, Supplier<String> errorMessage) {
        return getFeature(featureId)
                .orElseThrow(() -> new UnsupportedOperationException("unsupported feature: " + errorMessage.get()));
    }


}
