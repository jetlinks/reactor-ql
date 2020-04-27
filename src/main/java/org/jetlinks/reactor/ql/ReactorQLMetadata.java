package org.jetlinks.reactor.ql;

import net.sf.jsqlparser.statement.select.PlainSelect;
import org.jetlinks.reactor.ql.feature.Feature;
import org.jetlinks.reactor.ql.feature.FeatureId;

import java.util.Optional;
import java.util.function.Supplier;

public interface ReactorQLMetadata {

    <T extends Feature> Optional<T> getFeature(FeatureId<T> featureId);

    default <T extends Feature> T getFeatureNow(FeatureId<T> featureId) {
        return getFeatureNow(featureId, featureId::getId);
    }

    default <T extends Feature> T getFeatureNow(FeatureId<T> featureId, Supplier<String> errorMessage) {
        return getFeature(featureId)
                .orElseThrow(() -> new UnsupportedOperationException("不支持的操作: " + errorMessage.get()));
    }

    Optional<Object> getSetting(String key);

    PlainSelect getSql();

}
