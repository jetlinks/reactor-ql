package org.jetlinks.reactor.ql;

import net.sf.jsqlparser.statement.select.PlainSelect;
import org.jetlinks.reactor.ql.feature.Feature;
import org.jetlinks.reactor.ql.feature.FeatureId;

import java.util.Optional;

public interface ReactorQLMetadata {


    <T extends Feature> Optional<T> getFeature(FeatureId<T> featureId);

    default <T extends Feature> T getFeatureNow(FeatureId<T> featureId) {
        return getFeature(featureId)
                .orElseThrow(() -> new UnsupportedOperationException("unsupported feature:" + featureId.getId()));
    }


    PlainSelect getSql();

}
