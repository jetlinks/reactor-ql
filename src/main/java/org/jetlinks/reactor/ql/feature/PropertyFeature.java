package org.jetlinks.reactor.ql.feature;

import java.util.Optional;

public interface PropertyFeature extends Feature {


    String ID_STR = "property-resolver";

    FeatureId<PropertyFeature> ID = FeatureId.of(ID_STR);

    Optional<Object> getProperty(Object property, Object value);

    @Override
    default String getId() {
        return ID_STR;
    }
}
