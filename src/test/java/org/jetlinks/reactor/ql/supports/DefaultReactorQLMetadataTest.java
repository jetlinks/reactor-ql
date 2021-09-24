package org.jetlinks.reactor.ql.supports;

import org.jetlinks.reactor.ql.feature.FeatureId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DefaultReactorQLMetadataTest {

    @Test
    void testSettingByOracleHint() {
        DefaultReactorQLMetadata metadata = new DefaultReactorQLMetadata("select /*+ distinctBy(bloom),ignoreError */ * from test");

        assertEquals(metadata.getSetting("distinctBy").orElse(null), "bloom");
        assertEquals(metadata.getSetting("ignoreError").orElse(null), true);

        assertThrows(Throwable.class,()->metadata.getFeatureNow(FeatureId.of("test")));
    }
}