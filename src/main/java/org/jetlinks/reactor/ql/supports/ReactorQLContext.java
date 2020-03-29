package org.jetlinks.reactor.ql.supports;

import reactor.core.publisher.Flux;

import java.util.Map;
import java.util.Optional;

public interface ReactorQLContext {

    Flux<Object> getDataSource(String name);

    Optional<Object> getValue(String name);

    Optional<Object> getRecord(String source);

    void setValue(String name, Object value);

    void setValues(Map<String, Object> values);

    Map<String, Object> asMap();

    ReactorQLContext resultToRecord(String name);

    default ReactorQLContext resultToRecord() {
        return resultToRecord(null);
    }
}
