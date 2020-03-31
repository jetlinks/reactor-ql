package org.jetlinks.reactor.ql;

import reactor.core.publisher.Flux;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public interface ReactorQLRecord {


    ReactorQLContext getContext();

    String getName();

    Flux<Object> getDataSource(String name);

    Optional<Object> getValue(String name);

    Optional<Object> getRecord(String source);

    Object getRecord();

    void setValue(String name, Object value);

    ReactorQLRecord setValues(Map<String, Object> values);

    Map<String, Object> asMap();

    ReactorQLRecord resultToRecord(String name);

    ReactorQLRecord addRecord(String name, Object record);

    ReactorQLRecord removeRecord(String name);

    default ReactorQLRecord resultToRecord() {
        return resultToRecord(null);
    }
}
