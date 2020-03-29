package org.jetlinks.reactor.ql.supports;

import reactor.core.publisher.Flux;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public interface ReactorQLContext {

    String getName();

    Function<String,Flux<Object>> getDataSourceSupplier();

    Flux<Object> getDataSource(String name);

    Optional<Object> getValue(String name);

    Optional<Object> getRecord(String source);

    Object getRecord();

    void setValue(String name, Object value);

    void setValues(Map<String, Object> values);

    Map<String, Object> asMap();

    ReactorQLContext resultToRecord(String name);

    ReactorQLContext addRecord(String name,Object record);

    default ReactorQLContext resultToRecord() {
        return resultToRecord(null);
    }
}
