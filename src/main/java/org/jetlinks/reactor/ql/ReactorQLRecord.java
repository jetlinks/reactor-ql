package org.jetlinks.reactor.ql;

import reactor.core.publisher.Flux;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public interface ReactorQLRecord {


    ReactorQLContext getContext();

    String getName();

    Flux<Object> getDataSource(String name);

    Optional<Object> getRecord(String source);

    Object getRecord();

    void setResult(String name, Object value);

    ReactorQLRecord setResults(Map<String, Object> values);

    Map<String, Object> asMap();

    ReactorQLRecord resultToRecord(String name);

    ReactorQLRecord addRecord(String name, Object record);

    ReactorQLRecord removeRecord(String name);

    default ReactorQLRecord resultToRecord() {
        return resultToRecord(null);
    }

    static ReactorQLRecord newContext(String name, Object row, ReactorQLContext context) {
        if (row instanceof ReactorQLRecord) {
            return ((ReactorQLRecord) row);//.addRecord(name,((ReactorQLContext) row).getRecord());
        }
        return new DefaultReactorQLRecord(name, row, context);
    }

    static Flux<ReactorQLRecord> mapContext(String name, ReactorQLContext context, Flux<Object> flux) {
        return flux.map(obj -> newContext(name, obj, context));
    }
}
