package org.jetlinks.reactor.ql;

import reactor.core.publisher.Flux;

import java.util.Map;
import java.util.Optional;

public interface ReactorQLRecord {

    ReactorQLContext getContext();

    String getName();

    Flux<Object> getDataSource(String name);

    Optional<Object> getRecord(String source);

    Object getRecord();

    ReactorQLRecord putRecordToResult();

    void setResult(String name, Object value);

    ReactorQLRecord setResults(Map<String, Object> values);

    Map<String, Object> asMap();

    ReactorQLRecord resultToRecord(String name);

    ReactorQLRecord addRecord(String name, Object record);

    ReactorQLRecord addRecords(Map<String,Object> records);

    Map<String,Object> getRecords(boolean all);

    ReactorQLRecord removeRecord(String name);

    static ReactorQLRecord newRecord(String name, Object row, ReactorQLContext context) {
        if (row instanceof DefaultReactorQLRecord) {
            DefaultReactorQLRecord record= ((DefaultReactorQLRecord) row);
            if(null!=name){
                record.setName(name);
                record.addRecord(name,record.getRecord());
            }
            return record;
        }
        return new DefaultReactorQLRecord(name, row, context);
    }
}
