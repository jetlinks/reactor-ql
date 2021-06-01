package org.jetlinks.reactor.ql;

import lombok.Getter;
import lombok.Setter;
import org.jetlinks.reactor.ql.utils.CompareUtils;
import reactor.core.publisher.Flux;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultReactorQLRecord implements ReactorQLRecord, Comparable<DefaultReactorQLRecord> {

    @Getter
    private ReactorQLContext context;

    private final Map<String, Object> records = new ConcurrentHashMap<>();

    private final Map<String, Object> results = new ConcurrentHashMap<>();

    private final static String THIS_RECORD = "this";

    @Getter
    @Setter
    private String name;

    public DefaultReactorQLRecord(
            String name,
            Object thisRecord,
            ReactorQLContext context) {
        if (name != null) {
            records.put(name, thisRecord);
        }
        this.name = name;
        if (thisRecord != null) {
            records.put(THIS_RECORD, thisRecord);
        }
        this.context = context;
    }

    private DefaultReactorQLRecord() {
    }

    @Override
    public Flux<Object> getDataSource(String name) {
        return context.getDataSource(name);
    }

    @Override
    public Optional<Object> getRecord(String name) {
        return Optional.ofNullable(this.records.get(name));
    }

    @Override
    public Object getRecord() {
        return this.records.get(THIS_RECORD);
    }

    @Override
    public ReactorQLRecord setResult(String name, Object value) {
        if (name == null || value == null) {
            return this;
        }
        if(name.equals("$this")&& value instanceof Map){
            results.putAll(((Map) value));
        }else {
            results.put(name, value);
        }
        return this;
    }

    @Override
    public ReactorQLRecord setResults(Map<String, Object> values) {
        values.forEach(this::setResult);
        return this;
    }

    @Override
    public Map<String, Object> asMap() {
        return results;
    }

    @Override
    public ReactorQLRecord addRecord(String name, Object record) {
        if (name == null || record == null) {
            return this;
        }
        records.put(name, record);
        return this;
    }

    @Override
    public ReactorQLRecord addRecords(Map<String, Object> records) {
        records.forEach(this::addRecord);
        return this;
    }

    @Override
    public Map<String, Object> getRecords(boolean all) {
        Map<String, Object> tmp = new HashMap<>(records);
        if (!all) {
            tmp.remove(THIS_RECORD);
        }
        return tmp;
    }

    @Override
    public ReactorQLRecord removeRecord(String name) {
        if (name == null) {
            return this;
        }
        records.remove(name);
        return this;
    }

    @Override
    public ReactorQLRecord putRecordToResult() {
        Object record = getRecord();
        if (record instanceof Map) {
            setResults(((Map<String, Object>) record));
            return this;
        } else {
            setResult(THIS_RECORD, record);
        }
//        setResults(records);
        return this;
    }

    @Override
    public ReactorQLRecord resultToRecord(String name) {
        DefaultReactorQLRecord record = new DefaultReactorQLRecord();
        record.context = this.context;
        record.name = name;
        record.records.putAll(records);
        Map<String, Object> thisRecord = new ConcurrentHashMap<>(results);
        if (null != name && !record.records.containsKey(name)) {
            record.records.put(name, thisRecord);
        }
        record.records.put(THIS_RECORD, thisRecord);
        return record;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DefaultReactorQLRecord that = (DefaultReactorQLRecord) o;
        return Objects.equals(getRecord(), that.getRecord());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getRecord());
    }

    @Override
    public int compareTo(DefaultReactorQLRecord o) {
        return CompareUtils.compare(records, o.records);
    }

    @Override
    public ReactorQLRecord copy() {
        DefaultReactorQLRecord record = new DefaultReactorQLRecord();
        record.results.putAll(results);
        record.records.putAll(records);
        record.context = context;
        record.name=name;
        return record;
    }
}
