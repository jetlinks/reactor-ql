package org.jetlinks.reactor.ql;

import lombok.Getter;
import org.apache.commons.beanutils.PropertyUtils;
import org.jetlinks.reactor.ql.supports.ReactorQLContext;
import reactor.core.publisher.Flux;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class DefaultReactorQLContext implements ReactorQLContext {

    @Getter
    Function<String, Flux<Object>> dataSourceSupplier;

    private Map<String, Object> records = new ConcurrentHashMap<>();

    private Map<String, Object> results = new ConcurrentHashMap<>();

    static String THIS_RECORD = "this";

    @Getter
    private String name;

    public DefaultReactorQLContext(
            String name,
            Object thisRecord,
            Function<String, Flux<Object>> ctxSupplier) {
        if (name != null) {
            records.put(name, thisRecord);
        }
        this.name = name;
        records.put(THIS_RECORD, thisRecord);
        this.dataSourceSupplier = ctxSupplier;
    }

    private DefaultReactorQLContext() {
    }

    @Override
    public Flux<Object> getDataSource(String name) {
        if (name == null) {
            return Flux.just(1);
        }
        return Optional.of(name)
                .map(dataSourceSupplier)
                .orElse(Flux.empty());
    }

    @Override
    public Optional<Object> getValue(String name) {
        String[] arr = name.split("[.]", 2);
        Object record = arr.length == 1 ? records.get(THIS_RECORD) : records.get(arr[0]);
        if (record == null) {
            return Optional.empty();
        }
        String property = arr.length == 1 ? arr[0] : arr[1];
        return doGetValue(property, record);
    }

    protected Optional<Object> doGetValue(String property, Object value) {
        if (property.startsWith("\"")) {
            property = property.substring(1);
        }
        if (property.endsWith("\"")) {
            property = property.substring(0, property.length() - 1);
        }
        if (property.equalsIgnoreCase("this")) {
            return Optional.of(value);
        }
        if (results.containsKey(property)) {
            return Optional.ofNullable(results.get(property));
        }
        if (value instanceof Map) {
            return Optional.ofNullable(((Map<?, ?>) value).get(property));
        }
        try {
            PropertyUtils.getProperty(value, property);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Optional.empty();
    }

    @Override
    public Optional<Object> getRecord(String source) {

        return Optional.ofNullable(this.records.get(source));
    }

    @Override
    public Object getRecord() {
        return this.records.get(THIS_RECORD);
    }

    @Override
    public void setValue(String name, Object value) {
        results.put(name, value);
    }

    @Override
    public ReactorQLContext setValues(Map<String, Object> values) {
        results.putAll(values);
        return this;
    }

    @Override
    public Map<String, Object> asMap() {
        return results;
    }

    @Override
    public ReactorQLContext addRecord(String name, Object record) {
        if (name == null) {
            return this;
        }
        records.put(name, record);
        return this;
    }

    @Override
    public ReactorQLContext removeRecord(String name) {
        if (name == null) {
            return this;
        }
        records.remove(name);
        return this;
    }

    @Override
    public ReactorQLContext resultToRecord(String name) {
        DefaultReactorQLContext context = new DefaultReactorQLContext();
        context.dataSourceSupplier = dataSourceSupplier;
        context.records.putAll(records);
        Map<String, Object> thisRecord = new ConcurrentHashMap<>(results);
        if (null != name && !context.records.containsKey(name)) {
            context.records.put(name, thisRecord);
        }
        context.records.put(THIS_RECORD, thisRecord);
        return context;
    }

}
