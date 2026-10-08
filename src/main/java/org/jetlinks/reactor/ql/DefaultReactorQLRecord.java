/*
 * Copyright 2025 JetLinks https://www.jetlinks.cn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jetlinks.reactor.ql;

import com.google.common.collect.Maps;
import lombok.Getter;
import org.jetlinks.reactor.ql.utils.CompareUtils;
import reactor.core.publisher.Flux;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultReactorQLRecord implements ReactorQLRecord, Comparable<DefaultReactorQLRecord> {

    @Getter
    private ReactorQLContext context;

    private Map<String, Object> records;

    private Map<String, Object> results;

    private Object thisRecord;

    private final static String THIS_RECORD = "this";

    @Getter
    private String name;

    public DefaultReactorQLRecord(
            String name,
            Map<String, Object> records,
            Map<String, Object> results,
            ReactorQLContext context) {
        this.name = name;
        this.records = records instanceof ConcurrentHashMap ? records : new ConcurrentHashMap<>(records);
        this.results = results instanceof ConcurrentHashMap ? results : new ConcurrentHashMap<>(results);
        this.thisRecord = this.records.get(THIS_RECORD);
        this.context = context;
    }

    public DefaultReactorQLRecord(
            String name,
            Object thisRecord,
            ReactorQLContext context) {
        this.context = context;
        this.name = name;
        this.thisRecord = thisRecord;
    }

    private DefaultReactorQLRecord(ReactorQLContext context) {
        this.context = context;
    }

    public void setName(String name) {
        if (records == null
                && this.name != null
                && !Objects.equals(this.name, name)
                && thisRecord != null) {
            ensureRecords();
        }
        this.name = name;
    }

    private Map<String, Object> ensureRecords() {
        if (records == null) {
            records = context.newContainer();
            if (name != null && thisRecord != null) {
                records.put(name, thisRecord);
            }
            if (thisRecord != null) {
                records.put(THIS_RECORD, thisRecord);
            }
        }
        return records;
    }

    private Map<String, Object> ensureResults() {
        if (results == null) {
            results = context.newContainer();
        }
        return results;
    }

    ReactorQLRecord setResult(String name, Object value, int expectedEntries) {
        // 仅内置默认容器可预估容量；null 及扩展 Record/Context 仍由公开路径保持原有契约。
        if (name != null
                && value != null
                && results == null
                && expectedEntries > 3
                && context.getClass() == DefaultReactorQLContext.class) {
            results = new HashMap<>(hashMapInitialCapacity(expectedEntries));
        }
        return setResult(name, value);
    }

    private static int hashMapInitialCapacity(int expectedEntries) {
        return (int) Math.min(Integer.MAX_VALUE,
                              ((long) expectedEntries * 4 + 2) / 3);
    }

    @Override
    public Flux<Object> getDataSource(String name) {
        return context.getDataSource(name);
    }

    @Override
    public Optional<Object> getRecord(String name) {
        return Optional.ofNullable(getRecordValue(name));
    }

    @Override
    public Object getRecordValue(String name) {
        if (records != null) {
            return records.get(name);
        }
        if (Objects.equals(THIS_RECORD, name) || Objects.equals(this.name, name)) {
            return thisRecord;
        }
        return null;
    }

    @Override
    public Object getRecord() {
        return records == null ? thisRecord : records.get(THIS_RECORD);
    }

    @Override
    public ReactorQLRecord setResult(String name, Object value) {
        if (name == null || value == null) {
            return this;
        }
        if (name.equals("$this") && value instanceof Map) {
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                if (null != entry.getKey() && null != entry.getValue()) {
                    ensureResults().put(String.valueOf(entry.getKey()), entry.getValue());
                }
            }
        } else {
            ensureResults().put(name, value);
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
        return ensureResults();
    }

    @Override
    public ReactorQLRecord addRecord(String name, Object record) {
        if (name == null || record == null) {
            return this;
        }
        if (records == null) {
            if (Objects.equals(name, this.name) && Objects.equals(record, thisRecord)) {
                return this;
            }
            if (Objects.equals(name, THIS_RECORD) && this.name == null) {
                thisRecord = record;
                return this;
            }
        }
        ensureRecords().put(name, record);
        if (Objects.equals(name, THIS_RECORD)) {
            thisRecord = record;
        }
        return this;
    }

    @Override
    public ReactorQLRecord addRecords(Map<String, Object> records) {
        records.forEach(this::addRecord);
        return this;
    }

    @Override
    public ReactorQLRecord addNamedRecords(ReactorQLRecord source) {
        if (!(source instanceof DefaultReactorQLRecord)) {
            return ReactorQLRecord.super.addNamedRecords(source);
        }
        DefaultReactorQLRecord other = (DefaultReactorQLRecord) source;
        if (other.records == null) {
            // 未物化来源只有隐式别名；避免仅为复制创建源 Map 和过滤视图。
            if (!THIS_RECORD.equals(other.name)) {
                addRecord(other.name, other.thisRecord);
            }
        } else {
            other.records.forEach((name, value) -> {
                if (!THIS_RECORD.equals(name)) {
                    addRecord(name, value);
                }
            });
        }
        return this;
    }

    @Override
    public ReactorQLContext bindNamedRecords(ReactorQLContext target) {
        if (records == null) {
            // 隐式别名无需先物化来源 Map；与 getRecords(false) 一样排除 this。
            if (name != null && !THIS_RECORD.equals(name) && thisRecord != null) {
                target.bind(name, thisRecord);
            }
        } else {
            records.forEach((name, value) -> {
                if (!THIS_RECORD.equals(name)) {
                    target.bind(name, value);
                }
            });
        }
        return target;
    }

    @Override
    public Map<String, Object> getRecords(boolean all) {
        Map<String, Object> records = ensureRecords();
        if (all) {
            return records;
        }
        return Maps.filterKeys(records, (k) -> !Objects.equals(THIS_RECORD, k));
    }

    @Override
    public ReactorQLRecord removeRecord(String name) {
        if (name == null) {
            return this;
        }
        if (records == null) {
            if (Objects.equals(name, THIS_RECORD)) {
                thisRecord = null;
                return this;
            }
            if (!Objects.equals(name, this.name)) {
                return this;
            }
        }
        ensureRecords().remove(name);
        if (Objects.equals(name, THIS_RECORD)) {
            thisRecord = null;
        }
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
        DefaultReactorQLRecord record = new DefaultReactorQLRecord(context);
        record.name = name;
        record.records = context.newContainer();
        if (records == null) {
            // 隐式具名来源直接复制；源 this 随后会被派生结果覆盖，无需先物化源容器。
            if (this.name != null && thisRecord != null) {
                record.records.put(this.name, thisRecord);
            }
        } else {
            record.records.putAll(records);
        }
        Map<String, Object> resultRecord = results == null
                ? new HashMap<>()
                : new HashMap<>(results);
        record.thisRecord = resultRecord;
        if (null != name && !record.records.containsKey(name)) {
            record.records.put(name, resultRecord);
        }
        record.records.put(THIS_RECORD, resultRecord);
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
        return Objects.hashCode(getRecord());
    }

    @Override
    public int compareTo(DefaultReactorQLRecord o) {
        return CompareUtils.compare(getRecords(true), o.getRecords(true));
    }

    @Override
    public ReactorQLRecord copy() {
        DefaultReactorQLRecord record = new DefaultReactorQLRecord(context);
        record.thisRecord = thisRecord;
        if (results != null) {
            record.results = context.newContainer();
            record.results.putAll(results);
        }
        if (records != null) {
            record.records = context.newContainer();
            record.records.putAll(records);
        }
        record.name = name;
        return record;
    }

    @Override
    public String toString() {
        return String.valueOf(asMap());
    }
}
