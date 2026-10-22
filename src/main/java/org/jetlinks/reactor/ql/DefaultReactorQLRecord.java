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

    // null: implicit this/name aliases; AdditionalRecord: one extra named source;
    // Map: fully materialized sources. Reuse this slot so ordinary Records do not grow.
    private Object records;

    private Map<String, Object> results;

    private Object thisRecord;

    private static final class AdditionalRecord {
        private final String name;
        private Object value;

        private AdditionalRecord(String name, Object value) {
            this.name = name;
            this.value = value;
        }
    }

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
        this.thisRecord = ((Map<?, ?>) this.records).get(THIS_RECORD);
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
        // Renaming never adds or moves source aliases. Preserve the original lazy mapping.
        if (!(records instanceof Map) && !Objects.equals(this.name, name)) {
            ensureRecords();
        }
        this.name = name;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> ensureRecords() {
        if (!(records instanceof Map)) {
            AdditionalRecord additional = (AdditionalRecord) records;
            Map<String, Object> materialized = newContainer();
            records = materialized;
            if (name != null && thisRecord != null) {
                materialized.put(name, thisRecord);
            }
            if (thisRecord != null) {
                materialized.put(THIS_RECORD, thisRecord);
            }
            if (additional != null) {
                materialized.put(additional.name, additional.value);
            }
        }
        return (Map<String, Object>) records;
    }

    private Map<String, Object> ensureResults() {
        if (results == null) {
            results = newContainer();
        }
        return results;
    }

    void prepareConcurrentResults(int expectedEntries) {
        // Mixed projections write on each column's signal thread. Only the exact default
        // Context uses our container; extensions retain newContainer's public contract.
        if (context.getClass() != DefaultReactorQLContext.class) {
            ensureResults();
        } else if (!(results instanceof ConcurrentHashMap)) {
            results = results == null
                    ? new ConcurrentHashMap<>(Math.max(4, expectedEntries))
                    : new ConcurrentHashMap<>(results);
        }
    }

    private Map<String, Object> newContainer() {
        // Only exact default Records use single-writer maps; inherited extension paths retain
        // the Context's concurrent container. Mixed default projections prepare theirs explicitly.
        return getClass() == DefaultReactorQLRecord.class
                && context.getClass() == DefaultReactorQLContext.class
                ? new HashMap<>(4)
                : context.newContainer();
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
        return Optional.ofNullable(getRecordValueDirect(name));
    }

    @Override
    public Object getRecordValue(String name) {
        // Legacy subclasses override getRecord, not this new raw-value entry point.
        return getClass() == DefaultReactorQLRecord.class
                ? getRecordValueDirect(name)
                : ReactorQLRecord.super.getRecordValue(name);
    }

    private Object getRecordValueDirect(String name) {
        if (records instanceof Map) {
            return ((Map<?, ?>) records).get(name);
        }
        AdditionalRecord additional = (AdditionalRecord) records;
        if (additional != null) {
            // Like the materialized source Map, an added named source has no null alias.
            if (name == null) {
                return null;
            }
            if (additional.name.equals(name)) {
                return additional.value;
            }
        }
        if (Objects.equals(THIS_RECORD, name) || Objects.equals(this.name, name)) {
            return thisRecord;
        }
        return null;
    }

    @Override
    public Object getRecord() {
        return records instanceof Map ? ((Map<?, ?>) records).get(THIS_RECORD) : thisRecord;
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
    public Object getResultValue(String name) {
        // Legacy subclasses can override asMap; custom Context containers may contain defaults.
        if (getClass() != DefaultReactorQLRecord.class
                || context.getClass() != DefaultReactorQLContext.class) {
            return ReactorQLRecord.super.getResultValue(name);
        }
        return results == null ? null : results.get(name);
    }

    @Override
    public ReactorQLRecord addRecord(String name, Object record) {
        if (name == null || record == null) {
            return this;
        }
        if (!(records instanceof Map)) {
            if (Objects.equals(name, this.name) && record == thisRecord) {
                return this;
            }
            if (Objects.equals(name, THIS_RECORD) && this.name == null) {
                thisRecord = record;
                return this;
            }
            AdditionalRecord additional = (AdditionalRecord) records;
            // Only the exact default implementation uses a compact named-source holder.
            // Extensions keep newContainer defaults and mutation timing on the public path.
            if (getClass() == DefaultReactorQLRecord.class
                    && context.getClass() == DefaultReactorQLContext.class
                    && !THIS_RECORD.equals(name)
                    && !name.equals(this.name)
                    && (additional == null || additional.name.equals(name))) {
                if (additional == null) {
                    records = new AdditionalRecord(name, record);
                } else {
                    additional.value = record;
                }
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
        if (getClass() != DefaultReactorQLRecord.class
                || source.getClass() != DefaultReactorQLRecord.class) {
            return ReactorQLRecord.super.addNamedRecords(source);
        }
        DefaultReactorQLRecord other = (DefaultReactorQLRecord) source;
        if (!(other.records instanceof Map)) {
            // Copy lazy named sources without creating a source Map or filtered view.
            if (!THIS_RECORD.equals(other.name)) {
                addRecord(other.name, other.thisRecord);
            }
            AdditionalRecord additional = (AdditionalRecord) other.records;
            if (additional != null) {
                addRecord(additional.name, additional.value);
            }
        } else {
            other.ensureRecords().forEach((name, value) -> {
                if (!THIS_RECORD.equals(name)) {
                    addRecord(name, value);
                }
            });
        }
        return this;
    }

    @Override
    public ReactorQLContext bindNamedRecords(ReactorQLContext target) {
        if (getClass() != DefaultReactorQLRecord.class
                || target.getClass() != DefaultReactorQLContext.class) {
            return ReactorQLRecord.super.bindNamedRecords(target);
        }
        if (!(records instanceof Map)) {
            // 隐式别名无需先物化来源 Map；与 getRecords(false) 一样排除 this。
            if (name != null && !THIS_RECORD.equals(name) && thisRecord != null) {
                target.bind(name, thisRecord);
            }
            AdditionalRecord additional = (AdditionalRecord) records;
            if (additional != null) {
                target.bind(additional.name, additional.value);
            }
        } else {
            ensureRecords().forEach((name, value) -> {
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
        if (!(records instanceof Map)) {
            AdditionalRecord additional = (AdditionalRecord) records;
            if (!Objects.equals(name, THIS_RECORD)
                    && !Objects.equals(name, this.name)
                    && (additional == null || !name.equals(additional.name))) {
                return this;
            }
        }
        // Removing this must leave the original named source (and vice versa) intact.
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
        Map<String, Object> resultSources = record.ensureRecords();
        if (!(records instanceof Map)) {
            // 隐式具名来源直接复制；源 this 随后会被派生结果覆盖，无需先物化源容器。
            if (this.name != null && thisRecord != null) {
                resultSources.put(this.name, thisRecord);
            }
            AdditionalRecord additional = (AdditionalRecord) records;
            if (additional != null) {
                resultSources.put(additional.name, additional.value);
            }
        } else {
            resultSources.putAll(ensureRecords());
        }
        Map<String, Object> resultRecord = results == null
                ? new HashMap<>()
                : new HashMap<>(results);
        record.thisRecord = resultRecord;
        if (null != name && !resultSources.containsKey(name)) {
            resultSources.put(name, resultRecord);
        }
        resultSources.put(THIS_RECORD, resultRecord);
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
            record.results = record.newContainer();
            record.results.putAll(results);
        }
        if (records instanceof Map) {
            // Copy the live Map verbatim; callers may have removed implicit aliases.
            Map<String, Object> copiedSources = record.newContainer();
            copiedSources.putAll(ensureRecords());
            record.records = copiedSources;
        } else if (records != null) {
            AdditionalRecord additional = (AdditionalRecord) records;
            record.records = new AdditionalRecord(additional.name, additional.value);
        }
        record.name = name;
        return record;
    }

    @Override
    public String toString() {
        return String.valueOf(asMap());
    }
}
