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
package org.jetlinks.reactor.ql.supports.from;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.TableFunction;
import org.apache.commons.collections.CollectionUtils;
import org.jetlinks.reactor.ql.DefaultReactorQL;
import org.jetlinks.reactor.ql.ReactorQLContext;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FromFeature;
import org.jetlinks.reactor.ql.feature.PropertyFeature;
import org.jetlinks.reactor.ql.supports.DefaultReactorQLMetadata;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.jetlinks.reactor.ql.utils.CompareUtils;
import org.jetlinks.reactor.ql.utils.ExpressionUtils;
import org.jetlinks.reactor.ql.utils.SqlUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Merge two or more sorted sources by key.
 *
 * <pre>{@code
 * select *
 * from merge_by_key(
 *   'ts',
 *   (select ts,a from t1),
 *   (select ts,b from t2),
 *   (select ts,c from t3),
 *   'full',
 *   'error'
 * ) m
 * }</pre>
 */
public class MergeByKeyFeature implements FromFeature {

    public static final String SETTING_MAX_ROWS_PER_KEY = "merge_by_key.maxRowsPerKey";
    public static final String SETTING_PREFETCH = "merge_by_key.prefetch";

    private static final String ID = FeatureId.From.of("merge_by_key").getId();
    private static final int DEFAULT_MAX_ROWS_PER_KEY = 1024;
    private static final int HARD_MAX_ROWS_PER_KEY = 100_000;
    private static final int DEFAULT_PREFETCH = 32;
    private static final int HARD_MAX_PREFETCH = 1024;

    @Override
    public Function<ReactorQLContext, Flux<ReactorQLRecord>> createFromMapper(FromItem fromItem, ReactorQLMetadata metadata) {
        TableFunction table = ((TableFunction) fromItem);
        net.sf.jsqlparser.expression.Function function = table.getFunction();
        List<Expression> args = ExpressionUtils.getFunctionParameter(function);
        if (CollectionUtils.isEmpty(args)) {
            throw new IllegalArgumentException("merge_by_key requires key and at least two sources: " + fromItem);
        }

        MergeCall call = parseArguments(args, metadata, fromItem);
        int prefetch = readSetting(metadata, SETTING_PREFETCH, DEFAULT_PREFETCH, HARD_MAX_PREFETCH);
        String alias = table.getAlias() == null ? null : table.getAlias().getName();

        List<SourceSpec> sources = createSourceSpecs(call.sources, metadata);
        PropertyFeature property = metadata.getFeatureNow(PropertyFeature.ID);

        MergeOptions options = new MergeOptions(call.key,
                                                call.mode,
                                                call.duplicateStrategy,
                                                call.maxRowsPerKey,
                                                sources);

        return ctx -> {
            List<Flux<TaggedRecord>> sortedSources = new ArrayList<>(sources.size());
            for (SourceSpec source : sources) {
                sortedSources.add(checkSorted(source.mapper.apply(ctx), source, call.key, property));
            }
            return merge(sortedSources, options, prefetch)
                    .map(row -> ReactorQLRecord.newRecord(alias, row, ctx));
        };
    }

    private Function<ReactorQLContext, Flux<ReactorQLRecord>> createSortedSourceMapper(FromItem source,
                                                                                       ReactorQLMetadata metadata) {
        if (source instanceof SubSelect) {
            SubSelect subSelect = ((SubSelect) source);
            SelectBody body = subSelect.getSelectBody();
            if (body instanceof PlainSelect) {
                String alias = subSelect.getAlias() == null ? null : subSelect.getAlias().getName();
                DefaultReactorQLMetadata sourceMetadata = new DefaultReactorQLMetadata(metadata, ((PlainSelect) body));
                // merge_by_key relies on each source staying sorted; subquery projection must not reorder rows.
                sourceMetadata.setConcurrency(1);
                DefaultReactorQL reactorQL = new DefaultReactorQL(sourceMetadata);
                return ctx -> reactorQL
                        .start(ctx)
                        .map(record -> record.resultToRecord(alias == null ? record.getName() : alias));
            }
        }
        return FromFeature.createFromMapperByFrom(source, metadata);
    }

    @SuppressWarnings("unchecked")
    protected Flux<Map<String, Object>> merge(List<Flux<TaggedRecord>> sources,
                                              MergeOptions options,
                                              int prefetch) {
        Comparator<TaggedRecord> comparator = (l, r) -> {
            int keyCompare = CompareUtils.compare(l.key, r.key);
            if (keyCompare != 0) {
                return keyCompare;
            }
            return Integer.compare(l.source.order, r.source.order);
        };

        Publisher<? extends TaggedRecord>[] publishers = sources.toArray(new Publisher[0]);
        return Flux
                .mergeComparing(prefetch, comparator, publishers)
                .windowUntilChanged(TaggedRecord::getKey, (l, r) -> CompareUtils.compare(l, r) == 0)
                .concatMap(window -> window
                        .collect(() -> new KeyBucket(options), KeyBucket::add)
                        .flatMapMany(bucket -> mergeBucket(bucket, options)));
    }

    private Flux<TaggedRecord> checkSorted(Flux<ReactorQLRecord> source,
                                           SourceSpec sourceSpec,
                                           String key,
                                           PropertyFeature property) {
        AtomicReference<Object> previous = new AtomicReference<>();
        AtomicLong index = new AtomicLong();
        return source.map(record -> {
            Object keyValue = getKey(record, key, property);
            if (keyValue == null) {
                throw new UnsupportedOperationException("merge_by_key source " + sourceSpec.name
                                                                + " missing key: " + key);
            }
            Object last = previous.get();
            if (last != null && CompareUtils.compare(last, keyValue) > 0) {
                throw new UnsupportedOperationException("merge_by_key source " + sourceSpec.name
                                                                + " is not sorted by " + key
                                                                + ": previous=" + last
                                                                + ", current=" + keyValue
                                                                + ", index=" + index.get());
            }
            previous.set(keyValue);
            return new TaggedRecord(sourceSpec, keyValue, record, index.getAndIncrement());
        });
    }

    private Flux<Map<String, Object>> mergeBucket(KeyBucket bucket, MergeOptions options) {
        switch (options.duplicateStrategy) {
            case error:
                return mergeError(bucket, options);
            case zip:
                return mergeZip(bucket, options);
            case cartesian:
                return mergeCartesian(bucket, options);
            case array:
                return mergeArray(bucket, options);
            default:
                return Flux.error(new UnsupportedOperationException("Unsupported duplicate strategy: "
                                                                            + options.duplicateStrategy));
        }
    }

    private Flux<Map<String, Object>> mergeError(KeyBucket bucket, MergeOptions options) {
        for (List<TaggedRecord> records : bucket.rows) {
            if (records.size() > 1) {
                return Flux.error(new UnsupportedOperationException(
                        "merge_by_key duplicate key " + bucket.key + " found. "
                                + "Use duplicate strategy 'zip', 'array' or 'cartesian' explicitly."));
            }
        }
        if (!options.mode.shouldEmit(bucket)) {
            return Flux.empty();
        }
        List<TaggedRecord> records = new ArrayList<>(bucket.rows.size());
        for (List<TaggedRecord> sourceRows : bucket.rows) {
            records.add(sourceRows.isEmpty() ? null : sourceRows.get(0));
        }
        return Flux.just(mergeRows(records));
    }

    private Flux<Map<String, Object>> mergeZip(KeyBucket bucket, MergeOptions options) {
        int size = zipSize(bucket, options.mode);
        if (size <= 0) {
            return Flux.empty();
        }
        List<Map<String, Object>> rows = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            List<TaggedRecord> records = new ArrayList<>(bucket.rows.size());
            for (List<TaggedRecord> sourceRows : bucket.rows) {
                records.add(i < sourceRows.size() ? sourceRows.get(i) : null);
            }
            rows.add(mergeRows(records));
        }
        return Flux.fromIterable(rows);
    }

    private Flux<Map<String, Object>> mergeCartesian(KeyBucket bucket, MergeOptions options) {
        if (!options.mode.shouldEmit(bucket)) {
            return Flux.empty();
        }
        List<Map<String, Object>> rows = new ArrayList<>();
        appendCartesian(bucket, options, 0, new ArrayList<>(), rows);
        return Flux.fromIterable(rows);
    }

    private Flux<Map<String, Object>> mergeArray(KeyBucket bucket, MergeOptions options) {
        if (!options.mode.shouldEmit(bucket)) {
            return Flux.empty();
        }

        Map<String, Object> row = new LinkedHashMap<>();
        row.put(options.key, bucket.key);
        for (SourceSpec source : options.sources) {
            row.put(source.name, toRows(bucket.rows.get(source.order)));
        }
        return Flux.just(row);
    }

    private int zipSize(KeyBucket bucket, MergeMode mode) {
        if (!mode.shouldEmit(bucket)) {
            return 0;
        }
        int size = mode == MergeMode.inner ? Integer.MAX_VALUE : 0;
        for (List<TaggedRecord> sourceRows : bucket.rows) {
            if (mode == MergeMode.inner) {
                size = Math.min(size, sourceRows.size());
            } else {
                size = Math.max(size, sourceRows.size());
            }
        }
        return size == Integer.MAX_VALUE ? 0 : size;
    }

    private void appendCartesian(KeyBucket bucket,
                                 MergeOptions options,
                                 int index,
                                 List<TaggedRecord> selected,
                                 List<Map<String, Object>> rows) {
        if (index >= bucket.rows.size()) {
            rows.add(mergeRows(selected));
            return;
        }
        List<TaggedRecord> sourceRows = bucket.rows.get(index);
        if (sourceRows.isEmpty()) {
            if (options.mode == MergeMode.full
                    || options.mode == MergeMode.left
                    || options.mode == MergeMode.right) {
                selected.add(null);
                appendCartesian(bucket, options, index + 1, selected, rows);
                selected.remove(selected.size() - 1);
            }
            return;
        }
        for (TaggedRecord record : sourceRows) {
            selected.add(record);
            appendCartesian(bucket, options, index + 1, selected, rows);
            selected.remove(selected.size() - 1);
        }
    }

    private List<Map<String, Object>> toRows(List<TaggedRecord> records) {
        List<Map<String, Object>> rows = new ArrayList<>(records.size());
        for (TaggedRecord record : records) {
            rows.add(toMap(record.record));
        }
        return rows;
    }

    private Map<String, Object> mergeRows(List<TaggedRecord> records) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (TaggedRecord record : records) {
            if (record == null) {
                continue;
            }
            Map<String, Object> values = toMap(record.record);
            for (Map.Entry<String, Object> entry : values.entrySet()) {
                mergeValue(row, record.source.name, entry.getKey(), entry.getValue());
            }
        }
        return row;
    }

    private void mergeValue(Map<String, Object> row, String sourceName, String key, Object value) {
        Object old = row.get(key);
        if (old == null || Objects.equals(old, value)) {
            row.put(key, value);
        } else if (CompareUtils.compare(old, value) == 0) {
            row.put(key, old);
        } else {
            row.put(sourceName + "." + key, value);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> toMap(ReactorQLRecord record) {
        Object value = record.getRecord();
        if (value instanceof Map) {
            return new LinkedHashMap<>((Map<String, Object>) value);
        }
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(record.getName() == null ? "value" : record.getName(), value);
        return map;
    }

    private Object getKey(ReactorQLRecord record, String key, PropertyFeature property) {
        return property
                .getProperty(key, record.getRecord())
                .orElseGet(() -> property.getProperty(key, record.getRecords(true)).orElse(null));
    }

    private MergeCall parseArguments(List<Expression> args, ReactorQLMetadata metadata, FromItem fromItem) {
        String key;
        int optionIndex;
        List<FromItem> sources = new ArrayList<>();
        if (args.get(0) instanceof FromItem) {
            if (args.size() < 3) {
                throw new IllegalArgumentException("merge_by_key requires left source, right source and key: " + fromItem);
            }
            sources.add(asSourceFromItem(args.get(0), "source1", fromItem));
            sources.add(asSourceFromItem(args.get(1), "source2", fromItem));
            key = expressionAsString(args.get(2), "key");
            optionIndex = 3;
        } else {
            key = expressionAsString(args.get(0), "key");
            optionIndex = 1;
            while (optionIndex < args.size() && isSourceExpression(args.get(optionIndex))) {
                sources.add(asSourceFromItem(args.get(optionIndex), "source" + (sources.size() + 1), fromItem));
                optionIndex++;
            }
        }
        if (sources.size() < 2) {
            throw new IllegalArgumentException("merge_by_key requires at least two sources: " + fromItem);
        }
        MergeMode mode = optionIndex < args.size()
                ? MergeMode.of(expressionAsString(args.get(optionIndex), "mode"))
                : MergeMode.full;
        DuplicateStrategy duplicateStrategy = optionIndex + 1 < args.size()
                ? DuplicateStrategy.of(expressionAsString(args.get(optionIndex + 1), "duplicateStrategy"))
                : DuplicateStrategy.error;
        int maxRowsPerKey = optionIndex + 2 < args.size()
                ? positiveInt(args.get(optionIndex + 2), "maxRowsPerKey")
                : readSetting(metadata, SETTING_MAX_ROWS_PER_KEY, DEFAULT_MAX_ROWS_PER_KEY, HARD_MAX_ROWS_PER_KEY);
        if (optionIndex + 3 < args.size()) {
            throw new IllegalArgumentException("merge_by_key has too many arguments: " + fromItem);
        }
        return new MergeCall(key, sources, mode, duplicateStrategy, maxRowsPerKey);
    }

    private boolean isSourceExpression(Expression expression) {
        return expression instanceof FromItem || expression instanceof Column;
    }

    private FromItem asSourceFromItem(Expression expression, String name, FromItem fromItem) {
        if (expression instanceof FromItem) {
            return ((FromItem) expression);
        }
        if (expression instanceof Column) {
            return new Table(SqlUtils.getCleanStr(((Column) expression).getFullyQualifiedName()));
        }
        throw new UnsupportedOperationException("merge_by_key " + name + " source must be a table or subquery: "
                                                        + fromItem);
    }

    private String expressionAsString(Expression expression, String name) {
        if (expression instanceof StringValue) {
            return ((StringValue) expression).getValue();
        }
        if (expression instanceof Column) {
            return SqlUtils.getCleanStr(((Column) expression).getFullyQualifiedName());
        }
        return ExpressionUtils
                .getSimpleValue(expression)
                .map(String::valueOf)
                .orElseThrow(() -> new UnsupportedOperationException("merge_by_key unsupported " + name + ": "
                                                                             + expression));
    }

    private int positiveInt(Expression expression, String name) {
        Object value = ExpressionUtils
                .getSimpleValue(expression)
                .orElseThrow(() -> new UnsupportedOperationException("merge_by_key unsupported " + name + ": "
                                                                             + expression));
        int number = CastUtils.castNumber(value).intValue();
        if (number <= 0) {
            throw new UnsupportedOperationException("merge_by_key " + name + " must be positive: " + value);
        }
        return Math.min(number, HARD_MAX_ROWS_PER_KEY);
    }

    private int readSetting(ReactorQLMetadata metadata, String key, int defaultValue, int hardMax) {
        int value = metadata
                .getSetting(key)
                .map(CastUtils::castNumber)
                .map(Number::intValue)
                .orElse(defaultValue);
        if (value <= 0) {
            return defaultValue;
        }
        return Math.min(value, hardMax);
    }

    private List<SourceSpec> createSourceSpecs(List<FromItem> sources, ReactorQLMetadata metadata) {
        List<SourceSpec> specs = new ArrayList<>(sources.size());
        Map<String, Integer> nameCounts = new HashMap<>();
        for (int i = 0; i < sources.size(); i++) {
            FromItem source = sources.get(i);
            String name = uniqueSourceName(getSourceName(source, "source" + (i + 1)), nameCounts);
            specs.add(new SourceSpec(i, name, createSortedSourceMapper(source, metadata)));
        }
        return specs;
    }

    private String uniqueSourceName(String name, Map<String, Integer> nameCounts) {
        int count = nameCounts.getOrDefault(name, 0);
        nameCounts.put(name, count + 1);
        return count == 0 ? name : name + "_" + (count + 1);
    }

    private String getSourceName(FromItem item, String fallback) {
        if (item.getAlias() != null) {
            return SqlUtils.getCleanStr(item.getAlias().getName());
        }
        if (item instanceof Table) {
            return SqlUtils.getCleanStr(((Table) item).getName());
        }
        return fallback;
    }

    @Override
    public String getId() {
        return ID;
    }

    enum MergeMode {
        inner,
        left,
        right,
        full;

        boolean shouldEmit(KeyBucket bucket) {
            switch (this) {
                case inner:
                    for (List<TaggedRecord> records : bucket.rows) {
                        if (records.isEmpty()) {
                            return false;
                        }
                    }
                    return true;
                case left:
                    return !bucket.rows.get(0).isEmpty();
                case right:
                    return !bucket.rows.get(bucket.rows.size() - 1).isEmpty();
                case full:
                    for (List<TaggedRecord> records : bucket.rows) {
                        if (!records.isEmpty()) {
                            return true;
                        }
                    }
                    return false;
                default:
                    return false;
            }
        }

        static MergeMode of(String value) {
            String mode = SqlUtils.getCleanStr(value).toLowerCase();
            switch (mode) {
                case "inner":
                    return inner;
                case "left":
                case "left_outer":
                    return left;
                case "right":
                case "right_outer":
                    return right;
                case "full":
                case "outer":
                case "full_outer":
                    return full;
                default:
                    throw new UnsupportedOperationException("Unsupported merge_by_key mode: " + value);
            }
        }
    }

    enum DuplicateStrategy {
        error,
        zip,
        cartesian,
        array;

        static DuplicateStrategy of(String value) {
            String strategy = SqlUtils.getCleanStr(value).toLowerCase();
            switch (strategy) {
                case "error":
                    return error;
                case "zip":
                    return zip;
                case "cartesian":
                    return cartesian;
                case "array":
                case "list":
                    return array;
                default:
                    throw new UnsupportedOperationException("Unsupported merge_by_key duplicate strategy: " + value);
            }
        }
    }

    static class MergeCall {
        final String key;
        final List<FromItem> sources;
        final MergeMode mode;
        final DuplicateStrategy duplicateStrategy;
        final int maxRowsPerKey;

        MergeCall(String key,
                  List<FromItem> sources,
                  MergeMode mode,
                  DuplicateStrategy duplicateStrategy,
                  int maxRowsPerKey) {
            this.key = key;
            this.sources = sources;
            this.mode = mode;
            this.duplicateStrategy = duplicateStrategy;
            this.maxRowsPerKey = maxRowsPerKey;
        }
    }

    static class MergeOptions {
        final String key;
        final MergeMode mode;
        final DuplicateStrategy duplicateStrategy;
        final int maxRowsPerKey;
        final List<SourceSpec> sources;

        MergeOptions(String key,
                     MergeMode mode,
                     DuplicateStrategy duplicateStrategy,
                     int maxRowsPerKey,
                     List<SourceSpec> sources) {
            this.key = key;
            this.mode = mode;
            this.duplicateStrategy = duplicateStrategy;
            this.maxRowsPerKey = maxRowsPerKey;
            this.sources = sources;
        }
    }

    static class SourceSpec {
        final int order;
        final String name;
        final Function<ReactorQLContext, Flux<ReactorQLRecord>> mapper;

        SourceSpec(int order,
                   String name,
                   Function<ReactorQLContext, Flux<ReactorQLRecord>> mapper) {
            this.order = order;
            this.name = name;
            this.mapper = mapper;
        }
    }

    static class TaggedRecord {
        final SourceSpec source;
        final Object key;
        final ReactorQLRecord record;
        final long index;

        TaggedRecord(SourceSpec source, Object key, ReactorQLRecord record, long index) {
            this.source = source;
            this.key = key;
            this.record = record;
            this.index = index;
        }

        Object getKey() {
            return key;
        }
    }

    static class KeyBucket {
        final MergeOptions options;
        final List<List<TaggedRecord>> rows = new ArrayList<>();
        Object key;
        int size;

        KeyBucket(MergeOptions options) {
            this.options = options;
            for (int i = 0; i < options.sources.size(); i++) {
                rows.add(new ArrayList<>());
            }
        }

        void add(TaggedRecord record) {
            if (size >= options.maxRowsPerKey) {
                throw new UnsupportedOperationException("merge_by_key key " + record.key
                                                                + " exceeds maxRowsPerKey "
                                                                + options.maxRowsPerKey);
            }
            if (key == null) {
                key = record.key;
            }
            rows.get(record.source.order).add(record);
            size++;
        }
    }
}
