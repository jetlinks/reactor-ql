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
import org.jetlinks.reactor.ql.exception.ReactorQLException;
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
 *   'error',
 *   'desc'
 * ) m
 * }</pre>
 */
public class MergeByKeyFeature implements FromFeature {

    public static final String SETTING_MAX_ROWS_PER_KEY = "merge_by_key.maxRowsPerKey";
    public static final String SETTING_PREFETCH = "merge_by_key.prefetch";

    private static final String ID = FeatureId.From.of("merge_by_key").getId();
    private static final int DEFAULT_MAX_ROWS_PER_KEY = 1024;
    private static final int HARD_MAX_ROWS_PER_KEY = 100_000;
    private static final int HARD_MAX_PREFETCH = 1024;

    @Override
    public Function<ReactorQLContext, Flux<ReactorQLRecord>> createFromMapper(FromItem fromItem, ReactorQLMetadata metadata) {
        TableFunction table = ((TableFunction) fromItem);
        net.sf.jsqlparser.expression.Function function = table.getFunction();
        List<Expression> args = ExpressionUtils.getFunctionParameter(function);
        if (CollectionUtils.isEmpty(args)) {
            throw invalidMergeArgument(
                    fromItem,
                    "merge_by_key 至少需要排序键和两个输入源",
                    "第一个参数写排序键，后续写两个或更多已按该键排序的表或子查询。",
                    mergeExample()
            );
        }

        MergeCall call = parseArguments(args, metadata, fromItem);
        String alias = table.getAlias() == null ? null : table.getAlias().getName();

        // The merged result is still a sorted stream. Keep downstream projection/filter mapping sequential so a
        // merge_by_key result can be materialized and safely reused as another sorted source.
        metadata.setConcurrency(1);

        List<SourceSpec> sources = createSourceSpecs(call.sources, metadata);
        // mergeComparing subscribes all sources and requests prefetch from each one; keep the default tied to
        // the fan-in size so large file sources are not over-read while waiting for every source to advance.
        int prefetch = readSetting(metadata, SETTING_PREFETCH, sources.size(), HARD_MAX_PREFETCH);
        PropertyFeature property = metadata.getFeatureNow(PropertyFeature.ID);

        MergeOptions options = new MergeOptions(call.key,
                                                call.mode,
                                                call.duplicateStrategy,
                                                call.order,
                                                call.maxRowsPerKey,
                                                sources);

        return ctx -> {
            List<Flux<TaggedRecord>> sortedSources = new ArrayList<>(sources.size());
            for (SourceSpec source : sources) {
                sortedSources.add(checkSorted(source.mapper.apply(ctx), source, options, property));
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
            int keyCompare = options.order.compare(l.key, r.key);
            if (keyCompare != 0) {
                return keyCompare;
            }
            return Integer.compare(l.source.order, r.source.order);
        };

        Publisher<? extends TaggedRecord>[] publishers = sources.toArray(new Publisher[0]);
        return Flux
                .mergeComparing(prefetch, comparator, publishers)
                // The input is sorted, so buffering holds only the current key bucket and avoids live window inners
                // waiting on outer demand while concatMap waits for the current window to complete.
                .bufferUntilChanged(TaggedRecord::getKey, (l, r) -> CompareUtils.compare(l, r) == 0)
                .concatMap(records -> mergeBucket(toBucket(records, options), options));
    }

    private KeyBucket toBucket(List<TaggedRecord> records, MergeOptions options) {
        KeyBucket bucket = new KeyBucket(options);
        for (TaggedRecord record : records) {
            bucket.add(record);
        }
        return bucket;
    }

    private Flux<TaggedRecord> checkSorted(Flux<ReactorQLRecord> source,
                                           SourceSpec sourceSpec,
                                           MergeOptions options,
                                           PropertyFeature property) {
        AtomicReference<Object> previous = new AtomicReference<>();
        AtomicLong index = new AtomicLong();
        return source.map(record -> {
            Object keyValue = getKey(record, options.key, property);
            if (keyValue == null) {
                throw invalidMergeArgument(
                        null,
                        "merge_by_key 输入源 " + sourceSpec.name + " 缺少排序键: " + options.key,
                        "确保每个输入源都输出排序键列，或将第一个参数改为实际存在的列名。",
                        mergeExample()
                );
            }
            Object last = previous.get();
            if (last != null && !options.order.isOrdered(last, keyValue)) {
                throw invalidMergeArgument(
                        null,
                        "merge_by_key 输入源 " + sourceSpec.name + " 未按 " + options.key
                                + " " + options.order.name() + " 排序，检测位置: " + index.get(),
                        "在每个输入源中使用相同排序键和排序方向；如果数据为降序，请将 order 参数设为 desc。",
                        "select * from merge_by_key('ts', (select ts,a from t1 order by ts), (select ts,b from t2 order by ts), 'asc') m"
                );
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
                return Flux.error(invalidMergeArgument(
                        null,
                        "merge_by_key duplicate strategy 不受支持: " + options.duplicateStrategy,
                        "duplicate strategy 可使用 error、zip、array 或 cartesian。",
                        "select * from merge_by_key('ts', t1, t2, 'full', 'zip') m"
                ));
        }
    }

    private Flux<Map<String, Object>> mergeError(KeyBucket bucket, MergeOptions options) {
        for (List<TaggedRecord> records : bucket.rows) {
            if (records.size() > 1) {
                return Flux.error(invalidMergeArgument(
                        null,
                        "merge_by_key 存在重复排序键，当前 duplicate strategy 为 error",
                        "如果允许重复键，请显式使用 zip、array 或 cartesian 作为重复键处理策略。",
                        "select * from merge_by_key('ts', t1, t2, 'full', 'zip') m"
                ));
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
        ParsedSources parsedSources = parseSourceArguments(args, fromItem);
        ParsedOptions parsedOptions = parseOptionArguments(args, parsedSources.optionIndex, metadata);
        return new MergeCall(parsedSources.key,
                             parsedSources.sources,
                             parsedOptions.mode,
                             parsedOptions.duplicateStrategy,
                             parsedOptions.order,
                             parsedOptions.maxRowsPerKey);
    }

    private ParsedSources parseSourceArguments(List<Expression> args, FromItem fromItem) {
        if (args.get(0) instanceof FromItem) {
            return parseLegacySourceArguments(args, fromItem);
        }
        return parseKeyFirstSourceArguments(args, fromItem);
    }

    private ParsedSources parseLegacySourceArguments(List<Expression> args, FromItem fromItem) {
        if (args.size() < 3) {
            throw invalidMergeArgument(
                    fromItem,
                    "merge_by_key 使用源优先写法时至少需要 left source、right source 和 key",
                    "推荐使用 key 优先写法：merge_by_key('ts', source1, source2, ...)。",
                    mergeExample()
            );
        }
        List<FromItem> sources = new ArrayList<>();
        sources.add(asSourceFromItem(args.get(0), "source1"));
        sources.add(asSourceFromItem(args.get(1), "source2"));
        return new ParsedSources(expressionAsString(args.get(2), "key"), sources, 3);
    }

    private ParsedSources parseKeyFirstSourceArguments(List<Expression> args, FromItem fromItem) {
        String key = expressionAsString(args.get(0), "key");
        int optionIndex = 1;
        List<FromItem> sources = new ArrayList<>();
        while (optionIndex < args.size() && isSourceExpression(args.get(optionIndex))) {
            sources.add(asSourceFromItem(args.get(optionIndex), "source" + (sources.size() + 1)));
            optionIndex++;
        }
        if (sources.size() < 2) {
            throw invalidMergeArgument(
                    fromItem,
                    "merge_by_key 至少需要两个输入源",
                    "第一个参数写排序键，后续写两个或更多已按该键排序的表或子查询。",
                    mergeExample()
            );
        }
        return new ParsedSources(key, sources, optionIndex);
    }

    private ParsedOptions parseOptionArguments(List<Expression> args, int optionIndex, ReactorQLMetadata metadata) {
        ParsedOptions options = new ParsedOptions(readSetting(metadata,
                                                              SETTING_MAX_ROWS_PER_KEY,
                                                              DEFAULT_MAX_ROWS_PER_KEY,
                                                              HARD_MAX_ROWS_PER_KEY));
        for (int i = optionIndex; i < args.size(); i++) {
            applyOption(args.get(i), options);
        }
        return options;
    }

    private void applyOption(Expression option, ParsedOptions options) {
        if (ExpressionUtils.getSimpleValue(option).filter(Number.class::isInstance).isPresent()) {
            applyMaxRowsPerKeyOption(option, options);
            return;
        }

        String value = expressionAsString(option, "option");
        if (applyOrderOption(option, value, options)
                || applyModeOption(option, value, options)
                || applyDuplicateStrategyOption(option, value, options)) {
            return;
        }
        throw invalidMergeArgument(
                option,
                "merge_by_key option 不受支持: " + value,
                "可选参数支持 join 模式 inner/left/right/full、重复键策略 error/zip/array/cartesian、排序方向 asc/desc，以及正整数 maxRowsPerKey。",
                "select * from merge_by_key('ts', t1, t2, 'full', 'zip', 'asc', 1000) m"
        );
    }

    private void applyMaxRowsPerKeyOption(Expression option, ParsedOptions options) {
        if (options.maxRowsPerKeySet) {
            throw duplicateMergeOption(option, "maxRowsPerKey");
        }
        options.maxRowsPerKey = positiveInt(option, "maxRowsPerKey");
        options.maxRowsPerKeySet = true;
    }

    private boolean applyOrderOption(Expression option, String value, ParsedOptions options) {
        if (!KeyOrder.supports(value)) {
            return false;
        }
        if (options.orderSet) {
            throw duplicateMergeOption(option, "order");
        }
        options.order = KeyOrder.of(value);
        options.orderSet = true;
        return true;
    }

    private boolean applyModeOption(Expression option, String value, ParsedOptions options) {
        if (!MergeMode.supports(value)) {
            return false;
        }
        if (options.modeSet) {
            throw duplicateMergeOption(option, "mode");
        }
        options.mode = MergeMode.of(value);
        options.modeSet = true;
        return true;
    }

    private boolean applyDuplicateStrategyOption(Expression option, String value, ParsedOptions options) {
        if (!DuplicateStrategy.supports(value)) {
            return false;
        }
        if (options.duplicateStrategySet) {
            throw duplicateMergeOption(option, "duplicate strategy");
        }
        options.duplicateStrategy = DuplicateStrategy.of(value);
        options.duplicateStrategySet = true;
        return true;
    }

    private boolean isSourceExpression(Expression expression) {
        return expression instanceof FromItem || expression instanceof Column;
    }

    private FromItem asSourceFromItem(Expression expression, String name) {
        if (expression instanceof FromItem) {
            return ((FromItem) expression);
        }
        if (expression instanceof Column) {
            return new Table(SqlUtils.getCleanStr(((Column) expression).getFullyQualifiedName()));
        }
        throw invalidMergeArgument(
                expression,
                "merge_by_key " + name + " 必须是表名或子查询",
                "把输入源写成表名，或写成带别名的子查询。",
                mergeExample()
        );
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
                .orElseThrow(() -> invalidMergeArgument(
                        expression,
                        "merge_by_key " + name + " 参数必须是字符串、列名或简单常量",
                        "排序键和选项建议使用字符串字面量；表名直接使用标识符。",
                        mergeExample()
                ));
    }

    private int positiveInt(Expression expression, String name) {
        Object value = ExpressionUtils
                .getSimpleValue(expression)
                .orElseThrow(() -> invalidMergeArgument(
                        expression,
                        "merge_by_key " + name + " 必须是正整数",
                        "将该参数写成大于 0 的数字常量。",
                        "select * from merge_by_key('ts', t1, t2, 'full', 'zip', 1000) m"
                ));
        int number = CastUtils.castNumber(value).intValue();
        if (number <= 0) {
            throw invalidMergeArgument(
                    expression,
                    "merge_by_key " + name + " 必须大于 0",
                    "将该参数写成大于 0 的数字常量。",
                    "select * from merge_by_key('ts', t1, t2, 'full', 'zip', 1000) m"
            );
        }
        return Math.min(number, HARD_MAX_ROWS_PER_KEY);
    }

    private static ReactorQLException duplicateMergeOption(Expression expression, String option) {
        return invalidMergeArgument(
                expression,
                "merge_by_key 重复指定 " + option + " 参数",
                "每类可选参数只保留一个；如需调整模式、重复键策略、排序方向或上限，请删除重复项。",
                "select * from merge_by_key('ts', t1, t2, 'full', 'zip', 'asc', 1000) m"
        );
    }

    private static ReactorQLException invalidMergeArgument(Object expression,
                                                           String reason,
                                                           String suggestion,
                                                           String example) {
        return ReactorQLException.invalidArgument(expression, reason, suggestion, example);
    }

    private static String mergeExample() {
        return "select * from merge_by_key('ts', (select ts,a from t1 order by ts), (select ts,b from t2 order by ts)) m";
    }

    private int readSetting(ReactorQLMetadata metadata, String key, int defaultValue, int hardMax) {
        int value;
        try {
            value = metadata
                    .getSetting(key)
                    .map(CastUtils::castNumber)
                    .map(Number::intValue)
                    .orElse(defaultValue);
        } catch (RuntimeException e) {
            throw ReactorQLException.builder(ReactorQLException.INVALID_ARGUMENT)
                    .reason("merge_by_key setting " + key + " 必须是数字")
                    .suggestion("将 " + key + " 设置为大于 0 的整数；未设置时使用默认值。")
                    .example(key + "=" + defaultValue)
                    .cause(e)
                    .build();
        }
        if (value <= 0) {
            value = defaultValue;
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
                    throw invalidMergeArgument(
                            null,
                            "merge_by_key join 模式不受支持: " + value,
                            "join 模式可使用 inner、left、right 或 full。",
                            "select * from merge_by_key('ts', t1, t2, 'full') m"
                    );
            }
        }

        static boolean supports(String value) {
            String mode = SqlUtils.getCleanStr(value).toLowerCase();
            switch (mode) {
                case "inner":
                case "left":
                case "left_outer":
                case "right":
                case "right_outer":
                case "full":
                case "outer":
                case "full_outer":
                    return true;
                default:
                    return false;
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
                    throw invalidMergeArgument(
                            null,
                            "merge_by_key 重复键策略不受支持: " + value,
                            "重复键策略可使用 error、zip、array 或 cartesian。",
                            "select * from merge_by_key('ts', t1, t2, 'full', 'zip') m"
                    );
            }
        }

        static boolean supports(String value) {
            String strategy = SqlUtils.getCleanStr(value).toLowerCase();
            switch (strategy) {
                case "error":
                case "zip":
                case "cartesian":
                case "array":
                case "list":
                    return true;
                default:
                    return false;
            }
        }
    }

    enum KeyOrder {
        asc {
            @Override
            int compare(Object left, Object right) {
                return CompareUtils.compare(left, right);
            }

            @Override
            boolean isOrdered(Object previous, Object current) {
                return CompareUtils.compare(previous, current) <= 0;
            }
        },
        desc {
            @Override
            int compare(Object left, Object right) {
                return CompareUtils.compare(right, left);
            }

            @Override
            boolean isOrdered(Object previous, Object current) {
                return CompareUtils.compare(previous, current) >= 0;
            }
        };

        abstract int compare(Object left, Object right);

        abstract boolean isOrdered(Object previous, Object current);

        static KeyOrder of(String value) {
            String order = SqlUtils.getCleanStr(value).toLowerCase();
            switch (order) {
                case "asc":
                case "ascending":
                    return asc;
                case "desc":
                case "descending":
                    return desc;
                default:
                    throw invalidMergeArgument(
                            null,
                            "merge_by_key 排序方向不受支持: " + value,
                            "排序方向可使用 asc 或 desc。",
                            "select * from merge_by_key('ts', t1, t2, 'asc') m"
                    );
            }
        }

        static boolean supports(String value) {
            String order = SqlUtils.getCleanStr(value).toLowerCase();
            switch (order) {
                case "asc":
                case "ascending":
                case "desc":
                case "descending":
                    return true;
                default:
                    return false;
            }
        }
    }

    static class MergeCall {
        final String key;
        final List<FromItem> sources;
        final MergeMode mode;
        final DuplicateStrategy duplicateStrategy;
        final KeyOrder order;
        final int maxRowsPerKey;

        MergeCall(String key,
                  List<FromItem> sources,
                  MergeMode mode,
                  DuplicateStrategy duplicateStrategy,
                  KeyOrder order,
                  int maxRowsPerKey) {
            this.key = key;
            this.sources = sources;
            this.mode = mode;
            this.duplicateStrategy = duplicateStrategy;
            this.order = order;
            this.maxRowsPerKey = maxRowsPerKey;
        }
    }

    static class ParsedSources {
        final String key;
        final List<FromItem> sources;
        final int optionIndex;

        ParsedSources(String key, List<FromItem> sources, int optionIndex) {
            this.key = key;
            this.sources = sources;
            this.optionIndex = optionIndex;
        }
    }

    static class ParsedOptions {
        MergeMode mode = MergeMode.full;
        DuplicateStrategy duplicateStrategy = DuplicateStrategy.error;
        KeyOrder order = KeyOrder.asc;
        int maxRowsPerKey;
        boolean modeSet;
        boolean duplicateStrategySet;
        boolean orderSet;
        boolean maxRowsPerKeySet;

        ParsedOptions(int maxRowsPerKey) {
            this.maxRowsPerKey = maxRowsPerKey;
        }
    }

    static class MergeOptions {
        final String key;
        final MergeMode mode;
        final DuplicateStrategy duplicateStrategy;
        final KeyOrder order;
        final int maxRowsPerKey;
        final List<SourceSpec> sources;

        MergeOptions(String key,
                     MergeMode mode,
                     DuplicateStrategy duplicateStrategy,
                     KeyOrder order,
                     int maxRowsPerKey,
                     List<SourceSpec> sources) {
            this.key = key;
            this.mode = mode;
            this.duplicateStrategy = duplicateStrategy;
            this.order = order;
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
                throw ReactorQLException.resourceLimit(
                        "同一个排序键的数据量超过 merge_by_key.maxRowsPerKey: " + options.maxRowsPerKey,
                        "提高 merge_by_key.maxRowsPerKey 设置，或改用 zip/array 等重复键策略减少单个键下的组合数量。",
                        "select * from merge_by_key('ts', t1, t2, 'full', 'array') m"
                );
            }
            if (key == null) {
                key = record.key;
            }
            rows.get(record.source.order).add(record);
            size++;
        }
    }
}
