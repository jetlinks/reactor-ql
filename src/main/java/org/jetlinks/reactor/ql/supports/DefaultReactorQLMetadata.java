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
package org.jetlinks.reactor.ql.supports;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import org.apache.commons.collections.CollectionUtils;
import org.jetlinks.reactor.ql.Column;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.feature.Feature;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.exception.ReactorQLException;
import org.jetlinks.reactor.ql.supports.agg.CollectListAggFeature;
import org.jetlinks.reactor.ql.supports.agg.CollectRowAggMapFeature;
import org.jetlinks.reactor.ql.supports.agg.CountAggFeature;
import org.jetlinks.reactor.ql.supports.agg.MapAggFeature;
import org.jetlinks.reactor.ql.supports.distinct.DefaultDistinctFeature;
import org.jetlinks.reactor.ql.supports.filter.*;
import org.jetlinks.reactor.ql.supports.fmap.ArrayValueFlatMapFeature;
import org.jetlinks.reactor.ql.supports.from.*;
import org.jetlinks.reactor.ql.supports.group.*;
import org.jetlinks.reactor.ql.supports.map.*;
import org.jetlinks.reactor.ql.utils.CalculateUtils;
import org.jetlinks.reactor.ql.utils.CastUtils;
import org.jetlinks.reactor.ql.utils.CompareUtils;
import org.reactivestreams.Publisher;
import reactor.bool.BooleanUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.math.MathFlux;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.IsoFields;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DefaultReactorQLMetadata implements ReactorQLMetadata {

    //全局支持
    private static final Map<String, Feature> globalFeatures = new ConcurrentHashMap<>();

    public static final String SETTING_MAX_GENERATED_STRING_LENGTH = "function.maxGeneratedStringLength";
    public static final String SETTING_MAX_REGEX_INPUT_LENGTH = "function.regex.maxInputLength";
    public static final String SETTING_MAX_REGEX_PATTERN_LENGTH = "function.regex.maxPatternLength";
    public static final String SETTING_MAX_REGEX_REPLACEMENT_LENGTH = "function.regex.maxReplacementLength";

    private static final int DEFAULT_MAX_GENERATED_STRING_LENGTH = 1_000_000;
    private static final int DEFAULT_MAX_REGEX_INPUT_LENGTH = 256 * 1024;
    private static final int DEFAULT_MAX_REGEX_PATTERN_LENGTH = 1024;
    private static final int DEFAULT_MAX_REGEX_REPLACEMENT_LENGTH = 64 * 1024;
    private static final int HARD_MAX_GENERATED_STRING_LENGTH = 16 * 1024 * 1024;
    private static final int HARD_MAX_REGEX_INPUT_LENGTH = 4 * 1024 * 1024;
    private static final int HARD_MAX_REGEX_PATTERN_LENGTH = 8192;
    private static final int HARD_MAX_REGEX_REPLACEMENT_LENGTH = 1024 * 1024;
    private static final int MAX_DURATION_TEXT_LENGTH = 128;
    private static final Pattern NESTED_QUANTIFIER_PATTERN = Pattern.compile(
            "\\((?:[^()\\\\]|\\\\.|\\[[^\\]]*])*[+*](?:[^()\\\\]|\\\\.|\\[[^\\]]*])*\\)\\s*(?:[+*]|\\{)"
    );

    private PlainSelect selectSql;

    private SelectBody selectBody;

    private List<WithItem> withItemsList;

    private List<Column> selectColumns;

    private Map<String, Feature> features = null;

    private Map<String, Object> settings = null;

    private static final Object MAP_NULL_VALUE = new Object();

    static <T> void createCalculator(BiFunction<String, BiFunction<Number, Number, Object>, T> builder, Consumer<T> consumer) {

        consumer.accept(builder.apply("+", CalculateUtils::add));
        consumer.accept(builder.apply("-", CalculateUtils::subtract));
        consumer.accept(builder.apply("*", CalculateUtils::multiply));
        consumer.accept(builder.apply("/", CalculateUtils::division));
        consumer.accept(builder.apply("%", CalculateUtils::mod));
        consumer.accept(builder.apply("&", CalculateUtils::bitAnd));
        consumer.accept(builder.apply("|", CalculateUtils::bitOr));
        consumer.accept(builder.apply("^", CalculateUtils::bitMutex));
        consumer.accept(builder.apply("<<", CalculateUtils::leftShift));
        consumer.accept(builder.apply(">>", CalculateUtils::rightShift));
        consumer.accept(builder.apply(">>>", CalculateUtils::unsignedRightShift));
        consumer.accept(builder.apply("bit_left_shift", CalculateUtils::leftShift));
        consumer.accept(builder.apply("bit_right_shift", CalculateUtils::rightShift));
        consumer.accept(builder.apply("bit_unsigned_shift", CalculateUtils::unsignedRightShift));
        consumer.accept(builder.apply("bit_and", CalculateUtils::bitAnd));
        consumer.accept(builder.apply("bit_or", CalculateUtils::bitOr));
        consumer.accept(builder.apply("bit_mutex", CalculateUtils::bitMutex));

        consumer.accept(builder.apply("math.plus", CalculateUtils::add));
        consumer.accept(builder.apply("math.sub", CalculateUtils::subtract));
        consumer.accept(builder.apply("math.mul", CalculateUtils::multiply));
        consumer.accept(builder.apply("math.divi", CalculateUtils::division));
        consumer.accept(builder.apply("math.mod", CalculateUtils::mod));

        consumer.accept(builder.apply("math.atan2", (v1, v2) -> Math.atan2(v1.doubleValue(), v2.doubleValue())));
        consumer.accept(builder.apply("math.ieee_rem", (v1, v2) -> Math.IEEEremainder(v1.doubleValue(), v2.doubleValue())));
        consumer.accept(builder.apply("math.copy_sign", (v1, v2) -> Math.copySign(v1.doubleValue(), v2.doubleValue())));

    }


    private static void addCommonFunctionFeatures() {
        addGlobal(new FunctionMapFeature("round", 2, 1, stream -> stream.collectList().map(list -> {
            double value = CastUtils.castNumber(list.get(0)).doubleValue();
            if (list.size() == 1) {
                return Math.round(value);
            }
            int scale = CastUtils.castNumber(list.get(1)).intValue();
            double factor = Math.pow(10, scale);
            return Math.round(value * factor) / factor;
        })));
        addGlobal(new SingleParameterFunctionMapFeature("floor", v -> Math.floor(CastUtils.castNumber(v).doubleValue())));
        addGlobal(new SingleParameterFunctionMapFeature("ceil", v -> Math.ceil(CastUtils.castNumber(v).doubleValue())));
        addGlobal(new SingleParameterFunctionMapFeature("abs", v -> Math.abs(CastUtils.castNumber(v).doubleValue())));
        addGlobal(new SingleParameterFunctionMapFeature("sqrt", v -> Math.sqrt(CastUtils.castNumber(v).doubleValue())));
        addGlobal(new FunctionMapFeature("pow", 2, 2, stream -> stream.collectList().map(list ->
                Math.pow(CastUtils.castNumber(list.get(0)).doubleValue(), CastUtils.castNumber(list.get(1)).doubleValue()))));
        addGlobal(new FunctionMapFeature("power", 2, 2, stream -> stream.collectList().map(list ->
                Math.pow(CastUtils.castNumber(list.get(0)).doubleValue(), CastUtils.castNumber(list.get(1)).doubleValue()))));

        addGlobal(new SingleParameterFunctionMapFeature("lower", v -> String.valueOf(v).toLowerCase(Locale.ENGLISH)));
        addGlobal(new SingleParameterFunctionMapFeature("upper", v -> String.valueOf(v).toUpperCase(Locale.ENGLISH)));
        addGlobal(new SingleParameterFunctionMapFeature("length", v -> String.valueOf(v).length()));
        addGlobal(new SingleParameterFunctionMapFeature("char_length", v -> String.valueOf(v).length()));
        addGlobal(new SingleParameterFunctionMapFeature("trim", v -> String.valueOf(v).trim()));
        addGlobal(new SingleParameterFunctionMapFeature("ltrim", v -> String.valueOf(v).replaceAll("^\\s+", "")));
        addGlobal(new SingleParameterFunctionMapFeature("rtrim", v -> String.valueOf(v).replaceAll("\\s+$", "")));
        addGlobal(new FunctionMapFeature("replace", 3, 3, (metadata, stream) -> stream.collectList().map(list -> replaceText(functionLimits(metadata), list))));
        addGlobal(new FunctionMapFeature("substring", 3, 2, stream -> stream.collectList().map(DefaultReactorQLMetadata::substring)));
        addGlobal(new FunctionMapFeature("regexp_replace", 4, 3, (metadata, stream) -> stream.collectList().map(list -> regexpReplace(functionLimits(metadata), list))));
        addGlobal(new FunctionMapFeature("regexp_like", 3, 2, (metadata, stream) -> stream.collectList().map(list -> {
            FunctionLimits limits = functionLimits(metadata);
            String source = assertRegexInput(limits, list.get(0));
            Pattern pattern = compileRegex(limits, list.get(1), list.size() > 2 ? list.get(2) : null);
            return pattern.matcher(source).find();
        })));
        addGlobal(new FunctionMapFeature("regexp_extract", 3, 2, (metadata, stream) -> stream.collectList().flatMap(list -> Mono.justOrEmpty(regexpExtract(functionLimits(metadata), list)))));
        addGlobal(new FunctionMapFeature("regexp_substr", 3, 2, (metadata, stream) -> stream.collectList().flatMap(list -> Mono.justOrEmpty(regexpExtract(functionLimits(metadata), list)))));

        addGlobal(new FunctionMapFeature("left", 2, 2, stream -> stream.collectList().map(list -> strLeft(list.get(0), list.get(1)))));
        addGlobal(new FunctionMapFeature("str_left", 2, 2, stream -> stream.collectList().map(list -> strLeft(list.get(0), list.get(1)))));
        addGlobal(new FunctionMapFeature("right", 2, 2, stream -> stream.collectList().map(list -> strRight(list.get(0), list.get(1)))));
        addGlobal(new FunctionMapFeature("str_right", 2, 2, stream -> stream.collectList().map(list -> strRight(list.get(0), list.get(1)))));
        addGlobal(new FunctionMapFeature("split_part", 3, 3, stream -> stream.collectList().map(DefaultReactorQLMetadata::splitPart)));
        addGlobal(new FunctionMapFeature("starts_with", 2, 2, stream -> stream.collectList().map(list -> String.valueOf(list.get(0)).startsWith(String.valueOf(list.get(1))))));
        addGlobal(new FunctionMapFeature("ends_with", 2, 2, stream -> stream.collectList().map(list -> String.valueOf(list.get(0)).endsWith(String.valueOf(list.get(1))))));
        addGlobal(new FunctionMapFeature("str_contains", 2, 2, stream -> stream.collectList().flatMap(list -> Mono.justOrEmpty(stringContains(list))))
                          .defaultValue(MAP_NULL_VALUE));
        addGlobal(new FunctionMapFeature("contains", 2, 2, stream -> stream.collectList().flatMap(list -> Mono.justOrEmpty(stringContains(list))))
                          .defaultValue(MAP_NULL_VALUE));
        addGlobal(new FunctionMapFeature("strpos", 2, 2, stream -> stream.collectList().flatMap(list -> Mono.justOrEmpty(stringPosition(list))))
                          .defaultValue(MAP_NULL_VALUE));
        addGlobal(new FunctionMapFeature("position", 2, 2, stream -> stream.collectList().flatMap(list -> Mono.justOrEmpty(stringPosition(list))))
                          .defaultValue(MAP_NULL_VALUE));
        addGlobal(new FunctionMapFeature("instr", 2, 2, stream -> stream.collectList().flatMap(list -> Mono.justOrEmpty(stringPosition(list))))
                          .defaultValue(MAP_NULL_VALUE));
        addGlobal(new FunctionMapFeature("locate", 3, 2, stream -> stream.collectList().flatMap(list -> Mono.justOrEmpty(locateString(list))))
                          .defaultValue(MAP_NULL_VALUE));
        addGlobal(new FunctionMapFeature("concat_ws", 9999, 1, (metadata, stream) -> concatWs(functionLimits(metadata), stream))
                          .defaultValue(MAP_NULL_VALUE));
        addGlobal(new FunctionMapFeature("lpad", 3, 3, (metadata, stream) -> stream
                .collectList()
                .flatMap(list -> Mono.justOrEmpty(padText(functionLimits(metadata), list, true))))
                          .defaultValue(MAP_NULL_VALUE));
        addGlobal(new FunctionMapFeature("rpad", 3, 3, (metadata, stream) -> stream
                .collectList()
                .flatMap(list -> Mono.justOrEmpty(padText(functionLimits(metadata), list, false))))
                          .defaultValue(MAP_NULL_VALUE));
        addGlobal(new FunctionMapFeature("repeat", 2, 2, (metadata, stream) -> stream.collectList().map(list -> repeatText(functionLimits(metadata), list.get(0), list.get(1)))));
        addGlobal(new SingleParameterFunctionMapFeature("reverse", v -> new StringBuilder(String.valueOf(v)).reverse().toString()));

        addGlobal(new FunctionMapFeature("date_add", 3, 3, stream -> stream.collectList().map(DefaultReactorQLMetadata::dateAdd)));
        addGlobal(new FunctionMapFeature("date_sub", 3, 3, stream -> stream.collectList().map(list -> dateAdd(list, -1))));
        addGlobal(new FunctionMapFeature("date_diff", 3, 2, stream -> stream.collectList().map(DefaultReactorQLMetadata::dateDiff)));
        addGlobal(new FunctionMapFeature("datediff", 2, 2, stream -> stream.collectList().map(list -> dateDiff(Arrays.asList(list.get(0), list.get(1), "day")))));
        addGlobal(new FunctionMapFeature("date_trunc", 2, 2, stream -> stream.collectList().map(DefaultReactorQLMetadata::dateTrunc)));
        addGlobal(new FunctionMapFeature("time_bucket", 2, 2, stream -> stream.collectList().map(DefaultReactorQLMetadata::timeBucket)));
        addGlobal(new FunctionMapFeature("date_part", 2, 2, stream -> stream.collectList().map(DefaultReactorQLMetadata::datePart)));
        addGlobal(new FunctionMapFeature("extract", 2, 2, stream -> stream.collectList().map(DefaultReactorQLMetadata::datePart)));
        addGlobal(new FunctionMapFeature("unix_timestamp", 1, 0, stream -> stream.collectList().map(list -> {
            LocalDateTime time = list.isEmpty() ? LocalDateTime.now() : CastUtils.castLocalDateTime(list.get(0));
            return time.atZone(ZoneId.systemDefault()).toEpochSecond();
        })));
        addGlobal(new FunctionMapFeature("to_unixtime", 1, 1, stream -> stream.collectList().map(list -> toUnixTime(list.get(0)))));
        addGlobal(new FunctionMapFeature("to_millis", 1, 1, stream -> stream.collectList().map(list -> toMillis(list.get(0)))));
        addGlobal(new FunctionMapFeature("epoch_ms", 1, 1, stream -> stream.collectList().map(list -> toMillis(list.get(0)))));
        addGlobal(new FunctionMapFeature("from_unixtime", 2, 1, stream -> stream.collectList().map(DefaultReactorQLMetadata::fromUnixTime)));
        addGlobal(new FunctionMapFeature("to_iso_instant", 1, 1, stream -> stream.collectList().map(list -> toIsoInstant(list.get(0)))));
        addGlobal(new FunctionMapFeature("current_timestamp", 0, 0, stream -> Mono.just(LocalDateTime.now())));
        addGlobal(new FunctionMapFeature("current_date", 0, 0, stream -> Mono.just(LocalDate.now())));
        addGlobal(new FunctionMapFeature("current_time", 0, 0, stream -> Mono.just(LocalTime.now())));
        addGlobal(new FunctionMapFeature("greatest", 9999, 1, stream -> stream.as(CastUtils::flatStream).reduce((left, right) -> CompareUtils.compare(left, right) >= 0 ? left : right)));
        addGlobal(new FunctionMapFeature("least", 9999, 1, stream -> stream.as(CastUtils::flatStream).reduce((left, right) -> CompareUtils.compare(left, right) <= 0 ? left : right)));
    }

    private static void addJsonFunctionFeatures() {
        addGlobal(JsonPathFunctionMapFeature.jsonGet("json_get", 2, 3, false));
        addGlobal(JsonPathFunctionMapFeature.jsonGet("json_path", 2, 3, false));
        addGlobal(JsonPathFunctionMapFeature.jsonExtract("json_extract", 2, 999));
        addGlobal(JsonPathFunctionMapFeature.jsonGet("json_value", 2, 3, true));
        addGlobal(JsonPathFunctionMapFeature.jsonGet("json_query", 2, 2, false));
        addGlobal(JsonPathFunctionMapFeature.jsonExists("json_exists", 1, 2));
        Arrays.asList("json_extract_path", "jsonb_extract_path")
              .forEach(function -> addGlobal(JsonPathFunctionMapFeature.jsonExtractPath(function, 2, 999, false)));
        Arrays.asList("json_extract_path_text", "jsonb_extract_path_text")
              .forEach(function -> addGlobal(JsonPathFunctionMapFeature.jsonExtractPath(function, 2, 999, true)));
        addGlobal(JsonPathFunctionMapFeature.jsonContainsPath("json_contains_path", 3, 999));
        Arrays.asList("json_intersect", "json_intersection")
              .forEach(function -> addGlobal(JsonPathFunctionMapFeature.jsonIntersect(function, 2, 999)));
        addGlobal(JsonPathFunctionMapFeature.jsonUnion("json_union", 2, 999));
        Arrays.asList("json_diff", "json_except")
              .forEach(function -> addGlobal(JsonPathFunctionMapFeature.jsonDiff(function, 2, 999)));
        Arrays.asList("json_merge", "json_merge_preserve")
              .forEach(function -> addGlobal(JsonPathFunctionMapFeature.jsonMergePreserve(function, 2, 999)));
        addGlobal(JsonPathFunctionMapFeature.jsonMergePatch("json_merge_patch", 2, 999));
        addGlobal(JsonPathFunctionMapFeature.jsonUnquote("json_unquote", 1, 1));
        addGlobal(JsonPathFunctionMapFeature.jsonQuote("json_quote", 1, 1));
        addGlobal(JsonPathFunctionMapFeature.jsonDepth("json_depth", 1, 1));
        addGlobal(JsonPathFunctionMapFeature.jsonType("json_type", 1, 1, true));
        Arrays.asList("json_typeof", "jsonb_typeof")
              .forEach(function -> addGlobal(JsonPathFunctionMapFeature.jsonType(function, 1, 1, false)));
        addGlobal(JsonPathFunctionMapFeature.jsonValid("json_valid", 1, 1));
        addGlobal(JsonPathFunctionMapFeature.jsonLength("json_length", 1, 2));
        addGlobal(JsonPathFunctionMapFeature.jsonKeys("json_keys", 1, 2));
        Arrays.asList("json_array_length", "jsonb_array_length")
              .forEach(function -> addGlobal(JsonPathFunctionMapFeature.jsonArrayLength(function, 1, 1)));
        Arrays.asList("json_object_keys", "jsonb_object_keys")
              .forEach(function -> addGlobal(JsonPathFunctionMapFeature.jsonObjectKeys(function, 1, 1)));
        addGlobal(JsonPathFunctionMapFeature.toJson("to_json", 1, 1));
        addGlobal(JsonPathFunctionMapFeature.jsonContains("json_contains", 2, 3));
        addGlobal(JsonPathFunctionMapFeature.jsonOverlaps("json_overlaps", 2, 2));
        Arrays.asList("json_equal", "json_equals")
              .forEach(function -> addGlobal(JsonPathFunctionMapFeature.jsonEqual(function, 2, 2)));
        Arrays.asList("json_array", "json_build_array", "jsonb_build_array")
              .forEach(function -> addGlobal(JsonPathFunctionMapFeature.jsonArray(function, 0, 999)));
        Arrays.asList("json_object", "json_build_object", "jsonb_build_object")
              .forEach(function -> addGlobal(JsonPathFunctionMapFeature.jsonObject(function, 0, 999)));
    }

    private static Object substring(List<Object> list) {
        String source = String.valueOf(list.get(0));
        int start = CastUtils.castNumber(list.get(1)).intValue();
        int begin = start > 0 ? start - 1 : source.length() + start;
        if (begin < 0 || begin >= source.length()) {
            return "";
        }
        int end = list.size() > 2 ? Math.min(source.length(), begin + CastUtils.castNumber(list.get(2)).intValue()) : source.length();
        return source.substring(begin, Math.max(begin, end));
    }

    private static Object replaceText(FunctionLimits limits, List<Object> list) {
        String source = String.valueOf(list.get(0));
        String search = String.valueOf(list.get(1));
        String replacement = String.valueOf(list.get(2));
        assertTextLength("replace source", source, limits.maxGeneratedStringLength);
        assertTextLength("replace replacement", replacement, limits.maxGeneratedStringLength);

        int matchCount = countMatches(source, search);
        long length = search.isEmpty()
                ? (long) source.length() + (long) (source.length() + 1) * replacement.length()
                : (long) source.length() + (long) matchCount * (replacement.length() - search.length());
        assertGeneratedStringLength(limits, "replace result", length);
        return source.replace(search, replacement);
    }

    private static Object regexpReplace(FunctionLimits limits, List<Object> list) {
        String source = assertRegexInput(limits, list.get(0));
        Pattern pattern = compileRegex(limits, list.get(1), list.size() > 3 ? list.get(3) : null);
        String replacement = String.valueOf(list.get(2));
        assertTextLength("regexp replacement", replacement, limits.maxRegexReplacementLength);

        Matcher matcher = pattern.matcher(source);
        StringBuffer buffer = new StringBuffer(Math.min(source.length(), limits.maxGeneratedStringLength));
        try {
            while (matcher.find()) {
                matcher.appendReplacement(buffer, replacement);
                assertGeneratedStringLength(limits, "regexp_replace result", buffer.length());
            }
            matcher.appendTail(buffer);
        } catch (IllegalArgumentException | IndexOutOfBoundsException e) {
            // JDK 会把 "$9" 这类非法分组引用抛成 IndexOutOfBoundsException，统一收敛为查询参数错误。
            throw ReactorQLException.builder(ReactorQLException.INVALID_ARGUMENT)
                    .reason("regexp_replace replacement 参数无效: " + e.getMessage())
                    .suggestion("使用普通替换文本或合法捕获组引用，例如 $1；如果要输出美元符号请按 Java 正则替换规则转义。")
                    .example("select regexp_replace(name, 'dev-(\\\\d+)', 'device-$1') name from test")
                    .cause(e)
                    .build();
        }
        assertGeneratedStringLength(limits, "regexp_replace result", buffer.length());
        return buffer.toString();
    }

    private static Object regexpExtract(FunctionLimits limits, List<Object> list) {
        String source = assertRegexInput(limits, list.get(0));
        Pattern pattern = compileRegex(limits, list.get(1), null);
        Matcher matcher = pattern.matcher(source);
        if (!matcher.find()) {
            return null;
        }
        int group = list.size() > 2 ? CastUtils.castNumber(list.get(2)).intValue() : (matcher.groupCount() > 0 ? 1 : 0);
        if (group < 0 || group > matcher.groupCount()) {
            return null;
        }
        return matcher.group(group);
    }

    private static String repeatText(FunctionLimits limits, Object source, Object times) {
        String text = String.valueOf(source);
        long count = Math.max(0, CastUtils.castNumber(times).longValue());
        if (text.isEmpty() || count == 0) {
            return "";
        }
        long length = (long) text.length() * count;
        assertGeneratedStringLength(limits, "repeat result", length);

        StringBuilder builder = new StringBuilder((int) length);
        for (long i = 0; i < count; i++) {
            builder.append(text);
        }
        return builder.toString();
    }

    private static Pattern compileRegex(FunctionLimits limits, Object patternValue, Object flagValue) {
        String pattern = String.valueOf(patternValue);
        assertTextLength("regexp pattern", pattern, limits.maxRegexPatternLength);
        assertSafeRegexPattern(pattern);
        int flags = flagValue != null && String.valueOf(flagValue).toLowerCase(Locale.ENGLISH).contains("i")
                ? Pattern.CASE_INSENSITIVE
                : 0;
        try {
            return Pattern.compile(pattern, flags);
        } catch (RuntimeException e) {
            throw ReactorQLException.builder(ReactorQLException.INVALID_ARGUMENT)
                    .reason("正则表达式无法编译: " + e.getMessage())
                    .suggestion("检查括号、字符组和转义符是否完整；避免使用会触发灾难性回溯的嵌套量词。")
                    .example("select regexp_like(name, '^dev-[0-9]+$') matched from test")
                    .cause(e)
                    .build();
        }
    }

    private static String assertRegexInput(FunctionLimits limits, Object value) {
        String source = String.valueOf(value);
        assertTextLength("regexp input", source, limits.maxRegexInputLength);
        return source;
    }

    private static void assertSafeRegexPattern(String pattern) {
        // 拒绝典型嵌套量词，避免短正则也能触发灾难性回溯导致执行线程长时间占用。
        if (NESTED_QUANTIFIER_PATTERN.matcher(pattern).find()) {
            throw ReactorQLException.invalidArgument(
                    "正则表达式包含高风险嵌套量词: " + pattern,
                    "改用更明确的字符范围或非贪婪表达式，避免 (a+)+、(.*){...} 这类可能导致灾难性回溯的写法。",
                    "select regexp_like(name, '^[a-z0-9_-]+$') matched from test"
            );
        }
    }

    private static int countMatches(String source, String search) {
        if (search.isEmpty()) {
            return source.length() + 1;
        }
        int count = 0;
        int from = 0;
        while (true) {
            int index = source.indexOf(search, from);
            if (index < 0) {
                return count;
            }
            count++;
            from = index + search.length();
        }
    }

    private static void assertTextLength(String name, String text, int max) {
        if (text.length() > max) {
            throw ReactorQLException.invalidArgument(
                    name + " 超过最大长度: " + text.length() + ", max=" + max,
                    "缩短输入文本，或在可信场景下通过 ReactorQLMetadata setting 调整对应上限；实现仍会保留硬上限。",
                    "ReactorQL.builder().setting(DefaultReactorQLMetadata.SETTING_MAX_REGEX_INPUT_LENGTH, 262144)"
            );
        }
    }

    private static void assertGeneratedStringLength(FunctionLimits limits, String name, long length) {
        if (length > limits.maxGeneratedStringLength) {
            throw ReactorQLException.invalidArgument(
                    name + " 结果长度超过最大限制: " + length + ", max=" + limits.maxGeneratedStringLength,
                    "减少 repeat/lpad/rpad/concat_ws/replace 的输出长度，或在可信场景下通过 function.maxGeneratedStringLength 调整上限。",
                    "select lpad(code, 8, '0') code from test"
            );
        }
    }

    private static FunctionLimits functionLimits(ReactorQLMetadata metadata) {
        return new FunctionLimits(
                intSetting(metadata, SETTING_MAX_GENERATED_STRING_LENGTH, DEFAULT_MAX_GENERATED_STRING_LENGTH, HARD_MAX_GENERATED_STRING_LENGTH),
                intSetting(metadata, SETTING_MAX_REGEX_INPUT_LENGTH, DEFAULT_MAX_REGEX_INPUT_LENGTH, HARD_MAX_REGEX_INPUT_LENGTH),
                intSetting(metadata, SETTING_MAX_REGEX_PATTERN_LENGTH, DEFAULT_MAX_REGEX_PATTERN_LENGTH, HARD_MAX_REGEX_PATTERN_LENGTH),
                intSetting(metadata, SETTING_MAX_REGEX_REPLACEMENT_LENGTH, DEFAULT_MAX_REGEX_REPLACEMENT_LENGTH, HARD_MAX_REGEX_REPLACEMENT_LENGTH)
        );
    }

    private static int intSetting(ReactorQLMetadata metadata, String key, int defaultValue, int hardMax) {
        if (metadata == null) {
            return defaultValue;
        }
        long value = metadata
                .getSetting(key)
                .map(CastUtils::castNumber)
                .map(Number::longValue)
                .orElse((long) defaultValue);
        // SQL hint 也能写入 metadata settings，因此必须保留硬上限，避免查询侧关闭资源保护。
        if (value <= 0 || value > hardMax) {
            throw ReactorQLException.invalidArgument(
                    "非法 setting[" + key + "]: " + value + ", hardMax=" + hardMax,
                    "确认 setting 为正整数且不超过硬上限；不可信 SQL 不允许把资源限制调成无界。",
                    "select /*+ " + key + "(1024) */ * from test"
            );
        }
        return (int) value;
    }

    private static final class FunctionLimits {
        private final int maxGeneratedStringLength;
        private final int maxRegexInputLength;
        private final int maxRegexPatternLength;
        private final int maxRegexReplacementLength;

        private FunctionLimits(int maxGeneratedStringLength,
                               int maxRegexInputLength,
                               int maxRegexPatternLength,
                               int maxRegexReplacementLength) {
            this.maxGeneratedStringLength = maxGeneratedStringLength;
            this.maxRegexInputLength = maxRegexInputLength;
            this.maxRegexPatternLength = maxRegexPatternLength;
            this.maxRegexReplacementLength = maxRegexReplacementLength;
        }
    }


    private static String strLeft(Object source, Object length) {
        String text = String.valueOf(source);
        int len = Math.max(0, CastUtils.castNumber(length).intValue());
        return text.substring(0, Math.min(text.length(), len));
    }

    private static String strRight(Object source, Object length) {
        String text = String.valueOf(source);
        int len = Math.max(0, CastUtils.castNumber(length).intValue());
        return len >= text.length() ? text : text.substring(text.length() - len);
    }

    private static Object stringContains(List<Object> list) {
        if (isFunctionNull(list.get(0)) || isFunctionNull(list.get(1))) {
            return null;
        }
        return String.valueOf(list.get(0)).contains(String.valueOf(list.get(1)));
    }

    private static Object stringPosition(List<Object> list) {
        if (isFunctionNull(list.get(0)) || isFunctionNull(list.get(1))) {
            return null;
        }
        return stringPosition(list.get(0), list.get(1));
    }

    private static int stringPosition(Object source, Object search) {
        return String.valueOf(source).indexOf(String.valueOf(search)) + 1;
    }

    private static Object locateString(List<Object> list) {
        if (isFunctionNull(list.get(0)) || isFunctionNull(list.get(1)) || (list.size() > 2 && isFunctionNull(list.get(2)))) {
            return null;
        }
        String search = String.valueOf(list.get(0));
        String source = String.valueOf(list.get(1));
        int from = list.size() > 2 ? CastUtils.castNumber(list.get(2)).intValue() - 1 : 0;
        if (from < 0 || from > source.length()) {
            return 0;
        }
        return source.indexOf(search, from) + 1;
    }

    private static Publisher<Object> concatWs(FunctionLimits limits, Flux<Object> stream) {
        return CastUtils.handleFirst(stream, (first, flux) -> {
            if (isFunctionNull(first)) {
                return Mono.empty();
            }
            String separator = String.valueOf(first);
            return flux
                    .skip(1)
                    .as(CastUtils::flatStream)
                    .filter(value -> !isFunctionNull(value))
                    .map(String::valueOf)
                    .collect(() -> new BoundedStringJoiner(limits, separator, "concat_ws result"), BoundedStringJoiner::add)
                    .map(BoundedStringJoiner::toString);
        });
    }

    private static Object padText(FunctionLimits limits, List<Object> list, boolean left) {
        Object sourceValue = list.get(0);
        Object lengthValue = list.get(1);
        Object padValue = list.get(2);
        if (isFunctionNull(sourceValue) || isFunctionNull(lengthValue) || isFunctionNull(padValue)) {
            return null;
        }
        String source = String.valueOf(sourceValue);
        long targetLength = CastUtils.castNumber(lengthValue).longValue();
        if (targetLength <= 0) {
            return "";
        }
        assertGeneratedStringLength(limits, left ? "lpad result" : "rpad result", targetLength);

        int length = (int) targetLength;
        if (source.length() >= length) {
            return source.substring(0, length);
        }

        String pad = String.valueOf(padValue);
        if (pad.isEmpty()) {
            return null;
        }
        int paddingLength = length - source.length();
        String padding = repeatPad(pad, paddingLength);
        return left ? padding.concat(source) : source.concat(padding);
    }

    private static String repeatPad(String pad, int length) {
        StringBuilder builder = new StringBuilder(length);
        while (builder.length() < length) {
            int remaining = length - builder.length();
            if (pad.length() <= remaining) {
                builder.append(pad);
            } else {
                builder.append(pad, 0, remaining);
            }
        }
        return builder.toString();
    }

    private static final class BoundedStringJoiner {
        private final FunctionLimits limits;
        private final String separator;
        private final String resultName;
        private final StringBuilder builder = new StringBuilder();
        private boolean hasValue;
        private long length;

        private BoundedStringJoiner(FunctionLimits limits, String separator, String resultName) {
            this.limits = limits;
            this.separator = separator;
            this.resultName = resultName;
        }

        private void add(String value) {
            long appendedLength = value.length();
            if (hasValue) {
                appendedLength += separator.length();
            }
            length += appendedLength;
            assertGeneratedStringLength(limits, resultName, length);
            if (hasValue) {
                builder.append(separator);
            }
            builder.append(value);
            hasValue = true;
        }

        @Override
        public String toString() {
            return builder.toString();
        }
    }

    private static boolean isFunctionNull(Object value) {
        return value == null || MAP_NULL_VALUE.equals(value);
    }

    private static Object splitPart(List<Object> list) {
        String source = String.valueOf(list.get(0));
        String delimiter = String.valueOf(list.get(1));
        int index = CastUtils.castNumber(list.get(2)).intValue();
        if (index == 0) {
            return "";
        }
        if (delimiter.isEmpty()) {
            return Math.abs(index) == 1 ? source : "";
        }
        if (index > 0) {
            return splitPartFromStart(source, delimiter, index);
        }
        return splitPartFromEnd(source, delimiter, -index);
    }

    private static String splitPartFromStart(String source, String delimiter, int index) {
        int start = 0;
        for (int current = 1; current < index; current++) {
            int next = source.indexOf(delimiter, start);
            if (next < 0) {
                return "";
            }
            start = next + delimiter.length();
        }
        int end = source.indexOf(delimiter, start);
        return end < 0 ? source.substring(start) : source.substring(start, end);
    }

    private static String splitPartFromEnd(String source, String delimiter, int indexFromEnd) {
        int end = source.length();
        for (int current = 1; current < indexFromEnd; current++) {
            int previous = source.lastIndexOf(delimiter, end - delimiter.length());
            if (previous < 0) {
                return "";
            }
            end = previous;
        }
        int start = source.lastIndexOf(delimiter, end - delimiter.length());
        return source.substring(start < 0 ? 0 : start + delimiter.length(), end);
    }

    private static Object dateAdd(List<Object> list) {
        return dateAdd(list, 1);
    }

    private static Object dateAdd(List<Object> list, int direction) {
        LocalDateTime time = CastUtils.castLocalDateTime(list.get(0));
        long amount = CastUtils.castNumber(list.get(1)).longValue() * direction;
        if ("quarter".equals(dateUnitName(list.get(2)))) {
            return time.plusMonths(Math.multiplyExact(amount, 3L));
        }
        return time.plus(amount, chronoUnit(list.get(2)));
    }

    private static Object dateDiff(List<Object> list) {
        LocalDateTime left = CastUtils.castLocalDateTime(list.get(0));
        LocalDateTime right = CastUtils.castLocalDateTime(list.get(1));
        if (list.size() > 2 && "quarter".equals(dateUnitName(list.get(2)))) {
            return ChronoUnit.MONTHS.between(right, left) / 3;
        }
        ChronoUnit unit = list.size() > 2 ? chronoUnit(list.get(2)) : ChronoUnit.DAYS;
        return unit.between(right, left);
    }

    private static Object dateTrunc(List<Object> list) {
        String unit = dateUnitName(list.get(0));
        LocalDateTime time = CastUtils.castLocalDateTime(list.get(1));
        switch (unit) {
            case "year":
                return LocalDateTime.of(time.getYear(), 1, 1, 0, 0);
            case "quarter": {
                int month = ((time.getMonthValue() - 1) / 3) * 3 + 1;
                return LocalDateTime.of(time.getYear(), month, 1, 0, 0);
            }
            case "month":
                return LocalDateTime.of(time.getYear(), time.getMonthValue(), 1, 0, 0);
            case "week":
                return time.minusDays(time.getDayOfWeek().getValue() - 1).toLocalDate().atStartOfDay();
            case "day":
                return time.toLocalDate().atStartOfDay();
            case "hour":
                return time.truncatedTo(ChronoUnit.HOURS);
            case "minute":
                return time.truncatedTo(ChronoUnit.MINUTES);
            case "second":
                return time.truncatedTo(ChronoUnit.SECONDS);
            case "millisecond":
                return time.truncatedTo(ChronoUnit.MILLIS);
            case "microsecond":
                return time.truncatedTo(ChronoUnit.MICROS);
            default:
                throw ReactorQLException.invalidArgument(
                        "date_trunc 不支持的时间单位: " + list.get(0),
                        "使用 year、quarter、month、week、day、hour、minute、second、millisecond 或 microsecond。",
                        "select date_trunc('minute', timestamp) ts from test"
                );
        }
    }

    private static Object timeBucket(List<Object> list) {
        long bucketMillis = durationMillis(list.get(0));
        if (bucketMillis <= 0) {
            throw ReactorQLException.invalidArgument(
                    "time_bucket interval 必须大于 0: " + list.get(0),
                    "使用正数毫秒或 Duration 表达式，例如 60000、'1m'、'15 minutes'、'PT1M'。",
                    "select time_bucket('1m', timestamp) ts from test"
            );
        }
        long epochMillis = CastUtils.castDate(list.get(1)).getTime();
        long bucketStart = epochMillis - Math.floorMod(epochMillis, bucketMillis);
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(bucketStart), ZoneId.systemDefault());
    }

    private static long durationMillis(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        String text = String.valueOf(value).trim();
        if (text.isEmpty()) {
            throw ReactorQLException.invalidArgument(
                    "Duration 参数不能为空",
                    "使用正数毫秒或 Duration 表达式，例如 60000、'1m'、'15 minutes'、'PT1M'。",
                    "select time_bucket('1m', timestamp) ts from test"
            );
        }
        if (text.length() > MAX_DURATION_TEXT_LENGTH) {
            throw ReactorQLException.invalidArgument(
                    "Duration 参数过长: " + text.length(),
                    "限制 Duration 字面量长度，避免恶意构造的超长参数消耗解析资源。",
                    "select time_bucket('15m', timestamp) ts from test"
            );
        }
        try {
            return CastUtils.castNumber(text).longValue();
        } catch (RuntimeException ignore) {
            // 非纯数字时按 Duration 表达式继续解析，支持 1m、15 minutes、PT1M 等常见写法。
        }
        if (text.startsWith("P") || text.startsWith("-P")) {
            return Duration.parse(text).toMillis();
        }
        String normalized = text.toLowerCase(Locale.ENGLISH).replace(" ", "");
        normalized = normalized
                .replace("milliseconds", "ms")
                .replace("millisecond", "ms")
                .replace("millis", "ms")
                .replace("minutes", "m")
                .replace("minute", "m")
                .replace("mins", "m")
                .replace("min", "m")
                .replace("seconds", "s")
                .replace("second", "s")
                .replace("secs", "s")
                .replace("sec", "s")
                .replace("hours", "h")
                .replace("hour", "h")
                .replace("hrs", "h")
                .replace("hr", "h")
                .replace("days", "d")
                .replace("day", "d")
                .replace("weeks", "w")
                .replace("week", "w");
        return CastUtils.parseDuration(normalized).toMillis();
    }

    private static Object fromUnixTime(List<Object> list) {
        LocalDateTime time = LocalDateTime.ofInstant(
                Instant.ofEpochSecond(CastUtils.castNumber(list.get(0)).longValue()),
                ZoneId.systemDefault()
        );
        if (list.size() < 2 || list.get(1) == null) {
            return time;
        }
        return DateTimeFormatter.ofPattern(String.valueOf(list.get(1))).format(time);
    }

    private static String toIsoInstant(Object value) {
        return CastUtils.castDate(value).toInstant().toString();
    }

    private static Double toUnixTime(Object value) {
        return CastUtils.castDate(value).getTime() / 1000D;
    }

    private static Long toMillis(Object value) {
        return CastUtils.castDate(value).getTime();
    }

    private static Object datePart(List<Object> list) {
        String part = dateUnitName(list.get(0));
        LocalDateTime time = CastUtils.castLocalDateTime(list.get(1));
        switch (part) {
            case "year":
            case "yy":
            case "yyyy":
                return time.getYear();
            case "quarter":
                return time.get(IsoFields.QUARTER_OF_YEAR);
            case "month":
            case "mon":
            case "mm":
                return time.getMonthValue();
            case "week":
                return time.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
            case "day":
            case "dd":
                return time.getDayOfMonth();
            case "hour":
            case "hh":
                return time.getHour();
            case "minute":
            case "mi":
                return time.getMinute();
            case "second":
            case "ss":
                return time.getSecond();
            case "dow":
            case "dayofweek":
                return time.getDayOfWeek().getValue();
            case "doy":
            case "dayofyear":
                return time.getDayOfYear();
            case "millisecond":
                return time.getSecond() * 1_000 + time.getNano() / 1_000_000;
            case "microsecond":
                return time.getSecond() * 1_000_000 + time.getNano() / 1_000;
            case "epoch":
                return time.atZone(ZoneId.systemDefault()).toEpochSecond();
            default:
                throw ReactorQLException.invalidArgument(
                        "date_part 不支持的时间字段: " + part,
                        "使用 year、quarter、month、week、day、hour、minute、second、dow、doy、epoch、millisecond 或 microsecond。",
                        "select date_part('hour', timestamp) hour from test"
                );
        }
    }

    private static String dateUnitName(Object unit) {
        String name = String.valueOf(unit).toLowerCase(Locale.ENGLISH);
        switch (name) {
            case "years":
            case "yy":
            case "yyyy":
                return "year";
            case "months":
            case "mon":
            case "mm":
                return "month";
            case "quarters":
            case "quarter":
            case "qq":
            case "q":
                return "quarter";
            case "weeks":
                return "week";
            case "days":
            case "dd":
                return "day";
            case "hours":
            case "hh":
                return "hour";
            case "minutes":
            case "mi":
                return "minute";
            case "seconds":
            case "ss":
                return "second";
            case "milliseconds":
            case "millis":
            case "milli":
            case "ms":
                return "millisecond";
            case "microseconds":
            case "micros":
            case "micro":
            case "us":
                return "microsecond";
            default:
                return name;
        }
    }

    private static ChronoUnit chronoUnit(Object unit) {
        String name = dateUnitName(unit);
        switch (name) {
            case "year":
            case "years":
            case "yy":
            case "yyyy":
                return ChronoUnit.YEARS;
            case "month":
            case "months":
            case "mon":
            case "mm":
                return ChronoUnit.MONTHS;
            case "week":
            case "weeks":
                return ChronoUnit.WEEKS;
            case "day":
            case "days":
            case "dd":
                return ChronoUnit.DAYS;
            case "hour":
            case "hours":
            case "hh":
                return ChronoUnit.HOURS;
            case "minute":
            case "minutes":
            case "mi":
                return ChronoUnit.MINUTES;
            case "second":
            case "seconds":
            case "ss":
                return ChronoUnit.SECONDS;
            case "millisecond":
                return ChronoUnit.MILLIS;
            case "microsecond":
                return ChronoUnit.MICROS;
            default:
                throw ReactorQLException.invalidArgument(
                        "不支持的日期单位: " + unit,
                        "使用 year、quarter、month、week、day、hour、minute、second、millisecond 或 microsecond。",
                        "select date_add(timestamp, 1, 'day') nextDay from test"
                );
        }
    }

    static {
        addCommonFunctionFeatures();
        addJsonFunctionFeatures();
        //distinct
        addGlobal(new DefaultDistinctFeature());
        //from ()
        addGlobal(new SubSelectFromFeature());
        //from table
        addGlobal(new FromTableFeature());
        //from zip((select * from a),(select * from b))
        addGlobal(new ZipSelectFeature());
        //from combine((select * from a),(select * from b))
        addGlobal(new CombineSelectFeature());
        //from merge_by_key((select * from a),(select * from b),'timestamp')
        addGlobal(new MergeByKeyFeature());
        //from (values())
        addGlobal(new FromValuesFeature());
        //select collect_list(value)
        addGlobal(new CollectListAggFeature());
        //PropertyFeature
        addGlobal(DefaultPropertyFeature.GLOBAL);
        //select val value
        addGlobal(new PropertyMapFeature());
        //select count()
        addGlobal(new CountAggFeature());
        //select case when
        addGlobal(new CaseMapFeature());
        //select (select id from xx) val
        addGlobal(new SelectFeature());
        //select if(value>1,true,false)
        addGlobal(new IfValueMapFeature());


        {
            EqualsFilter eq = new EqualsFilter("=", false);
            addGlobal(eq);
            addGlobal(new BinaryMapFeature("eq", eq::test));
        }

        {
            EqualsFilter neq = new EqualsFilter("!=", true);

            addGlobal(neq);
            addGlobal(new EqualsFilter("<>", true));
            addGlobal(new BinaryMapFeature("neq", neq::test));
        }

        //where name like
        addGlobal(new LikeFilter());

        //select str_like('a','%b%')
        addGlobal(new BinaryMapFeature("str_like", (left, right) -> LikeFilter.doTest(false, left, right)));
        //select str_nlike('a','%b%')
        addGlobal(new BinaryMapFeature("str_nlike", (left, right) -> LikeFilter.doTest(true, left, right)));


        addGlobal(new FunctionMapFeature("year", 1, 1, args -> args
                .map(val -> CastUtils.castLocalDateTime(val).getYear())));
        addGlobal(new FunctionMapFeature("month", 1, 1, args -> args
                .map(val -> CastUtils.castLocalDateTime(val).getMonthValue())));
        addGlobal(new FunctionMapFeature("day_of_month", 1, 1, args -> args
                .map(val -> CastUtils.castLocalDateTime(val).getDayOfMonth())));
        addGlobal(new FunctionMapFeature("day_of_year", 1, 1, args -> args
                .map(val -> CastUtils.castLocalDateTime(val).getDayOfYear())));
        addGlobal(new FunctionMapFeature("day_of_week", 1, 1, args -> args
                .map(val -> CastUtils.castLocalDateTime(val).getDayOfWeek().getValue())));
        addGlobal(new FunctionMapFeature("hour", 1, 1, args -> args
                .map(val -> CastUtils.castLocalDateTime(val).getHour())));
        addGlobal(new FunctionMapFeature("minute", 1, 1, args -> args
                .map(val -> CastUtils.castLocalDateTime(val).getMinute())));
        addGlobal(new FunctionMapFeature("second", 1, 1, args -> args
                .map(val -> CastUtils.castLocalDateTime(val).getSecond())));
        addGlobal(new FunctionMapFeature("choose", 99999, 1, args ->
                CastUtils.handleFirst(args, (first, flux) -> {
                    int index = CastUtils.castNumber(first).intValue();
                    if (index <= 0) {
                        return Mono.empty();
                    }
                    return flux
                            .skip(index)
                            .take(1)
                            .singleOrEmpty();
                })));


        {
            GreaterTanFilter gt = new GreaterTanFilter(">");
            addGlobal(gt);
            addGlobal(new BinaryMapFeature("gt", gt::test));
        }
        {
            GreaterEqualsTanFilter gte = new GreaterEqualsTanFilter(">=");
            addGlobal(gte);
            addGlobal(new BinaryMapFeature("gte", gte::test));
        }
        {
            LessTanFilter lt = new LessTanFilter("<");
            addGlobal(lt);
            addGlobal(new BinaryMapFeature("lt", lt::test));
        }

        {
            LessEqualsTanFilter lte = new LessEqualsTanFilter("<=");
            addGlobal(lte);
            addGlobal(new BinaryMapFeature("lte", lte::test));
        }


        addGlobal(new AndFilter());
        addGlobal(new OrFilter());
        addGlobal(new BetweenFilter());


//        addGlobal(new RangeFilter());
        addGlobal(new InFilter());

        //select now()
        addGlobal(new NowFeature());
        //select cast(val as string )
        addGlobal(new CastFeature());
        //select
        addGlobal(new DateFormatFeature());
        addGlobal(new DateFormatFeature("format_datetime"));
        addGlobal(new DateFormatFeature("dateformat"));

        // group by interval('1s')
        addGlobal(new GroupByIntervalFeature());
        // group by take(10)
        addGlobal(new GroupByTakeFeature());
        //按分组支持

        Arrays.asList(
                //group by property('this.val[0]')
                "property",
                //group by concat(val,val2)
                "concat",
                //group by val||val2
                "||",
                //group by ceil(val)
                "ceil",
                //group by round(val)
                "round",
                //group by floor(val)
                "floor",
                //group by date_format(val,'yyyy')
                "date_format",
                "format_datetime",
                "dateformat",
                //group by cast(val as int)
                "cast",
                "lower", "upper", "length", "char_length", "trim", "ltrim", "rtrim", "replace", "substring",
                "regexp_replace", "regexp_extract", "regexp_substr",
                "left", "str_left", "right", "str_right", "split_part", "starts_with", "ends_with", "str_contains",
                "contains", "strpos", "position", "instr", "locate", "concat_ws", "lpad", "rpad", "repeat", "reverse",
                "abs", "sqrt", "pow", "power", "greatest", "least",
                "date_add", "date_sub", "date_diff", "datediff", "date_trunc", "time_bucket", "date_part", "extract",
                "unix_timestamp", "to_unixtime", "to_millis", "epoch_ms", "from_unixtime", "to_iso_instant",
                "json_get", "json_path", "json_extract", "json_value", "json_query", "json_exists",
                "json_extract_path", "json_extract_path_text", "jsonb_extract_path", "jsonb_extract_path_text",
                "json_unquote", "json_quote", "json_depth", "json_type", "json_typeof", "jsonb_typeof", "json_valid", "json_length", "json_keys",
                "json_array_length", "jsonb_array_length", "json_object_keys", "jsonb_object_keys", "to_json"
        ).forEach(type -> addGlobal(new GroupByValueFeature(type)));

        //group by _window(10)
        addGlobal(new GroupByWindowFeature());

        // group by a+1
        createCalculator(GroupByCalculateBinaryFeature::new, DefaultReactorQLMetadata::addGlobal);
        // select val+10
        createCalculator(BinaryCalculateMapFeature::new, DefaultReactorQLMetadata::addGlobal);

        //coalesce
        addGlobal(new CoalesceMapFeature());

        //concat
        BiFunction<Object, Object, Object> concat = (left, right) -> {
            if (left == null) left = "";
            if (right == null) right = "";
            return String.valueOf(left).concat(String.valueOf(right));
        };
        //select value||'s'
        addGlobal(new BinaryMapFeature("||", concat));
        //select concat(value,'s')
        addGlobal(new FunctionMapFeature("concat", 9999, 1, stream -> stream
                .as(CastUtils::flatStream)
                .map(String::valueOf)
                .collect(Collectors.joining())));
        //substr
        addGlobal(new FunctionMapFeature("substr", 3, 2, stream -> stream
                .collectList()
                .filter(l -> l.size() >= 2)
                .map(list -> {
                    String str = String.valueOf(list.get(0));
                    int start = CastUtils.castNumber(list.get(1)).intValue();
                    int length = list.size() == 2 ? str.length() : CastUtils.castNumber(list.get(2)).intValue();

                    if (start < 0) {
                        start = str.length() + start;
                    }

                    if (start < 0 || str.length() < start) {
                        return "";
                    }
                    int endIndex = start + length;
                    if (str.length() < endIndex) {
                        return str.substring(start);
                    }
                    return str.substring(start, start + length);

                })));

        addGlobal(new FunctionMapFeature("isnull", 9999, 1, stream -> BooleanUtils.not(stream.hasElements())));
        addGlobal(new FunctionMapFeature("notnull", 9999, 1, Flux::hasElements));

        addGlobal(new FunctionMapFeature("all_match", 9999, 1, stream -> stream.all(CastUtils::castBoolean))
                          .defaultValue(Boolean.FALSE));
        addGlobal(new FunctionMapFeature("any_match", 9999, 1, stream -> stream.any(CastUtils::castBoolean)));

        {
            BiFunction<Flux<Object>, BiFunction<Collection<Object>, Flux<Object>, Publisher<?>>, Flux<?>> containsHandler =
                    (stream, handler) -> CastUtils
                            .handleFirst(stream, (first, flux) -> {
                                             TreeSet<Object> set =
                                                     CastUtils.castCollection(first, new TreeSet<>(CompareUtils::compare));

                                             return handler.apply(set, flux
                                                     .skip(1)
                                                     .as(CastUtils::flatStream));
                                         }
                            );
            //select contains_all(val,'a','b','c')
            addGlobal(
                    new FunctionMapFeature(
                            "contains_all",
                            999,
                            2,
                            stream -> CastUtils
                                .handleFirst(stream, (first, flux) -> {
                                                 TreeSet<Object> set =
                                                     CastUtils.castCollection(first, new TreeSet<>(CompareUtils::compare));

                                                 return flux
                                                     .skip(1)
                                                     .concatMap(val -> Flux
                                                         .just(val)
                                                         .as(CastUtils::flatStream)
                                                         .map(_val -> handleContain(set, _val))
                                                         // 如果是空数组，则contains_all结果为true
                                                         .defaultIfEmpty(true))
                                                     // 参数为空，返回false
                                                     .defaultIfEmpty(false)
                                                     .all(Boolean::booleanValue);
                                             }
                                )));

            //select not_contains(val,'a','b','c')
            addGlobal(
                    new FunctionMapFeature(
                            "not_contains",
                            999,
                            2,
                            stream -> containsHandler
                                    .apply(stream, (left, data) -> data
                                            .any(val -> handleContain(left, val))
                                            .as(BooleanUtils::not))));

            //select contains_any(val,'a','b','c')
            addGlobal(
                    new FunctionMapFeature(
                            "contains_any",
                            999,
                            2,
                            stream -> containsHandler
                                    .apply(stream, (left, data) -> data.any(val -> handleContain(left, val)))));
        }


        //select in(val,'a','b','c')
        addGlobal(
                new FunctionMapFeature(
                        "in",
                        9999,
                        2,
                        stream -> CastUtils
                                .handleFirst(stream, (first, flux) -> flux
                                        .skip(1)
                                        .as(CastUtils::flatStream)
                                        .any(v -> CompareUtils.equals(v, first))))
        );

        //select nin(val,'a','b','c')
        addGlobal(
                new FunctionMapFeature(
                        "nin",
                        9999,
                        2,
                        stream -> CastUtils
                                .handleFirst(stream, (first, flux) -> flux
                                        .skip(1)
                                        .as(CastUtils::flatStream)
                                        .all(v -> !CompareUtils.equals(v, first))))
        );
        Function<Flux<Object>, Publisher<?>> btw =
                stream -> CastUtils
                        .handleFirst(stream, (first, flux) -> flux
                                .skip(1)
                                .as(CastUtils::flatStream)
                                .collectList()
                                .map(list -> {
                                    Object left = !list.isEmpty() ? list.get(0) : null;
                                    Object right = list.size() > 1 ? list.get(list.size() - 1) : null;
                                    return BetweenFilter
                                            .predicate(first, left, right);
                                })
                        );

        //select btw(val,1,10)
        addGlobal(
                new FunctionMapFeature("btw", 3, 2, btw)
        );
        //range
        addGlobal(
                new FunctionMapFeature("range", 3, 2, btw)
        );

        //select nbtw(val,1,10)
        addGlobal(
                new FunctionMapFeature(
                        "nbtw",
                        3,
                        2,
                        stream ->
                                BooleanUtils.not(Mono.from(btw.apply(stream)).cast(Boolean.class))
                )
        );
        // select array_len(arr) len
        addGlobal(
                new FunctionMapFeature(
                        "array_len",
                        9999,
                        1,
                        stream -> CastUtils.flatStream(stream).count())
        );


        //select row_to_array((select 1 a1))
        addGlobal(new FunctionMapFeature("row_to_array", 9999, 1, stream -> stream
                .concatMap(v -> Mono.justOrEmpty(CastUtils.tryGetFirstValueOptional(v)), 0)
                .collect(Collectors.toList())));

        // select array_to_row(list,'name','value')
        addGlobal(new FunctionMapFeature("array_to_row", 3, 3, stream -> stream
                .collectList()
                .map((arr) -> {
                    List<Object> values = CastUtils.castArray(arr.get(0));
                    Object key = arr.get(1);
                    Object valueKey = arr.get(2);
                    return CastUtils.listToMap(values, key, valueKey);
                })
        ));
        addGlobal(new FunctionMapFeature("rows_to_array", 9999, 1, stream -> stream
                .as(CastUtils::flatStream)
                .concatMap(v -> Mono.justOrEmpty(CastUtils.tryGetFirstValueOptional(v)), 0)
                .collect(Collectors.toList())));

        //select new_array(1,2,3);
        addGlobal(new FunctionMapFeature("new_array", 9999, 1, stream -> stream.collect(Collectors.toList())));

        //select new_map('k1',v1,'k2',v2);
        addGlobal(new FunctionMapFeature("new_map", 9999, 1, stream -> stream
                .collectList()
                .map(list -> CastUtils
                        .castMap(list,
                                 Function.identity(),
                                 value -> MAP_NULL_VALUE.equals(value) ? null : value)))
                          .defaultValue(MAP_NULL_VALUE));


        // addGlobal(new BinaryMapFeature("concat", concat));

        addGlobal(new SingleParameterFunctionMapFeature("bit_not", v -> CalculateUtils.bitNot(CastUtils.castNumber(v))));
        addGlobal(new SingleParameterFunctionMapFeature("bit_count", v -> CalculateUtils.bitCount(CastUtils.castNumber(v))));

        addGlobal(new SingleParameterFunctionMapFeature("math.log", v ->
                Math.log(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.log1p", v ->
                Math.log1p(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.log10", v ->
                Math.log10(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.exp", v ->
                Math.exp(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.expm1", v ->
                Math.expm1(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.rint", v ->
                Math.rint(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.sin", v ->
                Math.sin(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.asin", v ->
                Math.asin(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.sinh", v ->
                Math.sinh(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.cos", v ->
                Math.cos(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.cosh", v ->
                Math.cosh(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.acos", v ->
                Math.acos(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.tan", v ->
                Math.tan(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.tanh", v ->
                Math.tanh(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.atan", v ->
                Math.atan(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.ceil", v ->
                Math.ceil(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.round", v ->
                Math.round(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.floor", v ->
                Math.floor(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.abs", v ->
                Math.abs(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.degrees", v ->
                Math.toDegrees(CastUtils.castNumber(v).doubleValue())));

        addGlobal(new SingleParameterFunctionMapFeature("math.radians", v ->
                Math.toRadians(CastUtils.castNumber(v).doubleValue())));


        // select take(name,1)
        // select take(name,-1)
        // select take(name,5,-1)
        addGlobal(new MapAggFeature("take", (arg, flux) -> {
            Flux<?> stream = flux;
            int n = !arg.isEmpty() ? CastUtils.castNumber(arg.get(0)).intValue() : 1;
            if (n >= 0) {
                stream = stream.take(n);
            } else {
                stream = stream.takeLast(-n);
            }
            if (arg.size() > 1) {
                int take = CastUtils.castNumber(arg.get(1)).intValue();
                if (take >= 0) {
                    stream = stream.take(take);
                } else {
                    stream = stream.takeLast(-take);
                }
            }
            return stream;
        }));

        addGlobal(new MapAggFeature("distinct_count", flux -> {
            return flux.distinct()
                       .count()
                       .flux();
        }));

        addGlobal(new MapAggFeature("sum", flux -> MathFlux.sumDouble(flux
                                                                              .map(CastUtils::castNumber))));
        addGlobal(new MapAggFeature("avg", flux -> MathFlux.averageDouble(flux
                                                                                  .map(CastUtils::castNumber))));

        addGlobal(new MapAggFeature("max", flux -> MathFlux.max(flux, CompareUtils::compare)));
        addGlobal(new MapAggFeature("min", flux -> MathFlux.min(flux, CompareUtils::compare)));

        addGlobal(new FunctionMapFeature("math.max", 9999, 1,
                                         flux -> MathFlux
                                                 .max(flux.as(CastUtils::flatStream), CompareUtils::compare)));

        addGlobal(new FunctionMapFeature("math.min", 9999, 1,
                                         flux -> MathFlux
                                                 .min(flux.as(CastUtils::flatStream), CompareUtils::compare)));

        addGlobal(new FunctionMapFeature("math.avg", 9999, 1,
                                         flux -> MathFlux
                                                 .averageDouble(flux
                                                                        .as(CastUtils::flatStream)
                                                                        .map(CastUtils::castNumber))));

        addGlobal(new FunctionMapFeature("math.count", 9999, 1, Flux::count));

        addGlobal(new TraceGroupRowFeature());

        addGlobal(new CollectRowAggMapFeature());


        addGlobal(new ArrayValueFlatMapFeature());
    }

    /**
     * 添加全局特性
     *
     * @param feature 特性
     */
    public static void addGlobal(Feature feature) {
        globalFeatures.put(feature.getId().toLowerCase(), feature);
    }

    private void init() {
        // select /*+ distinctBy(bloom),ignoreError */
        if (this.selectSql != null && this.selectSql.getOracleHint() != null) {
            String settings = this.selectSql.getOracleHint().getValue();
            String[] arr = settings.split(",");
            for (String set : arr) {
                set = set.trim().replace("\n", "");
                if (!set.contains("(")) {
                    this.settings().put(set, true);
                } else {
                    this.settings()
                        .put(set.substring(0, set.indexOf("(")), set.substring(set.indexOf("(") + 1, set.length() - 1));
                }
            }
        }
    }

    private synchronized Map<String, Object> settings() {
        return settings == null ? settings = new ConcurrentHashMap<>() : settings;
    }

    private synchronized Map<String, Feature> features() {
        return features == null ? features = new ConcurrentHashMap<>() : features;
    }

    public DefaultReactorQLMetadata(String sql) {
        Statement statement;
        try {
            statement = CCJSqlParserUtil.parse(SqlParserUtils.quoteNonAsciiAliases(sql));
        } catch (JSQLParserException e) {
            throw ReactorQLException.syntax(sql, e);
        }
        Select select = (Select) statement;
        this.selectBody = select.getSelectBody();
        this.selectSql = selectBody instanceof PlainSelect ? ((PlainSelect) selectBody) : null;
        this.withItemsList = select.getWithItemsList();
        init();
    }

    public DefaultReactorQLMetadata(PlainSelect selectSql) {
        this.selectSql = selectSql;
        this.selectBody = selectSql;
        init();
    }

    public DefaultReactorQLMetadata(ReactorQLMetadata source, PlainSelect selectSql) {
        this.selectSql = selectSql;
        this.selectBody = selectSql;
        init();
        addFeature(source.getFeatures());
    }


    @Override
    @SuppressWarnings("all")
    public <T extends Feature> Optional<T> getFeature(FeatureId<T> featureId) {
        String id = featureId.getId().toLowerCase();

        T feature = features == null ? null : (T) features.get(id);

        if (feature == null) {
            feature = (T) globalFeatures.get(id);
        }

        return Optional.ofNullable(feature);
    }

    public void addFeature(Feature... features) {
        addFeature(Arrays.asList(features));
    }

    public void addFeature(Collection<Feature> features) {
        if (CollectionUtils.isEmpty(features)) {
            return;
        }
        if (selectBody != null) {
            selectColumns = null;
        }
        for (Feature feature : features) {
            features().put(feature.getId().toLowerCase(), feature);
        }
    }

    @Override
    public PlainSelect getSql() {
        if (selectSql == null) {
            if (selectBody != null) {
                throw ReactorQLException.unsupportedExpression(
                        selectBody,
                        "集合查询或复杂 select 结构需要先包装为子查询，再在外层继续查询。",
                        "select * from (select a from t1 union all select a from t2) t"
                );
            }
            throw new IllegalStateException("sql released");
        }
        return selectSql;
    }

    @Override
    public void release() {
        getSelectColumns();
        selectSql = null;
        selectBody = null;
        withItemsList = null;
    }

    @Override
    public ReactorQLMetadata setting(String key, Object value) {
        settings().put(key, value);
        return this;
    }

    @Override
    public Optional<Object> getSetting(String key) {
        if (settings == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(settings.get(key));
    }

    @Override
    public List<Column> getSelectColumns() {
        if (selectColumns == null) {
            if (selectBody == null) {
                return Collections.emptyList();
            }
            selectColumns = new SelectColumnParser(
                    withItemsList,
                    name -> getFeature(FeatureId.ValueAggMap.of(name)).isPresent()
            ).parse(selectBody);
        }
        return selectColumns;
    }

    @Override
    public Collection<Feature> getFeatures() {
        return features == null ? Collections.emptyList() : features.values();
    }

    private static boolean handleContain(Collection<Object> left, Object val) {
       return CompareUtils.contains(left,val);
    }


}
