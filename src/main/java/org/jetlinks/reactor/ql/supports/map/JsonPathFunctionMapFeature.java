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
package org.jetlinks.reactor.ql.supports.map;

import com.jayway.jsonpath.JsonPath;
import lombok.Getter;
import net.sf.jsqlparser.expression.Expression;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.ValueMapFeature;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * JSONPath 函数抽象基类。
 *
 * 负责函数参数校验、JSONPath 静态参数预编译和 ReactorQL 参数流装配；具体数据库兼容行为
 * 由子类实现，避免每一行数据执行时再按函数名分派。
 *
 * @since 1.0.21
 */
public abstract class JsonPathFunctionMapFeature implements ValueMapFeature {

    public static final String SETTING_MAX_JSON_PATH_LENGTH = "function.json.maxPathLength";
    public static final String SETTING_MAX_JSON_TEXT_LENGTH = "function.json.maxTextLength";
    public static final String SETTING_MAX_JSON_OUTPUT_LENGTH = "function.json.maxOutputLength";
    public static final String SETTING_MAX_JSON_DEPTH = "function.json.maxDepth";
    public static final String SETTING_MAX_JSON_CONTAINER_SIZE = "function.json.maxContainerSize";

    @Getter
    private final String id;

    private final int minParamSize;
    private final int maxParamSize;

    protected JsonPathFunctionMapFeature(String name, int minParamSize, int maxParamSize) {
        this.minParamSize = minParamSize;
        this.maxParamSize = maxParamSize;
        this.id = FeatureId.ValueMap.of(name.toLowerCase(Locale.ENGLISH)).getId();
    }

    public static JsonPathFunctionMapFeature jsonGet(String name, int minParamSize, int maxParamSize, boolean scalar) {
        return JsonPathFunctionFeatures.jsonGet(name, minParamSize, maxParamSize, scalar);
    }

    public static JsonPathFunctionMapFeature jsonExtract(String name, int minParamSize, int maxParamSize) {
        return JsonPathFunctionFeatures.jsonExtract(name, minParamSize, maxParamSize);
    }

    public static JsonPathFunctionMapFeature jsonExists(String name, int minParamSize, int maxParamSize) {
        return JsonPathFunctionFeatures.jsonExists(name, minParamSize, maxParamSize);
    }

    public static JsonPathFunctionMapFeature jsonExtractPath(String name, int minParamSize, int maxParamSize, boolean text) {
        return JsonPathFunctionFeatures.jsonExtractPath(name, minParamSize, maxParamSize, text);
    }

    public static JsonPathFunctionMapFeature jsonUnquote(String name, int minParamSize, int maxParamSize) {
        return JsonPathFunctionFeatures.jsonUnquote(name, minParamSize, maxParamSize);
    }

    public static JsonPathFunctionMapFeature jsonQuote(String name, int minParamSize, int maxParamSize) {
        return JsonPathFunctionFeatures.jsonQuote(name, minParamSize, maxParamSize);
    }

    public static JsonPathFunctionMapFeature jsonDepth(String name, int minParamSize, int maxParamSize) {
        return JsonPathFunctionFeatures.jsonDepth(name, minParamSize, maxParamSize);
    }

    public static JsonPathFunctionMapFeature jsonType(String name, int minParamSize, int maxParamSize, boolean mysql) {
        return JsonPathFunctionFeatures.jsonType(name, minParamSize, maxParamSize, mysql);
    }

    public static JsonPathFunctionMapFeature jsonValid(String name, int minParamSize, int maxParamSize) {
        return JsonPathFunctionFeatures.jsonValid(name, minParamSize, maxParamSize);
    }

    public static JsonPathFunctionMapFeature jsonLength(String name, int minParamSize, int maxParamSize) {
        return JsonPathFunctionFeatures.jsonLength(name, minParamSize, maxParamSize);
    }

    public static JsonPathFunctionMapFeature jsonArrayLength(String name, int minParamSize, int maxParamSize) {
        return JsonPathFunctionFeatures.jsonArrayLength(name, minParamSize, maxParamSize);
    }

    public static JsonPathFunctionMapFeature jsonKeys(String name, int minParamSize, int maxParamSize) {
        return JsonPathFunctionFeatures.jsonKeys(name, minParamSize, maxParamSize);
    }

    public static JsonPathFunctionMapFeature jsonObjectKeys(String name, int minParamSize, int maxParamSize) {
        return JsonPathFunctionFeatures.jsonObjectKeys(name, minParamSize, maxParamSize);
    }

    public static JsonPathFunctionMapFeature jsonContains(String name, int minParamSize, int maxParamSize) {
        return JsonPathFunctionFeatures.jsonContains(name, minParamSize, maxParamSize);
    }

    public static JsonPathFunctionMapFeature jsonContainsPath(String name, int minParamSize, int maxParamSize) {
        return JsonPathFunctionFeatures.jsonContainsPath(name, minParamSize, maxParamSize);
    }

    public static JsonPathFunctionMapFeature jsonOverlaps(String name, int minParamSize, int maxParamSize) {
        return JsonPathFunctionFeatures.jsonOverlaps(name, minParamSize, maxParamSize);
    }

    public static JsonPathFunctionMapFeature jsonArray(String name, int minParamSize, int maxParamSize) {
        return JsonPathFunctionFeatures.jsonArray(name, minParamSize, maxParamSize);
    }

    public static JsonPathFunctionMapFeature jsonObject(String name, int minParamSize, int maxParamSize) {
        return JsonPathFunctionFeatures.jsonObject(name, minParamSize, maxParamSize);
    }

    public static JsonPathFunctionMapFeature toJson(String name, int minParamSize, int maxParamSize) {
        return JsonPathFunctionFeatures.toJson(name, minParamSize, maxParamSize);
    }

    public static JsonPathFunctionMapFeature jsonEqual(String name, int minParamSize, int maxParamSize) {
        return JsonPathFunctionFeatures.jsonEqual(name, minParamSize, maxParamSize);
    }

    public static JsonPathFunctionMapFeature jsonIntersect(String name, int minParamSize, int maxParamSize) {
        return JsonPathFunctionFeatures.jsonIntersect(name, minParamSize, maxParamSize);
    }

    public static JsonPathFunctionMapFeature jsonUnion(String name, int minParamSize, int maxParamSize) {
        return JsonPathFunctionFeatures.jsonUnion(name, minParamSize, maxParamSize);
    }

    public static JsonPathFunctionMapFeature jsonDiff(String name, int minParamSize, int maxParamSize) {
        return JsonPathFunctionFeatures.jsonDiff(name, minParamSize, maxParamSize);
    }

    public static JsonPathFunctionMapFeature jsonMergePreserve(String name, int minParamSize, int maxParamSize) {
        return JsonPathFunctionFeatures.jsonMergePreserve(name, minParamSize, maxParamSize);
    }

    public static JsonPathFunctionMapFeature jsonMergePatch(String name, int minParamSize, int maxParamSize) {
        return JsonPathFunctionFeatures.jsonMergePatch(name, minParamSize, maxParamSize);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Function<ReactorQLRecord, Publisher<?>> createMapper(Expression expression, ReactorQLMetadata metadata) {
        net.sf.jsqlparser.expression.Function function = (net.sf.jsqlparser.expression.Function) expression;
        List<Expression> expressions = function.getParameters() == null
                ? Collections.emptyList()
                : function.getParameters().getExpressions();
        if (expressions.size() < minParamSize || expressions.size() > maxParamSize) {
            throw new UnsupportedOperationException("函数[" + expression + "]参数数量错误");
        }

        JsonFunctionSupport.JsonLimits limits = JsonFunctionSupport.jsonLimits(metadata);
        JsonPath[] staticPaths = compileStaticPaths(limits, expressions);
        List<Function<ReactorQLRecord, Publisher<?>>> mappers = expressions
                .stream()
                .map(expr -> ValueMapFeature.createMapperNow(expr, metadata))
                .collect(Collectors.toList());
        Function<Publisher<?>, Publisher<?>> wrapper = metadata.createWrapper(expression);

        return record -> Flux
                .fromIterable(mappers)
                .concatMap(mapper -> Mono.fromDirect(mapper.apply(record)).cast(Object.class).defaultIfEmpty(JsonFunctionSupport.EMPTY), 0)
                .collectList()
                .flatMap(args -> Mono.justOrEmpty(evaluate(new JsonFunctionContext(limits, args, staticPaths))))
                .as(wrapper);
    }

    protected boolean isJsonPathArgument(int index) {
        return false;
    }

    protected abstract Object evaluate(JsonFunctionContext context);

    private JsonPath[] compileStaticPaths(JsonFunctionSupport.JsonLimits limits, List<Expression> expressions) {
        JsonPath[] paths = new JsonPath[expressions.size()];
        for (int i = 0; i < expressions.size(); i++) {
            if (isJsonPathArgument(i)) {
                paths[i] = JsonFunctionSupport.compileStaticPath(limits, expressions.get(i));
            }
        }
        return paths;
    }
}
