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

import java.util.stream.Collectors;

/**
 * JSON 函数行为实现集合。
 *
 * 每个内部类对应一种数据库兼容或 ReactorQL 扩展函数行为，由 createMapper 阶段选择具体实例，
 * 避免运行期逐行按函数名分派。
 */
final class JsonPathFunctionFeatures {

    private JsonPathFunctionFeatures() {
    }

    static JsonPathFunctionMapFeature jsonGet(String name, int minParamSize, int maxParamSize, boolean scalar) {
        return new JsonGet(name, minParamSize, maxParamSize, scalar);
    }

    static JsonPathFunctionMapFeature jsonExtract(String name, int minParamSize, int maxParamSize) {
        return new JsonExtract(name, minParamSize, maxParamSize);
    }

    static JsonPathFunctionMapFeature jsonExists(String name, int minParamSize, int maxParamSize) {
        return new JsonExists(name, minParamSize, maxParamSize);
    }

    static JsonPathFunctionMapFeature jsonExtractPath(String name, int minParamSize, int maxParamSize, boolean text) {
        return new JsonExtractPath(name, minParamSize, maxParamSize, text);
    }

    static JsonPathFunctionMapFeature jsonUnquote(String name, int minParamSize, int maxParamSize) {
        return new JsonUnquote(name, minParamSize, maxParamSize);
    }

    static JsonPathFunctionMapFeature jsonQuote(String name, int minParamSize, int maxParamSize) {
        return new JsonQuote(name, minParamSize, maxParamSize);
    }

    static JsonPathFunctionMapFeature jsonDepth(String name, int minParamSize, int maxParamSize) {
        return new JsonDepth(name, minParamSize, maxParamSize);
    }

    static JsonPathFunctionMapFeature jsonType(String name, int minParamSize, int maxParamSize, boolean mysql) {
        return new JsonType(name, minParamSize, maxParamSize, mysql);
    }

    static JsonPathFunctionMapFeature jsonValid(String name, int minParamSize, int maxParamSize) {
        return new JsonValid(name, minParamSize, maxParamSize);
    }

    static JsonPathFunctionMapFeature jsonLength(String name, int minParamSize, int maxParamSize) {
        return new JsonLength(name, minParamSize, maxParamSize);
    }

    static JsonPathFunctionMapFeature jsonArrayLength(String name, int minParamSize, int maxParamSize) {
        return new JsonArrayLength(name, minParamSize, maxParamSize);
    }

    static JsonPathFunctionMapFeature jsonKeys(String name, int minParamSize, int maxParamSize) {
        return new JsonKeys(name, minParamSize, maxParamSize);
    }

    static JsonPathFunctionMapFeature jsonObjectKeys(String name, int minParamSize, int maxParamSize) {
        return new JsonObjectKeys(name, minParamSize, maxParamSize);
    }

    static JsonPathFunctionMapFeature jsonContains(String name, int minParamSize, int maxParamSize) {
        return new JsonContains(name, minParamSize, maxParamSize);
    }

    static JsonPathFunctionMapFeature jsonContainsPath(String name, int minParamSize, int maxParamSize) {
        return new JsonContainsPath(name, minParamSize, maxParamSize);
    }

    static JsonPathFunctionMapFeature jsonOverlaps(String name, int minParamSize, int maxParamSize) {
        return new JsonOverlaps(name, minParamSize, maxParamSize);
    }

    static JsonPathFunctionMapFeature jsonArray(String name, int minParamSize, int maxParamSize) {
        return new JsonArray(name, minParamSize, maxParamSize);
    }

    static JsonPathFunctionMapFeature jsonObject(String name, int minParamSize, int maxParamSize) {
        return new JsonObject(name, minParamSize, maxParamSize);
    }

    static JsonPathFunctionMapFeature toJson(String name, int minParamSize, int maxParamSize) {
        return new ToJson(name, minParamSize, maxParamSize);
    }

    static JsonPathFunctionMapFeature jsonEqual(String name, int minParamSize, int maxParamSize) {
        return new JsonEqual(name, minParamSize, maxParamSize);
    }

    static JsonPathFunctionMapFeature jsonIntersect(String name, int minParamSize, int maxParamSize) {
        return new JsonIntersect(name, minParamSize, maxParamSize);
    }

    static JsonPathFunctionMapFeature jsonUnion(String name, int minParamSize, int maxParamSize) {
        return new JsonUnion(name, minParamSize, maxParamSize);
    }

    static JsonPathFunctionMapFeature jsonDiff(String name, int minParamSize, int maxParamSize) {
        return new JsonDiff(name, minParamSize, maxParamSize);
    }

    static JsonPathFunctionMapFeature jsonMergePreserve(String name, int minParamSize, int maxParamSize) {
        return new JsonMergePreserve(name, minParamSize, maxParamSize);
    }

    static JsonPathFunctionMapFeature jsonMergePatch(String name, int minParamSize, int maxParamSize) {
        return new JsonMergePatch(name, minParamSize, maxParamSize);
    }

    static final class JsonGet extends JsonPathFunctionMapFeature {
        private final boolean scalar;

        JsonGet(String name, int minParamSize, int maxParamSize, boolean scalar) {
            super(name, minParamSize, maxParamSize);
            this.scalar = scalar;
        }

        @Override
        protected boolean isJsonPathArgument(int index) {
            return index == 1;
        }

        @Override
        protected Object evaluate(JsonFunctionContext context) {
            return JsonFunctionSupport.jsonGet(context, scalar);
        }
    }

    static final class JsonExtract extends JsonPathFunctionMapFeature {

        JsonExtract(String name, int minParamSize, int maxParamSize) {
            super(name, minParamSize, maxParamSize);
        }

        @Override
        protected boolean isJsonPathArgument(int index) {
            return index >= 1;
        }

        @Override
        protected Object evaluate(JsonFunctionContext context) {
            return JsonFunctionSupport.jsonExtract(context);
        }
    }

    static final class JsonExists extends JsonPathFunctionMapFeature {

        JsonExists(String name, int minParamSize, int maxParamSize) {
            super(name, minParamSize, maxParamSize);
        }

        @Override
        protected boolean isJsonPathArgument(int index) {
            return index == 1;
        }

        @Override
        protected Object evaluate(JsonFunctionContext context) {
            return JsonFunctionSupport.jsonExists(context);
        }
    }

    static final class JsonExtractPath extends JsonPathFunctionMapFeature {
        private final boolean text;

        JsonExtractPath(String name, int minParamSize, int maxParamSize, boolean text) {
            super(name, minParamSize, maxParamSize);
            this.text = text;
        }

        @Override
        protected Object evaluate(JsonFunctionContext context) {
            return JsonFunctionSupport.jsonExtractPath(context, text);
        }
    }

    static final class JsonUnquote extends JsonPathFunctionMapFeature {

        JsonUnquote(String name, int minParamSize, int maxParamSize) {
            super(name, minParamSize, maxParamSize);
        }

        @Override
        protected Object evaluate(JsonFunctionContext context) {
            return JsonFunctionSupport.stringifyScalar(context.limits(), context.value(0));
        }
    }

    static final class JsonQuote extends JsonPathFunctionMapFeature {

        JsonQuote(String name, int minParamSize, int maxParamSize) {
            super(name, minParamSize, maxParamSize);
        }

        @Override
        protected Object evaluate(JsonFunctionContext context) {
            return JsonFunctionSupport.quoteJsonString(context.limits(), String.valueOf(context.value(0)));
        }
    }

    static final class JsonDepth extends JsonPathFunctionMapFeature {

        JsonDepth(String name, int minParamSize, int maxParamSize) {
            super(name, minParamSize, maxParamSize);
        }

        @Override
        protected Object evaluate(JsonFunctionContext context) {
            return JsonFunctionSupport.jsonDepth(context.limits(), context.value(0));
        }
    }

    static final class JsonType extends JsonPathFunctionMapFeature {
        private final boolean mysql;

        JsonType(String name, int minParamSize, int maxParamSize, boolean mysql) {
            super(name, minParamSize, maxParamSize);
            this.mysql = mysql;
        }

        @Override
        protected Object evaluate(JsonFunctionContext context) {
            return mysql
                    ? JsonFunctionSupport.mysqlJsonType(context.limits(), context.value(0))
                    : JsonFunctionSupport.postgresJsonType(context.limits(), context.value(0));
        }
    }

    static final class JsonValid extends JsonPathFunctionMapFeature {

        JsonValid(String name, int minParamSize, int maxParamSize) {
            super(name, minParamSize, maxParamSize);
        }

        @Override
        protected Object evaluate(JsonFunctionContext context) {
            return JsonFunctionSupport.jsonValid(context.limits(), context.value(0));
        }
    }

    static final class JsonLength extends JsonPathFunctionMapFeature {

        JsonLength(String name, int minParamSize, int maxParamSize) {
            super(name, minParamSize, maxParamSize);
        }

        @Override
        protected boolean isJsonPathArgument(int index) {
            return index == 1;
        }

        @Override
        protected Object evaluate(JsonFunctionContext context) {
            return JsonFunctionSupport.jsonLength(context);
        }
    }

    static final class JsonArrayLength extends JsonPathFunctionMapFeature {

        JsonArrayLength(String name, int minParamSize, int maxParamSize) {
            super(name, minParamSize, maxParamSize);
        }

        @Override
        protected Object evaluate(JsonFunctionContext context) {
            return JsonFunctionSupport.arrayLength(context.limits(), context.value(0));
        }
    }

    static final class JsonKeys extends JsonPathFunctionMapFeature {

        JsonKeys(String name, int minParamSize, int maxParamSize) {
            super(name, minParamSize, maxParamSize);
        }

        @Override
        protected boolean isJsonPathArgument(int index) {
            return index == 1;
        }

        @Override
        protected Object evaluate(JsonFunctionContext context) {
            return JsonFunctionSupport.jsonKeys(context);
        }
    }

    static final class JsonObjectKeys extends JsonPathFunctionMapFeature {

        JsonObjectKeys(String name, int minParamSize, int maxParamSize) {
            super(name, minParamSize, maxParamSize);
        }

        @Override
        protected Object evaluate(JsonFunctionContext context) {
            return JsonFunctionSupport.objectKeys(context.limits(), context.value(0));
        }
    }

    static final class JsonContains extends JsonPathFunctionMapFeature {

        JsonContains(String name, int minParamSize, int maxParamSize) {
            super(name, minParamSize, maxParamSize);
        }

        @Override
        protected boolean isJsonPathArgument(int index) {
            return index == 2;
        }

        @Override
        protected Object evaluate(JsonFunctionContext context) {
            return JsonFunctionSupport.jsonContains(context);
        }
    }

    static final class JsonContainsPath extends JsonPathFunctionMapFeature {

        JsonContainsPath(String name, int minParamSize, int maxParamSize) {
            super(name, minParamSize, maxParamSize);
        }

        @Override
        protected boolean isJsonPathArgument(int index) {
            return index >= 2;
        }

        @Override
        protected Object evaluate(JsonFunctionContext context) {
            return JsonFunctionSupport.jsonContainsPath(context);
        }
    }

    static final class JsonOverlaps extends JsonPathFunctionMapFeature {

        JsonOverlaps(String name, int minParamSize, int maxParamSize) {
            super(name, minParamSize, maxParamSize);
        }

        @Override
        protected Object evaluate(JsonFunctionContext context) {
            JsonFunctionSupport.JsonLimits limits = context.limits();
            Object left = JsonFunctionSupport.normalize(limits, context.value(0));
            Object right = JsonFunctionSupport.normalize(limits, context.value(1));
            return JsonFunctionSupport.overlaps(limits, left, right);
        }
    }

    static final class JsonArray extends JsonPathFunctionMapFeature {

        JsonArray(String name, int minParamSize, int maxParamSize) {
            super(name, minParamSize, maxParamSize);
        }

        @Override
        protected Object evaluate(JsonFunctionContext context) {
            JsonFunctionSupport.JsonLimits limits = context.limits();
            return context.args()
                          .stream()
                          .map(arg -> JsonFunctionSupport.normalize(limits, arg))
                          .collect(Collectors.toList());
        }
    }

    static final class JsonObject extends JsonPathFunctionMapFeature {

        JsonObject(String name, int minParamSize, int maxParamSize) {
            super(name, minParamSize, maxParamSize);
        }

        @Override
        protected Object evaluate(JsonFunctionContext context) {
            return JsonFunctionSupport.jsonObject(context.limits(), context.args());
        }
    }

    static final class ToJson extends JsonPathFunctionMapFeature {

        ToJson(String name, int minParamSize, int maxParamSize) {
            super(name, minParamSize, maxParamSize);
        }

        @Override
        protected Object evaluate(JsonFunctionContext context) {
            return JsonFunctionSupport.normalize(context.limits(), context.value(0));
        }
    }

    static final class JsonEqual extends JsonPathFunctionMapFeature {

        JsonEqual(String name, int minParamSize, int maxParamSize) {
            super(name, minParamSize, maxParamSize);
        }

        @Override
        protected Object evaluate(JsonFunctionContext context) {
            return JsonFunctionSupport.deepEquals(context.limits(), context.value(0), context.value(1));
        }
    }

    static final class JsonIntersect extends JsonPathFunctionMapFeature {

        JsonIntersect(String name, int minParamSize, int maxParamSize) {
            super(name, minParamSize, maxParamSize);
        }

        @Override
        protected Object evaluate(JsonFunctionContext context) {
            return JsonFunctionSupport.reduce(context, JsonFunctionSupport::intersect);
        }
    }

    static final class JsonUnion extends JsonPathFunctionMapFeature {

        JsonUnion(String name, int minParamSize, int maxParamSize) {
            super(name, minParamSize, maxParamSize);
        }

        @Override
        protected Object evaluate(JsonFunctionContext context) {
            return JsonFunctionSupport.reduce(context, JsonFunctionSupport::union);
        }
    }

    static final class JsonDiff extends JsonPathFunctionMapFeature {

        JsonDiff(String name, int minParamSize, int maxParamSize) {
            super(name, minParamSize, maxParamSize);
        }

        @Override
        protected Object evaluate(JsonFunctionContext context) {
            return JsonFunctionSupport.reduce(context, JsonFunctionSupport::diff);
        }
    }

    static final class JsonMergePreserve extends JsonPathFunctionMapFeature {

        JsonMergePreserve(String name, int minParamSize, int maxParamSize) {
            super(name, minParamSize, maxParamSize);
        }

        @Override
        protected Object evaluate(JsonFunctionContext context) {
            return JsonFunctionSupport.reduce(context, JsonFunctionSupport::mergePreserve);
        }
    }

    static final class JsonMergePatch extends JsonPathFunctionMapFeature {

        JsonMergePatch(String name, int minParamSize, int maxParamSize) {
            super(name, minParamSize, maxParamSize);
        }

        @Override
        protected Object evaluate(JsonFunctionContext context) {
            return JsonFunctionSupport.reduce(context, JsonFunctionSupport::mergePatch);
        }
    }
}
