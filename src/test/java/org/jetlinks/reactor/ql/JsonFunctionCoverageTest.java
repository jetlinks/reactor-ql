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

import org.jetlinks.reactor.ql.supports.map.JsonPathFunctionMapFeature;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class JsonFunctionCoverageTest {

    @Test
    void testJsonPathMultiPathAndPostgresPathFunctions() {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("json", "{\"a\":1,\"b\":[10,20],\"c\":{\"d\":3},\"obj\":{\"k\":\"v\"}}");
        Map<String, Object> javaMap = new LinkedHashMap<>();
        javaMap.put("a\\b", 9);
        row.put("javaMap", javaMap);
        row.put("backslashKey", "a\\b");

        ReactorQL
                .builder()
                .sql("select json_extract(json, '$.a', '$.b[1]', '$.missing', '$.c.d', '$.obj.k') ext, json_query(json, '$.obj') queryVal, json_value(json, '$.obj') valueVal, json_extract_path(json, 'b', '1') pgPath, json_extract_path_text(json, 'obj') pgText, json_extract_path(javaMap, backslashKey) escapedPath from test")
                .build()
                .start(Flux.just(row))
                .as(StepVerifier::create)
                .assertNext(result -> {
                    Assertions.assertEquals(Arrays.asList(1, 20, null, 3, "v"), result.get("ext"));
                    Assertions.assertEquals(Collections.singletonMap("k", "v"), result.get("queryVal"));
                    Assertions.assertEquals("{\"k\":\"v\"}", result.get("valueVal"));
                    Assertions.assertEquals(20, result.get("pgPath"));
                    Assertions.assertEquals("{\"k\":\"v\"}", result.get("pgText"));
                    Assertions.assertEquals(9, result.get("escapedPath"));
                })
                .verifyComplete();
    }

    @Test
    void testJsonDatabasePathPredicateEdges() {
        ReactorQL
                .builder()
                .sql("select json_exists('{\"a\":1}') rootExists, json_exists('{\"a\":1}', '$.missing') missingExists, json_contains_path('{\"a\":1,\"b\":2}', 'all', '$.a', '$.b') allPaths, json_contains_path('{\"a\":1}', 'all', '$.a', '$.b') allMissing, json_contains_path('{\"a\":1}', 'one', '$.x', '$.y') oneMissing, json_contains('{\"a\":[1,2],\"b\":2}', '2', '$.a') containsAtPath, json_contains('[{\"a\":1},{\"b\":2}]', '{\"b\":2}') containsObjectArray from dual")
                .build()
                .start(Flux.just(1))
                .as(StepVerifier::create)
                .assertNext(result -> {
                    Assertions.assertEquals(true, result.get("rootExists"));
                    Assertions.assertEquals(false, result.get("missingExists"));
                    Assertions.assertEquals(true, result.get("allPaths"));
                    Assertions.assertEquals(false, result.get("allMissing"));
                    Assertions.assertEquals(false, result.get("oneMissing"));
                    Assertions.assertEquals(true, result.get("containsAtPath"));
                    Assertions.assertEquals(true, result.get("containsObjectArray"));
                })
                .verifyComplete();
    }

    @Test
    void testJsonLengthKeysAndTypeEdges() {
        ReactorQL
                .builder()
                .sql("select json_length('{\"a\":1,\"b\":2}') objectLen, json_length('[1,2]') arrayLen, json_length('{\"a\":{\"x\":1,\"y\":2}}', '$.a') pathLen, json_array_length('{\"a\":1}') nonArrayLen, json_keys('{\"a\":{\"x\":1,\"y\":2}}', '$.a') pathKeys, json_object_keys('{\"x\":1}') objectKeys, jsonb_object_keys('{\"x\":1}') bObjectKeys, json_type('true') boolType, json_type('12.5') doubleType, json_typeof('null') nullType, json_valid('true') validTrue, json_valid('false') validFalse, json_valid('null') validNull, json_valid('abc') invalidText from dual")
                .build()
                .start(Flux.just(1))
                .as(StepVerifier::create)
                .assertNext(result -> {
                    Assertions.assertEquals(2, result.get("objectLen"));
                    Assertions.assertEquals(2, result.get("arrayLen"));
                    Assertions.assertEquals(2, result.get("pathLen"));
                    Assertions.assertFalse(result.containsKey("nonArrayLen"));
                    Assertions.assertEquals(Arrays.asList("x", "y"), result.get("pathKeys"));
                    Assertions.assertEquals(Collections.singletonList("x"), result.get("objectKeys"));
                    Assertions.assertEquals(Collections.singletonList("x"), result.get("bObjectKeys"));
                    Assertions.assertEquals("BOOLEAN", result.get("boolType"));
                    Assertions.assertEquals("DOUBLE", result.get("doubleType"));
                    Assertions.assertEquals("null", result.get("nullType"));
                    Assertions.assertEquals(true, result.get("validTrue"));
                    Assertions.assertEquals(true, result.get("validFalse"));
                    Assertions.assertEquals(true, result.get("validNull"));
                    Assertions.assertEquals(false, result.get("invalidText"));
                })
                .verifyComplete();
    }

    @Test
    void testJsonSetLikeExtensionObjectAndScalarCases() {
        ReactorQL
                .builder()
                .sql("select json_overlaps('{\"a\":1,\"b\":2}', '{\"b\":2,\"c\":3}') objOverlap, json_overlaps('{\"a\":1}', '{\"a\":2}') objNoOverlap, json_overlaps('1', '1.0') scalarOverlap, json_overlaps('[1,2]', '[3,4]') arrNoOverlap, json_intersect('{\"a\":1,\"b\":[1,2],\"c\":3}', '{\"b\":[2,3],\"c\":4}') objInter, json_union('{\"a\":1,\"b\":[1]}', '{\"b\":[1,2],\"c\":3}') objUnion, json_diff('{\"a\":1,\"b\":2}', '{\"b\":2}') objDiff, json_intersect('1', '2') scalarInter, json_diff('1', '1.0') scalarDiff, json_union('1', '2') scalarUnion from dual")
                .build()
                .start(Flux.just(1))
                .as(StepVerifier::create)
                .assertNext(result -> {
                    Assertions.assertEquals(true, result.get("objOverlap"));
                    Assertions.assertEquals(false, result.get("objNoOverlap"));
                    Assertions.assertEquals(true, result.get("scalarOverlap"));
                    Assertions.assertEquals(false, result.get("arrNoOverlap"));
                    Map<?, ?> objInter = (Map<?, ?>) result.get("objInter");
                    Assertions.assertEquals(Collections.singletonList(2), objInter.get("b"));
                    Assertions.assertFalse(objInter.containsKey("c"));
                    Map<?, ?> objUnion = (Map<?, ?>) result.get("objUnion");
                    Assertions.assertEquals(1, objUnion.get("a"));
                    Assertions.assertEquals(Arrays.asList(1, 2), objUnion.get("b"));
                    Assertions.assertEquals(3, objUnion.get("c"));
                    Assertions.assertEquals(Collections.singletonMap("a", 1), result.get("objDiff"));
                    Assertions.assertEquals(Collections.emptyList(), result.get("scalarInter"));
                    Assertions.assertFalse(result.containsKey("scalarDiff"));
                    Assertions.assertEquals(Arrays.asList(1, 2), result.get("scalarUnion"));
                })
                .verifyComplete();
    }

    @Test
    void testJsonMergePatchAndConstructionEdges() {
        Map<Object, Object> javaMap = new LinkedHashMap<>();
        javaMap.put(1, new int[]{1, 2});
        javaMap.put("nested", Collections.singletonMap(2, "v"));
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("javaMap", javaMap);
        row.put("javaArray", new String[]{"a", "b"});

        ReactorQL
                .builder()
                .sql("select to_json(javaMap) normalizedMap, to_json(javaArray) normalizedArray, json_build_object('a',1,'b') oddObject, json_build_array(1,'x',javaArray) jsonArray, json_merge('{\"a\":{\"x\":1}}', '{\"a\":{\"y\":2}}') mergeNested, json_merge('1', '2') mergeScalars, json_merge_patch('1', '{\"a\":2}') patchTargetScalar, json_merge_patch('{\"a\":1}', '2') patchScalar from test")
                .build()
                .start(Flux.just(row))
                .as(StepVerifier::create)
                .assertNext(result -> {
                    Map<?, ?> normalizedMap = (Map<?, ?>) result.get("normalizedMap");
                    Assertions.assertEquals(Arrays.asList(1, 2), normalizedMap.get("1"));
                    Assertions.assertEquals(Collections.singletonMap("2", "v"), normalizedMap.get("nested"));
                    Assertions.assertEquals(Arrays.asList("a", "b"), result.get("normalizedArray"));
                    Map<?, ?> oddObject = (Map<?, ?>) result.get("oddObject");
                    Assertions.assertEquals(1, ((Number) oddObject.get("a")).intValue());
                    Assertions.assertFalse(oddObject.containsKey("b"));
                    List<?> jsonArray = (List<?>) result.get("jsonArray");
                    Assertions.assertEquals(1, ((Number) jsonArray.get(0)).intValue());
                    Assertions.assertEquals("x", jsonArray.get(1));
                    Assertions.assertEquals(Arrays.asList("a", "b"), jsonArray.get(2));
                    Map<?, ?> mergeNested = (Map<?, ?>) result.get("mergeNested");
                    Assertions.assertEquals(1, ((Number) ((Map<?, ?>) mergeNested.get("a")).get("x")).intValue());
                    Assertions.assertEquals(2, ((Number) ((Map<?, ?>) mergeNested.get("a")).get("y")).intValue());
                    Assertions.assertEquals(Arrays.asList(1, 2), result.get("mergeScalars"));
                    Assertions.assertEquals(Collections.singletonMap("a", 2), result.get("patchTargetScalar"));
                    Assertions.assertEquals(2, result.get("patchScalar"));
                })
                .verifyComplete();
    }

    @Test
    void testJsonQuotingMalformedAndUnsafePathInputs() {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("text", "quote\"slash\\line\ncontrol" + (char) 1);
        row.put("malformed", "{bad json");
        row.put("missingPath", null);
        row.put("unsafePath", "$..secret");

        ReactorQL
                .builder()
                .sql("select json_quote(text) quoted, json_unquote(malformed) malformedText, json_get('{\"a\":1}', missingPath, 'fallback') nullPathFallback, json_get('{\"a\":1}', '$.missing', 'fallback*') unsafeDefault, json_extract_path_text('{\"a*b\":1}', 'a*b') starKeyPath, json_depth('[]') emptyArrayDepth, json_depth('{}') emptyObjectDepth from test")
                .build()
                .start(Flux.just(row))
                .as(StepVerifier::create)
                .assertNext(result -> {
                    Assertions.assertEquals("\"quote\\\"slash\\\\line\\ncontrol\\u0001\"", result.get("quoted"));
                    Assertions.assertEquals("{bad json", result.get("malformedText"));
                    Assertions.assertEquals("fallback", result.get("nullPathFallback"));
                    Assertions.assertEquals("fallback*", result.get("unsafeDefault"));
                    Assertions.assertEquals("1", result.get("starKeyPath"));
                    Assertions.assertEquals(1, result.get("emptyArrayDepth"));
                    Assertions.assertEquals(1, result.get("emptyObjectDepth"));
                })
                .verifyComplete();

        Assertions.assertThrows(UnsupportedOperationException.class, () -> ReactorQL.builder().sql("select json_contains('{\"a\":1}', '1', '$.*') v from dual").build());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> ReactorQL.builder().sql("select json_contains_path('{\"a\":1}', 'one', '$[?(@.a)]') v from dual").build());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> ReactorQL.builder().sql("select json_get('{\"a\":1}', '" + "$" + "." + repeat("a", 1025) + "') v from dual").build());

        ReactorQL
                .builder()
                .sql("select json_get('{\"a\":1}', unsafePath) v from test")
                .build()
                .start(Flux.just(row))
                .as(StepVerifier::create)
                .expectError(UnsupportedOperationException.class)
                .verify();

        row.put("tooDeep", nestedJson(130));
        ReactorQL
                .builder()
                .sql("select json_valid(tooDeep) validDeep from test")
                .build()
                .start(Flux.just(row))
                .as(StepVerifier::create)
                .assertNext(result -> Assertions.assertEquals(false, result.get("validDeep")))
                .verifyComplete();
        ReactorQL
                .builder()
                .sql("select json_depth(tooDeep) depth from test")
                .build()
                .start(Flux.just(row))
                .as(StepVerifier::create)
                .expectError(UnsupportedOperationException.class)
                .verify();
    }


    @Test
    void testJsonAdditionalBoundaryBranches() {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("nil", null);
        row.put("empty", "");
        row.put("plain", "x");
        row.put("dynamicNull", null);

        ReactorQL
                .builder()
                .sql("select json_quote('a\bb\fc\rd\te') escaped, json_type('[1]') arrayType, json_type('\"abc\"') stringType, json_valid(123) validNumber, json_valid(empty) validEmpty, json_extract('{\"a\":1}', '$.a') singleExtract, json_get('{\"a\":1}', '$.missing') missingNoFallback, json_exists('{\"a\":1}', dynamicNull) nullPathExists, json_keys('{\"a\":1}', '$.missing') missingKeys from test")
                .build()
                .start(Flux.just(row))
                .as(StepVerifier::create)
                .assertNext(result -> {
                    Assertions.assertEquals("\"a\\bb\\fc\\rd\\te\"", result.get("escaped"));
                    Assertions.assertEquals("ARRAY", result.get("arrayType"));
                    Assertions.assertEquals("STRING", result.get("stringType"));
                    Assertions.assertEquals(true, result.get("validNumber"));
                    Assertions.assertEquals(false, result.get("validEmpty"));
                    Assertions.assertEquals(1, result.get("singleExtract"));
                    Assertions.assertFalse(result.containsKey("missingNoFallback"));
                    Assertions.assertEquals(false, result.get("nullPathExists"));
                    Assertions.assertFalse(result.containsKey("missingKeys"));
                })
                .verifyComplete();

        ReactorQL
                .builder()
                .sql("select json_equal('{\"a\":1}', '{\"a\":1,\"b\":2}') objSizeEq, json_equal('{\"a\":1}', '{\"b\":1}') objKeyEq, json_equal('[1,2]', '[1,3]') listValueEq, json_equal('[1,2]', '[1,2,3]') listSizeEq, json_contains('[1,2]', '[2,3]') containsListFalse, json_contains('[1,2]', '[1,2]') containsListTrue, json_contains('1', '2') containsScalarFalse, json_overlaps('[1,2]', '[2,3]') arrayOverlap, json_intersect('1', '1.0') scalarInterEq, json_union('1', '1.0') scalarUnionEq, json_diff('1', '2') scalarDiffKeep, json_merge('[1,2]', '3') mergeArrayScalar from dual")
                .build()
                .start(Flux.just(1))
                .as(StepVerifier::create)
                .assertNext(result -> {
                    Assertions.assertEquals(false, result.get("objSizeEq"));
                    Assertions.assertEquals(false, result.get("objKeyEq"));
                    Assertions.assertEquals(false, result.get("listValueEq"));
                    Assertions.assertEquals(false, result.get("listSizeEq"));
                    Assertions.assertEquals(false, result.get("containsListFalse"));
                    Assertions.assertEquals(true, result.get("containsListTrue"));
                    Assertions.assertEquals(false, result.get("containsScalarFalse"));
                    Assertions.assertEquals(true, result.get("arrayOverlap"));
                    Assertions.assertEquals(1, result.get("scalarInterEq"));
                    Assertions.assertEquals(1, result.get("scalarUnionEq"));
                    Assertions.assertEquals(1, result.get("scalarDiffKeep"));
                    Assertions.assertEquals(Arrays.asList(1, 2, 3), result.get("mergeArrayScalar"));
                })
                .verifyComplete();
    }

    @Test
    void testJsonCollectionOperationNegativeAndNestedBranches() {
        String sql = "select "
                + "json_contains('{\"a\":{\"b\":1}}', '{\"a\":{\"c\":1}}') containsNestedMismatch, "
                + "json_contains('{\"a\":1}', '{\"b\":1}') containsMissingKey, "
                + "json_intersect('{\"a\":1,\"b\":{\"x\":2},\"c\":[1,2]}', '{\"a\":1.0,\"b\":{\"x\":3},\"c\":[2]}') objInter, "
                + "json_diff('{\"a\":1,\"b\":2}', '{\"a\":2,\"c\":3}') objDiff, "
                + "json_overlaps('{\"a\":{\"x\":1}}', '{\"a\":{\"x\":1}}') objNestedOverlap, "
                + "json_overlaps('{\"a\":{\"x\":1}}', '{\"b\":{\"x\":1}}') objMissingOverlap, "
                + "json_union('{\"a\":1}', '{\"a\":1.0}') objUnionEqualScalar, "
                + "json_merge('{\"a\":[1]}', '{\"a\":2}') mergeArrayScalarDuplicate "
                + "from dual";

        ReactorQL
                .builder()
                .sql(sql)
                .build()
                .start(Flux.just(1))
                .as(StepVerifier::create)
                .assertNext(result -> {
                    Assertions.assertEquals(false, result.get("containsNestedMismatch"));
                    Assertions.assertEquals(false, result.get("containsMissingKey"));

                    Map<?, ?> objInter = (Map<?, ?>) result.get("objInter");
                    Assertions.assertEquals(1, objInter.get("a"));
                    Assertions.assertFalse(objInter.containsKey("b"));
                    Assertions.assertEquals(Collections.singletonList(2), objInter.get("c"));

                    Map<?, ?> objDiff = (Map<?, ?>) result.get("objDiff");
                    Assertions.assertEquals(1, objDiff.get("a"));
                    Assertions.assertEquals(2, objDiff.get("b"));

                    Assertions.assertEquals(true, result.get("objNestedOverlap"));
                    Assertions.assertEquals(false, result.get("objMissingOverlap"));
                    Assertions.assertEquals(Collections.singletonMap("a", 1), result.get("objUnionEqualScalar"));
                    Assertions.assertEquals(Collections.singletonMap("a", Arrays.asList(1, 2)), result.get("mergeArrayScalarDuplicate"));
                })
                .verifyComplete();
    }

    @Test
    void testJsonValidationAndParameterGuardBranches() {
        ReactorQL
                .builder()
                .setting(JsonPathFunctionMapFeature.SETTING_MAX_JSON_TEXT_LENGTH, 4)
                .sql("select json_valid('[12345]') v from dual")
                .build()
                .start(Flux.just(1))
                .as(StepVerifier::create)
                .assertNext(result -> Assertions.assertEquals(false, result.get("v")))
                .verifyComplete();

        Assertions.assertThrows(UnsupportedOperationException.class, () -> ReactorQL
                .builder()
                .sql("select json_exists() v from dual")
                .build());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> ReactorQL
                .builder()
                .sql("select json_contains('{\"a\":1}') v from dual")
                .build());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> ReactorQL
                .builder()
                .sql("select json_get('{\"a\":1}', '$.a', 0, 1) v from dual")
                .build());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> ReactorQL
                .builder()
                .sql("select json_query('{\"a\":1}', '$.a', 0) v from dual")
                .build());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> ReactorQL
                .builder()
                .sql("select json_contains_path('{\"a\":1}', 'one') v from dual")
                .build());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> ReactorQL
                .builder()
                .sql("select json_quote('a', 'b') v from dual")
                .build());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> ReactorQL
                .builder()
                .sql("select json_overlaps('[]', '[]', 'ignored') v from dual")
                .build());
    }

    @Test
    void testJsonSecurityLimitsCanBeConfiguredByMetadataSettings() {
        String longPath = "$." + repeat("a", 1030);
        Assertions.assertThrows(UnsupportedOperationException.class, () -> ReactorQL
                .builder()
                .sql("select json_exists('{}', '" + longPath + "') v from dual")
                .build());

        ReactorQL
                .builder()
                .setting(JsonPathFunctionMapFeature.SETTING_MAX_JSON_PATH_LENGTH, longPath.length())
                .sql("select json_exists('{}', '" + longPath + "') v from dual")
                .build()
                .start(Flux.just(1))
                .as(StepVerifier::create)
                .assertNext(row -> Assertions.assertEquals(false, row.get("v")))
                .verifyComplete();

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("json", "{\"a\":{\"b\":1}}");
        ReactorQL
                .builder()
                .setting(JsonPathFunctionMapFeature.SETTING_MAX_JSON_DEPTH, 2)
                .sql("select json_depth(json) depth from test")
                .build()
                .start(Flux.just(row))
                .as(StepVerifier::create)
                .expectError(UnsupportedOperationException.class)
                .verify();

        Assertions.assertThrows(UnsupportedOperationException.class, () -> ReactorQL
                .builder()
                .setting(JsonPathFunctionMapFeature.SETTING_MAX_JSON_DEPTH, 513)
                .sql("select json_depth('{}') depth from dual")
                .build());
    }

    @Test
    void testJsonBoundaryBranchesAfterReviewRefactor() {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("nil", null);

        ReactorQL
                .builder()
                .sql("select json_exists(nil) nullRootExists, json_extract_path('{\"a\":1}', 'missing') missingPgPath, json_object_keys('[1,2]') nonObjectKeys, json_unquote(nil) nullUnquote, json_quote(nil) nullQuote, json_depth(nil) nullDepth, json_quote('null') quotedNullText, json_depth('null') nullJsonDepth, json_typeof('1') pgNumberType, json_valid('[1]') validArray, json_valid('-1') validNegative, json_valid('0') validZero, json_equal('[1]', '[1]') sameList from test")
                .build()
                .start(Flux.just(row))
                .as(StepVerifier::create)
                .assertNext(result -> {
                    Assertions.assertEquals(false, result.get("nullRootExists"));
                    Assertions.assertFalse(result.containsKey("missingPgPath"));
                    Assertions.assertFalse(result.containsKey("nonObjectKeys"));
                    Assertions.assertFalse(result.containsKey("nullUnquote"));
                    Assertions.assertFalse(result.containsKey("nullQuote"));
                    Assertions.assertFalse(result.containsKey("nullDepth"));
                    Assertions.assertEquals("\"null\"", result.get("quotedNullText"));
                    Assertions.assertEquals(1, result.get("nullJsonDepth"));
                    Assertions.assertEquals("number", result.get("pgNumberType"));
                    Assertions.assertEquals(true, result.get("validArray"));
                    Assertions.assertEquals(true, result.get("validNegative"));
                    Assertions.assertEquals(true, result.get("validZero"));
                    Assertions.assertEquals(true, result.get("sameList"));
                })
                .verifyComplete();

        Assertions.assertThrows(UnsupportedOperationException.class, () -> ReactorQL
                .builder()
                .setting(JsonPathFunctionMapFeature.SETTING_MAX_JSON_TEXT_LENGTH, 4)
                .sql("select json_quote('abcde') v from dual")
                .build()
                .start(Flux.just(1))
                .blockLast());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> ReactorQL
                .builder()
                .setting(JsonPathFunctionMapFeature.SETTING_MAX_JSON_OUTPUT_LENGTH, 4)
                .sql("select json_quote('abcd') v from dual")
                .build()
                .start(Flux.just(1))
                .blockLast());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> ReactorQL
                .builder()
                .setting(JsonPathFunctionMapFeature.SETTING_MAX_JSON_CONTAINER_SIZE, 1)
                .sql("select json_depth('[1,2]') v from dual")
                .build()
                .start(Flux.just(1))
                .blockLast());
    }


    private static String nestedJson(int depth) {
        StringBuilder builder = new StringBuilder(depth * 8 + 1);
        for (int i = 0; i < depth; i++) {
            builder.append("{\"a\":");
        }
        builder.append('1');
        for (int i = 0; i < depth; i++) {
            builder.append('}');
        }
        return builder.toString();
    }

    private static String repeat(String value, int count) {
        StringBuilder builder = new StringBuilder(value.length() * count);
        for (int i = 0; i < count; i++) {
            builder.append(value);
        }
        return builder.toString();
    }

}
