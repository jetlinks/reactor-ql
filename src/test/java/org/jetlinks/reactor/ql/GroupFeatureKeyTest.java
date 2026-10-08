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

import org.jetlinks.reactor.ql.feature.GroupFeature;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class GroupFeatureKeyTest {

    @Test
    void shouldWriteNewGroupKeyForEmptyRecord() {
        ReactorQLRecord record = newRecord();

        Assertions.assertSame(record, GroupFeature.writeGroupKey(record, "first"));
        Assertions.assertEquals(List.of("first"), GroupFeature.getGroupKey(record));
    }

    @Test
    void shouldAppendGroupKeysWithoutMutatingExistingList() {
        List<Object> upstream = new ArrayList<>(List.of("outer"));
        ReactorQLRecord record = newRecord();
        record.addRecord(GroupFeature.groupByKeyContext, upstream);

        GroupFeature.writeGroupKey(record, "middle");
        List<Object> middleKeys = (List<Object>) record.getRecordValue(GroupFeature.groupByKeyContext);
        GroupFeature.writeGroupKey(record, "inner");

        Assertions.assertEquals(List.of("outer"), upstream);
        Assertions.assertEquals(List.of("outer", "middle"), middleKeys);
        Assertions.assertEquals(List.of("outer", "middle", "inner"), GroupFeature.getGroupKey(record));
        Assertions.assertNotSame(upstream, record.getRecordValue(GroupFeature.groupByKeyContext));
        Assertions.assertNotSame(middleKeys, record.getRecordValue(GroupFeature.groupByKeyContext));
    }

    @Test
    void shouldKeepGetGroupKeyCopiesMutableAndIsolatedAcrossMultipleDimensions() {
        ReactorQLRecord record = newRecord();
        List<Object> upstream = new ArrayList<>(Arrays.<Object>asList("upstream", "region"));
        record.addRecord(GroupFeature.groupByKeyContext, upstream);

        GroupFeature.writeGroupKey(record, "site");
        List<Object> firstRead = GroupFeature.getGroupKey(record);
        firstRead.add("reader-only");
        GroupFeature.writeGroupKey(record, "device");

        Assertions.assertEquals(Arrays.asList("upstream", "region"), upstream);
        Assertions.assertEquals(Arrays.asList("upstream", "region", "site"), firstRead.subList(0, 3));
        Assertions.assertEquals(Arrays.asList("upstream", "region", "site", "device"),
                                GroupFeature.getGroupKey(record));
        Assertions.assertFalse(GroupFeature.getGroupKey(record).contains("reader-only"));
    }

    @Test
    void shouldAppendScalarAndArrayKeysWithExistingCastArrayOrdering() {
        ReactorQLRecord scalar = newRecord();
        scalar.addRecord(GroupFeature.groupByKeyContext, "outer");
        GroupFeature.writeGroupKey(scalar, "inner");

        ReactorQLRecord array = newRecord();
        Object[] upstream = {"outer", "middle"};
        array.addRecord(GroupFeature.groupByKeyContext, upstream);
        GroupFeature.writeGroupKey(array, "inner");

        Assertions.assertEquals(Arrays.asList("outer", "inner"), GroupFeature.getGroupKey(scalar));
        Assertions.assertEquals(Arrays.asList("outer", "middle", "inner"), GroupFeature.getGroupKey(array));
        Assertions.assertArrayEquals(new Object[]{"outer", "middle"}, upstream);
    }

    private ReactorQLRecord newRecord() {
        return ReactorQLRecord.newRecord(
                "test",
                1,
                ReactorQLContext.ofDatasource(ignore -> Flux.empty()));
    }
}
