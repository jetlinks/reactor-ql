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

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 事件日志清洗的16列投影：路由名、换行、标签规范化及普通函数／算术。
 * JMH trial预建输入并验证完整结果；测量仍逐行计算，不缓存输出或改变生产限制。
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC"})
@State(Scope.Benchmark)
public class TextNormalizationBenchmark {

    private static final int ROWS = 16_384;
    private static final String[] MESSAGES = {
            "",
            "Device connected; transport=MQTT; authenticated=true; session=primary",
            "Property report accepted\r\nsource=gateway; product=environment-monitor\r\n"
                    + "temperature=24.5; humidity=61.2; battery=86; signal=-68\r\n"
                    + "validation=passed; store=queued; rule-evaluation=completed; acknowledgement=sent",
            "Command timeout\noperation=read-properties; attempt=2; deadline=5000ms\n"
                    + "route=local-gateway; transport=tcp; socket=connected; pending-requests=4\n"
                    + "diagnostic=remote endpoint did not acknowledge before the configured deadline\n"
                    + "action=retain the original request identifier and report timeout to the caller",
            "设备属性上报\r\n产品=环境监测; 网关=车间入口; 温度=26.1; 湿度=58\r\n"
                    + "校验=通过; 入库=已排队; 规则=未触发告警; 状态=在线😀",
            "Firmware upgrade progress\nimage=stable-release; verified=true; checksum=matched\n"
                    + "stage=transferring; progress=72; bytes=1572864; retry-count=0\n"
                    + "network=ethernet; link=up; reported-by=device; requested-by=operator\n"
                    + "next-step=wait for verification and restart acknowledgement",
            "Subscription cancelled\r\nconsumer=rule-engine; reason=deployment-replaced\r\n"
                    + "last-event=processed; pending=0; completion=normal; resources=released",
            "Telemetry batch received\nsource=edge; content-type=application/json; schema=v2\n"
                    + "record-count=48; earliest=2026-10-07T08:00:00Z; latest=2026-10-07T08:00:01Z\n"
                    + "quality=valid; malformed-records=0; clock-status=synchronised\n"
                    + "storage-policy=append; retention-policy=standard; deduplication=not-required\n"
                    + "delivery=at-least-once; acknowledgement=after-processing; processing=completed"
    };
    private static final String[] SEPARATORS = {"_", ":", " "};
    private ReactorQL query;
    private ReactorQL propertyControl;
    private Map<String, Object>[] rows;

    @SuppressWarnings("unchecked")
    @Setup
    public void setup() {
        query = ReactorQL.builder().sql("select sequence,deviceId device_id,site,severity,"
                + "replace(topic,'/','.') routing_key,replace(message,lineBreak,' | ') message_text,"
                + "replace(label,separator,'-') label_text,lower(protocol) protocol_name,"
                + "cast(temperature + offset as long) adjusted_temperature,"
                + "cast(battery - signal as long) radio_margin,length(message) message_length,"
                + "upper(region) region_name,eventTime event_time,firmware,active,category from test").build();
        propertyControl = ReactorQL.builder().sql("select sequence,deviceId device_id,site,severity,"
                + "topic routing_key,message message_text,label label_text,protocol protocol_name,"
                + "temperature adjusted_temperature,battery radio_margin,messageSize message_length,"
                + "region region_name,eventTime event_time,firmware,active,category from test").build();
        rows = new Map[ROWS];
        for (int index = 0; index < ROWS; index++) {
            String separator = SEPARATORS[index % SEPARATORS.length];
            String message = MESSAGES[index % MESSAGES.length];
            Map<String, Object> row = new HashMap<>();
            row.put("sequence", index);
            row.put("deviceId", "device-" + (index & 255));
            row.put("site", "site-" + (index & 15));
            row.put("severity", index % 4);
            row.put("topic", "telemetry/site-" + (index & 15) + "/device-" + (index & 255) + "/properties");
            row.put("message", message.isEmpty() ? message : message + "; event=" + index);
            row.put("lineBreak", message.contains("\r\n") ? "\r\n" : "\n");
            row.put("label", "zone" + separator + (index & 15) + separator + "temperature");
            row.put("separator", separator);
            row.put("protocol", index % 2 == 0 ? "MQTT" : "CoAP");
            row.put("temperature", (long) (index % 100));
            row.put("offset", (long) (index % 7 - 3));
            row.put("battery", (long) (index % 101));
            row.put("signal", (long) (-40 - index % 51));
            row.put("messageSize", ((String) row.get("message")).length());
            row.put("region", index % 3 == 0 ? "华东" : "region-" + (index & 7));
            row.put("eventTime", 1_791_331_200_000L + index);
            row.put("firmware", "1." + (index & 3));
            row.put("active", index % 5 != 0);
            row.put("category", "sensor-" + (index & 3));
            rows[index] = row;
        }
        AtomicInteger subscriptions = new AtomicInteger();
        verify(query.start(countedSource(subscriptions)), true);
        requireOneSubscription(subscriptions);
        subscriptions.set(0);
        verify(nativeProjection(countedSource(subscriptions)), true);
        requireOneSubscription(subscriptions);
        subscriptions.set(0);
        verify(propertyControl.start(countedSource(subscriptions)), false);
        requireOneSubscription(subscriptions);
    }

    private Flux<Map<String, Object>> countedSource(AtomicInteger subscriptions) {
        return Flux.defer(() -> {
            subscriptions.incrementAndGet();
            return Flux.fromArray(rows);
        });
    }

    private static void requireOneSubscription(AtomicInteger subscriptions) {
        if (subscriptions.get() != 1) {
            throw new IllegalStateException("text normalization source subscription mismatch");
        }
    }

    private void verify(Flux<Map<String, Object>> source, boolean normalize) {
        // 仅trial setup收集这份固定有界输入，测量通过Subscriber逐行消费。
        List<Map<String, Object>> results = source.collectList().block();
        if (results == null || results.size() != ROWS) {
            throw new IllegalStateException("text normalization result count mismatch");
        }
        for (int index = 0; index < ROWS; index++) {
            Map<String, Object> expected = resultRow(rows[index], normalize);
            Map<String, Object> actual = results.get(index);
            if (actual.size() != 16 || !expected.equals(actual)) {
                throw new IllegalStateException("text normalization full row/order mismatch: " + index);
            }
            for (String key : expected.keySet()) {
                if (expected.get(key).getClass() != actual.get(key).getClass()) {
                    throw new IllegalStateException("text normalization value type mismatch: " + key);
                }
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void sqlNormalize(Blackhole blackhole) {
        consume(query.start(Flux.fromArray(rows)), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void nativeNormalize(Blackhole blackhole) {
        consume(nativeProjection(Flux.fromArray(rows)), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void propertyControl(Blackhole blackhole) {
        consume(propertyControl.start(Flux.fromArray(rows)), blackhole);
    }

    private static Flux<Map<String, Object>> nativeProjection(Flux<Map<String, Object>> source) {
        return source.map(row -> resultRow(row, true));
    }

    private static Map<String, Object> resultRow(Map<String, Object> row, boolean normalize) {
        String topic = (String) row.get("topic");
        String message = (String) row.get("message");
        String label = (String) row.get("label");
        String protocol = (String) row.get("protocol");
        String region = (String) row.get("region");
        long temperature = (Long) row.get("temperature");
        long margin = (Long) row.get("battery");
        if (normalize) {
            topic = topic.replace("/", ".");
            message = message.replace((String) row.get("lineBreak"), " | ");
            label = label.replace((String) row.get("separator"), "-");
            protocol = protocol.toLowerCase(Locale.ENGLISH);
            region = region.toUpperCase(Locale.ENGLISH);
            temperature += (Long) row.get("offset");
            margin -= (Long) row.get("signal");
        }
        Map<String, Object> result = new HashMap<>();
        result.put("sequence", row.get("sequence"));
        result.put("device_id", row.get("deviceId"));
        result.put("site", row.get("site"));
        result.put("severity", row.get("severity"));
        result.put("routing_key", topic);
        result.put("message_text", message);
        result.put("label_text", label);
        result.put("protocol_name", protocol);
        result.put("adjusted_temperature", temperature);
        result.put("radio_margin", margin);
        result.put("message_length", row.get("messageSize"));
        result.put("region_name", region);
        result.put("event_time", row.get("eventTime"));
        result.put("firmware", row.get("firmware"));
        result.put("active", row.get("active"));
        result.put("category", row.get("category"));
        return result;
    }

    private static void consume(Flux<Map<String, Object>> source, Blackhole blackhole) {
        ResultSubscriber subscriber = source.subscribeWith(new ResultSubscriber(blackhole));
        if (subscriber.error != null || !subscriber.complete || subscriber.count != ROWS) {
            throw new IllegalStateException("text normalization benchmark failed: " + subscriber.count,
                    subscriber.error);
        }
    }

    private static final class ResultSubscriber extends BaseSubscriber<Map<String, Object>> {
        private final Blackhole blackhole;
        private int count;
        private Throwable error;
        private boolean complete;

        private ResultSubscriber(Blackhole blackhole) {
            this.blackhole = blackhole;
        }

        @Override
        protected void hookOnNext(Map<String, Object> value) {
            blackhole.consume(value);
            count++;
        }

        @Override
        protected void hookOnComplete() {
            complete = true;
        }

        @Override
        protected void hookOnError(Throwable throwable) {
            error = throwable;
        }
    }
}
