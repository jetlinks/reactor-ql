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

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import reactor.core.publisher.Flux;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 设备事件的宽嵌套属性查询。输入行预构造，SQL 与原生实现输出逐行核对。
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms512m", "-Xmx512m", "-XX:+UseG1GC"})
@State(Scope.Benchmark)
public class NestedPropertyBenchmark {

    private static final int ROWS = 65_536;
    private static final String SQL = "select deviceId device_id,"
            + "payload.temperature temperature,payload.humidity humidity,"
            + "payload.pressure pressure,payload.voltage voltage,"
            + "payload.current amps,payload.battery battery,"
            + "payload.signal signal,payload.vibration vibration,"
            + "payload.energy energy,payload.quality quality,"
            + "meta.site site,meta.region region,meta.gateway gateway "
            + "from test where payload.temperature >= 20 and meta.enabled = true";

    @Param({"map", "bean"})
    public String inputShape;

    private ReactorQL query;
    private Object[] rows;
    private int expectedCount;

    @Setup
    @SuppressWarnings("unchecked")
    public void setup() {
        query = ReactorQL.builder().sql(SQL).build();
        rows = new Object[ROWS];
        for (int i = 0; i < ROWS; i++) {
            if ("bean".equals(inputShape)) {
                rows[i] = new DeviceEvent("device-" + i % 1024, new Payload(i), new Meta(i));
                continue;
            }
            Map<String, Object> payload = new HashMap<>(16);
            payload.put("temperature", i % 40);
            payload.put("humidity", i % 100);
            payload.put("pressure", 900 + i % 200);
            payload.put("voltage", 200 + i % 30);
            payload.put("current", i % 20);
            payload.put("battery", i % 101);
            payload.put("signal", -90 + i % 60);
            payload.put("vibration", i % 5);
            payload.put("energy", i);
            payload.put("quality", i % 4);
            Map<String, Object> meta = new HashMap<>(8);
            meta.put("site", "site-" + i % 16);
            meta.put("region", "region-" + i % 4);
            meta.put("gateway", "gateway-" + i % 64);
            meta.put("enabled", (i & 1) == 0);
            Map<String, Object> row = new HashMap<>(4);
            row.put("deviceId", "device-" + i % 1024);
            row.put("payload", payload);
            row.put("meta", meta);
            rows[i] = row;
        }
        AtomicInteger subscriptions = new AtomicInteger();
        List<Map<String, Object>> actual = query.start(Flux.defer(() -> {
            subscriptions.incrementAndGet();
            return Flux.fromArray(rows);
        })).collectList().block();
        AtomicInteger nativeSubscriptions = new AtomicInteger();
        List<Map<String, Object>> expected = nativeProjection()
                .doOnSubscribe(ignore -> nativeSubscriptions.incrementAndGet()).collectList().block();
        if (subscriptions.get() != 1 || nativeSubscriptions.get() != 1
                || actual == null || expected == null || !actual.equals(expected)) {
            throw new IllegalStateException("嵌套属性查询与原生输出或源订阅不等价");
        }
        for (int i = 0; i < actual.size(); i++) {
            for (String key : expected.get(i).keySet()) {
                if (actual.get(i).get(key).getClass() != expected.get(i).get(key).getClass()) {
                    throw new IllegalStateException("嵌套属性输出类型不等价: " + key);
                }
            }
        }
        expectedCount = expected.size();
        int expectedFilteredRows = (ROWS / 40) * 10 + Math.max(0, (ROWS % 40 - 19) / 2);
        if (expectedCount != expectedFilteredRows) {
            throw new IllegalStateException("嵌套属性过滤后的原生条数不正确: " + expectedCount);
        }
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void nestedSql(Blackhole blackhole) {
        consume(query.start(Flux.fromArray(rows)), blackhole);
    }

    @Benchmark
    @OperationsPerInvocation(ROWS)
    public void nestedNative(Blackhole blackhole) {
        consume(nativeProjection(), blackhole);
    }

    private Flux<Map<String, Object>> nativeProjection() {
        if ("bean".equals(inputShape)) {
            return nativeBeanProjection();
        }
        return Flux.fromArray(rows)
                   .filter(value -> {
                       Map<?, ?> row = (Map<?, ?>) value;
                       Map<?, ?> payload = (Map<?, ?>) row.get("payload");
                       Map<?, ?> meta = (Map<?, ?>) row.get("meta");
                       return ((Number) payload.get("temperature")).intValue() >= 20
                               && Boolean.TRUE.equals(meta.get("enabled"));
                   })
                   .map(value -> {
                       Map<?, ?> row = (Map<?, ?>) value;
                       Map<?, ?> payload = (Map<?, ?>) row.get("payload");
                       Map<?, ?> meta = (Map<?, ?>) row.get("meta");
                       Map<String, Object> result = new HashMap<>(24);
                       result.put("device_id", row.get("deviceId"));
                       result.put("temperature", payload.get("temperature"));
                       result.put("humidity", payload.get("humidity"));
                       result.put("pressure", payload.get("pressure"));
                       result.put("voltage", payload.get("voltage"));
                       result.put("amps", payload.get("current"));
                       result.put("battery", payload.get("battery"));
                       result.put("signal", payload.get("signal"));
                       result.put("vibration", payload.get("vibration"));
                       result.put("energy", payload.get("energy"));
                       result.put("quality", payload.get("quality"));
                       result.put("site", meta.get("site"));
                       result.put("region", meta.get("region"));
                       result.put("gateway", meta.get("gateway"));
                       return result;
                   });
    }

    private Flux<Map<String, Object>> nativeBeanProjection() {
        return Flux.fromArray(rows)
                .filter(value -> {
                    DeviceEvent event = (DeviceEvent) value;
                    return event.getPayload().getTemperature() >= 20 && event.getMeta().isEnabled();
                })
                .map(value -> {
                    DeviceEvent event = (DeviceEvent) value;
                    Payload payload = event.getPayload();
                    Meta meta = event.getMeta();
                    Map<String, Object> result = new HashMap<>(24);
                    result.put("device_id", event.getDeviceId());
                    result.put("temperature", payload.getTemperature());
                    result.put("humidity", payload.getHumidity());
                    result.put("pressure", payload.getPressure());
                    result.put("voltage", payload.getVoltage());
                    result.put("amps", payload.getCurrent());
                    result.put("battery", payload.getBattery());
                    result.put("signal", payload.getSignal());
                    result.put("vibration", payload.getVibration());
                    result.put("energy", payload.getEnergy());
                    result.put("quality", payload.getQuality());
                    result.put("site", meta.getSite());
                    result.put("region", meta.getRegion());
                    result.put("gateway", meta.getGateway());
                    return result;
                });
    }

    /** Ordinary nested event beans; getters and primitive boxing execute in measured projection. */
    public static final class DeviceEvent {
        private final String deviceId;
        private final Payload payload;
        private final Meta meta;

        public DeviceEvent(String deviceId, Payload payload, Meta meta) {
            this.deviceId = deviceId;
            this.payload = payload;
            this.meta = meta;
        }

        public String getDeviceId() { return deviceId; }
        public Payload getPayload() { return payload; }
        public Meta getMeta() { return meta; }
    }

    /** Primitive telemetry values, equivalent to the Map input fields. */
    public static final class Payload {
        private final int temperature;
        private final int humidity;
        private final int pressure;
        private final int voltage;
        private final int current;
        private final int battery;
        private final int signal;
        private final int vibration;
        private final int energy;
        private final int quality;

        public Payload(int index) {
            temperature = index % 40;
            humidity = index % 100;
            pressure = 900 + index % 200;
            voltage = 200 + index % 30;
            current = index % 20;
            battery = index % 101;
            signal = -90 + index % 60;
            vibration = index % 5;
            energy = index;
            quality = index % 4;
        }

        public int getTemperature() { return temperature; }
        public int getHumidity() { return humidity; }
        public int getPressure() { return pressure; }
        public int getVoltage() { return voltage; }
        public int getCurrent() { return current; }
        public int getBattery() { return battery; }
        public int getSignal() { return signal; }
        public int getVibration() { return vibration; }
        public int getEnergy() { return energy; }
        public int getQuality() { return quality; }
    }

    /** Standard String and boolean bean properties for routing and filtering. */
    public static final class Meta {
        private final String site;
        private final String region;
        private final String gateway;
        private final boolean enabled;

        public Meta(int index) {
            site = "site-" + index % 16;
            region = "region-" + index % 4;
            gateway = "gateway-" + index % 64;
            enabled = (index & 1) == 0;
        }

        public String getSite() { return site; }
        public String getRegion() { return region; }
        public String getGateway() { return gateway; }
        public boolean isEnabled() { return enabled; }
    }

    private void consume(Flux<Map<String, Object>> source, Blackhole blackhole) {
        Long count = source.doOnNext(blackhole::consume).count().block();
        if (count == null || count != expectedCount) {
            throw new IllegalStateException("嵌套属性查询输出数量不符: " + count);
        }
    }
}
