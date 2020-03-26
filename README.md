# 用SQL来描述reactor api

[![Maven Central](https://img.shields.io/maven-central/v/org.jetlinks/reactor-ql.svg)](http://search.maven.org/#search%7Cga%7C1%7Creactor-ql)
[![Maven metadata URL](https://img.shields.io/maven-metadata/v/https/oss.sonatype.org/content/repositories/snapshots/org/jetlinks/reactor-ql/maven-metadata.xml.svg)](https://oss.sonatype.org/content/repositories/snapshots/org/jetlinks/reactor-ql)
[![Build Status](https://travis-ci.com/jetlinks/reactor-ql.svg?branch=master)](https://travis-ci.com/jetlinks/reactor-ql)
[![codecov](https://codecov.io/gh/jetlinks/reactor-ql/branch/master/graph/badge.svg)](https://codecov.io/gh/jetlinks/reactor-ql)


# 例子

```java
  ReactorQL.builder()
                .sql("select avg(this) total from test group by interval('1s') having total > 2") //按每秒分组,并计算流中数据平均值,如果平均值大于2则下游收到数据.
                .build()
                .source(Flux.range(0, 10).delayElements(Duration.ofMillis(500)))
                .start()
                .doOnNext(System.out::println)
                .as(StepVerifier::create)
                .expectNextCount(4)
                .verifyComplete();
```