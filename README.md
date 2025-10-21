# 用SQL来描述ReactorAPI进行数据处理

![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/jetlinks/reactor-ql/maven-publish.yml?branch=master)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/9e72a110fc6744bcb183a630a827dba8)](https://app.codacy.com/gh/jetlinks/reactor-ql?utm_source=github.com&utm_medium=referral&utm_content=jetlinks/reactor-ql&utm_campaign=Badge_Grade_Settings)
[![Maven Central](https://img.shields.io/maven-central/v/org.jetlinks/reactor-ql.svg)](http://search.maven.org/#search%7Cga%7C1%7Creactor-ql)
[![Maven metadata URL](https://img.shields.io/maven-metadata/v.svg?metadataUrl=https%3A%2F%2Fcentral.sonatype.com%2Frepository%2Fmaven-snapshots%2Forg%2Fjetlinks%2Freactor-ql%2Fmaven-metadata.xml)](https://oss.sonatype.org/content/repositories/snapshots/org/jetlinks/reactor-ql/)
[![codecov](https://codecov.io/gh/jetlinks/reactor-ql/branch/master/graph/badge.svg)](https://codecov.io/gh/jetlinks/reactor-ql)

[Reactor](https://github.com/reactor) + [JSqlParser](https://github.com/JSQLParser/JSqlParser) = ReactorQL

## 场景

1. 规则引擎,在线编写SQL来定义数据处理规则.
2. 实时统计每分钟平均温度.
3. 统计每20条滚动数据平均值.
4. ........

## 特性

1. 支持字段映射 `select name username from user`.
2. 支持聚合函数 `count`,`sum`,`avg`,`max`,`min`.
3. 支持运算 `select val/100 percent from cpu_usage`.
4. 支持分组 `select sum(val) sum from topic group by interval('10s')`. 按时间分组.
5. 支持多列分组 `select count(1) total,productId,deviceId from messages group by productId,deviceId`.
6. 支持having `select avg(temp) avgTemp from temps group by interval('10s') having avgTemp>10 `.
7. 支持case when `select case type when 1 then '警告' when 2 then '故障' else '其他' end type from topic`.
8. 支持Join `select t1.name,t2.detail from t1,t2 where t1.id = t2.id`.
9. 响应式：数据源，函数执行都是异步非阻塞。

## 例子

引入依赖
```xml
<dependency>
 <groupId>org.jetlinks</groupId>
    <artifactId>reactor-ql</artifactId>
    <version>{version}</version>
</dependency>
```

用例:

```java
  ReactorQL.builder()
        .sql("select avg(this) total from test group by interval('1s') having total > 2") //按每秒分组,并计算流中数据平均值,如果平均值大于2则下游收到数据.
        .build()
        .start(Flux.range(0, 10).delayElements(Duration.ofMillis(500)))
        .doOnNext(System.out::println)
        .as(StepVerifier::create)
        .expectNextCount(4)
        .verifyComplete();
```

更多用法请看 [单元测试](https://github.com/jetlinks/reactor-ql/blob/master/src/test/java/org/jetlinks/reactor/ql/ReactorQLTest.java)

## 原理

1. 解析SQL查询语句，生成SQL抽象语法树。
2. 遍历SQL抽象语法树，使用策略模式，根据不同的语法类型，编译生成针对`Flux`的转换函数。
    * 条件使用(`FilterFeature`)进行创建。
    * 查询列(`columnMapper`)使用`ValueMapFeature`,`ValueFlatMapFeature`,`ValueAggMapFeature`进行创建。
    * 分组(`groupBy`)使用`GroupFeature`进行创建。
    * 数据源(`from`)使用`FromFeature`进行创建。
        * 表 ，从上下文中基于表名获取`Flux`数据流。
        * 子查询，基于子查询语句生成新的`ReactorQL`对象并执行获取数据源。
        * 函数，使用策略模式获取对应`FromFeature`进行创建。
    * 排序(`orderBy`)使用`ValueMapFeature`编译转换函数，基于转换函数执行结果进行排序。
    * 关联查询(`join`)，支持多种join源。
        * join 表，从上下文中基于表名获取`Flux`数据流进行数据关联。
        * 子查询，基于子查询语句生成新的`ReactorQL`对象并执行获取数据源进行数据关联。

3. 组合编译后各个片段对应的`Flux`转换函数。
    * 组合顺序: `limit->offset->distinct->orderBy->columnMapper->groupBy->where->join->from`
4. 传入上下文执行。
    * 支持参数绑定。
    * 指定数据源获取函数，使用表名获取数据源。

## 拓展

当内置的特性不满足需求时，可通过自定义的方式进行拓展。

拓展了特性后，在启动时注册到元数据中。

```java

import org.jetlinks.reactor.ql.ReactorQL;


public ReactorQL createQL(String sql) {
    return ReactorQL
            .builder()
            .sql(sql)
            //注册自定义的特性
            .feature(customFeature1, customFeature2)
            .build();
}


```

### 拓展转换函数

可通过拓展转换函数特性(`ValueMapFeature`)来自定义数据转换等操作，
如实现 `select device.state(t.deviceId) from "/device/**" t`。

```java

import org.jetlinks.reactor.ql.supports.map.FunctionMapFeature;

// FunctionMapFeature针对ValueMapFeature实现了基础操作
public class DeviceStateFunction extends FunctionMapFeature {
    public DeviceStateFunction() {
        super("device.state",
              1,//最大参数数量
              1,//最小参数数量
              flux -> flux 
                .collectList()//将响应式参数流中的数据收集为List
                .flatMap(args -> {
                    if (args.size() != 1) {
                        return Mono.empty();
                    }
                    String deviceId = String.valueOf(args.get(0));

                    return getDeviceState(deviceId);
                }));
    }
}


```

### 拓展数据源函数

可通过拓展数据源函数特性来自定义数据源，
如实现 `select * from mysql(....)`。

例:

```java

import net.sf.jsqlparser.statement.select.FromItem;
import org.jetlinks.reactor.ql.ReactorQLContext;
import org.jetlinks.reactor.ql.ReactorQLMetadata;
import org.jetlinks.reactor.ql.ReactorQLRecord;
import org.jetlinks.reactor.ql.feature.FeatureId;
import org.jetlinks.reactor.ql.feature.FromFeature;
import reactor.core.publisher.Flux;

import java.util.function.Function;

// select * from mysql(....);
public class MysqlFromFeature implements FromFeature {
   private static final String ID = FeatureId.From.of("mysql").getId();

   @Override
   public Function<ReactorQLContext, Flux<ReactorQLRecord>> createFromMapper(
           FromItem fromItem,
           ReactorQLMetadata metadata) {
      TableFunction from = ((TableFunction) fromItem);
      net.sf.jsqlparser.expression.Function function = from.getFunction();
      //函数参数列表
      ExpressionList list = function.getParameters();
      //别名
      String alias = from.getAlias() != null ? from.getAlias().getName() : null;

      Object params = prepareParameter(list);
      return ctx -> {
         return this
                 .execute0(params)
                 .map(val -> {
                    return ReactorQLRecord.newRecord(alias, val, ctx);
                 });
      };
   }

   public Flux<Object> execute0(Object args) {
      //执行真实逻辑，返回数据流。
   }
}

```
 
