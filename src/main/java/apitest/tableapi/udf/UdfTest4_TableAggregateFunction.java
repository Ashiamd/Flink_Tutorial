package apitest.tableapi.udf;

import apitest.beans.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * 有问题，跑不起来
 */

/**
 * @author : Ashiamd email: ashiamd@foxmail.com
 * @date : 2021/2/4 4:39 AM
 */
public class UdfTest4_TableAggregateFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. 读取数据
        DataStream<String> inputStream = env.readTextFile("/Users/ashiamd/mydocs/docs/study/javadocument/javadocument/IDEA_project/Flink_Tutorial/src/main/resources/sensor.txt");

        // 2. 转换成POJO
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 3. 将流转换成表
        Table sensorTable = tableEnv.fromDataStream(dataStream, "id, timestamp as ts, temperature");

        // 4. 自定义聚合函数，求当前传感器的平均温度值
        // 4.1 table API
        MyAggTabTemp myAggTabTemp = new MyAggTabTemp();

//        sensorTable.printSchema();

        // 需要在环境中注册UDF
        tableEnv.createTemporarySystemFunction("myAggTabTemp", myAggTabTemp);
        Table resultTable = sensorTable
                .groupBy("id")
                .flatAggregate("myAggTabTemp(temperature) as (temp, rankNum)")
                .select("id, temp, rankNum");

        // 4.2 SQL
        tableEnv.createTemporaryView("sensor", sensorTable);
        Table resultSqlTable = tableEnv.sqlQuery("select id, temp, rankNum " +
                " from sensor, lateral table(myAggTabTemp(temperature)) as tabTemp(temp, rankNum) " +
                "group by id");

        resultTable.printSchema();

        //   打印输出
        tableEnv.toRetractStream(resultTable, Row.class).print("result");
        tableEnv.toRetractStream(resultSqlTable, Row.class).print("sql");

        env.execute();
    }

    // 实现自定义的AggregateFunction
    public static class MyAggTabTemp extends TableAggregateFunction<Tuple2<Double,Integer>,Tuple2<Double, Double>> {

        @Override
        public Tuple2<Double, Double> createAccumulator() {
            return new Tuple2<Double, Double>(0.0,0.0);
        }

        public void accumulate(Tuple2<Double, Double> acc, Double temp){
            // 当前温度值，如果是最大的，则替换当前最大温度值
            if(temp>acc.f0){
                acc.f1 = acc.f0;
                acc.f0 = temp;
            }else if(temp >acc.f1){
                acc.f1 = temp;
            }
        }

        // 实现一个输出数据的方法，写入到结果表中
        public void emitUpdateWithRetract(Tuple2<Double, Double> acc, RetractableCollector<Tuple2<Double,Integer>> out){
            out.collect(new Tuple2<>(acc.f0,1));
            out.collect(new Tuple2<>(acc.f1,2));
        }
    }
}
