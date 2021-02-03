package apitest.tableapi.udf;

import apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

/**
 * @author : Ashiamd email: ashiamd@foxmail.com
 * @date : 2021/2/4 3:28 AM
 */
public class UdfTest1_ScalarFunction {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度设置为1
        env.setParallelism(1);

        // 创建Table执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. 读取数据
        DataStream<String> inputStream = env.readTextFile("/Users/ashiamd/mydocs/docs/study/javadocument/javadocument/IDEA_project/Flink_Tutorial/src/main/resources/sensor.txt");

        // 2. 转换成POJO
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 3. 将流转换为表
        Table sensorTable = tableEnv.fromDataStream(dataStream, "id,timestamp as ts,temperature");

        // 4. 自定义标量函数，实现求id的hash值
        HashCode hashCode = new HashCode(23);
        // 注册UDF
        tableEnv.registerFunction("hashCode", hashCode);

        // 4.1 table API
        Table resultTable = sensorTable.select("id, ts, hashCode(id)");

        // 4.2 SQL
        tableEnv.createTemporaryView("sensor", sensorTable);
        Table resultSqlTable = tableEnv.sqlQuery("select id, ts, hashCode(id) from sensor");

        // 打印输出
        tableEnv.toAppendStream(resultTable, Row.class).print();
        tableEnv.toAppendStream(resultSqlTable, Row.class).print();

        env.execute();
    }

    public static class HashCode extends ScalarFunction {

        private int factor = 13;

        public HashCode(int factor) {
            this.factor = factor;
        }

        public int eval(String id) {
            return id.hashCode() * 13;
        }
    }
}
