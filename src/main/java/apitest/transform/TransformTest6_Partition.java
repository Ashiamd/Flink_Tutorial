package apitest.transform;

import apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author : Ashiamd email: ashiamd@foxmail.com
 * @date : 2021/2/1 12:38 AM
 */
public class TransformTest6_Partition {
    public static void main(String[] args) throws Exception{

        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度 = 4
        env.setParallelism(4);

        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("/Users/ashiamd/mydocs/docs/study/javadocument/javadocument/IDEA_project/Flink_Tutorial/src/main/resources/sensor.txt");

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // SingleOutputStreamOperator多并行度默认就rebalance,轮询方式分配
        dataStream.print("input");

        // 1. shuffle (并非批处理中的获取一批后才打乱，这里每次获取到直接打乱且分区)
        DataStream<String> shuffleStream = inputStream.shuffle();
        shuffleStream.print("shuffle");

        // 2. keyBy (Hash，然后取模)
        dataStream.keyBy(SensorReading::getId).print("keyBy");

        // 3. global (直接发送给第一个分区，少数特殊情况才用)
        dataStream.global().print("global");

        env.execute();
    }
}
