package processfunction;

import apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author : Ashiamd email: ashiamd@foxmail.com
 * @date : 2021/2/3 2:07 AM
 */
public class ProcessTest3_SideOuptCase {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度 = 1
        env.setParallelism(1);
        // 从本地socket读取数据
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);
        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 定义一个OutputTag，用来表示侧输出流低温流
        // An OutputTag must always be an anonymous inner class
        // so that Flink can derive a TypeInformation for the generic type parameter.
        OutputTag<SensorReading> lowTempTag = new OutputTag<SensorReading>("lowTemp"){};

        // 测试ProcessFunction，自定义侧输出流实现分流操作
        SingleOutputStreamOperator<SensorReading> highTempStream = dataStream.process(new ProcessFunction<SensorReading, SensorReading>() {
            @Override
            public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
                // 判断温度，大于30度，高温流输出到主流；小于低温流输出到侧输出流
                if (value.getTemperature() > 30) {
                    out.collect(value);
                } else {
                    ctx.output(lowTempTag, value);
                }
            }
        });

        highTempStream.print("high-temp");
        highTempStream.getSideOutput(lowTempTag).print("low-temp");

        env.execute();
    }
}
