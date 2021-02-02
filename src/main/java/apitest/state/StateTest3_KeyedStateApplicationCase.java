package apitest.state;

import apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author : Ashiamd email: ashiamd@foxmail.com
 * @date : 2021/2/2 6:37 PM
 */
public class StateTest3_KeyedStateApplicationCase {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度 = 1
        env.setParallelism(1);
        // 从socket获取数据
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);
        // 转换为SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        SingleOutputStreamOperator<Tuple3<String, Double, Double>> resultStream = dataStream.keyBy(SensorReading::getId).flatMap(new MyFlatMapper(10.0));

        resultStream.print();

        env.execute();
    }

    // 如果 传感器温度 前后差距超过指定温度(这里指定10.0),就报警
    public static class MyFlatMapper extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {

        // 报警的温差阈值
        private final Double threshold;

        // 记录上一次的温度
        ValueState<Double> lastTemperature;

        public MyFlatMapper(Double threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 从运行时上下文中获取keyedState
            lastTemperature = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class));
        }

        @Override
        public void close() throws Exception {
            // 手动释放资源
            lastTemperature.clear();
        }

        @Override
        public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            Double lastTemp = lastTemperature.value();
            Double curTemp = value.getTemperature();

            // 如果不为空，判断是否温差超过阈值，超过则报警
            if (lastTemp != null) {
                if (Math.abs(curTemp - lastTemp) >= threshold) {
                    out.collect(new Tuple3<>(value.getId(), lastTemp, curTemp));
                }
            }

            // 更新保存的"上一次温度"
            lastTemperature.update(curTemp);
        }
    }
}
