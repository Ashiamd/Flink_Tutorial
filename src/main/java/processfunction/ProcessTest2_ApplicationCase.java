package processfunction;

import apitest.beans.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author : Ashiamd email: ashiamd@foxmail.com
 * @date : 2021/2/3 1:02 AM
 */
public class ProcessTest2_ApplicationCase {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为1
        env.setParallelism(1);
        // 从socket中获取数据
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);
        // 转换数据为SensorReading类型
        DataStream<SensorReading> sensorReadingStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });
        // 如果存在连续10s内温度持续上升的情况，则报警
        sensorReadingStream.keyBy(SensorReading::getId)
                .process(new TempConsIncreWarning(Time.seconds(10).toMilliseconds()))
                .print();
        env.execute();
    }

    // 如果存在连续10s内温度持续上升的情况，则报警
    public static class TempConsIncreWarning extends KeyedProcessFunction<String, SensorReading, String> {

        public TempConsIncreWarning(Long interval) {
            this.interval = interval;
        }

        // 报警的时间间隔(如果在interval时间内温度持续上升，则报警)
        private Long interval;

        // 上一个温度值
        private ValueState<Double> lastTemperature;
        // 最近一次定时器的触发时间(报警时间)
        private ValueState<Long> recentTimerTimeStamp;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTemperature = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTemperature", Double.class));
            recentTimerTimeStamp = getRuntimeContext().getState(new ValueStateDescriptor<Long>("recentTimerTimeStamp", Long.class));
        }

        @Override
        public void close() throws Exception {
            lastTemperature.clear();
            recentTimerTimeStamp.clear();
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            // 当前温度值
            double curTemp = value.getTemperature();
            // 上一次温度(没有则设置为当前温度)
            double lastTemp = lastTemperature.value() != null ? lastTemperature.value() : curTemp;
            // 计时器状态值(时间戳)
            Long timerTimestamp = recentTimerTimeStamp.value();

            // 如果 当前温度 > 上次温度 并且 没有设置报警计时器，则设置
            if (curTemp > lastTemp && null == timerTimestamp) {
                long warningTimestamp = ctx.timerService().currentProcessingTime() + interval;
                ctx.timerService().registerProcessingTimeTimer(warningTimestamp);
                recentTimerTimeStamp.update(warningTimestamp);
            }
            // 如果 当前温度 < 上次温度，且 设置了报警计时器，则清空计时器
            else if (curTemp <= lastTemp && timerTimestamp != null) {
                ctx.timerService().deleteProcessingTimeTimer(timerTimestamp);
                recentTimerTimeStamp.clear();
            }
            // 更新保存的温度值
            lastTemperature.update(curTemp);
        }

        // 定时器任务
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 触发报警，并且清除 定时器状态值
            out.collect("传感器" + ctx.getCurrentKey() + "温度值连续" + interval + "ms上升");
            recentTimerTimeStamp.clear();
        }
    }
}
