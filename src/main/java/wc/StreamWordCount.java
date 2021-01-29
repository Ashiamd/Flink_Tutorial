package wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author : Ashiamd email: ashiamd@foxmail.com
 * @date : 2021/1/29 11:13 PM
 */
public class StreamWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();

        // 从文件中读取数据
        String inputPath = "/Users/ashiamd/mydocs/docs/study/javadocument/javadocument/IDEA_project/Flink_Tutorial/src/main/resources/hello.txt";
        DataStream<String> inputDataStream = env.readTextFile(inputPath);

        DataStream<Tuple2<String,Integer>> resultStream = inputDataStream.flatMap(new WordCount.MyFlatMapper())
                .keyBy(item->item.f0)
                .sum(1);

        resultStream.print();

        env.execute();
    }
}
