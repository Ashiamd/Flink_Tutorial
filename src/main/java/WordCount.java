import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author : Ashiamd email: ashiamd@foxmail.com
 * @date : 2021/1/29 10:46 PM
 * 批处理 wordcount
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 从文件中读取数据
        String inputPath = "/Users/ashiamd/mydocs/docs/study/javadocument/javadocument/IDEA_project/Flink_Tutorial/src/main/resources/hello.txt";
        DataSet<String> inputDataSet = env.readTextFile(inputPath);

        // 对数据集进行处理，按空格分词展开，转换成(word, 1)二元组进行统计
        // 按照第一个位置的word分组
        // 按照第二个位置上的数据求和
        DataSet<Tuple2<String, Integer>> resultSet = inputDataSet.flatMap(new MyFlatMapper())
                .groupBy(0)
                .sum(1);

        resultSet.print();
    }

    // 自定义类，实现FlatMapFunction接口
    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 按空格分词
            String[] words = s.split(" ");
            // 遍历所有word，包成二元组输出
            for (String str : words) {
                out.collect(new Tuple2<>(str, 1));
            }
        }
    }

}
