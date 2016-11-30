// Import相關的Lib程式庫
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount {

    public static void main(String[] args) {
        try {
            // 取得hadoop組態物件實例
            Configuration hadoopConf = new Configuration();

            // 取得Job的物件實例，並設定Driver、Mapper、Combiner、與Reducer
            Job job = Job.getInstance(hadoopConf, "WordCount");
            job.setJarByClass(WordCount.class);
            job.setMapperClass(TokenizerMapper.class);
            job.setCombinerClass(SumReduce.class);
            job.setReducerClass(SumReduce.class);

            // 定義Key/Value的類別型別，在這裡Key為Text，Value為Integer
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            // 從args[0]取得需要運算的檔案位置，並將運算結果輸出到args[1]所指定的路徑
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            // 等待運算結束後退出程式
            int exitStatus = job.waitForCompletion(true) ? 0 : 1;
            System.exit(exitStatus);
        } catch (IOException | ClassNotFoundException | InterruptedException e) {
            e.printStackTrace(System.err);
        }
    }

    private static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 以空格(/u0020)切割Text
            StringTokenizer tokenizer = new StringTokenizer(value.toString());

            // 將切割後的結果作Map運算
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(word, one);
            }
        }
    }

    private static class SumReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // 計算該Key值的Values總和並回傳結果
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}
