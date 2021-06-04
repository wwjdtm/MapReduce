package seoul.p4;

/* 커맨드 쉬프트 o 하면 필요없는 import 지워짐 */
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Problem4 extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Problem4(), args);

    }

    public int run(String[] args) throws Exception {
        /* driver코드 작성 */
        String inputPath = args[0];
        String outputPath = args[0] + ".out";

        Job job = Job.getInstance(getConf());
        job.setJarByClass(Problem4.class);

        job.setMapperClass(P4Mapper.class);
        job.setReducerClass(P4Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);


        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));


        job.waitForCompletion(true);
        /* 진행상황을 로그로 찍을지 말지 */


        return 0;
    }


}
