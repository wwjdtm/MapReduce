package seoul.p2;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class P2Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

  /* 최종결과 emit 할때 위의 타입과 맞춰서 보내줘야함 */
  //
  IntWritable ov = new IntWritable();
  Text oe = new Text();
  int max = 0;
  String maxkey = new String();

  @Override
  protected void reduce(Text key, Iterable<IntWritable> values,
      Reducer<Text, IntWritable, Text, IntWritable>.Context context)
      throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable v : values) {
        sum += v.get();
      }

    if (key.toString()=="125"){
        ov.set(max);
        oe.set(maxkey);
        context.write(oe,ov);
    }

  }

}

