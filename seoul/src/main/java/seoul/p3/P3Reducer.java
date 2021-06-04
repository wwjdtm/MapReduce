package seoul.p3;

import java.io.IOException;
import java.util.StringTokenizer;

import com.google.common.base.Joiner;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class P3Reducer extends Reducer<Text, Text, Text, Text> {

  /* < 시간, 지역 > <1: ~ , 3 : ~, 8: ~ > 식으로 모으기 */
  Text ov = new Text();

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    String value = guavaJoin(values);

    ov.set("< " + value + " >");
    context.write(key, ov);

  }
  public static String guavaJoin(Iterable<Text> chars) {
    return Joiner.on("").join(chars);
  }
}



