package seoul.p1;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class P1Reducer extends Reducer<Text, DoubleWritable, Text, Text> {

  /* 최종결과 emit 할때 위의 타입과 맞춰서 보내줘야함 */
  Text ov = new Text();

  @Override
  protected void reduce(Text key, Iterable<DoubleWritable> values,
      Reducer<Text, DoubleWritable, Text, Text>.Context context)
      throws IOException, InterruptedException {

    /* 우리가 원하는것은 values의 평균값 */
    double sum =0;
    int cnt=0 ;
    /* 우리가 원하는것은 values의 최대값 */
    double max = Double.NEGATIVE_INFINITY;
    /* 우리가 원하는것은 values의 최소값 */
    double min = Double.POSITIVE_INFINITY;

    for (DoubleWritable d : values ) {
      double v = d.get();

      if (v < 0)
        continue;
      if (v < min)
        min = v;
      if (v > max)
        max = v;

      sum += v; /* d 의 get을 해야 값이 나옴 */
      cnt += 1;
    }
    
    double avg = sum / cnt ;
    ov.set(avg + "\t" + max + "\t" + min);
    context.write(key, ov);
  }



}