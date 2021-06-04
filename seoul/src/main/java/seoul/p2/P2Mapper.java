package seoul.p2;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class P2Mapper extends Mapper<Object, Text, Text, IntWritable> {

  Text ok = new Text();
  IntWritable ov = new IntWritable(1);

  @Override
  protected void map(Object key, Text value,
      Mapper<Object, Text, Text, IntWritable>.Context context)
      throws IOException, InterruptedException {

    /* 키는 무시 */
    /* value 의 각한줄씩 받아옴 2017-01-01 00:00,101,1,0.004,0 */
    /* 여기서 mapper 의 출력 키는 101. 1 */
    /* value는 0.004 */
    StringTokenizer st = new StringTokenizer(value.toString(), ","); /* ,를 기준으로 토크나이징(split)하겠다 */
    st.nextToken(); /* 처음 날짜는 skip */
    String station_code = st.nextToken(); /* 101 */
    String item_code = st.nextToken(); /* 1 */
    double item_value = Double.parseDouble(st.nextToken());


    if ((Integer.parseInt(item_code) == 8 && item_value <= 30)
        || (Integer.parseInt(item_code) == 9 && item_value <= 15)) {
      ok.set(station_code);

      context.write(ok, ov);



    }
  }



}
