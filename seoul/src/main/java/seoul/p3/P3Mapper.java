package seoul.p3;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class P3Mapper extends Mapper<Object, Text, Text, Text> {

  Text ok = new Text();
  Text ov = new Text();

  @Override
  protected void map(Object key, Text value,
      Mapper<Object, Text, Text, Text>.Context context)
      throws IOException, InterruptedException {

    StringTokenizer st = new StringTokenizer(value.toString(), ",");
    String Time_code = st.nextToken(); /* 날짜 */
    String station_code = st.nextToken(); /* 지역 */

    String item_code = st.nextToken(); /* 1 */
    String item_value = st.nextToken();

    ok.set(Time_code + "\t" + station_code);
    ov.set(item_code + " : " + item_value + " , ");

    context.write(ok, ov);

    }

}
