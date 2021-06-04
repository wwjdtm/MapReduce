package seoul.p1;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class P1Mapper extends Mapper<Object, Text, Text, DoubleWritable> {

  Text ok = new Text();

  /* 리듀서로 emit 할때 위의 타입과 맞춰서 보내줘야함 */
  DoubleWritable ov = new DoubleWritable();

  @Override
  protected void map(Object key, Text value,
      Mapper<Object, Text, Text, DoubleWritable>.Context context)
      throws IOException, InterruptedException {

    /* 키는 무시 */
    /*value 의 각한줄씩 받아옴 2017-01-01 00:00,101,1,0.004,0 */
    /*여기서 mapper 의 출력 키는 101. 1 */
    /*value는 0.004*/
    StringTokenizer st = new StringTokenizer(value.toString(), ","); /* ,를 기준으로 토크나이징(split)하겠다 */
    st.nextToken(); /* 처음 날짜는 skip */
    String station_code = st.nextToken(); /* 101 */
    String item_code = st.nextToken(); /* 1 */
    if (Integer.parseInt(item_code) == 8) {
      ok.set(station_code + "\t" + item_code);
    /* 두개 합쳐서 키로 만듦 */

    double item_value = Double.parseDouble(st.nextToken());

    ov.set(item_value);
    /* doublewirtable 로 맞춰서 value 만듦 */

    context.write(ok, ov);
    /* 키 벨류 묶어서 emit을 하는 작업 */

    /* 리듀서에서는 평균을 구하는 작업 진행 */
  }
}



}
