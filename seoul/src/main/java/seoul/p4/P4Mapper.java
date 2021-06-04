package seoul.p4;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class P4Mapper extends Mapper<Object, Text, Text, Text> {

    Text ok = new Text();
    Text ov = new Text();


    @Override
    protected void map(Object key, Text value,
                       Context context)
            throws IOException, InterruptedException {

        /*value 의 각한줄씩 받아옴 2017-01-01 00:00,101,1,0.004,0 */

        StringTokenizer st = new StringTokenizer(value.toString(), " ");
        st.nextToken(); /* 처음 날짜는 skip */

        String allmessage = st.nextToken(); /* 00:00,101,1,0.004,0 */
        StringTokenizer stt = new StringTokenizer(allmessage, ",");

        String time_code = stt.nextToken(); /* 00:00 */
        stt.nextToken(); /* 지역코드  skip */
        String item_code = stt.nextToken(); /* 1 */
        String item_value = stt.nextToken(); /* 0.004 */


        
        ok.set(time_code);
        ov.set(item_code + "," + item_value + "\t");

        context.write(ok, ov);

    }

}
