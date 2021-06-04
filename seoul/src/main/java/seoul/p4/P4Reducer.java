package seoul.p4;

import java.io.IOException;
import java.util.StringTokenizer;

import com.google.common.base.Joiner;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class P4Reducer extends Reducer<Text, Text, Text, Text> {
//  00:00 , S02 0.1, NO2 0.05, ~~ 식으로 시간대별로 평균값구하기
    Text ov = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String value = guavaJoin(values);
        String []tokens = value.split("\t");// O3,0.002

        double asum =0;
        double bsum =0;
        double csum =0;
        double dsum =0;
        double esum =0;
        double fsum =0;
        int acnt=0 ;
        int bcnt=0 ;
        int ccnt=0 ;
        int dcnt=0 ;
        int ecnt=0 ;
        int fcnt=0 ;

        for(int i=0; i < tokens.length; i++) {
            StringTokenizer st = new StringTokenizer(tokens[i], ","); // O3,0.002
            String item_code = st.nextToken();
            double item_name_value = Double.parseDouble(st.nextToken());/* 0.003 */
            if (item_name_value > 0) {
                if (Integer.parseInt(item_code) == 1) {
                    asum += item_name_value;
                    acnt++;
                } else if (Integer.parseInt(item_code) == 3) {
                    bsum += item_name_value;
                    bcnt++;
                } else if (Integer.parseInt(item_code) == 5) {
                    csum += item_name_value;
                    ccnt++;
                } else if (Integer.parseInt(item_code) == 6) {
                    dsum += item_name_value;
                    dcnt++;
                } else if (Integer.parseInt(item_code) == 8) {
                    esum += item_name_value;
                    ecnt++;
                } else if (Integer.parseInt(item_code) == 9) {
                    fsum += item_name_value;
                    fcnt++;
                }
            }
        }
        String aavg = Double.toString(asum / acnt );
        String bavg = Double.toString(bsum / bcnt );
        String cavg = Double.toString(csum / ccnt );
        String davg = Double.toString(dsum / dcnt );
        String eavg = Double.toString(esum / ecnt );
        String favg = Double.toString(fsum / fcnt );


        ov.set("< " + " SO2 : " + aavg + " NO2 : " + bavg + " CO : " + cavg + " O3 : " + davg + " PM10 : " + eavg + " PM2.5 : " + favg + " >" );
        context.write(key, ov);

    }
    public static String guavaJoin(Iterable<Text> chars) {
        return Joiner.on("").join(chars);
    }
}



