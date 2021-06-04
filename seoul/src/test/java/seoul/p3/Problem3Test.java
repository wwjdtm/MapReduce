package seoul.p3;

import org.apache.hadoop.util.ToolRunner;

public class Problem3Test {
  public static void main(String[] args) throws Exception {

    String[] myargs = {"data.csv"};

    ToolRunner.run(new Problem3(), myargs);
  }
}
