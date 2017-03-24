import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Rajiv on 3/23/17.
 */
public class AggregationReducer extends Reducer<Text, Text, Text,Text> {


    private Text myKey = new Text("Result");
    private int count;
    private int min = Integer.MAX_VALUE;
    private int max = Integer.MIN_VALUE;
    private long sum;

    public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException{

          for(Text val : values){
              String myVal[] = val.toString().split(" ");
              count = count + Integer.parseInt(myVal[0]);
              sum = sum + Integer.parseInt(myVal[1]);
              min = Integer.parseInt(myVal[2])<min ? Integer.parseInt(myVal[2]):min;
              max = Integer.parseInt(myVal[3])>max ? Integer.parseInt(myVal[3]):max;
          }

    }

    protected void cleanup(Context context) throws IOException, InterruptedException {

        context.write(myKey,new Text("Count:"+count+" Sum:"+sum+" Average:"+((float)sum/count)+" Min:"+min+" Max:"+max));
    }


}
