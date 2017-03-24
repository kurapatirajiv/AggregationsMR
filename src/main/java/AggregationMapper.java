import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Rajiv on 3/23/17.
 */
public class AggregationMapper extends Mapper<LongWritable,Text,Text,Text> {


    private Text myKey = new Text("MyKey");
    private int count;
    private int min = Integer.MAX_VALUE;
    private int max = Integer.MIN_VALUE;
    private long sum;

    public void map(LongWritable key,Text values, Context context) throws IOException,InterruptedException{

        count = count + 1;
        int valueCol = Integer.parseInt(values.toString().split(" ")[1]);
        sum = sum + valueCol;
        max = valueCol>max ? valueCol:max;
        min = valueCol<min ? valueCol:min;

    }

    public void cleanup(Mapper.Context context) throws IOException, InterruptedException {

        context.write(myKey,new Text(count+" "+sum+" "+min+" "+max));

    }






}
