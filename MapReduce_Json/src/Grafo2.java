import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONObject;
import org.apache.hadoop.io.ObjectWritable;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

/**
 * Created by mariapia on 18/11/15.
 */
class Grafo2 {
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        //private final static IntWritable one = new IntWritable(1);


        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String subreddit;
            String link_id;
            String line = value.toString();
            String[] tuple = line.split("\\n");

            try {
                for (int i = 0; i < tuple.length; i++) {
                    JSONObject obj = new JSONObject(tuple[i]);

                    subreddit = (String) obj.get("subreddit");
                    link_id = (String) obj.get("link_id");


                    //context.write(new StringPair(link_id, subreddit), one);
                    context.write(new Text(link_id),new Text(subreddit));


                }
            } catch (Exception e) {
                e.printStackTrace();
            }


        }
    }


    public static class Reduce extends Reducer<Text, Text, Text, HashSet> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            /*int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }*/
            ArrayList<Text> as = new ArrayList<>();
            HashSet<Text> hs = new HashSet<>();
            for (Text x : values) {

                //System.out.println("X : " + x);
                hs.add(new Text(x));
            }

            context.write(key,hs);
        }
    }
}
