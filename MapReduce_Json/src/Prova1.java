import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONObject;

import java.io.IOException;

/**
 * Created by mariapia on 18/11/15.
 */
class Prova1 {
    public static class Map extends Mapper<LongWritable, Text, Pair, IntWritable> {

        private final static IntWritable one = new IntWritable(1);


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

                    context.write(new Pair(link_id,subreddit), one);



                }
            } catch (Exception e) {
                e.printStackTrace();
            }


        }
    }


    public static class Reduce extends Reducer<Pair, IntWritable, Pair, IntWritable> {
        public void reduce(Pair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}
