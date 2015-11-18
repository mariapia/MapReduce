/**
 * Created by mariapia on 14/10/15.
 */
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.json.JSONException;
import org.json.JSONObject;


public class Prova {
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);


        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String subreddit;
            String author;
            String line = value.toString();
            String[] tuple = line.split("\\n");

            try {
                for (int i = 0; i < tuple.length; i++) {
                    JSONObject obj = new JSONObject(tuple[i]);

                    subreddit = (String) obj.get("subreddit");
                    author = (String) obj.get("author");

                    context.write(new Text(author + "," + subreddit + "," ), one);




                }
            }
            catch (Exception e){
                e.printStackTrace();
            }


        }
    }


    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "wordcount");

        Path path = new Path("/home/mariapia/Scaricati/org.json-20120521.jar");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        DistributedCache.addFileToClassPath(path, conf);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }

}