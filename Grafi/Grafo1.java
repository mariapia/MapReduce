package Grafi; /**
 * Created by mariapia on 14/10/15.
 */
import java.io.IOException;

import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;
import org.json.JSONObject;


public class Grafo1 {
    public static class Map extends Mapper<LongWritable, Text, StringPair, IntWritable> {

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

                    context.write(new StringPair(author, subreddit), one);


                }
            } catch (Exception e) {
                e.printStackTrace();
            }


        }
    }


    public static class Reduce extends Reducer<StringPair, IntWritable, StringPair, IntWritable> {
        public void reduce(StringPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }



}