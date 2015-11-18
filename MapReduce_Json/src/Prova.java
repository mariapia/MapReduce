/**
 * Created by mariapia on 14/10/15.
 */
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.json.JSONObject;


public class Prova {
    public static class Map extends Mapper<LongWritable, Text, Pair, IntWritable> {

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

                    context.write(new Pair(author, subreddit), one);


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






        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            conf.set("mapred.textoutputformat.separator", ",");
            Job job = new Job(conf, "wordcount");


            job.setMapOutputKeyClass(Pair.class);

            job.setOutputKeyClass(Pair.class);
            job.setOutputValueClass(IntWritable.class);


            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            if (args[2].equals("0")) {
                new Prova();

                job.setMapperClass(Prova.Map.class);
                job.setReducerClass(Prova.Reduce.class);

            } else if (args[2].equals("1")) {
                new Prova1();

                job.setMapperClass(Prova1.Map.class);
                job.setReducerClass(Prova1.Reduce.class);
            }

            job.waitForCompletion(true);

        }
    }

