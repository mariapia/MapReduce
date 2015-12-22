package Grafi;

import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONObject;


import java.io.IOException;
import java.util.*;

/**
 * Created by mariapia on 23/11/15.
 */
class Grafo3 {
    public static class Map extends Mapper<LongWritable, Text, Text, StringBoolPair> {

        //private final static IntWritable one = new IntWritable(1);


        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String parent;
            String id;
            String author;
            String line = value.toString();
            String[] tuple = line.split("\\n");

            try {
                for (int i = 0; i < tuple.length; i++) {
                    JSONObject obj = new JSONObject(tuple[i]);

                    parent = (String) obj.get("parent_id");
                    author = (String) obj.get("author");
                    id = (String) obj.get("id");
                    parent = parent.substring(3);

                    if(!author.equals("[deleted]")) {
                        context.write(new Text(id), new StringBoolPair(author, true));
                        context.write(new Text(parent), new StringBoolPair(author, false));
                    }


                }

            } catch (Exception e) {
                e.printStackTrace();
            }


        }
    }

    public static class Reduce extends Reducer<Text, StringBoolPair, StringPair, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        public void reduce(Text key, Iterable<StringBoolPair> values, Context context) throws IOException, InterruptedException {


            ArrayList<StringBoolPair> val = new ArrayList<>();


            String source = new String(" ");
            String target = new String(" ");


            for (StringBoolPair x : values) {
                val.add(new StringBoolPair(x));
            }

            for (StringBoolPair y : val) {
                if (y.returnSecond() == true) {
                    target = y.returnFirst();
                    break;
                }
            }

            for (StringBoolPair k : val) {
                if (k.returnSecond() == false && (!target.equals(" "))) {
                    source = k.returnFirst();
                    context.write(new StringPair(source,target), one );
                }
            }



        }
        }


   public static class Map1 extends Mapper<LongWritable, Text, StringPair, IntWritable> {

        private final static IntWritable one = new IntWritable(1);


        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] tuple = line.split(",");



            context.write(new StringPair(tuple[0].toString(), tuple[1].toString()), one);


        }
    }


    public static class Reduce1 extends Reducer<StringPair, IntWritable, StringPair, IntWritable> {
        public void reduce(StringPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));


        }
    }

}
