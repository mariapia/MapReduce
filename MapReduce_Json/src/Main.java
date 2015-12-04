import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;

/**
 * Created by mariapia on 02/12/15.
 */
public class Main {
    public static void main(String[] args) throws Exception {

        if (args[2].equals("grafo1")) {

            if (args.length != 3) {
                System.out.println("Errore! Inserire input, output e tipo di grafo");
                System.exit(0);
            }

            Configuration conf = new Configuration();
            conf.set("mapred.textoutputformat.separator", ",");
            Job job = new Job(conf, "wordcount");

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            job.setMapOutputKeyClass(StringPair.class);

            job.setOutputKeyClass(StringPair.class);
            job.setOutputValueClass(IntWritable.class);

            job.setMapperClass(Grafo1.Map.class);
            job.setReducerClass(Grafo1.Reduce.class);

            job.waitForCompletion(true);


        } else if (args[2].equals("grafo2")) {

            if (args.length != 3) {
                System.out.println("Errore! Inserire input, output e tipo di grafo");
                System.exit(0);
            }

            Configuration conf = new Configuration();
            conf.set("mapred.textoutputformat.separator", ",");
            Job job = new Job(conf, "wordcount");

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));



            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(HashSet.class);

            job.setMapperClass(Grafo2.Map.class);
            job.setReducerClass(Grafo2.Reduce.class);

            job.waitForCompletion(true);


        } else if (args[2].equals("grafo3")) {

            if (args.length != 4) {
                System.out.println("Errore! Inserire input, output, tipo di grafo e output temporale");
                System.exit(0);
            }

            Configuration conf = new Configuration();
            conf.set("mapred.textoutputformat.separator", ",");
            Job job3a = new Job(conf, "grafo3");

            job3a.setMapperClass(Grafo3.Map.class);
            job3a.setReducerClass(Grafo3.Reduce.class);

            job3a.setInputFormatClass(TextInputFormat.class);
            job3a.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job3a, new Path(args[0]));
            FileOutputFormat.setOutputPath(job3a, new Path(args[3]));

            job3a.setMapOutputKeyClass(Text.class);
            job3a.setMapOutputValueClass(StringBoolPair.class);

            job3a.setOutputKeyClass(StringPair.class);
            job3a.setOutputValueClass(IntWritable.class);



            job3a.waitForCompletion(true);



            Job job3b = new Job(conf, "grafo3");


            job3b.setInputFormatClass(TextInputFormat.class);
            job3b.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job3b, new Path(args[3]));
            FileOutputFormat.setOutputPath(job3b, new Path(args[1]));


            job3b.setMapOutputKeyClass(StringPair.class);
            job3b.setMapOutputValueClass(IntWritable.class);



            job3b.setOutputKeyClass(StringPair.class);
            job3b.setOutputValueClass(IntWritable.class);

            job3b.setMapperClass(Grafo3.Map1.class);
            job3b.setReducerClass(Grafo3.Reduce1.class);


            job3b.waitForCompletion(true);


        }  else {
            System.out.println("Errore nei parametri. Scegliere tra <grafo1, grafo2, grafo3>");
            System.exit(0);
        }



    }
}
