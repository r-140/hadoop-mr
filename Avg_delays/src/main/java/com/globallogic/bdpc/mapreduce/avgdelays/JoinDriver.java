package com.globallogic.bdpc.mapreduce.avgdelays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

//https://github.com/isurunuwanthilaka/map-reduce-average-java/blob/master/java-code/src/main/java/com/isuru/Average.java
public class JoinDriver {
    public static void main(final String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Hadoop Data Join");

        job.setJarByClass(JoinDriver.class);
        // job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, JoinMapperAirlineName.class);

        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, JoinMapperDelay.class);

//        JobClient.runJob(job).waitForCompletion();

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class JoinMapperAirlineName extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] words = value.toString().split(",");
            context.write(new Text(words[0]), new Text("name:" + words[1]));
        }
    }

    public static class JoinMapperDelay extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] words = value.toString().split(",");
            context.write(new Text(words[3]), new Text("delay:" + words[11]));
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, NullWritable, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            System.out.println("Key " + key);
            String airLineName = "";
            String delay = "";
//            double delay_avg;
            int counter = 0;
            for(Text value : values) {
                if(counter < 4) {
                    System.out.println("VALUE " + value);
                }
                if (value.toString().startsWith("name")) {
                    airLineName = value.toString().split(":")[1];
                } else if (value.toString().startsWith("delay")){
                    delay = value.toString().split(":")[1];
                }
                counter++;
            }
            counter = 0;
            String merge = key + "," + airLineName + "," + delay;
            System.out.println("output after reducing " + merge);
            context.write(NullWritable.get(), new Text(merge));
        }
    }
}
