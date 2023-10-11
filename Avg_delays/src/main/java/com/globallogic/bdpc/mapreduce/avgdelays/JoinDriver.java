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
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

//https://github.com/isurunuwanthilaka/map-reduce-average-java/blob/master/java-code/src/main/java/com/isuru/Average.java
public class JoinDriver {


    final static Logger logger = Logger.getLogger(JoinDriver.class);
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

        Path airlinesPath = new Path(args[0]);
        Path flightsPath = new Path(args[1]);

        MultipleInputs.addInputPath(job, airlinesPath, TextInputFormat.class, JoinMapperAirlineName.class);

        MultipleInputs.addInputPath(job, flightsPath, TextInputFormat.class, JoinMapperDelay.class);

        job.addCacheFile(airlinesPath.toUri());
        job.addCacheFile(flightsPath.toUri());

//        JobClient.runJob(job).waitForCompletion();

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class JoinMapperAirlineName extends Mapper<Object, Text, Text, Text> {
        String[] headerList;
        String header;

        private Configuration conf;

        private static final String AIRLINES_FILE_NAME = "airlines.csv";
        @Override
        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration();
            URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
            for (URI patternsURI : patternsURIs) {
                Path patternsPath = new Path(patternsURI.getPath());
                String patternsFileName = patternsPath.getName();
                logger.info("airlines patternsFileName " + patternsFileName);
                if(AIRLINES_FILE_NAME.equals(patternsFileName)) {
                    BufferedReader bufferedReader = new BufferedReader(new FileReader(patternsFileName));
                    header = bufferedReader.readLine();
                    headerList = header.split(",");
                    logger.info("airline header " + header);
                    logger.info("airline header List " + Arrays.toString(headerList));
                }
            }
        }
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            logger.info("JoinMapperAirlineName Key " + key);
            String line = value.toString();
            String[] values = value.toString().split(",");

            if(headerList.length == values.length && !header.equals(line)) {
                logger.info("airline values " + Arrays.toString(values));
                for(int i = 0; i < values.length; i++) {
                    context.write(new Text(values[0]), new Text("name:" + values[1]));
                }
            }
        }
    }

    public static class JoinMapperDelay extends Mapper<Object, Text, Text, Text> {

        String[] headerList;
        String header;

        private Configuration conf;

        private static final String FLIGHTS_FILE_NAME = "flights.csv";

        int counter;
        @Override
        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration();
            URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
            for (URI patternsURI : patternsURIs) {
                Path patternsPath = new Path(patternsURI.getPath());
                String patternsFileName = patternsPath.getName();

                logger.info("flights patternsFileName " + patternsFileName);
                if(FLIGHTS_FILE_NAME.equals(patternsFileName)) {
                    BufferedReader bufferedReader = new BufferedReader(new FileReader(patternsFileName));
                    header = bufferedReader.readLine();
                    headerList = header.split(",");
                    logger.info("delay header " + header);
                    logger.info("delay header List " + Arrays.toString(headerList));
                }
            }
        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] values = value.toString().split(",");

            if(headerList.length == values.length && !header.equals(line)) {
                if(counter < 4) {
                    logger.info("flight values " + Arrays.toString(values));
                }
                for(int i = 0; i < values.length; i++) {
                    context.write(new Text(values[4]), new Text("name:" + values[11]));
                    counter++;
                }
            }
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, NullWritable, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String airLineName = "";
            String delay = "";

            int counter = 0;
            for(Text value : values) {
                if(counter < 4) {
                    logger.info("Key reducer " + key + ", VALUE " + value);
                }

                try {
                    String valueStr = value.toString();
                    if (valueStr.startsWith("name")) {
                        airLineName = valueStr.split(":")[1];
                    } else if (valueStr.startsWith("delay")) {
                        delay = valueStr.substring(valueStr.length()-1).equals(":") ? String.valueOf(0)
                                : valueStr.split(":")[1];
                    }
                } catch (java.lang.ArrayIndexOutOfBoundsException e) {
                    logger.info("key " + key + " value for exception " + value);
                    delay = String.valueOf(0);
                }
                counter++;
            }
            counter = 0;
            String merge = key + "," + airLineName + "," + delay;
            logger.info("output after reducing " + merge);
            context.write(NullWritable.get(), new Text(merge));
        }
    }
}
