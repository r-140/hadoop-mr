package com.globallogic.bdpc.mapreduce.avgdelays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
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
import java.util.*;
import java.util.stream.Collectors;
//import java.util.stream.Collectors;

//https://github.com/isurunuwanthilaka/map-reduce-average-java/blob/master/java-code/src/main/java/com/isuru/Average.java
public class JoinDriver {

    final static Logger logger = Logger.getLogger(JoinDriver.class);
    public static void main(final String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Hadoop Data Join");

        job.setJarByClass(JoinDriver.class);
//        job.setNumReduceTasks(2);
        // job.setMapperClass(JoinMapper.class);
//        job.setCombinerClass(JoinReducer.class);
//        job.setSortComparatorClass(ValueComparator.class);
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

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

//        JobConf conf1 = new JobConf(JoinDriver.class);
//        conf1.setJobName("Join Job");
//        conf1.setJarByClass(JoinDriver.class);
////        conf1.setCombinerClass(JoinReducer.class);
//        conf1.setReducerClass(JoinReducer.class);

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
            String line = value.toString();
            String[] values = value.toString().split(",");

            if(headerList.length == values.length && !header.equals(line)) {
                logger.info("airline values " + Arrays.toString(values));
                context.write(new Text(values[0]), new Text("name:" + values[1]));
            }
        }
    }

    public static class JoinMapperDelay extends Mapper<Object, Text, Text, Text> {

        String[] headerList;
        String header;

        private Configuration conf;

        private static final String FLIGHTS_FILE_NAME = "flights.csv";

        int counter = 0;
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
                    logger.info("Delay mapper iata_code " + values[4]);
                    counter++;
                }
                context.write(new Text(values[4]), new Text("delay:" + values[11]));
            }
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String airLineName = "";
            String delay = "";
            double sum = 0;
            int counter = 0;
            for(Text value : values) {
                if(counter < 4) {
                    logger.info("Key reducer " + key + ", VALUE " + value);
                }

                String valueStr = value.toString();
                if (valueStr.startsWith("name")) {
                    airLineName = valueStr.split(":")[1];
                } else if (valueStr.startsWith("delay")) {
                    delay = valueStr.endsWith(":") ? String.valueOf(0)
                            : valueStr.split(":")[1];
                    sum += Double.parseDouble(delay);
                }

                counter++;
            }

            double average = sum/counter;

            logger.info("for the key " + key + " average delay " + average + " number of records " + counter);

            String mergedKey = key + ", " + airLineName;
            context.write(new Text(mergedKey), new DoubleWritable(average));
        }
    }

    public static class SortReducer extends Reducer<Text, Text, Text, DoubleWritable> {

        private Map<String, Double> delayMap = new HashMap<>();
        private Map<String, String> airlinesMap = new HashMap<>();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String airlineName = "";
            double delay = 0;
            for (Text value : values) {
                logger.info("Average reducer key " + key + " value " + value);
                String[] valueArr = value.toString().split(",");
                airlineName = valueArr[0];
                airlinesMap.put(key.toString(), airlineName);

                delay = Double.parseDouble(valueArr[1]);

                String keyStr = key.toString();
                delayMap.put(keyStr, delay);
            }

            final LinkedHashMap<String, Double> sortedByAvgMap = delayMap.entrySet().stream()
                    .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                            (oldValue, newValue) -> oldValue, LinkedHashMap::new));

            logger.info("sortedMap " + sortedByAvgMap);
            logger.info("airlinesMap " + airlinesMap);

            String merge = key + "," + airlinesMap.get(key.toString()) + "," + sortedByAvgMap.get(key.toString());

            logger.info("Final result " + merge);

            context.write(new Text(key + ", " + airlineName), new DoubleWritable(delay));
        }
    }

    public static class ValueComparator extends WritableComparator {

        protected ValueComparator() {
            super(DoubleWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            DoubleWritable aVal = (DoubleWritable) a;
            DoubleWritable bVal = (DoubleWritable) b;
            return -1 * aVal.compareTo(bVal);
        }

    }
}
