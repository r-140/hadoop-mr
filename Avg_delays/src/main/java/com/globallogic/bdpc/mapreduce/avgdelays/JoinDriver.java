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
import java.util.*;
import java.util.stream.Collectors;

//https://github.com/isurunuwanthilaka/map-reduce-average-java/blob/master/java-code/src/main/java/com/isuru/Average.java
public class JoinDriver {


    final static Logger logger = Logger.getLogger(JoinDriver.class);
    public static void main(final String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Hadoop Data Join");

        job.setJarByClass(JoinDriver.class);
//        job.setNumReduceTasks(2);
        // job.setMapperClass(JoinMapper.class);
        job.setCombinerClass(JoinReducer.class);
        job.setReducerClass(AverageReducer.class);

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
                context.write(new Text(values[4]), new Text("delay:" + values[11]));
            }
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {

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

//                String merge = airLineName + "," + delay;
//                context.write(key, new Text(merge));

                counter++;
            }

            double average = sum/counter;

            logger.info("for the key " + key + " average delay " + average + " number of records " + counter);

            String merge = airLineName + "," + average;
            context.write(key, new Text(merge));
        }
    }

    public static class AverageReducer extends Reducer<Text, Text, NullWritable, Text> {

        private static final int NUMBER_ELEMENTS_TO_OUTPUT = 5;
        private Map<String, Double> delayMap = new HashMap<>();
        private Map<String, String> airlinesMap = new HashMap<>();

        private boolean IS_WRITE = true;

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String airlineName = "";

            for (Text value : values) {
                logger.info("Average reducer key " + key + " value " + value);
                String[] valueArr = value.toString().split(",");
                airlineName = valueArr[0];
                airlinesMap.put(key.toString(), airlineName);

                double delay = Double.parseDouble(valueArr[1]);

                String keyStr = key.toString();
                delayMap.put(keyStr, delay);
            }

            final Map<String, Double> sortedByAvgMap = delayMap.entrySet().stream()
                    .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                            (oldValue, newValue) -> oldValue, LinkedHashMap::new));

            logger.info("sortedMap " + sortedByAvgMap);
            logger.info("airlinesMap " + airlinesMap);

//            String merge = key + "," + airlinesMap.get(key.toString()) + "," + sortedByAvgMap.get(key.toString());
//
//            logger.info("Final result " + merge);

//            context.write(NullWritable.get(), new Text(merge));
//            no need to run the same result for all keys
            if(IS_WRITE) {
                writeResult(context, sortedByAvgMap);
                IS_WRITE = false;
            }
        }
        private void writeResult(Context context, Map<String, Double> sortedByAvgMap) throws InterruptedException, IOException {

//            Set<String> keySet = sortedByAvgMap.keySet();
            sortedByAvgMap.entrySet().stream().limit(NUMBER_ELEMENTS_TO_OUTPUT).forEach(entry -> {
                String merge = entry.getKey() + "," + airlinesMap.get(entry.getKey()) + "," + entry.getValue();
                logger.info("Final result " + merge);
                try {
                    context.write(NullWritable.get(), new Text(merge));
                } catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }

            });
        }
    }
}
