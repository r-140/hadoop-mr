package com.globallogic.bdpc.mapreduce.avgdelays;

import com.globallogic.bdpc.mapreduce.avgdelays.join.mapper.JoinMapperAirlineName;
import com.globallogic.bdpc.mapreduce.avgdelays.join.mapper.JoinMapperDelay;
import com.globallogic.bdpc.mapreduce.avgdelays.join.reducer.JoinReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
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
public class AverageDriver {

    final static Logger logger = Logger.getLogger(AverageDriver.class);
    public static void main(final String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Hadoop Data Join");

        job.setJarByClass(AverageDriver.class);
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









//    public static class ValueComparator extends WritableComparator {
//
//        protected ValueComparator() {
//            super(DoubleWritable.class, true);
//        }
//
//        @Override
//        public int compare(WritableComparable a, WritableComparable b) {
//            DoubleWritable aVal = (DoubleWritable) a;
//            DoubleWritable bVal = (DoubleWritable) b;
//            return -1 * aVal.compareTo(bVal);
//        }
//
//    }
}
