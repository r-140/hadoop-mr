package com.globallogic.bdpc.mapreduce.avgdelays;

import com.globallogic.bdpc.mapreduce.avgdelays.join.mapper.JoinMapperAirlineName;
import com.globallogic.bdpc.mapreduce.avgdelays.join.mapper.JoinMapperDelay;
import com.globallogic.bdpc.mapreduce.avgdelays.join.reducer.JoinReducer;
import com.globallogic.bdpc.mapreduce.avgdelays.sort.comparator.ValueComparator;
import com.globallogic.bdpc.mapreduce.avgdelays.sort.mapper.SortMapper;
import com.globallogic.bdpc.mapreduce.avgdelays.sort.reducer.SortReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
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
    private static final String JOIN_JOB_OUTPUT_PATH = "/bdpc/hadoop_mr/avg_delay/outputjoin";
    public static void main(final String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job joinJob = Job.getInstance(conf, "Join Airlines and Delays");

        joinJob.setJarByClass(AverageDriver.class);
        joinJob.setReducerClass(JoinReducer.class);
        joinJob.setNumReduceTasks(1);
        joinJob.setMapOutputKeyClass(Text.class);
        joinJob.setMapOutputValueClass(Text.class);
        joinJob.setOutputKeyClass(Text.class);
        joinJob.setOutputValueClass(Text.class);
        joinJob.setOutputFormatClass(TextOutputFormat.class);

        Path airlinesPath = new Path(args[0]);
        Path flightsPath = new Path(args[1]);

        MultipleInputs.addInputPath(joinJob, airlinesPath, TextInputFormat.class, JoinMapperAirlineName.class);

        MultipleInputs.addInputPath(joinJob, flightsPath, TextInputFormat.class, JoinMapperDelay.class);

        joinJob.addCacheFile(airlinesPath.toUri());
        joinJob.addCacheFile(flightsPath.toUri());

        FileOutputFormat.setOutputPath(joinJob, new Path(JOIN_JOB_OUTPUT_PATH));
        joinJob.waitForCompletion(true);

        logger.info("join job has been finished, starting sorting job");

        Configuration conf2 = new Configuration();
        Job sortJob = Job.getInstance(conf2, "sortJob");
        sortJob.setJarByClass(AverageDriver.class);
        sortJob.setMapperClass(SortMapper.class);
        sortJob.setReducerClass(SortReducer.class);
        sortJob.setMapOutputKeyClass(DoubleWritable.class);
        sortJob.setMapOutputValueClass(Text.class);
        sortJob.setOutputKeyClass(Text.class);
        sortJob.setOutputValueClass(Text.class);
        sortJob.setNumReduceTasks(1);
        sortJob.setSortComparatorClass(ValueComparator.class);
        FileInputFormat.addInputPath(sortJob, new Path(JOIN_JOB_OUTPUT_PATH));
        FileOutputFormat.setOutputPath(sortJob, new Path(args[2]));

        System.exit(sortJob.waitForCompletion(true) ? 0 : 1);

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
