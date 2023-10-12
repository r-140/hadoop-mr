package com.globallogic.bdpc.mapreduce.avgdelays.sort.mapper;

import com.globallogic.bdpc.mapreduce.avgdelays.join.reducer.JoinReducer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;

public class SortMapper extends Mapper<Text, DoubleWritable, Text, DoubleWritable> {
    final static Logger logger = Logger.getLogger(SortMapper.class);
    public void map(Text key, DoubleWritable value, Context context)
            throws IOException, InterruptedException {

        logger.info("airline key  " + key + ", average delay values " + value);
        context.write(key, value);

    }
}
