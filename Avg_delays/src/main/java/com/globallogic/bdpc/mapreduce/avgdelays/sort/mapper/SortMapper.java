package com.globallogic.bdpc.mapreduce.avgdelays.sort.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;

public class SortMapper extends Mapper<Object, Object, DoubleWritable, Text> {
    final static Logger logger = Logger.getLogger(SortMapper.class);


    public void map(Object key, Object value, Context context)
            throws IOException, InterruptedException {

        logger.info("airline key  " + key + ", average delay values " + value);

        String valueStr = value.toString();
        String[] values = valueStr.split(":::");

        String outputKey = values[0] + ", " + values[1];
        String delayStr = values[2].split(":")[1];

        double delay = Double.parseDouble(delayStr);
        context.write(new DoubleWritable(delay), new Text(outputKey));
    }
}
