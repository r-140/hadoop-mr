package com.globallogic.bdpc.mapreduce.avgdelays.join.reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

public class JoinReducer extends Reducer<Text, Text, NullWritable, Text> {

    final static Logger logger = Logger.getLogger(JoinReducer.class);

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

        String merged = key + ":::" + airLineName + ":::" + "DELAY:" + average;
        context.write(NullWritable.get(), new Text(merged));
    }
}
