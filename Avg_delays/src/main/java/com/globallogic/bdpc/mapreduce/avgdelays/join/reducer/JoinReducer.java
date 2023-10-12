package com.globallogic.bdpc.mapreduce.avgdelays.join.reducer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

public class JoinReducer extends Reducer<Text, Text, NullWritable, Text> {

    private final static Logger logger = Logger.getLogger(JoinReducer.class);

    private static final String NAME_PREFIX = "name";
    private static final String DELAY_PREFIX = "delay";
    private static final String VALUE_SEPARATOR = ":";
    private static final String MERGE_SEPARATOR = ":::";
    private static final String MERGE_DELAY_PREFIX = "DELAY:";

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String airLineName = "";
        String delay = "";
        double sum = 0;
        int counter = 0;
        for(Text value : values) {
            final String valueStr = value.toString();
            if (valueStr.startsWith(NAME_PREFIX)) {
                airLineName = valueStr.split(VALUE_SEPARATOR)[1];
            } else if (valueStr.startsWith(DELAY_PREFIX)) {
                delay = valueStr.endsWith(VALUE_SEPARATOR) ? String.valueOf(0)
                        : valueStr.split(VALUE_SEPARATOR)[1];
                sum += Double.parseDouble(delay);
                counter++;
            }
        }

        double average = sum/counter;

        logger.info("for the key " + key + " average delay " + average + " number of records " + counter);

        final String merged = key + MERGE_SEPARATOR + airLineName + MERGE_SEPARATOR + MERGE_DELAY_PREFIX + average;
        logger.info("JoinReducer result " + merged);
        context.write(NullWritable.get(), new Text(merged));
    }
}
