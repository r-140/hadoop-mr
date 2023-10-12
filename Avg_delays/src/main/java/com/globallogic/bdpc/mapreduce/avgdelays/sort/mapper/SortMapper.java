package com.globallogic.bdpc.mapreduce.avgdelays.sort.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

public class SortMapper extends Mapper<Object, Object, DoubleWritable, Text> {
    final static Logger logger = Logger.getLogger(SortMapper.class);

    private TreeMap<Double, String> tmap;

    private static final int MAX_RESULT = 5;

    int outputSize;
    private static final String VALUE_SEPARATOR = ":::";

    private static final String DELAY_SEPARATOR = ":";

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        tmap = new TreeMap<>(Collections.reverseOrder());
        outputSize = context.getConfiguration().getInt("avgdelay.max.output.size", MAX_RESULT);
        logger.info("SortMapper maxoutputsize " + outputSize);
    }

    public void map(Object key, Object value, Context context) throws IOException, InterruptedException {

        logger.info("SortMapper airline key  " + key + ", average delay values " + value);

        String valueStr = value.toString();
        String[] values = valueStr.split(VALUE_SEPARATOR);

        String outputKey = values[0] + ", " + values[1];
        String delayStr = values[2].split(DELAY_SEPARATOR)[1];

        Double delay = Double.parseDouble(delayStr);

        tmap.put(delay, outputKey);

        if(tmap.size() > outputSize) {
            tmap.remove(tmap.lastKey());
        }
        logger.info("SortMapper map " + tmap);
    }

    @Override
    public void cleanup(Context context)
            throws IOException, InterruptedException {
        for(Map.Entry<Double, String> entry : tmap.entrySet()) {
            Double delay = entry.getKey();
            String outputKey = entry.getValue();
            context.write(new DoubleWritable(delay), new Text(outputKey));
        }
    }
}
