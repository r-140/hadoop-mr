package com.globallogic.bdpc.mapreduce.avgdelays.sort.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

public class SortMapper extends Mapper<Object, Object, DoubleWritable, Text> {
    final static Logger logger = Logger.getLogger(SortMapper.class);

    private TreeMap<Double, String> tmap;

    private static final int MAX_RESULT = 5;

    @Override
    public void setup(Context context)
            throws IOException, InterruptedException {
        tmap = new TreeMap<Double, String>();
    }

    public void map(Object key, Object value, Context context)
            throws IOException, InterruptedException {

        logger.info("airline key  " + key + ", average delay values " + value);

        String valueStr = value.toString();
        String[] values = valueStr.split(":::");

        String outputKey = values[0] + ", " + values[1];
        String delayStr = values[2].split(":")[1];

        Double delay = Double.parseDouble(delayStr);

        tmap.put(delay, outputKey);

        logger.info("SortMapper ouput " + outputKey + " delay " + delay);
        if(tmap.size() > MAX_RESULT) {
            tmap.remove(tmap.firstKey());
        }
    }

    @Override
    public void cleanup(Context context)
            throws IOException, InterruptedException {
        for (Map.Entry<Double, String> entry : tmap.entrySet()) {
            Double delay = entry.getKey();
            String outputKey = entry.getValue();
            context.write(new DoubleWritable(delay), new Text(outputKey));
        }
    }
}
