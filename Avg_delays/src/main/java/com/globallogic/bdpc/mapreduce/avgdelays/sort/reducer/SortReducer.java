package com.globallogic.bdpc.mapreduce.avgdelays.sort.reducer;

import com.globallogic.bdpc.mapreduce.avgdelays.join.reducer.JoinReducer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class SortReducer extends Reducer<Text, Text, Text, DoubleWritable> {

    final static Logger logger = Logger.getLogger(JoinReducer.class);

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
