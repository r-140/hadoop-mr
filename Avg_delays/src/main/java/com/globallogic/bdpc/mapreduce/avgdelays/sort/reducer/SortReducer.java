package com.globallogic.bdpc.mapreduce.avgdelays.sort.reducer;

import com.globallogic.bdpc.mapreduce.avgdelays.join.reducer.JoinReducer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class SortReducer extends Reducer<DoubleWritable, Text, NullWritable, Text> {

    final static Logger logger = Logger.getLogger(JoinReducer.class);

    private TreeMap<Double, String> tmap2;

    private static final int MAX_RESULT = 5;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        tmap2 = new TreeMap<>();
    }

    public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        logger.info("SortReducer key " + key);
        Double delay = key.get();
        String airline = "";
        for (Text value : values) {
            airline = value.toString();
        }

        tmap2.put(delay, airline);

        if (tmap2.size() > MAX_RESULT) {
            tmap2.remove(tmap2.firstKey());
        }

//        String merge = airline + ", " + delay;
//
//        logger.info("SortReducer result " + merge);
////        resultList.add(merge);
//        context.write(NullWritable.get(), new Text(merge));
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {

        for (Map.Entry<Double, String> entry : tmap2.entrySet()) {
            double delay = entry.getKey();
            String airline = entry.getValue();

            String merge = airline + ", " + delay;

            logger.info("SortReducer result " + merge);

            context.write(NullWritable.get(), new Text(merge));
        }
    }
}

//    @Override
//    public void cleanup(Context context) throws IOException, InterruptedException {
//        for (int i =0; i<=NUMBER_TO_OUTPUT; i++) {
//            context.write(NullWritable.get(), new Text(resultList.get(i)));
//        }
//    }
