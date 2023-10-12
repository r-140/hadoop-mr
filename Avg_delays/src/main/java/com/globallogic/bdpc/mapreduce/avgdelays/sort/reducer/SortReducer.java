package com.globallogic.bdpc.mapreduce.avgdelays.sort.reducer;

import com.globallogic.bdpc.mapreduce.avgdelays.join.reducer.JoinReducer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

public class SortReducer extends Reducer<DoubleWritable, Text, NullWritable, Text> {

    final static Logger logger = Logger.getLogger(JoinReducer.class);
    private static final String COMMA_SEPARATOR = ", ";

    public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        logger.info("SortReducer key " + key);
        double delay = key.get();
        String airline = "";
        for (Text value : values) {
            airline = value.toString();
        }
        final String merge = airline + COMMA_SEPARATOR + delay;

        logger.info("SortReducer result " + merge);
        context.write(NullWritable.get(), new Text(merge));

    }
}

