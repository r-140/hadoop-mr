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

public class SortReducer extends Reducer<DoubleWritable, Text, NullWritable, Text> {

    final static Logger logger = Logger.getLogger(JoinReducer.class);

//    private List<String> resultList = new ArrayList<>();



    public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        logger.info("SortReducer key " + key);
        double delay = key.get();
        String airline = "";
        for (Text value : values) {
            logger.info("sort reducer key " + key + " value " + value);
            airline = value.toString();

        }
        String merge = airline + ", " + delay;

        logger.info("SortReducer result " + merge);
//        resultList.add(merge);
        context.write(NullWritable.get(), new Text(merge));
    }

//    @Override
//    public void cleanup(Context context) throws IOException, InterruptedException {
//        for (int i =0; i<=NUMBER_TO_OUTPUT; i++) {
//            context.write(NullWritable.get(), new Text(resultList.get(i)));
//        }
//    }
}
