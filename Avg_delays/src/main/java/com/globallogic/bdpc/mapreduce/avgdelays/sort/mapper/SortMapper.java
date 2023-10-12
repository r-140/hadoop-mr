package com.globallogic.bdpc.mapreduce.avgdelays.sort.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;

public class SortMapper extends Mapper<Object, Object, Text, DoubleWritable> {
    final static Logger logger = Logger.getLogger(SortMapper.class);

//    private Configuration conf;
//    @Override
//    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
//        conf = context.getConfiguration();
//        URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
//        for (URI patternsURI : patternsURIs) {
//            Path patternsPath = new Path(patternsURI.getPath());
//            String patternsFileName = patternsPath.getName();
//            logger.info("airlines patternsFileName " + patternsFileName);
//            if(AIRLINES_FILE_NAME.equals(patternsFileName)) {
//                BufferedReader bufferedReader = new BufferedReader(new FileReader(patternsFileName));
//                header = bufferedReader.readLine();
//                headerList = header.split(",");
//                logger.info("airline header " + header);
//                logger.info("airline header List " + Arrays.toString(headerList));
//            }
//        }
//    }

    public void map(Object key, Object value, Context context)
            throws IOException, InterruptedException {

        logger.info("airline key  " + key + ", average delay values " + value);

        String valueStr = value.toString();
        String[] values = valueStr.split(":::");

        logger.info("splitted values " + Arrays.toString(values));

        String outputKey = values[0] + ", " + values[1];
        String delayStr = values[3].split(":")[1];
        logger.info("delay value " + delayStr);

        double delay = Double.parseDouble(delayStr);
        context.write(new Text(outputKey), new DoubleWritable(delay));

    }
}
