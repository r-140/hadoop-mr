package com.globallogic.bdpc.mapreduce.avgdelays.sort.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

public class SortMapper extends Mapper<Object, DoubleWritable, Text, DoubleWritable> {
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

    public void map(Object key, DoubleWritable value, Context context)
            throws IOException, InterruptedException {

        logger.info("airline key  " + key + ", average delay values " + value);
        context.write(new Text(key.toString()), value);

    }
}
