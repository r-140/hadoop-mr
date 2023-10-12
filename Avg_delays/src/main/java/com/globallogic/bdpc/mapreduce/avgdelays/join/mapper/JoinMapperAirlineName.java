package com.globallogic.bdpc.mapreduce.avgdelays.join.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

public  class JoinMapperAirlineName extends Mapper<Object, Text, Text, Text> {
    final static Logger logger = Logger.getLogger(JoinMapperAirlineName.class);
    String[] headerList;
    String header;

    private Configuration conf;

    private static final String AIRLINES_FILE_NAME = "airlines.csv";
    @Override
    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
        conf = context.getConfiguration();
        URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
        for (URI patternsURI : patternsURIs) {
            Path patternsPath = new Path(patternsURI.getPath());
            String patternsFileName = patternsPath.getName();
            logger.info("airlines patternsFileName " + patternsFileName);
            if(AIRLINES_FILE_NAME.equals(patternsFileName)) {
                BufferedReader bufferedReader = new BufferedReader(new FileReader(patternsFileName));
                header = bufferedReader.readLine();
                headerList = header.split(",");
                logger.info("airline header " + header);
                logger.info("airline header List " + Arrays.toString(headerList));
            }
        }
    }
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] values = value.toString().split(",");

        if(headerList.length == values.length && !header.equals(line)) {
            logger.info("airline values " + Arrays.toString(values));
            context.write(new Text(values[0]), new Text("name:" + values[1]));
        }
    }
}
