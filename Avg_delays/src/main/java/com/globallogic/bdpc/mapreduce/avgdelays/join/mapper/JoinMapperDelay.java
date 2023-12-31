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

public class JoinMapperDelay extends Mapper<Object, Text, Text, Text> {

    final static Logger logger = Logger.getLogger(JoinMapperDelay.class);

    String[] headerList;
    String header;

    private static final String FLIGHTS_FILE_NAME = "flights.csv";

    private static final String AIRLINE_DELAY_PREFIX = "delay:";
    private static final String SEPARATOR = ",";
    int counter = 0;
    @Override
    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
        final Configuration conf = context.getConfiguration();
        URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
        for (URI patternsURI : patternsURIs) {
            Path patternsPath = new Path(patternsURI.getPath());
            String patternsFileName = patternsPath.getName();

            logger.info("flights patternsFileName " + patternsFileName);
            if(FLIGHTS_FILE_NAME.equals(patternsFileName)) {
                BufferedReader bufferedReader = new BufferedReader(new FileReader(patternsFileName));
                header = bufferedReader.readLine();
                headerList = header.split(SEPARATOR);
                logger.info("delay header List " + Arrays.toString(headerList));
            }
        }
    }

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        final String line = value.toString();
        final String[] values = value.toString().split(SEPARATOR);

        if(headerList.length == values.length && !header.equals(line)) {
            if(counter < 4) {
                logger.info("Delay mapper iata_code " + values[4]);
                counter++;
            }
            context.write(new Text(values[4]), new Text(AIRLINE_DELAY_PREFIX + values[11]));
        }
    }
}
