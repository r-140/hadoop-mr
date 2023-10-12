package com.globallogic.bdpc.mapreduce.avgdelays.join.mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class JoinMapperDelayTest {
    @Mock
    private Context mockContext;

    private JoinMapperDelay joinMapperDelay;

    @Before
    public void setup(){
        joinMapperDelay = new JoinMapperDelay();
    }

    @Test
    public void mapTest_whenNormalText_shouldWriteToOutput() throws IOException, InterruptedException {

        String line1 = "2015,1,3,4,AA,258,\n" +
                "N3HYAA,ANC,SEA,0005,\n" +
                "2354,-11,21";
        String[] headerList = {"YEAR", "MONTH","DAY", "DAY_OF_WEEK", "AIRLINE", "FLIGHT_NUMBER",
                "TAIL_NUMBER", "ORIGIN_AIRPORT", "DESTINATION_AIRPORT", "SCHEDULED_DEPARTURE",
        "DEPARTURE_TIME", "DEPARTURE_DELAY", "TAXI_OUT"};
        String header = "YEAR,MONTH,DAY,DAY_OF_WEEK,AIRLINE,FLIGHT_NUMBER,\n" +
                "TAIL_NUMBER,ORIGIN_AIRPORT,DESTINATION_AIRPORT,SCHEDULED_DEPARTURE,\n" +
                "DEPARTURE_TIME,DEPARTURE_DELAY,TAXI_OUT";
        Whitebox.setInternalState(joinMapperDelay,"headerList", headerList);
        Whitebox.setInternalState(joinMapperDelay,"header",header);
        joinMapperDelay.map(null, new Text(line1), mockContext);
        verify(mockContext).write(new Text("AA"), new Text("delay:-11"));
    }

    @Test
    public void mapTest_whenNormalText_nothingShouldWriteToOutput() throws IOException, InterruptedException {

        String line1 = "YEAR,MONTH,DAY,DAY_OF_WEEK,AIRLINE,FLIGHT_NUMBER,\n" +
                "TAIL_NUMBER,ORIGIN_AIRPORT,DESTINATION_AIRPORT,SCHEDULED_DEPARTURE,\n" +
                "DEPARTURE_TIME,DEPARTURE_DELAY,TAXI_OUT";
        String[] headerList = {"YEAR", "MONTH","DAY", "DAY_OF_WEEK", "AIRLINE", "FLIGHT_NUMBER",
                "TAIL_NUMBER", "ORIGIN_AIRPORT", "DESTINATION_AIRPORT", "SCHEDULED_DEPARTURE",
                "DEPARTURE_TIME", "DEPARTURE_DELAY", "TAXI_OUT"};
        String header = "YEAR,MONTH,DAY,DAY_OF_WEEK,AIRLINE,FLIGHT_NUMBER,\n" +
                "TAIL_NUMBER,ORIGIN_AIRPORT,DESTINATION_AIRPORT,SCHEDULED_DEPARTURE,\n" +
                "DEPARTURE_TIME,DEPARTURE_DELAY,TAXI_OUT";
        Whitebox.setInternalState(joinMapperDelay,"headerList", headerList);
        Whitebox.setInternalState(joinMapperDelay,"header",header);
        joinMapperDelay.map(null, new Text(line1), mockContext);

        verify(mockContext, never()).write(new Text(anyString()), new Text(anyString()));
    }

    @After
    public void tearDown() throws Exception {
        verifyNoMoreInteractions(mockContext);
    }
}
