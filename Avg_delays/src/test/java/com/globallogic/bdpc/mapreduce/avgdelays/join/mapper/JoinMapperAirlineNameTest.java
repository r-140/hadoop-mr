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
public class JoinMapperAirlineNameTest {
    @Mock
    private Context mockContext;

    private JoinMapperAirlineName joinMapperAirlineName;

    @Before
    public void setup(){
        joinMapperAirlineName = new JoinMapperAirlineName();
    }

    @Test
    public void mapTest_whenNormalText_shouldWriteToOutput() throws IOException, InterruptedException {

        String line1 = "AA,American Airlines";
        String[] headerList = {"IATA_CODE", "AIRLINE"};
        String header = "IATA_CODE,AIRLINE";
        Whitebox.setInternalState(joinMapperAirlineName,"headerList", headerList);
        Whitebox.setInternalState(joinMapperAirlineName,"header",header);
        joinMapperAirlineName.map(null, new Text(line1), mockContext);
        verify(mockContext).write(new Text("AA"), new Text("name:American Airlines"));
    }

    @Test
    public void mapTest_whenNormalText_nothingShouldWriteToOutput() throws IOException, InterruptedException {

        String line1 = "IATA_CODE,AIRLINE";
        String[] headerList = {"IATA_CODE", "AIRLINE"};
        String header = "IATA_CODE,AIRLINE";
        Whitebox.setInternalState(joinMapperAirlineName,"headerList", headerList);
        Whitebox.setInternalState(joinMapperAirlineName,"header",header);
        joinMapperAirlineName.map(null, new Text(line1), mockContext);
        verify(mockContext, never()).write(new Text(anyString()), new Text(anyString()));
    }

    @After
    public void tearDown() throws Exception {
        verifyNoMoreInteractions(mockContext);
    }
}
