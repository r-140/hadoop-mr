package com.globallogic.bdpc.mapreduce.avgdelays.join.com.globallogic.bdpc.mapreduce.avgdelays.sort.mapper;

import com.globallogic.bdpc.mapreduce.avgdelays.sort.mapper.SortMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class SortMapperTest {

    @Mock
    private Context mockContext;

    private SortMapper sortMapper;

    @Before
    public void setup(){
        sortMapper = new SortMapper();
    }


    @Test
    public void mapTest_WhenMaxSize6_WriterMustCall6Times() throws IOException, InterruptedException {
        when(mockContext.getConfiguration()).thenReturn(mockConfiguration(6));
        sortMapper.setup(mockContext);

        sortMapper.map(null, new Text("AA:::Airlines:::delay:11.0"), mockContext);
        sortMapper.map(null, new Text("BC:::AirlinesB:::delay:14.23"), mockContext);
        sortMapper.map(null, new Text("CC:::AirlinesC:::delay:15.123"), mockContext);
        sortMapper.map(null, new Text("DD:::AirlinesD:::delay:10.21"), mockContext);
        sortMapper.map(null, new Text("EE:::AirlinesE:::delay:18.444"), mockContext);
        sortMapper.map(null, new Text("FF:::AirlinesF:::delay:11.666"), mockContext);
        sortMapper.map(null, new Text("JJ:::AirlinesJ:::delay:12.777"), mockContext);
        sortMapper.map(null, new Text("LL:::AirlinesL:::delay:6.9"), mockContext);

        sortMapper.cleanup(mockContext);

        verify(mockContext, times(6)).write(anyObject(), anyObject());
        verify(mockContext, times(1)).write(new DoubleWritable(18.444), new Text("EE, AirlinesE"));
        verify(mockContext, times(1)).write(new DoubleWritable(15.123), new Text("CC, AirlinesC"));
        verify(mockContext, times(1)).write(new DoubleWritable(14.23), new Text("BC, AirlinesB"));
        verify(mockContext, times(1)).write(new DoubleWritable(18.444), new Text("EE, AirlinesE"));
        verify(mockContext, times(1)).write(new DoubleWritable(12.777), new Text("JJ, AirlinesJ"));
        verify(mockContext, times(1)).write(new DoubleWritable(11.666), new Text("FF, AirlinesF"));
        verify(mockContext, times(1)).write(new DoubleWritable(11.0), new Text("AA, Airlines"));
        verify(mockContext, never()).write(new DoubleWritable(6.9), new Text("LL, AirlinesL"));
    }

    private Configuration mockConfiguration(int maxOutputSize) {
        Configuration configuration = new Configuration();
        configuration.setInt("avgdelay.max.output.size", maxOutputSize);

        return configuration;
    }
}
