package com.globallogic.bdpc.mapreduce.avgdelays.join.reducer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class JoinReducerTest {
    @Mock
    private Context mockContext;

    private JoinReducer joinReducer;

    @Before
    public void setup(){
        joinReducer = new JoinReducer();
    }

    @Test
    public void reducerTest() throws IOException, InterruptedException {
        List<Text> values = Arrays.asList(new Text("name:American"), new Text("delay:12"),
                new Text("name:American"), new Text("delay:10"),
                new Text("name:American"), new Text("delay:8"),
                new Text("name:American"), new Text("delay:14"));

        joinReducer.reduce(new Text("AA"), values, mockContext);
        verify(mockContext).write(NullWritable.get(), new Text("AA:::American:::DELAY:11.0"));
    }

    @After
    public void tearDown() throws Exception {
        verifyNoMoreInteractions(mockContext);
    }
}
