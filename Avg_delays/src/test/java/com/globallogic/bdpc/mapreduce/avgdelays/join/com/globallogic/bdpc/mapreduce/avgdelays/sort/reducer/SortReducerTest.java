package com.globallogic.bdpc.mapreduce.avgdelays.join.com.globallogic.bdpc.mapreduce.avgdelays.sort.reducer;

import com.globallogic.bdpc.mapreduce.avgdelays.sort.reducer.SortReducer;
import org.apache.hadoop.io.DoubleWritable;
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

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(MockitoJUnitRunner.class)
public class SortReducerTest {
    @Mock
    private Context mockContext;

    private SortReducer sortReducer;

    @Before
    public void setup(){
        sortReducer = new SortReducer();
    }

    @Test
    public void sortReducerTest() throws IOException, InterruptedException {
        sortReducer.reduce(new DoubleWritable(123.23), Arrays.asList(new Text("AA, Airlines")), mockContext);
        verify(mockContext).write(NullWritable.get(), new Text("AA, Airlines, 123.23"));
    }

    @After
    public void tearDown() throws Exception {
        verifyNoMoreInteractions(mockContext);
    }

}
