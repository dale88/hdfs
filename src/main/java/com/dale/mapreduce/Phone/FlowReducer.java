package com.dale.mapreduce.Phone;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text,FlowBean, Text,FlowBean> {
    private FlowBean sumFlow = new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        Long sumUpFlow=0L;
        Long sumDownFlow=0L;
        for (FlowBean flow :
                values) {
            sumUpFlow += flow.getUpFlow();
            sumDownFlow += flow.getDownFlow();
        }
        sumFlow.set(sumUpFlow,sumDownFlow);
        context.write(key,sumFlow);

    }
}
