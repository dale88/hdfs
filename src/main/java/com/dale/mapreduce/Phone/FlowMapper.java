package com.dale.mapreduce.Phone;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text,FlowBean> {
    private Text phone = new Text();
    private FlowBean flow = new FlowBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] s = value.toString().split("\t");
        phone.set(s[1]);
        long upFlow = Long.parseLong(s[s.length - 3]);
        long downFow = Long.parseLong(s[s.length - 2]);
        flow.setUpFlow(upFlow);
        flow.setDownFlow(downFow);
        context.write(phone,flow);
    }
}
