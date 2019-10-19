package com.dale.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class MyDriver {
    public static void main(String[] args) throws IOException {
        Job instance = Job.getInstance(new Configuration());
        instance.setJarByClass(MyDriver.class);
        instance.setOutputFormatClass(MyOutputFormat.class);

        instance.setOutputKeyClass(LongWritable.class);
        instance.setOutputValueClass(Text.class);




    }
}
