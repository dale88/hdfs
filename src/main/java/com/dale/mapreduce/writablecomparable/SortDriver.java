package com.dale.mapreduce.writablecomparable;

import com.dale.mapreduce.Phone.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SortDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job instance = Job.getInstance(new Configuration());
        instance.setJarByClass(SortDriver.class);
        instance.setMapperClass(SortMapper.class);
        instance.setReducerClass(SortReducer.class);

        instance.setMapOutputKeyClass(FlowBean.class);
        instance.setMapOutputValueClass(Text.class);

        instance.setOutputKeyClass(Text.class);
        instance.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(instance,new Path("d:\\mr\\output"));
        FileOutputFormat.setOutputPath(instance,new Path("d:\\mr\\output2"));

        boolean b = instance.waitForCompletion(true);
        System.exit(b ? 0:1);

    }
}
