package com.dale.mapreduce.writablecomparable2;

import com.dale.mapreduce.Phone.FlowBean;
import com.dale.mapreduce.writablecomparable.SortMapper;
import com.dale.mapreduce.writablecomparable.SortReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SortDriver2 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job instance = Job.getInstance(new Configuration());
        instance.setJarByClass(SortDriver2.class);
        instance.setMapperClass(SortMapper.class);
        instance.setReducerClass(SortReducer.class);

        instance.setMapOutputKeyClass(FlowBean.class);
        instance.setMapOutputValueClass(Text.class);

        instance.setPartitionerClass(MyPartitioner2.class);
        instance.setNumReduceTasks(5);

        instance.setOutputKeyClass(Text.class);
        instance.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(instance,new Path("d:\\mr\\output"));
        FileOutputFormat.setOutputPath(instance,new Path("d:\\mr\\output2"));

        boolean b = instance.waitForCompletion(true);
        System.exit(b ? 0:1);

    }
}
