package com.dale.mapreduce.partition;

import com.dale.mapreduce.Phone.FlowBean;
import com.dale.mapreduce.Phone.FlowMapper;
import com.dale.mapreduce.Phone.FlowReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PartitionerDirver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //获取job
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(PartitionerDirver.class);
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);



        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job, new Path("d:\\mr\\input"));
        FileOutputFormat.setOutputPath(job, new Path("d:\\mr\\output"));

        //5提交Job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
