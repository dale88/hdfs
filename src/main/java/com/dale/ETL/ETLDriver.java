package com.dale.ETL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ETLDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(ETLDriver.class);
        job.setMapperClass(ETLMapper.class);
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path("d:\\mr\\input"));
        FileOutputFormat.setOutputPath(job,new Path("d:\\mr\\output"));

        boolean aTrue = job.waitForCompletion(true);
        System.exit(aTrue?0:1);
    }
}
