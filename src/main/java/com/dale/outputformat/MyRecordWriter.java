package com.dale.outputformat;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class MyRecordWriter extends RecordWriter<LongWritable, Text> {

    private FileOutputStream atguigu;
    private FileOutputStream other;

    /**
     * 初始化方法
     */
    public void initialize() throws FileNotFoundException {
        atguigu = new FileOutputStream("d:\\atguigu.log");
        other = new FileOutputStream("d:\\other.log");
    }

    /**
     * 将kv写出，每对kv调用一次
     * @param key
     * @param value
     * @throws IOException
     */
    @Override
    public void write(LongWritable key, Text value) throws IOException {
        String out = value.toString() + "\n";
        if(out.contains("atguigu")){
            atguigu.write(out.getBytes());
        }else{
            other.write(out.getBytes());
        }

    }
    /**
     * 关闭资源
     * @param context
     * @throws IOException
     */
    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {

        IOUtils.closeStream(atguigu);
        IOUtils.closeStream(other);
    }


}
