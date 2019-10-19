package com.dale.inputformat;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;



/**
 * 一个rr处理一个文件
 * 并且把这个文件直接都城一个kv值
 */
public class WholeFileRecordReader extends RecordReader<Text, BytesWritable> {

    private Boolean notRead = true;

    private Text key = new Text();
    private BytesWritable value = new BytesWritable();

    private FSDataInputStream inputStream ;
    private FileSplit fs;


    /**
     * 初始化，框架会在开始的时候调用一次
     * @param split
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        //转换切片类型到文件切片
        fs = (FileSplit) split;
        //通过切片获取路径
        Path path = fs.getPath();
        //通过路径获取文件
        FileSystem fileSystem = path.getFileSystem(context.getConfiguration());
        //开流
        inputStream =fileSystem.open(path);

    }

    /**
     * 尝试读取下一组数据，看看是否存在，返回
     * @return 返回存在结果 true or false
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(notRead){
            //具体读文件的过程

            //读取key
            key.set(fs.getPath().toString());

            //读Value
            byte[] buf = new byte[(int) fs.getLength()];
            inputStream.read(buf);
            value.set(buf, 0, buf.length);

            notRead = false;
            return true;
        }else {
            return false;
        }
    }

    /**
     * 获取当前读到的key
     * @return 当前的key
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }
    /**
     * 获取当前读到的value
     * @return 当前的value
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }
    /**
     * 获取当前读取的进度
     * @return 返回float类型的小数
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return notRead ? 0 : 1;
    }
    /**
     * 关闭资源
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void close() throws IOException {
        IOUtils.closeStream(inputStream);

    }
}
