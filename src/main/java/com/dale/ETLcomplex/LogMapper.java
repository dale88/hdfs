package com.dale.ETLcomplex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogMapper extends Mapper<LongWritable, Text,Text, NullWritable> {

    private LogBean logBean = new LogBean();
    private Text k = new Text();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();

        LogBean logBean = parseLog(s);

        if(this.logBean.isValid()){
            k.set(this.logBean.toString());
            context.write(k,NullWritable.get());
            context.getCounter("ETL","TRUE");
        }else {
            context.getCounter("ETL","FALSE");
        }

    }

    private LogBean parseLog(String s) {
        String[] fields = s.split(" ");
        if(fields.length>11){
            // 2封装数据
            logBean.setRemote_addr(fields[0]);
            logBean.setRemote_user(fields[1]);
            logBean.setTime_local(fields[3].substring(1));
            logBean.setRequest(fields[6]);
            logBean.setStatus(fields[8]);
            logBean.setBody_bytes_sent(fields[9]);
            logBean.setHttp_referer(fields[10]);
            if (fields.length > 12) {
                logBean.setHttp_user_agent(fields[11] + " "+ fields[12]);
            }else {
                logBean.setHttp_user_agent(fields[11]);
            }

            // 大于400，HTTP错误
            if (Integer.parseInt(logBean.getStatus()) >= 400) {
                logBean.setValid(false);
            }
         }else {
            logBean.setValid(false);
        }

        return logBean;


        }
    }