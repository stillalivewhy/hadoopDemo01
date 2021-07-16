package com.xmx.hadoop.mapreduce.log;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class LogRecordWriter extends RecordWriter<Text, NullWritable> {

    private FSDataOutputStream one;

    private FSDataOutputStream other;

    public LogRecordWriter(TaskAttemptContext job) {
        try{
            FileSystem fs = FileSystem.get(job.getConfiguration());
            one = fs.create(new Path("E:\\test\\one.txt"));
            other = fs.create(new Path("E:\\test\\other.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        String value = text.toString();
        if(value.contains("atguigu")) {
            one.writeBytes(value+"\n");
        } else {
            other.writeBytes(value + "\n");
        }
    }

    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(one);
        IOUtils.closeStream(other);
    }
}
